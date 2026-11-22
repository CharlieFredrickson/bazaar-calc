#!/usr/bin/env node
// Lightweight alternative to build-site.mjs for continuous paper-trade validation.
// It deliberately reuses Bazaar Calc's original stats, market assembly, opportunity
// ranking and paperCandidates logic; no profitability/fill formulas are modified.
//
// Unlike build-site.mjs, it imports only recent contributed recordings, in small
// batches, into a fresh on-disk PGlite database on the GitHub Actions runner.
// This is a bounded-history approximation, NOT a full-history website rebuild.
import { existsSync, mkdirSync, readdirSync, readFileSync, renameSync, statSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import zlib from 'node:zlib';
import {
  computeHoldStats, computeStats, createPool, importDataFiles,
  ingestElection, ingestItems, loadMayors, migrate,
} from '@bc/server-core';
import {
  DEFAULT_PROFILE, DEFAULT_SETTINGS, HYPIXEL, assembleMarket,
  buildOpportunities, currentTerm, decodeDataFile, paperCandidates,
  perkEffects, quotesFromBazaar, sanityCheck,
} from '@bc/shared';

const arg = (key, fallback) => {
  const i = process.argv.indexOf(key);
  return i < 0 ? fallback : process.argv[i + 1];
};
const dataRoot = arg('--data', 'data');
const output = arg('--out', 'data/paper-candidates.json');
const dbDir = resolve(arg('--db', '.paper-candidates-db'));
const hours = Number(arg('--hours', '48'));
if (!Number.isFinite(hours) || hours < 24 || hours > 168) throw new Error('--hours must be between 24 and 168');
const H = 3_600_000;
const MAX_FILES = 150; // guard against unusually many simultaneous contributors
const BATCH = 6;
const say = (...args) => console.log(new Date().toISOString(), ...args);

function timestampFromName(name) {
  const m = /_(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(?:_|\.json\.gz$)/.exec(name);
  if (!m) return null;
  const t = Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5]);
  return Number.isFinite(t) && new Date(t).toISOString().slice(0, 16) === `${m[1]}-${m[2]}-${m[3]}T${m[4]}:${m[5]}` ? t : null;
}
const entries = [];
function walk(dir) {
  if (!existsSync(dir)) return;
  for (const name of readdirSync(dir).sort()) {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) walk(path);
    else if (name.endsWith('.json.gz')) {
      const ts = timestampFromName(name);
      if (ts != null) entries.push({ path, ts });
    }
  }
}
walk(join(dataRoot, 'contrib'));
if (!entries.length) throw new Error('No timestamped contribution recordings found');
entries.sort((a, b) => a.ts - b.ts || a.path.localeCompare(b.path));
const newest = entries.at(-1).ts;
if (Date.now() - newest > 2 * H) throw new Error('Latest source recording is more than 2 hours old; refusing to publish stale recommendations');
const recent = entries.filter(e => e.ts >= newest - hours * H).slice(-MAX_FILES);
say(`Found ${entries.length} recordings; importing ${recent.length} newest within ${hours}h (${new Date(recent[0].ts).toISOString()} onward)`);
if (recent.length < 12) throw new Error('Fewer than 12 recent recordings; not enough history for candidate generation');

const db = createPool(`pglite:${dbDir}`);
let importedPolls = 0, rejected = 0;
try {
  await migrate(db);
  for (let i = 0; i < recent.length; i += BATCH) {
    const files = [];
    for (const e of recent.slice(i, i + BATCH)) {
      try {
        const file = decodeDataFile(zlib.gunzipSync(readFileSync(e.path)).toString('utf8'));
        const { errors } = sanityCheck(file);
        if (errors.length) throw new Error(errors.join('; '));
        files.push({ label: e.path, file });
      } catch (err) {
        rejected++;
        say(`Invalid recording: ${e.path}: ${err.message}`);
      }
    }
    if (files.length) {
      const report = await importDataFiles(db, files, { skipCovered: true });
      importedPolls += report.files.reduce((s, f) => s + f.polls, 0);
    }
    say(`Imported ${Math.min(i + BATCH, recent.length)}/${recent.length} recordings`);
  }
  if (importedPolls < 500) throw new Error(`Only ${importedPolls} polls imported; refusing to publish questionable recommendations`);
  if (rejected > recent.length * 0.1) throw new Error(`${rejected} recordings invalid (>10%); refusing to publish`);

  // Reference data used by the original bazaar market assembler. NEU recipes and AH
  // calculations are unnecessary for bazaar-only paper trading.
  try { say(`Hypixel items imported: ${await ingestItems(db)}`); }
  catch (err) { say(`Items unavailable; using item IDs: ${err.message}`); }
  try { await ingestElection(db); }
  catch (err) { say(`Live election unavailable; using recorded election: ${err.message}`); }

  const asOf = Number((await db.query("SELECT extract(epoch from max(ts)) * 1000 AS t FROM bazaar_snapshots WHERE origin = 2")).rows[0]?.t ?? 0)
    || Number((await db.query('SELECT extract(epoch from max(ts)) * 1000 AS t FROM bazaar_snapshots')).rows[0]?.t ?? 0);
  if (!Number.isFinite(asOf) || !asOf || Date.now() - asOf > 2 * H) throw new Error('Imported Bazaar polls are stale');
  const items = await computeStats(db, 14, asOf);
  const hold = await computeHoldStats(db, 24, asOf);
  say(`Stats ready: ${items} items, ${hold.items} held sides from ${hold.episodes} recent episodes`);

  const rows = async (sql) => (await db.query(sql)).rows;
  const names = new Map((await rows('SELECT id, name FROM items')).map(r => [r.id, r.name]));
  const stats = new Map((await rows('SELECT item_id, data FROM item_stats')).map(r => [r.item_id, r.data]));
  const holds = new Map();
  for (const r of await rows('SELECT item_id, side, data FROM item_hold_stats')) {
    const item = holds.get(r.item_id) ?? {};
    item[r.side === 'b' ? 'bid' : 'ask'] = r.data;
    holds.set(r.item_id, item);
  }
  const npcSell = new Map((await rows('SELECT id, npc_sell_price FROM items WHERE npc_sell_price IS NOT NULL'))
    .map(r => [r.id, Number(r.npc_sell_price)]));
  const unstackable = new Set((await rows('SELECT id FROM items WHERE unstackable')).map(r => r.id));

  const response = await fetch(`${HYPIXEL}/skyblock/bazaar`, { signal: AbortSignal.timeout(60_000) });
  if (!response.ok) throw new Error(`Live Hypixel Bazaar returned HTTP ${response.status}`);
  const live = await response.json();
  if (!live?.success || !Number.isFinite(live.lastUpdated) || Math.abs(Date.now() - live.lastUpdated) > 10 * 60_000)
    throw new Error('Live Hypixel data is missing or stale');

  const now = Date.now();
  const market = assembleMarket({
    quotes: quotesFromBazaar(live), stats, hold: holds, ah: new Map(),
    names, npcSell, unstackable, now,
  });
  const perks = perkEffects(currentTerm(await loadMayors(db, 0, now + 400 * 86_400_000), now));
  const { list } = buildOpportunities({ market, recipes: new Map(), perks,
    statsAgeH: Math.max(0, (live.lastUpdated - asOf) / H) }, 'bazaar', DEFAULT_SETTINGS, DEFAULT_PROFILE);
  const candidates = paperCandidates(list).slice(0, 20);
  if (candidates.length < 5) throw new Error(`Only ${candidates.length} candidate(s) generated; refusing to publish`);

  const body = {
    at: live.lastUpdated, statsAt: asOf, settings: 'defaults', quadTaxes: perks.quadTaxes,
    candidates,
    // Extra metadata is ignored by the existing Oracle scanner.
    source: 'bounded-history-original-calc', historyHours: hours,
    importedPolls, rejected,
  };
  mkdirSync(dirname(output), { recursive: true });
  writeFileSync(`${output}.tmp`, JSON.stringify(body));
  renameSync(`${output}.tmp`, output);
  say(`Published ${candidates.length} picks (first: ${candidates[0].title}); at ${new Date(body.at).toISOString()}`);
} finally {
  await db.end();
}
