/**
 * Screener API — a thin Worker backend over the CBOE Iceberg lake (R2 SQL).
 *
 * The loader project (cboe-to-r2) owns ingestion: CBOE → Cloudflare Pipelines →
 * R2 Data Catalog Iceberg tables (options.option_contracts / options.underlyings /
 * options.refresh_runs). This Worker is the *consumer*: it runs the screener's
 * read-only analytics SQL against the lake over the R2 SQL REST API and returns
 * JSON. No DuckDB, no Parquet, no local download — the lake is the single source
 * of truth and the browser is a plain React client.
 *
 * Why this works: the dataset only changes on nightly loader runs, so query
 * results are cached aggressively (in-isolate Map, 5–10 min TTL). The first
 * request after a cold cache is ~1–6s (a handful of R2 SQL scans); cached
 * responses are instant.
 *
 * R2 SQL gotchas this accounts for:
 *  - No OFFSET: screen/underlyings fetch the ordered result (≤ R2 SQL's 10000-row
 *    LIMIT) once per filter signature and page slices in-memory from cache.
 *  - No parameter binding over REST: literals are inlined via lit() with
 *    single-quote escaping; sort columns are whitelisted.
 *  - DataFusion dialect: spot_price (not spot), WHERE before QUALIFY, DTE via
 *    CAST(expiration AS DATE) - CURRENT_DATE (returns integer days).
 */
import { routeAgentRequest } from "agents";
import { isAdminEmail } from "./admin";
import { enrichAdminChatItems } from "./admin-chats";
import { listAdminSuggestedTrades } from "./admin-trades";
import { listAdminUsers } from "./admin-users";
import { createAuth, getSessionUser, googleConfigured, isTrustedOrigin, type SessionUser } from "./auth";
import {
  inventBotPrompt,
  resolveBotGeneratePrompt,
} from "./bot-prompt";
import {
  runDueBotSchedules,
  runOneBotSchedule,
} from "./bot-runner";
import {
  deleteBotSchedule,
  getBotSchedule,
  upsertBotSchedule,
} from "./bot-schedule";
import {
  botShareReuseDecision,
  createBotProfile,
  createBotRun,
  deleteBotProfile,
  getBotProfile,
  getBotRun,
  listBotProfiles,
  listBotRunPrompts,
  listBotRuns,
  requireBotAdmin,
  updateBotProfile,
  updateBotRun,
} from "./bots";
import { chartFitsResult, type ChartSpec } from "./chart-spec";
import { createCopilotModel } from "./copilot-contract";
import { describeCopilotCapabilities } from "./copilot-capabilities";
import { CopilotAgentBase } from "./copilot";
import { normalizeDeskBrief, type DeskBrief } from "./copilot-desk";
import { normalizeSuggestedTrades, type SuggestedTrades } from "./copilot-trades";
import { coalesceAssistantMessageRecords } from "./share-turns";
import { applyColumnSynonyms } from "./copilot-sql";
import { AVATAR_MAX_BYTES, clearAvatar, putAvatar, serveAvatar } from "./avatars";
import { getHandle, getUserProfile, profilePublicFields, suggestHandle, updateProfile } from "./profiles";
import { getTimelineAuthor, handleTimeline, recordShareOwner } from "./timeline";
import { moderateTimelineShare } from "./timeline-moderation";
import { scheduleImprovementReport } from "./improvement-reporter";
import { fetchYahooIntraday } from "./yahoo-intraday";
import {
  authorizeCopilotAgent,
  applyGeneratedChatTitle,
  claimChat,
  deleteChat,
  firstUserContent,
  listUserChats,
  loadOwnedChatTranscript,
  ownerOf,
  parseChatId,
  renameChat,
  shareDisplayTitle,
  titleFromMessages,
  touchUserChat,
  clipTitle,
} from "./user-chats";
import { enrichChatMeta, backfillShareMeta, shareNeedsMetaBackfill } from "./chat-meta";
import {
  chatIdForShare,
  listToolEvents,
  parseToolEventListQuery,
  purgeExpiredToolEvents,
} from "./copilot-tool-events";
import { listChatTickers, listSecurityChats } from "./chat-tickers";
import { loadChatRail } from "./timeline-rail";
import {
  emptyFundamentals,
  emptyShorting,
  getOrComputeResearch,
  parseTickerParam,
  RESEARCH_OHLC_LIMIT,
  summarizeResearch,
  warmResearchTickers,
  type EarningsBrief,
  type FundamentalsBrief,
  type OhlcBar,
  type RealizedVolBrief,
  type ResearchDeps,
  type ShortingBrief,
  type TickerResearch,
} from "./research";
import { securityIdForTicker } from "./symbology";
import { getOrComputeCommentary, sanitizeResearchCommentary } from "./research-commentary";
import { mergeSymbolUniverse, rankSymbolSuggestions } from "./catalog-symbols";
import type { LakeSecurityRow } from "./figi";
import {
  enrollTickerWithLoader,
  maybeEnrollMissingTicker,
  isEnrollableEquityTicker,
} from "./enroll-symbol";


// ---------------------------------------------------------------------------
// Env
// ---------------------------------------------------------------------------
export interface Env extends Cloudflare.Env {
  ADMIN_TOKEN?: string;
  /** Bearer for loader /symbols/enroll and /run (same secret as cboe-to-r2). */
  LOADER_TOKEN?: string;
  PIPELINE_CHAT_HISTORY_URL?: string;
  PIPELINE_AUTH_TOKEN?: string;
  BETTER_AUTH_SECRET?: string;
  GOOGLE_CLIENT_ID?: string;
  GOOGLE_CLIENT_SECRET?: string;
  /** OpenFIGI Mapping API key — live ticker normalize for research / chat links. */
  OPEN_FIGI?: string;
  /** Bot schedule market gate — set "false" to run schedules around the clock. */
  MARKET_HOURS_ENABLED?: string;
  MARKET_OPEN_MINUTES?: string;
  MARKET_CLOSE_MINUTES?: string;
  MARKET_EARLY_CLOSE_MINUTES?: string;
  /**
   * Fine-grained GitHub PAT (Issues: Read and write) for the moderation
   * improvement reporter. Optional — unset = no issue filing.
   * Named IMPROVEMENT_ISSUE_TOKEN because GitHub Actions forbids secrets
   * whose names start with GITHUB_.
   */
  IMPROVEMENT_ISSUE_TOKEN?: string;
  /** owner/repo override; defaults to rlancer/lobster-market-pricing. */
  IMPROVEMENT_ISSUE_REPO?: string;
}

// Latest snapshot per symbol: the lake is append-only (multiple loader runs
// accumulate). Pick each symbol's newest underlying run from the decoupled
// options.underlying_snapshots table (security master facts live in
// options.securities; the worker reads the denormalized name/sector here). The
// `ticker` column plays the role `symbol` had in the retired underlyings table,
// so it is aliased to keep every downstream query byte-identical.
const LATEST_UNDERLYING =
  "SELECT ticker AS symbol, name, sector, spot_price, run_id, fetched_at " +
  "FROM options.underlying_snapshots " +
  "QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY fetched_at DESC) = 1";

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------
// TTL tiers: the lake is refreshed at most nightly (loader runs), so staleness
// is bounded by the loader cadence, not by these values. Endpoints quote
// different data at different rates:
//   RESULT_TTL_MS — screener endpoints (stats, screen, underlyings…)
//   QUERY_TTL_MS  — /api/query (arbitrary SQL) + symbol detail chains: the
//                   heaviest lake compute; a 60-min memo bounds cross-user
//                   repeat cost. The in-isolate Map dies with the isolate
//                   (redeploy/eviction), so this is a bound, not a leak.
//   REF_TTL_MS    — near-static reference rows (symbol/name/sector for the
//                   typeahead); refreshed effectively only when names/constituents change.
const RESULT_TTL_MS = 30 * 60 * 1000;
const QUERY_TTL_MS = 60 * 60 * 1000;
const REF_TTL_MS = 12 * 60 * 60 * 1000;
const R2SQL_LIMIT_MAX = 10000;

interface CacheEntry { ts: number; val: unknown; }
const cache = new Map<string, CacheEntry>();

function cached<T>(key: string, ttlMs: number, compute: () => Promise<T>): Promise<T> {
  const hit = cache.get(key);
  const now = Date.now();
  if (hit && now - hit.ts < ttlMs) return Promise.resolve(hit.val as T);
  return compute().then((val) => { cache.set(key, { ts: now, val }); return val; });
}

/** FNV-1a 32-bit hex — deterministic memo key for arbitrary SQL strings. */
function sqlHash(s: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(16).padStart(8, "0");
}

// ---------------------------------------------------------------------------
// R2 SQL REST client
// ---------------------------------------------------------------------------
function apiUrl(env: Env): string {
  return `https://api.sql.cloudflarestorage.com/api/v1/accounts/${env.R2_SQL_ACCOUNT_ID}/r2-sql/query/${env.R2_SQL_BUCKET}`;
}

interface R2Row { [k: string]: unknown; }

async function r2sql(env: Env, sql: string, key?: string, ttlMs: number = RESULT_TTL_MS): Promise<R2Row[]> {
  const run = () => fetch(apiUrl(env), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.R2_SQL_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query: sql }),
  }).then(async (r) => {
    if (!r.ok) throw new Error(`R2 SQL ${r.status}: ${(await r.text()).slice(0, 200)}`);
    const b = await r.json() as { success: boolean; result?: { rows: R2Row[] }; errors?: { message: string }[] };
    if (!b.success) {
      const msg = b.errors?.[0]?.message ?? "unknown R2 SQL error";
      throw new Error(`R2 SQL: ${msg}`);
    }
    return b.result?.rows ?? [];
  });
  return key ? cached<R2Row[]>(key, ttlMs, run) : run();
}

// ---------------------------------------------------------------------------
// SQL literal helpers (R2 SQL REST has no parameter binding)
// ---------------------------------------------------------------------------
function lit(v: unknown): string {
  if (v === null || v === undefined) return "NULL";
  if (typeof v === "number") return Number.isFinite(v) ? String(v) : "NULL";
  if (typeof v === "boolean") return v ? "true" : "false";
  // string: double single quotes
  return "'" + String(v).replace(/'/g, "''") + "'";
}

// ---------------------------------------------------------------------------
// Endpoint handlers — return shapes match frontend/src/api.ts interfaces exactly
// ---------------------------------------------------------------------------
async function stats(env: Env): Promise<{
  underlyings: number; contracts: number; calls: number; puts: number; last_updated: string;
}> {
  const [u, c, calls] = await Promise.all([
    r2sql(env, `SELECT COUNT(*) n FROM (${LATEST_UNDERLYING}) u`, "stats_u"),
    r2sql(env, `SELECT COUNT(*) n FROM options.option_contracts`, "stats_c"),
    r2sql(env, `SELECT type, COUNT(*) n FROM options.option_contracts GROUP BY type`, "stats_type"),
  ]);
  let nc = 0, np = 0;
  for (const r of calls) { if (r.type === "call") nc = num(r.n); else np = num(r.n); }
  const last = await r2sql(env, `SELECT COALESCE(MAX(fetched_at), '') AS last FROM options.option_contracts WHERE fetched_at LIKE '2%'`, "stats_last");
  return {
    underlyings: num(u[0]?.n), contracts: num(c[0]?.n), calls: nc, puts: np,
    last_updated: String(last[0]?.last ?? ""),
  };
}

// Recent loader refresh runs (the consumer view of options.refresh_runs).
async function runs(env: Env, limit = 5): Promise<{
  run_id: string; started_at: string; completed_at: string | null; as_of_date: string;
  expected_symbols: number; successful_symbols: number; failed_symbols: number;
  contract_count: number; status: string; error_summary: string | null;
}[]> {
  const lim = clamp(limit, 1, 50);
  const rows = await r2sql(env,
    `SELECT run_id, started_at, completed_at, as_of_date, expected_symbols, ` +
    `successful_symbols, failed_symbols, contract_count, status, error_summary ` +
    `FROM options.refresh_runs ORDER BY started_at DESC LIMIT ${lim}`, "runs_" + lim);
  return rows.map((r) => ({
    run_id: String(r.run_id ?? ""),
    started_at: String(r.started_at ?? ""),
    completed_at: strOrNull(r.completed_at),
    as_of_date: String(r.as_of_date ?? ""),
    expected_symbols: num(r.expected_symbols),
    successful_symbols: num(r.successful_symbols),
    failed_symbols: num(r.failed_symbols),
    contract_count: num(r.contract_count),
    status: String(r.status ?? ""),
    error_summary: strOrNull(r.error_summary),
  }));
}

async function sectors(env: Env): Promise<{ sector: string; symbols: number; avg_spot: number | null }[]> {
  const rows = await r2sql(env,
    `SELECT sector, COUNT(*) AS symbols, AVG(spot_price) AS avg_spot ` +
    `FROM (${LATEST_UNDERLYING}) u GROUP BY sector ORDER BY sector`,
    "sectors");
  return rows.map((r) => ({ sector: String(r.sector), symbols: num(r.symbols), avg_spot: numOrNull(r.avg_spot) }));
}

async function underlyings(env: Env, p: {
  sector?: string; q?: string; limit?: number; offset?: number;
}): Promise<{ total: number; items: { symbol: string; name: string | null; sector: string | null; spot: number | null; contracts: number }[] }> {
  const where: string[] = [];
  if (p.sector) where.push(`u.sector = ${lit(p.sector)}`);
  if (p.q) { const s = `%${p.q.toUpperCase()}%`; where.push(`(UPPER(u.symbol) LIKE ${lit(s)} OR UPPER(COALESCE(u.name,'')) LIKE ${lit(s)})`); }
  const clause = where.length ? "WHERE " + where.join(" AND ") : "";
  // latest underlying (≤502 rows) + per-symbol contract count. Fetch all, page in-memory.
  const key = "und:" + clause;
  const all = await cached<{ symbol: string; name: string | null; sector: string | null; spot: number | null; contracts: number }[]>(key, RESULT_TTL_MS, async () => {
    const rows = await r2sql(env,
      `SELECT u.symbol, u.name, u.sector, u.spot_price AS spot, ` +
      `(SELECT COUNT(*) FROM options.option_contracts c WHERE c.symbol = u.symbol) AS contracts ` +
      `FROM (${LATEST_UNDERLYING}) u ${clause} ORDER BY u.symbol`);
    return rows.map((r) => ({
      symbol: String(r.symbol), name: strOrNull(r.name), sector: strOrNull(r.sector),
      spot: numOrNull(r.spot), contracts: num(r.contracts),
    }));
  });
  const limit = clamp(p.limit ?? 50, 1, 1000);
  const offset = clamp(p.offset ?? 0, 0, 100000);
  return { total: all.length, items: all.slice(offset, offset + limit) };
}

type Row = Record<string, unknown>;

const SORT_WHITELIST = new Set(["volume", "open_interest", "strike", "implied_vol", "delta", "gamma", "theta", "vega", "bid", "ask", "last", "expiration"]);

async function screen(env: Env, p: {
  symbol?: string; type?: string; sector?: string;
  min_strike?: number; max_strike?: number; min_volume?: number; min_open_interest?: number;
  min_iv?: number; max_iv?: number; min_delta?: number; max_delta?: number;
  in_the_money?: boolean;   expiration_before?: string; expiration_after?: string;
  near_spot_strikes?: number;
  sort?: string; order?: string; limit?: number; offset?: number;
}): Promise<{ total: number; items: Row[]; truncated?: boolean }> {
  const where: string[] = ["c.symbol IS NOT NULL"];
  if (p.symbol) where.push(`c.symbol = ${lit(p.symbol.toUpperCase())}`);
  if (p.type === "call" || p.type === "put") where.push(`c.type = ${lit(p.type)}`);
  if (p.sector) where.push(`u.sector = ${lit(p.sector)}`);
  if (p.min_strike != null) where.push(`c.strike >= ${lit(p.min_strike)}`);
  if (p.max_strike != null) where.push(`c.strike <= ${lit(p.max_strike)}`);
  if (p.min_volume != null) where.push(`COALESCE(c.volume,0) >= ${lit(p.min_volume)}`);
  if (p.min_open_interest != null) where.push(`COALESCE(c.open_interest,0) >= ${lit(p.min_open_interest)}`);
  if (p.min_iv != null) where.push(`c.implied_vol >= ${lit(p.min_iv)}`);
  if (p.max_iv != null) where.push(`c.implied_vol <= ${lit(p.max_iv)}`);
  if (p.min_delta != null) where.push(`c.delta >= ${lit(p.min_delta)}`);
  if (p.max_delta != null) where.push(`c.delta <= ${lit(p.max_delta)}`);
  if (p.in_the_money != null) where.push(`c.in_the_money = ${lit(p.in_the_money)}`);
  if (p.expiration_before) where.push(`c.expiration <= ${lit(p.expiration_before)}`);
  if (p.expiration_after) where.push(`c.expiration >= ${lit(p.expiration_after)}`);

  const sortCol = SORT_WHITELIST.has(p.sort ?? "") ? (p.sort as string) : "volume";
  const order = p.order === "asc" ? "ASC" : "DESC";
  const sortExpr = sortCol === "expiration"
    ? `c.${sortCol}`
    : `COALESCE(c.${sortCol},0)`;

  let cte = "";
  const nss = p.near_spot_strikes ?? 50;
  if (nss && nss > 0) {
    cte =
      "WITH atm_strikes AS (\n" +
      "  SELECT symbol, strike FROM (\n" +
      "    SELECT ds.symbol, ds.strike,\n" +
      "           ROW_NUMBER() OVER (PARTITION BY ds.symbol ORDER BY ABS(ds.strike - au.spot_price)) AS rn\n" +
      "    FROM (SELECT DISTINCT c2.symbol, c2.strike FROM options.option_contracts c2\n" +
      "          JOIN (" + LATEST_UNDERLYING + ") u2 ON u2.symbol = c2.symbol WHERE u2.spot_price IS NOT NULL) ds\n" +
      "    JOIN (" + LATEST_UNDERLYING + ") au ON au.symbol = ds.symbol\n" +
      "  ) WHERE rn <= " + lit(nss) + "\n)\n";
    where.push("EXISTS (SELECT 1 FROM atm_strikes a WHERE a.symbol = c.symbol AND a.strike = c.strike)");
  }

  const clause = "WHERE " + where.join(" AND ");
  const key = "screen:" + cte + "|" + clause + "|" + sortExpr + "|" + order;

  // Fetch the ordered result set (capped at R2 SQL's 10000-row LIMIT) once, page in-memory.
  const fetched = await cached<{ rows: Row[]; total: number; truncated: boolean }>(key, RESULT_TTL_MS, async () => {
    const [rows, totalRows] = await Promise.all([
      r2sql(env,
        cte +
        "SELECT c.symbol, u.name AS name, u.sector AS sector, u.spot_price AS spot, " +
        "c.expiration, c.type, c.strike, c.last, c.bid, c.ask, " +
        "c.volume, c.open_interest, c.implied_vol, " +
        "c.delta, c.gamma, c.theta, c.vega, c.rho, c.in_the_money, " +
        "c.theo, c.bid_size, c.ask_size, " +
        "CASE WHEN u.spot_price IS NOT NULL AND c.strike > 0 " +
        "THEN ROUND((c.strike - u.spot_price)/u.spot_price*100, 2) END AS moneyness_pct " +
        "FROM options.option_contracts c " +
        "JOIN (" + LATEST_UNDERLYING + ") u ON u.symbol = c.symbol " +
        clause + " " +
        "ORDER BY " + sortExpr + " " + order + ", c.symbol ASC " +
        "LIMIT " + R2SQL_LIMIT_MAX),
      r2sql(env, cte + "SELECT COUNT(*) AS n FROM options.option_contracts c JOIN (" + LATEST_UNDERLYING + ") u ON u.symbol = c.symbol " + clause),
    ]);
    const total = num(totalRows[0]?.n);
    return { rows: rows as Row[], total, truncated: total > R2SQL_LIMIT_MAX };
  });

  const limit = clamp(p.limit ?? 100, 1, 1000);
  const offset = clamp(p.offset ?? 0, 0, 100000);
  const items = fetched.rows.slice(offset, offset + limit);
  return { total: fetched.total, items, truncated: fetched.truncated && (offset + limit) >= R2SQL_LIMIT_MAX };
}

async function symbols(env: Env, q?: string, limit = 50): Promise<{ symbol: string; name: string | null; sector: string | null }[]> {
  const lim = clamp(limit, 1, 1000);
  // Equity/ETF names come from underlying_snapshots; indexes + continuous
  // futures only exist in options.ohlc, so merge the loader catalogs in.
  const rows = await r2sql(env,
    `SELECT symbol, name, sector FROM (${LATEST_UNDERLYING}) ORDER BY symbol LIMIT 1000`,
    "syms_lake_v2", REF_TTL_MS);
  const merged = mergeSymbolUniverse(rows.map(row));
  return rankSymbolSuggestions(merged, q ?? undefined, lim);
}
function row(r: R2Row): { symbol: string; name: string | null; sector: string | null } {
  return { symbol: String(r.symbol), name: strOrNull(r.name), sector: strOrNull(r.sector) };
}

// ---------------------------------------------------------------------------
// Symbol detail enrichment: daily OHLC bars, realized vol, corporate actions,
// and (for ETFs) fund profile + top holdings. These tables are newer than the
// chain data, so each query is isolated and degrades to empty on failure.
// OHLC is append-only across daily runs, so rows are deduped per (symbol,date)
// keeping the newest run; bars come back newest-first and are flipped to
// ascending for the chart.
// ---------------------------------------------------------------------------
const OHLC_BARS_LIMIT = 260; // ~1y of trading days → 52w high/low + sparkline
/** Calendar-day window wide enough for ~260 sessions without scanning full history. */
const OHLC_LOOKBACK_CALENDAR_DAYS = 420;

function enrichQuery(env: Env, symbol: string, kind: string, sql: string, key: string) {
  return r2sql(env, sql, key).catch((e) => {
    console.error(`symbol ${symbol} ${kind} enrichment failed`, e);
    return [];
  });
}

/** Chart / sparkline OHLC only — date-bounded, no RV / CA / ETF side queries. */
async function loadSymbolOhlc(env: Env, symbol: string): Promise<Row[]> {
  const since = new Date(Date.now() - OHLC_LOOKBACK_CALENDAR_DAYS * 24 * 60 * 60 * 1000)
    .toISOString()
    .slice(0, 10);
  const ohlcRows = await enrichQuery(
    env,
    symbol,
    "ohlc",
    `SELECT date, open, high, low, close, volume FROM (` +
      `  SELECT date, open, high, low, close, volume,` +
      `    ROW_NUMBER() OVER (PARTITION BY date ORDER BY fetched_at DESC, run_id DESC) rn` +
      `  FROM options.ohlc WHERE symbol = ${lit(symbol)} AND date >= ${lit(since)} AND close IS NOT NULL` +
      `) WHERE rn = 1 ORDER BY date DESC LIMIT ${OHLC_BARS_LIMIT}`,
    // v3: ignore null-close rows (Yahoo holes + stripNones) that would shadow good bars.
    `sym_ohlc_v3_${symbol}`,
  );
  return (ohlcRows as Row[]).map((r) => ({
    date: String(r.date),
    open: numOrNull(r.open),
    high: numOrNull(r.high),
    low: numOrNull(r.low),
    close: numOrNull(r.close),
    volume: numOrNull(r.volume),
  })).reverse();
}

async function enrichSymbol(env: Env, symbol: string): Promise<{
  ohlc: Row[]; realized_vol: Row | null; corporate_actions: Row[];
  etf_profile: Row | null; etf_holdings: Row[];
}> {
  const sym = lit(symbol);
  const [ohlc, rvRows, caRows, etfProfileRows, etfHoldingRows] = await Promise.all([
    loadSymbolOhlc(env, symbol),
    enrichQuery(env, symbol, "realized_vol",
      `SELECT as_of_date, realized_vol_30d, realized_vol_90d, n_returns_30, n_returns_90 FROM (` +
      `  SELECT as_of_date, realized_vol_30d, realized_vol_90d, n_returns_30, n_returns_90,` +
      `    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY fetched_at DESC, run_id DESC) rn` +
      `  FROM options.realized_vol WHERE symbol = ${sym}` +
      `) WHERE rn = 1`,
      `sym_rv_${symbol}`),
    enrichQuery(env, symbol, "corporate_actions",
      `SELECT action_type, ex_date, numerator, denominator, amount FROM (` +
      `  SELECT action_type, ex_date, numerator, denominator, amount,` +
      `    ROW_NUMBER() OVER (PARTITION BY action_type, ex_date ORDER BY fetched_at DESC, run_id DESC) rn` +
      `  FROM options.corporate_actions WHERE ticker = ${sym}` +
      `) WHERE rn = 1 ORDER BY ex_date DESC LIMIT 8`,
      `sym_ca_${symbol}`),
    enrichQuery(env, symbol, "etf_profile",
      `SELECT ticker, name, family, category, asset_class, expense_ratio, net_expense_ratio,` +
      `  net_assets, trailing_yield, inception_date FROM (` +
      `  SELECT ticker, name, family, category, asset_class, expense_ratio, net_expense_ratio,` +
      `    net_assets, trailing_yield, inception_date,` +
      `    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY fetched_at DESC, run_id DESC) rn` +
      `  FROM options.etf_profiles WHERE ticker = ${sym}` +
      `) WHERE rn = 1`,
      `sym_etf_${symbol}`),
    enrichQuery(env, symbol, "etf_holdings",
      `SELECT rank, holding_symbol, holding_name, weight FROM (` +
      `  SELECT rank, holding_symbol, holding_name, weight,` +
      `    ROW_NUMBER() OVER (PARTITION BY ticker, rank ORDER BY fetched_at DESC, run_id DESC) rn` +
      `  FROM options.etf_holdings WHERE ticker = ${sym}` +
      `) WHERE rn = 1 ORDER BY rank LIMIT 15`,
      `sym_etfh_${symbol}`),
  ]);
  return {
    ohlc,
    realized_vol: rvRows.length ? {
      as_of_date: String(rvRows[0].as_of_date),
      realized_vol_30d: numOrNull(rvRows[0].realized_vol_30d),
      realized_vol_90d: numOrNull(rvRows[0].realized_vol_90d),
      n_returns_30: numOrNull(rvRows[0].n_returns_30),
      n_returns_90: numOrNull(rvRows[0].n_returns_90),
    } : null,
    corporate_actions: (caRows as Row[]).map((r) => ({
      action_type: String(r.action_type),
      ex_date: String(r.ex_date),
      numerator: numOrNull(r.numerator),
      denominator: numOrNull(r.denominator),
      amount: numOrNull(r.amount),
    })),
    etf_profile: etfProfileRows.length ? {
      ticker: String(etfProfileRows[0].ticker),
      name: strOrNull(etfProfileRows[0].name),
      family: strOrNull(etfProfileRows[0].family),
      category: strOrNull(etfProfileRows[0].category),
      asset_class: strOrNull(etfProfileRows[0].asset_class),
      expense_ratio: numOrNull(etfProfileRows[0].expense_ratio),
      net_expense_ratio: numOrNull(etfProfileRows[0].net_expense_ratio),
      net_assets: numOrNull(etfProfileRows[0].net_assets),
      trailing_yield: numOrNull(etfProfileRows[0].trailing_yield),
      inception_date: strOrNull(etfProfileRows[0].inception_date),
    } : null,
    etf_holdings: (etfHoldingRows as Row[]).map((r) => ({
      rank: numOrNull(r.rank),
      holding_symbol: strOrNull(r.holding_symbol),
      holding_name: strOrNull(r.holding_name),
      weight: numOrNull(r.weight),
    })),
  };
}

type SymbolDetailParts = "full" | "ohlc" | "ohlc_intraday" | "chain";

interface SymbolDetailOpts {
  /** `ohlc` skips the chain; `ohlc_intraday` is Yahoo 5m Day bars; `chain` skips enrichment; `full` is the legacy dump. */
  parts?: SymbolDetailParts;
  /** When set with parts=chain, only this expiration's contracts are returned. */
  expiration?: string;
  /**
   * Keep the N strikes closest to spot (calls+puts for those strikes).
   * 0 / unset with no expiration → full chain; with expiration → all strikes
   * for that expiry unless near_spot > 0.
   */
  nearSpot?: number;
}

function parseSymbolDetailParts(raw: string | null): SymbolDetailParts {
  const v = (raw ?? "full").trim().toLowerCase();
  if (v === "ohlc" || v === "ohlc_intraday" || v === "chain") return v;
  return "full";
}

/** Keep contracts whose strike is among the N closest to spot. */
function filterContractsNearSpot(rows: Row[], spot: number | null, nearSpot: number): Row[] {
  if (!nearSpot || nearSpot <= 0 || spot == null || !Number.isFinite(spot)) return rows;
  const strikes = Array.from(new Set(
    rows.map((r) => Number(r.strike)).filter((n) => Number.isFinite(n)),
  ));
  if (strikes.length <= nearSpot) return rows;
  const keep = new Set(
    [...strikes]
      .sort((a, b) => Math.abs(a - spot) - Math.abs(b - spot))
      .slice(0, nearSpot),
  );
  return rows.filter((r) => keep.has(Number(r.strike)));
}

async function symbolDetail(env: Env, symbol: string, opts: SymbolDetailOpts = {}): Promise<{
  underlying: { symbol: string; name: string | null; sector: string | null; spot: number | null; fetched_at: string | null } | null;
  contracts: Row[]; expirations: string[]; n_contracts: number;
  ohlc: Row[]; realized_vol: Row | null; corporate_actions: Row[];
  etf_profile: Row | null; etf_holdings: Row[];
}> {
  const parts = opts.parts ?? "full";
  const empty = {
    underlying: null, contracts: [] as Row[], expirations: [] as string[], n_contracts: 0,
    ohlc: [] as Row[], realized_vol: null as Row | null, corporate_actions: [] as Row[],
    etf_profile: null as Row | null, etf_holdings: [] as Row[],
  };

  // Chart path: one date-bounded OHLC query. Skip underlying lookup, chain, and
  // the RV/CA/ETF enrichment suite — those made parts=ohlc as slow as parts=full.
  if (parts === "ohlc") {
    return { ...empty, ohlc: await loadSymbolOhlc(env, symbol) };
  }

  // Day chart: live Yahoo 5m bars for the current (or last) session. Same source
  // as the loader daily OHLC job; not lake-backed because session bars churn.
  if (parts === "ohlc_intraday") {
    const bars = await fetchYahooIntraday(symbol);
    return {
      ...empty,
      ohlc: bars.map((b) => ({
        date: b.date,
        open: b.open,
        high: b.high,
        low: b.low,
        close: b.close,
        volume: b.volume,
      })),
    };
  }

  const u = await r2sql(env, `SELECT symbol, name, sector, spot_price, run_id, fetched_at FROM (${LATEST_UNDERLYING}) WHERE symbol = ${lit(symbol)}`, "symu_" + symbol, QUERY_TTL_MS);
  if (!u.length) return empty;
  const ud = u[0];
  const underlying = {
    symbol: String(ud.symbol), name: strOrNull(ud.name), sector: strOrNull(ud.sector),
    spot: numOrNull(ud.spot_price), fetched_at: strOrNull(ud.fetched_at),
  };
  const spot = underlying.spot;
  const wantChain = parts === "full" || parts === "chain";
  const wantEnrich = parts === "full";

  // contracts for the latest run of this symbol + enrichment, in parallel.
  // The chain key embeds run_id, so a new loader run naturally invalidates it.
  // parts=ohlc / parts=chain let the research page stage payloads instead of
  // downloading the full ~1MB chain before the Lobster take can paint.
  const expiration = opts.expiration?.trim() || undefined;
  const nearSpot = opts.nearSpot != null && Number.isFinite(opts.nearSpot) ? Math.max(0, Math.floor(opts.nearSpot)) : 0;

  const chainPromise = wantChain
    ? (async () => {
      // Expirations are cheap and always returned so the UI can populate the picker
      // without a second round-trip when the client already knows the expiry.
      const expRows = await r2sql(env,
        `SELECT DISTINCT expiration FROM options.option_contracts WHERE symbol = ${lit(symbol)} ` +
        `AND run_id = ${lit(ud.run_id)} ORDER BY expiration LIMIT 500`,
        "symexp_" + symbol + "_" + ud.run_id, QUERY_TTL_MS);
      const expirations = sortedUnique(expRows.map((x) => String(x.expiration)));
      const today = new Date().toISOString().slice(0, 10);
      const frontMonth = expirations.find((e) => e >= today) ?? expirations[expirations.length - 1];
      const activeExp = expiration && expirations.includes(expiration)
        ? expiration
        : frontMonth;
      if (!activeExp) return { rows: [] as Row[], expirations, n_contracts: 0 };

      const expFilter = `AND expiration = ${lit(activeExp)}`;
      // Bound the SQL when near-spot is set: pull a wider strike window then trim.
      // Without near_spot (or parts=full legacy), keep the historical full dump for
      // one expiration when expiration is set; full parts still dumps all expiries.
      const scoped = parts === "chain" || Boolean(expiration);
      const sql =
        `SELECT expiration, type, strike, last, bid, ask, volume, open_interest, implied_vol, ` +
        `delta, gamma, theta, vega, rho, in_the_money, theo, bid_size, ask_size, fetched_at ` +
        `FROM options.option_contracts WHERE symbol = ${lit(symbol)} ` +
        `AND run_id = ${lit(ud.run_id)} ` +
        `${scoped ? expFilter + " " : ""}` +
        `ORDER BY expiration, strike, type LIMIT ${R2SQL_LIMIT_MAX}`;
      const cacheKey = scoped
        ? `symc_${symbol}_${ud.run_id}_${activeExp}`
        : `symc_${symbol}_${ud.run_id}`;
      const raw = await r2sql(env, sql, cacheKey, QUERY_TTL_MS);
      const trimmed = scoped && nearSpot > 0
        ? filterContractsNearSpot(raw as Row[], spot, nearSpot)
        : (raw as Row[]);
      return { rows: trimmed, expirations, n_contracts: trimmed.length };
    })()
    : Promise.resolve({ rows: [] as Row[], expirations: [] as string[], n_contracts: 0 });

  const enrichPromise = wantEnrich
    ? enrichSymbol(env, symbol)
    : Promise.resolve({
      ohlc: [] as Row[], realized_vol: null as Row | null, corporate_actions: [] as Row[],
      etf_profile: null as Row | null, etf_holdings: [] as Row[],
    });

  const [{ rows, expirations, n_contracts }, enrich] = await Promise.all([
    chainPromise,
    enrichPromise,
  ]);

  return {
    underlying,
    contracts: rows,
    expirations,
    n_contracts,
    ohlc: enrich.ohlc,
    realized_vol: enrich.realized_vol,
    corporate_actions: enrich.corporate_actions,
    etf_profile: enrich.etf_profile,
    etf_holdings: enrich.etf_holdings,
  };
}

// ---------------------------------------------------------------------------
// Lake schema (/api/tables) — D1-backed cache, serve-stale + background refresh
// ---------------------------------------------------------------------------
// The schema payload is expensive to compute: SHOW TABLES, then per table
// DESCRIBE + COUNT(*) + a 3-row sample — several R2 SQL round trips against
// the lake (~8s). The AI copilot loads it on every chat question and the SQL
// Lab sidebar on every mount, so users must NEVER block on that compute: fresh
// rows serve immediately, stale rows serve the cached payload instantly while
// a background refresh (ctx.waitUntil) recomputes, and a throttled background
// probe diffs the lake's table/column shape against the cache so loader schema
// changes refresh proactively instead of waiting for the next TTL expiry. Only
// an explicit ?force=1 (Data catalog refresh) or a truly empty cache (first-ever
// call on a fresh D1) computes synchronously. D1 failures degrade to live
// compute (the R2 SQL lake remains the source of truth — the cache is a
// performance layer only).
const SCHEMA_TTL_MS = 10 * 60 * 1000; // schema structure is near-static
const SCHEMA_CACHE_KEY = "lake_tables";
const SCHEMA_SAMPLE_LIMIT = 3;
// Background change-detection cadence: on fresh reads, diff the lake's shape
// against the cached payload at most once per interval, per isolate.
const SCHEMA_PROBE_INTERVAL_MS = 60 * 1000;

interface LakeTable {
  name: string;
  row_count: number | null;
  columns: { name: string; type: string }[];
  sample: Record<string, unknown>[];
}

// Lake tables that must NEVER surface to users: options.chat_history holds
// full Copilot transcripts (admin-only by design). Excluded here from the
// /api/tables payload AND from the background change-probe's name diff (both
// sides must compare the same filtered set or the probe would see a permanent
// mismatch and churn a refresh every interval). /api/query separately blocks
// references to these tables unless the request carries ADMIN_TOKEN.
const PRIVATE_LAKE_TABLES = new Set(["chat_history"]);
const PRIVATE_LAKE_TABLES_RE = /\bchat_history\b/;
const isPublicLakeTable = (name: string): boolean => !PRIVATE_LAKE_TABLES.has(name);

/** Compute the full schema payload straight from the lake (uncached). */
async function loadLakeTables(env: Env): Promise<LakeTable[]> {
  const list = await r2sql(env, "SHOW TABLES IN options");
  return Promise.all(
    list
      .map((t) => String(t.table_name))
      .filter(isPublicLakeTable)
      .map(async (name): Promise<LakeTable> => {
        const [cols, cnt, sample] = await Promise.all([
          r2sql(env, `DESCRIBE options.${name}`),
          r2sql(env, `SELECT COUNT(*) n FROM options.${name}`, "tbl_count_" + name),
          r2sql(env, `SELECT * FROM options."${name}" LIMIT ${SCHEMA_SAMPLE_LIMIT}`, "tbl_sample_" + name),
        ]);
        return {
          name,
          row_count: num(cnt[0]?.n),
          columns: cols.map((c) => ({ name: String(c.column_name), type: String(c.type) })),
          sample: sample as Record<string, unknown>[],
        };
      }),
  );
}

/** Read the cached payload row; null when missing or D1 read fails. */
async function readSchemaRow(env: Env): Promise<{ payload: string; expires_at: number } | null> {
  try {
    return (await env.SCHEMA_DB.prepare(
      "SELECT payload, expires_at FROM schema_cache WHERE key = ?1",
    ).bind(SCHEMA_CACHE_KEY).first<{ payload: string; expires_at: number }>()) ?? null;
  } catch (e) {
    // D1 read failed — the caller falls back to live compute; the lake is
    // authoritative, the cache is a performance layer only.
    console.error("schema cache read failed", e);
    return null;
  }
}

async function writeSchemaRow(env: Env, tables: LakeTable[]): Promise<void> {
  try {
    await env.SCHEMA_DB.prepare(
      "INSERT INTO schema_cache (key, payload, expires_at) VALUES (?1, ?2, ?3) " +
        "ON CONFLICT(key) DO UPDATE SET payload = excluded.payload, expires_at = excluded.expires_at",
    ).bind(SCHEMA_CACHE_KEY, JSON.stringify(tables), Date.now() + SCHEMA_TTL_MS).run();
  } catch (e) {
    // D1 write failed — serve the freshly computed payload anyway.
    console.error("schema cache write failed", e);
  }
}

// Single-flight guard: at most one background refresh per isolate.
let schemaRefreshRunning = false;
let lastSchemaProbeAt = 0;

/** Recompute the payload from the lake and upsert into D1 (background-safe). */
async function refreshLakeSchema(env: Env): Promise<void> {
  if (schemaRefreshRunning) return;
  schemaRefreshRunning = true;
  try {
    await writeSchemaRow(env, await loadLakeTables(env));
  } catch (e) {
    // Lake/D1 hiccup — leave the (stale) row in place; the next read that
    // finds it stale retries the refresh. Never surfaced to the user.
    console.error("background schema refresh failed", e);
  } finally {
    schemaRefreshRunning = false;
  }
}

/** Fingerprint of the cached payload: sorted "table:column" pairs. */
function schemaFingerprint(tables: LakeTable[]): string {
  const pairs: string[] = [];
  for (const t of tables) for (const c of t.columns) pairs.push(`${t.name}:${c.name}`);
  return pairs.sort().join("|");
}

/** Background diff of the lake's current shape vs the cached payload. */
async function probeLakeSchema(env: Env, payload: string): Promise<void> {
  const cached = JSON.parse(payload) as LakeTable[];
  let listed: { table_name?: unknown }[];
  try {
    listed = await r2sql(env, "SHOW TABLES IN options");
  } catch (e) {
    console.error("schema change probe failed", e);
    return;
  }
  const tableNames = listed
    .map((t) => String(t.table_name))
    .filter(isPublicLakeTable)
    .sort();
  const cachedNames = cached.map((t) => t.name).sort();
  if (tableNames.join("|") !== cachedNames.join("|")) {
    // Table added/removed — skip the column pass, refresh now.
    console.log("lake schema changed (tables) — refreshing schema cache in background");
    await refreshLakeSchema(env);
    return;
  }
  // Table set matches — diff columns via one DESCRIBE per table. The lake has
  // a handful of `options.*` tables, so this is a few cheap metadata calls.
  try {
    const shaped = await Promise.all(
      tableNames.map(async (name) => ({
        name,
        cols: (await r2sql(env, `DESCRIBE options.${name}`)) as { column_name?: unknown }[],
      })),
    );
    const current = shaped
      .flatMap((t) => t.cols.map((c) => `${t.name}:${String(c.column_name)}`))
      .sort()
      .join("|");
    if (current !== schemaFingerprint(cached)) {
      console.log("lake schema changed (columns) — refreshing schema cache in background");
      await refreshLakeSchema(env);
    }
  } catch (e) {
    console.error("schema column probe failed", e);
  }
}

/** Throttled probe kick-off from the fresh-read path (never blocks the request). */
function maybeProbeSchema(ctx: Pick<ExecutionContext, "waitUntil">, env: Env, payload: string): void {
  const now = Date.now();
  if (now - lastSchemaProbeAt < SCHEMA_PROBE_INTERVAL_MS) return;
  lastSchemaProbeAt = now;
  ctx.waitUntil(probeLakeSchema(env, payload));
}

/**
 * Schema payload served by /api/tables — users never wait on the ~8s lake
 * recompute. Fresh rows serve immediately (plus a throttled background
 * change-detection probe); stale rows serve the cached payload instantly while
 * the refresh runs in the background via ctx.waitUntil; only a truly empty
 * cache (first-ever call on a fresh D1) or an explicit ?force=1 (Data catalog
 * refresh) computes synchronously. Trade-off: after a TTL expiry a reader sees
 * the previous payload for at most one refresh cycle (~10s) instead of an 8s
 * stall, and background refreshes also fire when the lake's shape changes.
 */
async function schemaTables(env: Env, ctx: Pick<ExecutionContext, "waitUntil">, force: boolean): Promise<LakeTable[]> {
  const row = await readSchemaRow(env);
  if (force) {
    // Explicit user intent (Data catalog refresh): recompute now, serve fresh.
    const tables = await loadLakeTables(env);
    await writeSchemaRow(env, tables);
    return tables;
  }
  if (row) {
    if (Date.now() < row.expires_at) {
      maybeProbeSchema(ctx, env, row.payload);
      return JSON.parse(row.payload) as LakeTable[];
    }
    // Stale — serve the cached payload now, refresh in the background.
    ctx.waitUntil(refreshLakeSchema(env));
    return JSON.parse(row.payload) as LakeTable[];
  }
  // Nothing cached yet: compute once synchronously so there is a row for every
  // future read. One-time cost per environment (prod's cache is already warm).
  const tables = await loadLakeTables(env);
  await writeSchemaRow(env, tables);
  return tables;
}

async function runQuery(env: Env, sqlIn: string, limit = 1000): Promise<{ columns: string[]; rows: Row[]; row_count: number; truncated: boolean; limit: number; error?: string }> {
  const cleaned = sqlIn.trim().replace(/;$/, "").trim();
  if (!cleaned) return { columns: [], rows: [], row_count: 0, truncated: false, limit, error: "Empty query" };
  const head = cleaned.replace(/^\(/, "").trim().split(/\s+/)[0]?.toUpperCase() ?? "";
  if (!["SELECT", "WITH", "DESCRIBE", "DESC", "SHOW", "EXPLAIN"].includes(head))
    return { columns: [], rows: [], row_count: 0, truncated: false, limit, error: "Only read-only queries allowed" };
  const lower = cleaned.toLowerCase();
  for (const kw of ["insert into", "update ", "delete from", "drop ", "create ", "alter ", "truncate ", "attach ", "detach "])
    if (lower.includes(kw)) return { columns: [], rows: [], row_count: 0, truncated: false, limit, error: "Disallowed keyword" };
  // Cartesian products of large tables are the one expensive pattern R2 SQL does
  // NOT budget-gate up front (it may just time out). Reject them outright: options
  // analytics never needs a CROSS JOIN. R2 SQL's other cost guards (read-only,
  // LIMIT default 500, budget-gated DISTINCT/aggregate/window) already apply.
  if (/\bcross\s+join\b/.test(lower))
    return { columns: [], rows: [], row_count: 0, truncated: false, limit, error: "CROSS JOIN is not allowed (cartesian products of large tables). Join on a key with WHERE filters instead." };
  const lim = clamp(limit, 1, R2SQL_LIMIT_MAX);
  try {
    // Memoize by full outer SQL: identical queries (the chat frame pulls, SQL
    // Lab reruns) hit the isolate cache instead of the lake. QUERY_TTL_MS is a
    // bound, not a leak — the Map dies with the isolate and data refreshes nightly.
    const outer = `SELECT * FROM (${cleaned}) AS __q LIMIT ${lim}`;
    const rows = await r2sql(env, outer, "q_" + sqlHash(outer), QUERY_TTL_MS);
    const columns = rows.length ? Object.keys(rows[0]) : [];
    return { columns, rows, row_count: rows.length, truncated: rows.length >= lim, limit: lim };
  } catch (e) {
    return { columns: [], rows: [], row_count: 0, truncated: false, limit: lim, error: String(e) };
  }
}

// ---------------------------------------------------------------------------
// Notebook: 45-day premium leaders
// ---------------------------------------------------------------------------
async function notebookPremium(env: Env, p: {
  target_dte?: number; tolerance?: number; moneyness_band?: number; min_volume?: number; limit?: number;
}): Promise<{ notebook: string; target_dte: number; tolerance: number; moneyness_band: number; min_volume: number; calls: Row[]; puts: Row[] }> {
  const targetDte = p.target_dte ?? 45;
  const tol = p.tolerance ?? 7;
  const band = p.moneyness_band ?? 0.15;
  const minVol = p.min_volume ?? 0;
  const limit = clamp(p.limit ?? 25, 1, 200);
  const lo = Math.max(targetDte - tol, 0);
  const hi = targetDte + tol;
  const sql =
    "WITH exp AS (\n" +
    "  SELECT symbol, expiration, (CAST(expiration AS DATE) - CURRENT_DATE) AS dte,\n" +
    "         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ABS((CAST(expiration AS DATE) - CURRENT_DATE) - " + lit(targetDte) + ")) AS rn\n" +
    "  FROM (SELECT DISTINCT symbol, expiration FROM options.option_contracts) e\n" +
    "  WHERE (CAST(expiration AS DATE) - CURRENT_DATE) BETWEEN " + lit(lo) + " AND " + lit(hi) + "\n" +
    "),\n" +
    "ranked AS (\n" +
    "  SELECT c.symbol, u.name, u.sector, u.spot_price AS spot, c.expiration, c.type, c.strike,\n" +
    "         c.last, c.bid, c.ask, c.volume, c.open_interest, c.implied_vol, c.delta, c.in_the_money,\n" +
    "         COALESCE(c.last, (c.bid + c.ask) / 2.0) AS premium,\n" +
    "         CASE WHEN u.spot_price IS NOT NULL AND u.spot_price > 0 THEN (c.strike - u.spot_price) / u.spot_price END AS moneyness,\n" +
    "         CASE WHEN u.spot_price IS NOT NULL AND u.spot_price > 0 THEN COALESCE(c.last, (c.bid + c.ask) / 2.0) / u.spot_price END AS premium_ratio,\n" +
    "         ROW_NUMBER() OVER (PARTITION BY c.symbol, c.type ORDER BY " +
    "           (CASE WHEN u.spot_price IS NOT NULL AND u.spot_price > 0 THEN COALESCE(c.last, (c.bid + c.ask) / 2.0) / u.spot_price END) DESC NULLS LAST, " +
    "           COALESCE(c.volume,0) DESC) AS prn\n" +
    "  FROM options.option_contracts c JOIN exp e ON e.symbol = c.symbol AND e.expiration = c.expiration\n" +
    "  JOIN (" + LATEST_UNDERLYING + ") u ON u.symbol = c.symbol\n" +
    "  WHERE e.rn = 1 AND u.spot_price IS NOT NULL AND u.spot_price > 0\n" +
    "    AND COALESCE(c.last, (c.bid + c.ask) / 2.0) IS NOT NULL\n" +
    "    AND COALESCE(c.last, (c.bid + c.ask) / 2.0) > 0\n" +
    "    AND COALESCE(c.volume, 0) >= " + lit(minVol) + "\n" +
    "    AND ABS((c.strike - u.spot_price) / u.spot_price) <= " + lit(band) + "\n" +
    "),\n" +
    "calls_ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY type ORDER BY premium_ratio DESC NULLS LAST) AS trn FROM ranked WHERE prn = 1)\n" +
    "SELECT symbol, name, sector, spot, expiration, type, strike, last, bid, ask, volume, open_interest, implied_vol, delta, in_the_money, premium, moneyness, premium_ratio\n" +
    "FROM calls_ranked WHERE trn <= " + lit(limit) + " ORDER BY type, premium_ratio DESC NULLS LAST";
  const rows = (await r2sql(env, sql,
    "nb_" + `${targetDte}_${tol}_${band}_${minVol}_${limit}`, QUERY_TTL_MS)) as Row[];
  return {
    notebook: "45-day-premium-leaders", target_dte: targetDte, tolerance: tol,
    moneyness_band: band, min_volume: minVol,
    calls: rows.filter((r) => r.type === "call"), puts: rows.filter((r) => r.type === "put"),
  };
}

// ---------------------------------------------------------------------------
// News — /api/news (Tavily news search proxy)
// ---------------------------------------------------------------------------
// Narrative half of "why is this moving" for the AI copilot: per-ticker
// headlines via Tavily's licensed news-search API. The worker holds the
// TAVILY_API_KEY secret (server-side — never reaches the browser, unlike the
// unofficial Yahoo/Bing RSS endpoints it replaces). Tavily returns
// relevance-sorted, symbol-scoped results, which we strip to
// {title, link, published, snippet, source}. Upstream failures degrade to an
// empty item list with an error string — never a 500 — so a chat turn is
// never blocked by a news outage. Results are memoized in-isolate for 10 min
// keyed by symbol (only successes cache; failures retry on the next call).
const NEWS_TTL_MS = 10 * 60 * 1000;
const NEWS_DEFAULT_LIMIT = 8;
const NEWS_MAX_LIMIT = 20;
const NEWS_SYMBOL_RE = /^[A-Z0-9][A-Z0-9.\-]*$/;
const NEWS_SNIPPET_MAX = 240;
const TAVILY_API_URL = "https://api.tavily.com/search";
// News-only recency window (days) — keeps results current without spending the
// free monthly credit pool on stale stories.
const TAVILY_NEWS_DAYS = 7;

type NewsSource = "tavily";

interface NewsItem {
  title: string;
  link: string;
  published: string | null;
  snippet: string;
  source: NewsSource;
}

function truncateAtSentence(s: string, max = NEWS_SNIPPET_MAX): string {
  if (s.length <= max) return s;
  const window = s.slice(0, max + 1);
  const boundary = Math.max(window.lastIndexOf(". "), window.lastIndexOf("! "), window.lastIndexOf("? "));
  return (boundary >= max * 0.6 ? window.slice(0, boundary + 1) : window).trimEnd();
}

async function tavilyNews(env: Env, symbol: string): Promise<NewsItem[]> {
  const response = await fetch(TAVILY_API_URL, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${env.TAVILY_API_KEY}`,
    },
    body: JSON.stringify({
      query: `${symbol} stock news`,
      topic: "news",
      search_depth: "basic",
      max_results: NEWS_MAX_LIMIT,
      days: TAVILY_NEWS_DAYS,
      include_answer: false,
      include_raw_content: false,
    }),
  });
  if (!response.ok) throw new Error(`tavily news returned HTTP ${response.status}`);
  const data = await response.json() as { results?: { title?: string; url?: string; content?: string; published_date?: string | null }[] };
  return (data.results ?? [])
    .map((r): NewsItem => ({
      title: (r.title ?? "").trim(),
      link: (r.url ?? "").trim(),
      published: r.published_date || null,
      snippet: truncateAtSentence((r.content ?? "").trim()),
      source: "tavily",
    }))
    .filter((it) => it.title)
    .slice(0, NEWS_MAX_LIMIT);
}

async function news(env: Env, symbolIn: string | null, limitIn: number): Promise<{
  symbol: string; items: NewsItem[]; source?: NewsSource; error?: string; fetched_at: string;
}> {
  const symbol = (symbolIn ?? "").trim().toUpperCase();
  const fetchedAt = () => new Date().toISOString();
  if (!NEWS_SYMBOL_RE.test(symbol)) {
    return { symbol, items: [], error: "invalid symbol", fetched_at: fetchedAt() };
  }
  const limit = clamp(limitIn || NEWS_DEFAULT_LIMIT, 1, NEWS_MAX_LIMIT);
  try {
    const items = await cached<NewsItem[]>(`news:${symbol}`, NEWS_TTL_MS, () => tavilyNews(env, symbol));
    return { symbol, items: items.slice(0, limit), source: "tavily", fetched_at: fetchedAt() };
  } catch (error) {
    return { symbol, items: [], error: String((error && (error as Error).message) || error), fetched_at: fetchedAt() };
  }
}

// ---------------------------------------------------------------------------
// Web search — /api/web_search (Tavily general search proxy)
// ---------------------------------------------------------------------------
// Open-ended web search for fresh analyst/market commentary beyond the
// per-ticker news feed: "what are analysts saying about X", "what happened
// with sector Y", "latest take on theme Z". Same Tavily key + proxy shape as
// /api/news but without the news-topic pin or recency window, so the model can
// surface current articles/notes with citable links. Results are stripped to
// {title, link, snippet, source}, capped at 5, snippets trimmed to ~240 chars,
// and memoized in-isolate for 10 min keyed by the lowercased query (only
// successes cache — failures retry on the next call). Upstream failures
// degrade to a 200 with an empty `results` list + `error` — never a 500 — so a
// search outage cannot block a chat turn.
const WEB_SEARCH_TTL_MS = 10 * 60 * 1000;
const WEB_SEARCH_DEFAULT_LIMIT = 5;
const WEB_SEARCH_MAX_LIMIT = 5;
const WEB_SEARCH_QUERY_MAX = 200;

interface WebSearchResult {
  title: string;
  link: string;
  snippet: string;
  source: string | null;
}

async function tavilySearch(env: Env, query: string): Promise<WebSearchResult[]> {
  const response = await fetch(TAVILY_API_URL, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${env.TAVILY_API_KEY}`,
    },
    body: JSON.stringify({
      query,
      include_answer: false,
      include_raw_content: false,
      search_depth: "basic",
      max_results: WEB_SEARCH_MAX_LIMIT,
    }),
  });
  if (!response.ok) throw new Error(`tavily search returned HTTP ${response.status}`);
  const data = await response.json() as { results?: { title?: string; url?: string; content?: string; source?: string | null }[] };
  return (data.results ?? [])
    .map((r): WebSearchResult => ({
      title: (r.title ?? "").trim(),
      link: (r.url ?? "").trim(),
      snippet: truncateAtSentence((r.content ?? "").trim()),
      source: (r.source ?? "").trim() || null,
    }))
    .filter((it) => it.title)
    .slice(0, WEB_SEARCH_MAX_LIMIT);
}

async function webSearch(env: Env, queryIn: string | null, limitIn: number): Promise<{
  query: string; results: WebSearchResult[]; error?: string; fetched_at: string;
}> {
  const query = (queryIn ?? "").trim();
  const fetchedAt = () => new Date().toISOString();
  if (!query || query.length > WEB_SEARCH_QUERY_MAX) {
    return { query, results: [], error: "invalid query", fetched_at: fetchedAt() };
  }
  const limit = clamp(limitIn || WEB_SEARCH_DEFAULT_LIMIT, 1, WEB_SEARCH_MAX_LIMIT);
  try {
    const results = await cached<WebSearchResult[]>(`websearch:${query.toLowerCase()}`, WEB_SEARCH_TTL_MS, () => tavilySearch(env, query));
    return { query, results: results.slice(0, limit), fetched_at: fetchedAt() };
  } catch (error) {
    return { query, results: [], error: String((error && (error as Error).message) || error), fetched_at: fetchedAt() };
  }
}

// ---------------------------------------------------------------------------
// IV rank / percentile — /api/iv_rank
// ---------------------------------------------------------------------------
// "Is this IV expensive or cheap vs its own history?" The lake is append-only
// with intraday chain snapshots, so per-symbol ATM-IV history exists. One
// pre-aggregated query collapses each day to a single ATM-IV point (GROUP BY
// fetched_at date dedupes the ~15-min snapshots), then rank_pct is the
// fraction of the daily series at or below today's median — computed in JS.
// Bounded (per-symbol + window cap) and cached 60 min; lake errors degrade to
// a 200 with `points: []` + `error`, matching the /api/news contract.
const IV_RANK_TTL_MS = 60 * 60 * 1000;
const IV_RANK_DEFAULT_DAYS = 90;
const IV_RANK_MAX_DAYS = 120;
// Near-ATM moneyness band (5%) — pick the liquid, at-the-money contracts.
const IV_RANK_ATM_BAND = 0.05;

interface IvRankPoint { d: string; iv: number | null; }

interface IvRankResponse {
  symbol: string;
  days: number;
  points: IvRankPoint[];
  rank_pct: number | null;
  iv_now: number | null;
  iv_median: number | null;
  iv_min: number | null;
  iv_max: number | null;
  as_of: string | null;
  error?: string;
}

async function ivRank(env: Env, symbolIn: string | null, daysIn: number): Promise<IvRankResponse> {
  const symbol = (symbolIn ?? "").trim().toUpperCase();
  const days = clamp(daysIn || IV_RANK_DEFAULT_DAYS, 1, IV_RANK_MAX_DAYS);
  if (!NEWS_SYMBOL_RE.test(symbol)) {
    return { symbol, days, points: [], rank_pct: null, iv_now: null, iv_median: null, iv_min: null, iv_max: null, as_of: null, error: "invalid symbol" };
  }
  // option_contracts has no spot column — the near-ATM band compares each
  // contract's strike against the symbol's latest underlying spot, joined via
  // LATEST_UNDERLYING (same pattern as the screen's moneyness filter).
  const sql =
    `SELECT CAST(c.fetched_at AS DATE) AS d, approx_median(c.implied_vol) AS iv ` +
    `FROM options.option_contracts c ` +
    `JOIN (${LATEST_UNDERLYING}) u ON u.symbol = c.symbol ` +
    `WHERE c.symbol = ${lit(symbol)} AND c.implied_vol IS NOT NULL ` +
    `AND u.spot_price IS NOT NULL AND u.spot_price > 0 ` +
    `AND ABS(c.strike - u.spot_price) / u.spot_price <= ${IV_RANK_ATM_BAND} ` +
    `AND CAST(c.fetched_at AS DATE) >= CAST(CURRENT_DATE - INTERVAL '${days}' DAY AS DATE) ` +
    `GROUP BY CAST(c.fetched_at AS DATE)`;
  try {
    const result = await cached<IvRankResponse>(`iv_rank:${symbol}:${days}`, IV_RANK_TTL_MS, async () => {
      const rows = await r2sql(env, sql);
      const points = rows
        .map((r) => ({ d: String(r.d), iv: numOrNull(r.iv) }))
        .sort((a, b) => a.d.localeCompare(b.d)); // chronological
      const ivs = points.map((p) => p.iv).filter((v): v is number => v != null);
      if (ivs.length === 0) {
        return { symbol, days, points, rank_pct: null, iv_now: null, iv_median: null, iv_min: null, iv_max: null, as_of: null };
      }
      const sorted = [...ivs].sort((a, b) => a - b);
      const iv_median = sorted[Math.floor(sorted.length / 2)];
      const iv_min = sorted[0];
      const iv_max = sorted[sorted.length - 1];
      const last = points[points.length - 1];
      const iv_now = last && last.iv != null ? last.iv : null;
      const rank_pct = iv_now != null ? ivs.filter((v) => v <= iv_now).length / ivs.length : null;
      return {
        symbol, days,
        points,
        rank_pct: rank_pct != null ? Math.round(rank_pct * 1000) / 1000 : null,
        iv_now, iv_median, iv_min, iv_max,
        as_of: last ? last.d : null,
      };
    });
    // cached() stores the error-less value; guarantee the shape typed here.
    return result;
  } catch (error) {
    return { symbol, days, points: [], rank_pct: null, iv_now: null, iv_median: null, iv_min: null, iv_max: null, as_of: null, error: String((error && (error as Error).message) || error) };
  }
}

// ---------------------------------------------------------------------------
// Macro / economic calendar — /api/econ_calendar
// ---------------------------------------------------------------------------
// "Binary-event weeks" (FOMC, CPI, jobs, PCE) lift broad implied vol, so the
// Copilot gets the upcoming scheduled releases. With FRED_API_KEY set, the
// endpoint matches the curated high-impact macro release names (exact —
// see below) from FRED's releases/dates API and merges in FOMC/Beige events
// from the Fed's keyless calendar JSON (FRED emits daily placeholders for
// unscheduled press releases, so FOMC dates come from the Fed calendar, which
// pre-schedules them accurately). Without a key, only the Fed calendar is
// used. Cached 12h: release schedules move on a quarterly-ish cadence and the
// Fed JSON is ~0.5MB. Same degrade-to-empty contract as the other endpoints.
const ECON_TTL_MS = 12 * 60 * 60 * 1000;
const ECON_DEFAULT_DAYS = 30;
const ECON_MAX_DAYS = 90;
// Source tag on options.econ_calendar rows (matches loader/src/econ.ts) —
// used to derive the response `provider` from merged lake rows.
const ECON_SOURCE_FRED = "fred";
const FRED_API_URL = "https://api.stlouisfed.org/fred/releases/dates";
const FED_CALENDAR_URL = "https://www.federalreserve.gov/json/calendar.json";
// Exact-match allowlist of high-impact macro releases. Substring matching
// pulls in lookalikes ("Research Consumer Price Index", "Debt to Gross
// Domestic Product Ratios"), and with include_release_dates_with_no_data=true
// FRED returns a placeholder holder date for *every* day for unscheduled
// press-release-style releases ("FOMC Press Release" appears daily) — hence
// FOMC/Beige are sourced from the Fed calendar instead.
const ECON_FRED_RELEASES = new Set([
  "Consumer Price Index",
  "Producer Price Index",
  "Employment Situation",
  "Personal Income and Outlays",
  "Gross Domestic Product",
  "Surveys of Consumers",
]);
// Fed-calendar event types that move broad vol (the rest are weekly H-stat
// releases, speeches, and testimonies).
const FED_CALENDAR_TYPES = new Set(["FOMC", "Beige"]);

interface EconCalendarEvent { date: string; title: string; kind: "macro" | "fed"; time?: string; }

interface EconCalendarResponse {
  window_days: number;
  as_of: string;
  provider: "fred" | "federalreserve";
  items: EconCalendarEvent[];
  error?: string;
}

// Upcoming FOMC (meetings/statements/minutes/press conferences) + Beige Book
// from the Fed's keyless calendar JSON. Date = `month` + `days`; `days` can be
// a comma list for recurring releases (take the first). `time` is the Fed's ET
// wall-clock release time, normalized to "HH:MM" (matches the lake's
// options.econ_calendar.event_time).
function normalizeEventTime(raw: string | undefined | null): string | undefined {
  if (!raw) return undefined;
  const m = /^(\d{1,2}):(\d{2})\s*(a\.?m\.?|p\.?m\.?)$/i.exec(raw.trim());
  if (!m) return undefined;
  let h = Number(m[1]) % 12;
  if (/p/i.test(m[3])) h += 12;
  return `${String(h).padStart(2, "0")}:${m[2]}`;
}
async function fedCalendarEvents(days: number): Promise<EconCalendarEvent[]> {
  const res = await fetch(FED_CALENDAR_URL);
  if (!res.ok) throw new Error(`federalreserve calendar returned HTTP ${res.status}`);
  const data = await res.json() as { events?: { title?: string; type?: string; month?: string; days?: string; time?: string }[] };
  const now = Date.now();
  const endMs = now + days * 86400_000;
  const items: EconCalendarEvent[] = [];
  for (const e of data.events ?? []) {
    if (!FED_CALENDAR_TYPES.has(e.type ?? "")) continue;
    const day = Number((e.days ?? "").split(",")[0].trim());
    if (!Number.isFinite(day) || day < 1 || day > 31) continue;
    const m = /^(\d{4})-(\d{2})$/.exec(e.month ?? "");
    if (!m) continue;
    const date = `${m[1]}-${m[2]}-${String(day).padStart(2, "0")}`;
    const ts = Date.parse(date + "T00:00:00Z");
    if (!Number.isFinite(ts) || ts < now || ts > endMs) continue;
    const time = normalizeEventTime(e.time);
    items.push({ date, title: (e.title ?? "").trim() || String(e.type ?? ""), kind: "fed", ...(time ? { time } : {}) });
  }
  return items.sort((a, b) => a.date.localeCompare(b.date));
}

async function econCalendar(env: Env, daysIn: number): Promise<EconCalendarResponse> {
  const days = clamp(daysIn || ECON_DEFAULT_DAYS, 1, ECON_MAX_DAYS);
  const asOf = () => new Date().toISOString();
  try {
    return await cached<EconCalendarResponse>(`econ:${days}`, ECON_TTL_MS, async () => {
      // Primary path: read the upcoming window from the lake's econ_calendar
      // table (fred-econ-daily job → options.econ_calendar), newest run per
      // (event_date, title). Fall back to the live FRED/Fed fetch when the
      // table is missing or empty (e.g. before the first sync lands).
      const now = Date.now();
      const iso = (d: Date) => d.toISOString().slice(0, 10);
      const start = iso(new Date(now - 86400_000));
      const end = iso(new Date(now + days * 86400_000));
      let lakeItems: { date: string; title: string; kind: "macro" | "fed" }[] = [];
      let lakeProvider: "fred" | "federalreserve" | null = null;
      try {
        const rows = await r2sql(
          env,
          "WITH latest AS (" +
            "SELECT event_date, title, kind, source, event_time, fetched_at, " +
            "ROW_NUMBER() OVER (PARTITION BY event_date, title ORDER BY fetched_at DESC) rn " +
            "FROM options.econ_calendar) " +
            `SELECT event_date, title, kind, source, event_time FROM latest ` +
            `WHERE rn = 1 AND event_date >= ${lit(start)} AND event_date <= ${lit(end)} ` +
            `ORDER BY event_date, title LIMIT ${R2SQL_LIMIT_MAX}`,
          "econ_lake:" + days,
          QUERY_TTL_MS,
        );
        if (rows.length > 0) {
          lakeItems = rows.map((r) => ({
            date: String(r.event_date),
            title: String(r.title),
            kind: r.kind === "fed" ? "fed" : "macro",
            ...(r.event_time ? { time: String(r.event_time) } : {}),
          }));
          lakeProvider = rows.some((r) => r.source === ECON_SOURCE_FRED)
            ? "fred"
            : "federalreserve";
        }
      } catch (error) {
        // Lake missing/empty — fall through to the live fetch below.
        console.log(`econ lake read failed (${String((error && (error as Error).message) || error)}) — falling back to live fetch`);
      }
      if (lakeItems.length > 0) {
        return { window_days: days, as_of: asOf(), provider: lakeProvider!, items: lakeItems };
      }
      // Fallback: live FRED releases/dates + Fed calendar (original behavior).
      if (env.FRED_API_KEY) {
        const end2 = new Date(now + days * 86400_000);
        const url =
          `${FRED_API_URL}?api_key=${encodeURIComponent(env.FRED_API_KEY)}&file_type=json` +
          `&realtime_start=${iso(new Date(now - 86400_000))}&realtime_end=${iso(end2)}` +
          `&include_release_dates_with_no_data=true&sort_order=asc&limit=1000`;
        const res = await fetch(url);
        if (!res.ok) throw new Error(`fred releases/dates returned HTTP ${res.status}`);
        const data = await res.json() as { release_dates?: { release_name?: string; date?: string }[] };
        const items: EconCalendarEvent[] = (data.release_dates ?? [])
          .filter((r) => r.date && r.release_name && ECON_FRED_RELEASES.has(r.release_name))
          .map((r): EconCalendarEvent => ({ date: String(r.date), title: String(r.release_name).trim(), kind: "macro" }))
          .concat(await fedCalendarEvents(days))
          .sort((a, b) => a.date.localeCompare(b.date));
        return { window_days: days, as_of: asOf(), provider: "fred", items };
      }
      const items = await fedCalendarEvents(days);
      return { window_days: days, as_of: asOf(), provider: "federalreserve", items };
    });
  } catch (error) {
    return { window_days: days, as_of: asOf(), provider: env.FRED_API_KEY ? "fred" : "federalreserve", items: [], error: String((error && (error as Error).message) || error) };
  }
}
function num(v: unknown): number { const n = Number(v); return Number.isFinite(n) ? n : 0; }
function numOrNull(v: unknown): number | null { const n = Number(v); return v == null || !Number.isFinite(n) ? null : n; }
function strOrNull(v: unknown): string | null { return v == null ? null : String(v); }
function clamp(n: number, lo: number, hi: number): number { return Math.max(lo, Math.min(hi, Math.round(n))); }
function sortedUnique(arr: string[]): string[] { return Array.from(new Set(arr)).sort(); }


// ---------------------------------------------------------------------------
// Copilot chat history — capture + admin-only read
// ---------------------------------------------------------------------------
// The browser posts each completed chat turn to POST /api/chat/history; the
// worker normalizes it and publishes one record per turn to the
// cboe_chat_history_v2 stream → options.chat_history (append-only, one row
// per turn with the full conversation so far under the conversation's
// chat_id — consumers keep the newest row per chat_id, the lake's standard
// latest-wins pattern).
//
// Access control. The table is PRIVATE: it is excluded from /api/tables (see
// PRIVATE_LAKE_TABLES above) and /api/query refuses any SQL referencing it
// unless the request carries the admin token. The only lake read path is
// GET /api/admin/chat_history (session admin or Bearer ADMIN_TOKEN). Failed Copilot tool calls
// are NOT in the lake — they live in D1 `copilot_tool_events` and are read via
// the public GET /api/tool_calls (capped args/errors/SQL only; no ip/user_id).
// No admin UI; the endpoints are the admin/debug surface.
//
// Capture is best-effort and server-side. `ip` (CF-Connecting-IP) and
// `user_agent` (request header) are recorded for abuse tracking and are never
// accepted from the client body. `user_id` is stamped from the Better Auth
// session when the request is logged in — never from the client body. The
// same session upserts D1 `user_chats` (the user-facing catalog); the lake
// table stays admin/analytics only. Records are buffered in D1
// (pending_chat_history, migration 0002) BEFORE the response and published
// in a background waitUntil task, so the pipeline's occasionally-slow ingest
// never holds a browser connection slot; failures leave the row pending and
// a later call drains it (no transcript loss). A chat is never blocked by
// history persistence: all failures return 2xx to the browser.
const CHAT_HISTORY_MAX_MESSAGES = 100;
const CHAT_HISTORY_MAX_CONTENT = 20_000; // chars per message / sql / url
const CHAT_HISTORY_MAX_AUX = 512; // ip / user_agent / chat_id / model length
const CHAT_HISTORY_DRAIN_BATCH = 10; // pending rows re-published per call
const CHAT_HISTORY_MAX_ATTEMPTS = 5; // per pending row, then left for manual
const PIPELINE_HTTP_RETRIES = 3; // mirror the loader's requestJson retry policy
const PIPELINE_RETRY_BACKOFF_MS = 500;
// Hard cap on a single ingest POST so a stalled pipeline can never hold a
// browser connection slot indefinitely (the browser fires the history save
// fire-and-forget; a hung fetch would occupy one of Chromium's per-host
// connections and could delay the user's NEXT api call). Missed ingest →
// D1 buffer + later drain, so timeout-safe.
const PIPELINE_POST_TIMEOUT_MS = 10_000;
const LOADER_UA = "cboe-to-r2/0.2"; // loader User-Agent convention for Pipeline POSTs
const CHAT_HISTORY_ADMIN_LIMIT_MAX = 500;

function str(v: unknown): string | null {
  return v == null ? null : String(v);
}

/** Constant-time string comparison for the admin token (length is not secret). */
function secureEqual(a: string, b: string): boolean {
  const ea = new TextEncoder().encode(a);
  const eb = new TextEncoder().encode(b);
  if (ea.length !== eb.length) return false;
  let diff = 0;
  for (let i = 0; i < ea.length; i++) diff |= (ea[i] ?? 0) ^ (eb[i] ?? 0);
  return diff === 0;
}

/** True when the request carries the ADMIN_TOKEN bearer secret. */
function adminAuthorized(req: Request, env: Env): boolean {
  const expected = (env.ADMIN_TOKEN ?? "").trim();
  if (!expected) return false;
  const header = req.headers.get("Authorization") ?? "";
  const token = (header.startsWith("Bearer ") ? header.slice("Bearer ".length) : "").trim();
  return token.length > 0 && secureEqual(token, expected);
}

// Get the messages array out of an unknown JSON node (each entry is an object
// or we throw — a malformed transcript is a client bug, reject it).
function chatMessageRecord(v: unknown): Record<string, unknown> {
  if (v && typeof v === "object" && !Array.isArray(v)) return v as Record<string, unknown>;
  throw new Error("each message must be an object");
}

/** Validate + normalize a client transcript into a lake record. Throws on bad input. */
function normalizeChatHistoryRecord(
  body: Record<string, unknown>,
  ip: string,
  ua: string,
): Record<string, unknown> {
  const chatId = str(body.chat_id)?.trim().slice(0, CHAT_HISTORY_MAX_AUX);
  if (!chatId) throw new Error("chat_id is required");
  const mode = str(body.mode);
  if (mode !== "funded") throw new Error("mode must be 'funded'");
  const startedAt = str(body.started_at)!;
  const endedAt = str(body.ended_at)!;
  if (!startedAt || !endedAt || !Number.isFinite(Date.parse(startedAt)) || !Number.isFinite(Date.parse(endedAt)))
    throw new Error("started_at and ended_at must be ISO timestamps");
  if (!Array.isArray(body.messages) || body.messages.length === 0)
    throw new Error("messages must be a non-empty array");

  // Strip to {role, content, sql?, ts?} — drops bulky UI state (query result
  // tables, chart specs, error stacks) that would bloat the record and has no
  // analytic value; content/sql are the history.
  const messages = body.messages.slice(0, CHAT_HISTORY_MAX_MESSAGES).map((m) => {
    const rec = chatMessageRecord(m);
    const role = rec.role;
    if (role !== "user" && role !== "assistant") throw new Error("each message needs role 'user' | 'assistant'");
    const out: Record<string, unknown> = {
      role,
      content: typeof rec.content === "string" ? rec.content.slice(0, CHAT_HISTORY_MAX_CONTENT) : "",
    };
    if (typeof rec.sql === "string" && rec.sql) out.sql = rec.sql.slice(0, CHAT_HISTORY_MAX_CONTENT);
    if (typeof rec.ts === "number" && Number.isFinite(rec.ts)) out.ts = rec.ts;
    return out;
  });

  const record: Record<string, unknown> = {
    chat_id: chatId,
    mode,
    started_at: startedAt,
    ended_at: endedAt,
    messages: JSON.stringify(messages),
    source: "copilot",
    fetched_at: new Date().toISOString(),
  };
  const model = str(body.model)?.trim().slice(0, CHAT_HISTORY_MAX_AUX);
  if (model) record.model = model;
  if (ip) record.ip = ip;
  if (ua) record.user_agent = ua;
  return record;
}

function stripClientUserId(body: Record<string, unknown>): Record<string, unknown> {
  const { user_id: _ignored, userId: _ignoredCamel, ...rest } = body;
  return rest;
}

// POST a payload to a Pipeline stream with the loader User-Agent, idempotency
// key and retry/backoff parity with the loader's requestJson (retry only on
// 5xx/transport; 4xx hard-fails). The idempotency key is only sent when a
// token is present (same as the loader).
async function pipelinePost(
  env: Env,
  url: string,
  payload: unknown,
  idempotencyKey: string,
): Promise<void> {
  const authToken = env.PIPELINE_AUTH_TOKEN || "";
  let lastErr: unknown = new Error("pipeline post failed");
  for (let attempt = 0; attempt <= PIPELINE_HTTP_RETRIES; attempt++) {
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "user-agent": LOADER_UA,
          ...(authToken
            ? { authorization: `Bearer ${authToken}`, "idempotency-key": idempotencyKey }
            : {}),
        },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(PIPELINE_POST_TIMEOUT_MS),
      });
      if (res.ok) return;
      if (res.status < 500) throw new Error(`pipeline rejected record (HTTP ${res.status})`);
      lastErr = new Error(`pipeline HTTP ${res.status}`);
    } catch (e) {
      lastErr = e;
    }
    if (attempt < PIPELINE_HTTP_RETRIES) {
      await new Promise((r) => setTimeout(r, PIPELINE_RETRY_BACKOFF_MS * 2 ** attempt));
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
}

// Publish ONE pending row, deleting it once the pipeline accepts the record.
// Failures increment `attempts` (capped at CHAT_HISTORY_MAX_ATTEMPTS; beyond
// the cap the row is left for manual inspection). Returns nothing — errors
// are logged and absorbed (this runs in the background).
async function publishPendingRow(env: Env, row: { id: number; chat_id: string; payload: string; attempts: number }): Promise<void> {
  if (!env.PIPELINE_CHAT_HISTORY_URL) return;
  try {
    await pipelinePost(env, env.PIPELINE_CHAT_HISTORY_URL, JSON.parse(row.payload), `chat:${row.chat_id}`);
    await env.SCHEMA_DB.prepare("DELETE FROM pending_chat_history WHERE id = ?1").bind(row.id).run();
  } catch (e) {
    console.error("pending chat-history publish failed", e);
    if (row.attempts < CHAT_HISTORY_MAX_ATTEMPTS) {
      await env.SCHEMA_DB.prepare(
        "UPDATE pending_chat_history SET attempts = attempts + 1 WHERE id = ?1",
      ).bind(row.id).run();
    }
  }
}

// Re-publish buffered pending_chat_history rows (oldest first). Never throws —
// run via ctx.waitUntil.
async function drainPendingChatHistory(env: Env): Promise<void> {
  if (!env.PIPELINE_CHAT_HISTORY_URL) return;
  try {
    const rows = await env.SCHEMA_DB.prepare(
      "SELECT id, chat_id, payload, attempts FROM pending_chat_history ORDER BY created_at ASC LIMIT ?1",
    ).bind(CHAT_HISTORY_DRAIN_BATCH).all<{ id: number; chat_id: string; payload: string; attempts: number }>();
    for (const row of rows.results ?? []) {
      await publishPendingRow(env, row);
    }
  } catch (e) {
    console.error("pending chat-history drain failed", e);
  }
}

/**
 * POST /api/chat/history — capture one completed chat turn into the lake.
 *
 * Durability-first: the normalized record is buffered in D1 (pending_chat_history)
 * BEFORE the response, then published to the pipeline in a background
 * waitUntil task (the pipeline POST can take ~1s — occasionally much longer —
 * and must never hold the browser's connection slot: the browser fires this
 * fire-and-forget and other API calls share Chromium's per-host connections).
 * A background failure leaves the row pending (attempts+1); a later
 * /api/chat/history call's drain re-publishes it, so no transcript is lost.
 */
async function saveChatHistory(env: Env, ctx: ExecutionContext, req: Request): Promise<Response> {
  let body: Record<string, unknown>;
  try {
    body = await req.json() as Record<string, unknown>;
  } catch {
    return json(env, { ok: false, error: "invalid JSON body" }, 400);
  }
  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    return json(env, { ok: false, error: "invalid JSON body" }, 400);
  }
  body = stripClientUserId(body);

  // Abuse-tracking metadata is captured server-side only — never trusted from
  // the client body. CF-Connecting-IP is set by Cloudflare on every request.
  // user_id is stamped from the session below, never from this body.
  const ip = (req.headers.get("CF-Connecting-IP") ?? "").slice(0, CHAT_HISTORY_MAX_AUX);
  const ua = (req.headers.get("User-Agent") ?? "").slice(0, CHAT_HISTORY_MAX_AUX);

  let record: Record<string, unknown>;
  try {
    record = normalizeChatHistoryRecord(body, ip, ua);
  } catch (e) {
    return json(env, { ok: false, error: String((e && (e as Error).message) || e) }, 400);
  }

  const user = await getSessionUser(env, req);
  if (user) {
    record.user_id = user.id;
    const chatId = String(record.chat_id);
    let messages: unknown = body.messages;
    try {
      messages = JSON.parse(String(record.messages));
    } catch {
      messages = body.messages;
    }
    // Instant clip title for the sidebar, then cheap-model headline + ticker NER.
    ctx.waitUntil((async () => {
      await touchUserChat(env.SCHEMA_DB, user.id, chatId, titleFromMessages(messages));
      if (!env.OPEN_ROUTER_KEY?.trim() || !env.COPILOT_MODEL?.trim()) return;
      try {
        const model = createCopilotModel(
          { OPEN_ROUTER_KEY: env.OPEN_ROUTER_KEY, COPILOT_MODEL: env.COPILOT_MODEL },
          new URL(req.url).origin,
        );
        const meta = await enrichChatMeta(env, chatId, messages, model);
        await applyGeneratedChatTitle(
          env.SCHEMA_DB,
          chatId,
          meta.title,
          firstUserContent(messages),
        );
      } catch (error) {
        console.warn("chat-meta enrich failed", error);
      }
    })());
  }

  if (!env.PIPELINE_CHAT_HISTORY_URL) {
    // Not configured (local dev without secrets): accept the turn silently so
    // history capture never breaks a chat.
    return json(env, { ok: true, stored: false }, 200);
  }

  let rowId: number;
  let payload: string;
  try {
    payload = JSON.stringify(record);
    const inserted = await env.SCHEMA_DB.prepare(
      "INSERT INTO pending_chat_history (chat_id, payload, attempts, created_at) VALUES (?1, ?2, 0, ?3)",
    ).bind(record.chat_id, payload, Date.now()).run();
    rowId = Number(inserted.meta.last_row_id);
  } catch (dbErr) {
    // D1 down — drop the record rather than hold the request (capture is
    // best-effort by design; a chat is never blocked by history persistence).
    console.error("pending chat-history buffer failed", dbErr);
    return json(env, { ok: true, stored: false, error: "buffer unavailable" }, 202);
  }

  // Background publish (new row first, then any backlog) — never in the path.
  ctx.waitUntil((async () => {
    await publishPendingRow(env, { id: rowId, chat_id: String(record.chat_id), payload, attempts: 0 });
    await drainPendingChatHistory(env);
  })());
  return json(env, { ok: true, stored: true });
}

/**
 * GET /api/admin/chat_history — newest-first transcripts (session admin or
 * Bearer ADMIN_TOKEN). Joins lake user_id → Better Auth profile when present;
 * anonymous rows get a visitor fingerprint from server-stamped IP + UA.
 */
async function adminChatHistory(
  env: Env,
  limitIn: number,
  beforeIn: string | null,
): Promise<{
  ok: boolean;
  limit: number;
  before: string | null;
  items: Awaited<ReturnType<typeof enrichAdminChatItems>>;
  next_before: string | null;
  as_of: string;
}> {
  const limit = clamp(limitIn || 100, 1, CHAT_HISTORY_ADMIN_LIMIT_MAX);
  const before = beforeIn && Number.isFinite(Date.parse(beforeIn)) ? beforeIn : null;
  // No cache key: rows land within seconds of a chat, so admin reads must be
  // live. `before` is an ISO fetched_at cursor (R2 SQL has no OFFSET).
  //
  // The lake is append-only and the stream does not dedupe on idempotency key:
  // a publish-success-but-timeout leaves a row pending and a later drain
  // re-publishes the SAME chat_id (one row per chat turn, so each turn's row
  // carries the full conversation so far). Collapse to the NEWEST row per
  // chat_id (the complete conversation) — the lake's standard latest-wins
  // QUALIFY pattern — so the admin view shows one transcript per chat.
  const where = before ? `WHERE fetched_at < ${lit(before)}` : "";
  const rows = await r2sql(env,
    `SELECT chat_id, mode, model, user_id, ip, user_agent, started_at, ended_at, messages, source, fetched_at ` +
    `FROM options.chat_history ${where} ` +
    `QUALIFY ROW_NUMBER() OVER (PARTITION BY chat_id ORDER BY fetched_at DESC, ended_at DESC) = 1 ` +
    `ORDER BY fetched_at DESC, ended_at DESC LIMIT ${limit}`);
  const raw = rows.map((r) => {
    let messages: unknown = null;
    try {
      messages = JSON.parse(String(r.messages ?? "null"));
    } catch {
      messages = null;
    }
    return {
      chat_id: String(r.chat_id),
      mode: String(r.mode),
      model: strOrNull(r.model),
      user_id: strOrNull(r.user_id),
      ip: strOrNull(r.ip),
      user_agent: strOrNull(r.user_agent),
      started_at: String(r.started_at),
      ended_at: String(r.ended_at),
      source: String(r.source),
      fetched_at: String(r.fetched_at),
      messages,
    };
  });
  const items = await enrichAdminChatItems(env.SCHEMA_DB, raw);
  const next_before = items.length > 0 ? items[items.length - 1].fetched_at : null;
  return { ok: true, limit, before, items, next_before, as_of: new Date().toISOString() };
}


// ---------------------------------------------------------------------------
// Copilot chat shares — public unlisted transcripts in D1
// ---------------------------------------------------------------------------
// POST /api/share/chat snapshots a conversation (the same ChatHistoryRecord
// shape the lake capture uses, normalized by the SAME pass-1 normalizer) into
// shared_chats (migration 0003) and returns /share/<share_id>. The share_id
// is base62 of 18 random bytes — high entropy makes the URL an implicit
// capability: anyone with the link can view, nobody can enumerate. Unlike the
// best-effort lake capture, a share is explicitly user-requested, so it
// either succeeds or fails loudly (502 on D1 failure, 413/429 on budget/rate)
// letting the user retry. Creating a share does not require auth (the id is
// the key); a session, when present, stamps share_owners so the author can
// opt the share onto GET /api/timeline. Reads are cached public, max-age=60
// via json().
//
// Row budgets. D1 caps a row at 2,000,000 bytes, and the lake strike caps are
// LOOSER than that allows (100 msg × 20k content + 20k sql → ~4 MB worst
// case). Pass 2 (normalizeShareRecord) applies share-only tightening (content
// ≤ 5,000 chars, sql ≤ 10,000 chars, ≤ 20 tool entries per message), then
// truncates OLDEST turns first when the serialized messages JSON exceeds 1.2
// MB of UTF-8 bytes, then verifies the assembled row stays under the 2 MB D1
// ceiling before INSERT. All sizes are UTF-8 BYTES (TextEncoder.byteLength) —
// JS string length counts UTF-16 units, and CJK/emoji expand 2–3× per unit,
// which would overflow the row silently.
//
// Abuse levers (no client fingerprinting — the site's privacy stance is "no
// login, no personal data"): created_ip/created_ua set server-side from the
// request headers and NEVER served; oversized raw bodies rejected (413) before
// JSON.parse; and a D1-backed per-IP rate check (429) that survives isolate
// recycling. An in-isolate Map is only a cheap first filter, never a
// stand-alone control.
const SHARE_RAW_BODY_MAX = 1_300_000;       // reject raw body before JSON.parse (413)
const SHARE_MESSAGES_MAX_BYTES = 1_200_000; // serialized messages JSON, UTF-8 bytes
const SHARE_ROW_MAX_BYTES = 2_000_000;      // D1 row ceiling (messages + source_sql + columns)
const SHARE_MAX_CONTENT = 5_000;            // chars — per message content
const SHARE_MAX_SQL = 10_000;               // chars — per message sql
const SHARE_MAX_REASONING = 20_000;         // chars — per message thinking trace
const SHARE_MAX_TOOLS = 20;                 // tool entries per message
const SHARE_MAX_TOOL_ARG = 2_000;           // chars — per tool args
const SHARE_MAX_TITLE = 120;                // chars — auto-derived title (see clipTitle)
const SHARE_MAX_RESULT_ROWS = 200;          // query rows snapshotted onto a share
const SHARE_MAX_RESULT_COLS = 40;
const SHARE_RATE_WINDOW_MS = 10 * 60_000;   // per-IP window
const SHARE_RATE_LIMIT = 20;                // shares per window per IP
const SHARE_ID_BYTES = 18;
const SHARE_ID_RE = /^[0-9A-Za-z]{1,48}$/;  // base62 slug; rejects junk lookups
const BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/** UTF-8 byte length of a string (D1 row/message caps are byte budgets). */
function utf8Bytes(s: string): number {
  return new TextEncoder().encode(s).byteLength;
}

/** base62 of N random bytes — the URL slug / implicit capability. */
function base62Encode(bytes: Uint8Array): string {
  let n = 0n;
  for (const b of bytes) n = (n << 8n) | BigInt(b);
  let out = "";
  while (n > 0n) {
    out = BASE62_ALPHABET[Number(n % 62n)] + out;
    n /= 62n;
  }
  return out || "0";
}

// In-isolate first filter only (per-IP recent-share timestamps). NOT a
// stand-alone control: it dies with the isolate and consecutive requests from
// one IP routinely land on different isolates — the D1 COUNT in createShare
// is the authoritative check.
const shareRateLocal = new Map<string, number[]>();
function shareRateHitLocal(ip: string): boolean {
  if (!ip) return false;
  const t = Date.now();
  if (shareRateLocal.size > 10_000) {
    // Sweep stale entries so a many-IP fan-out can't grow the map unboundedly.
    for (const [k, v] of shareRateLocal) {
      const alive = v.filter((ts) => t - ts < SHARE_RATE_WINDOW_MS);
      if (alive.length === 0) shareRateLocal.delete(k);
      else shareRateLocal.set(k, alive);
    }
  }
  const list = (shareRateLocal.get(ip) ?? []).filter((ts) => t - ts < SHARE_RATE_WINDOW_MS);
  shareRateLocal.set(ip, list);
  return list.length >= SHARE_RATE_LIMIT;
}
function shareRateRecordLocal(ip: string): void {
  if (!ip) return;
  const list = shareRateLocal.get(ip) ?? [];
  list.push(Date.now());
  shareRateLocal.set(ip, list);
}

function jsonCell(value: unknown): unknown {
  if (value == null) return value;
  const t = typeof value;
  if (t === "string" || t === "number" || t === "boolean") return value;
  return String(value);
}

function capShareResult(raw: unknown): Record<string, unknown> | undefined {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return undefined;
  const rec = raw as Record<string, unknown>;
  if (typeof rec.error === "string" && rec.error) return undefined;
  const columns = Array.isArray(rec.columns)
    ? rec.columns.filter((c): c is string => typeof c === "string" && c.length > 0).slice(0, SHARE_MAX_RESULT_COLS)
    : [];
  if (!columns.length) return undefined;
  const rawRows = Array.isArray(rec.rows) ? rec.rows : [];
  const rows = rawRows.slice(0, SHARE_MAX_RESULT_ROWS).flatMap((row) => {
    if (!row || typeof row !== "object" || Array.isArray(row)) return [];
    const src = row as Record<string, unknown>;
    const out: Record<string, unknown> = {};
    for (const col of columns) out[col] = jsonCell(src[col]);
    return [out];
  });
  const result: Record<string, unknown> = {
    columns,
    rows,
    row_count: typeof rec.row_count === "number" && Number.isFinite(rec.row_count) ? rec.row_count : rows.length,
  };
  if (rec.truncated === true) result.truncated = true;
  if (typeof rec.limit === "number" && Number.isFinite(rec.limit)) result.limit = rec.limit;
  return result;
}

function capShareChart(raw: unknown, columns?: string[]): ChartSpec | undefined {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return undefined;
  const rec = raw as Record<string, unknown>;
  const kind = rec.kind;
  if (kind !== "line" && kind !== "area" && kind !== "scatter" && kind !== "bar") return undefined;
  const x = str(rec.x)?.slice(0, 80);
  const y = str(rec.y)?.slice(0, 80);
  if (!x || !y) return undefined;
  const spec: ChartSpec = {
    kind,
    x,
    y,
    ...(typeof rec.series === "string" && rec.series ? { series: rec.series.slice(0, 80) } : {}),
    ...(typeof rec.title === "string" && rec.title ? { title: rec.title.slice(0, 120) } : {}),
    ...(typeof rec.xLabel === "string" && rec.xLabel ? { xLabel: rec.xLabel.slice(0, 80) } : {}),
    ...(typeof rec.yLabel === "string" && rec.yLabel ? { yLabel: rec.yLabel.slice(0, 80) } : {}),
  };
  if (columns?.length && !chartFitsResult(spec, columns)) return undefined;
  return spec;
}

function capShareDesk(raw: unknown): DeskBrief | undefined {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return undefined;
  const rec = raw as Record<string, unknown>;
  const desk = normalizeDeskBrief({
    fundamental: typeof rec.fundamental === "string" ? rec.fundamental : undefined,
    technical: typeof rec.technical === "string" ? rec.technical : undefined,
    options: typeof rec.options === "string" ? rec.options : undefined,
    risk: typeof rec.risk === "string" ? rec.risk : undefined,
    macro: typeof rec.macro === "string" ? rec.macro : undefined,
    overview: typeof rec.overview === "string" ? rec.overview : undefined,
  });
  return desk ?? undefined;
}

function capShareTrades(raw: unknown): SuggestedTrades | undefined {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return undefined;
  const trades = normalizeSuggestedTrades(raw as { trades?: unknown; skip_reason?: unknown });
  return trades ?? undefined;
}

/**
 * Pass 2 — share-only tightening on top of the lake normalizer's output.
 * The shipped caps (20k chars, no byte budget) are looser than the D1 row
 * allows, so every message gets the tighter per-field caps here, then the
 * serialized JSON is trimmed to the 1.2 MB byte budget (oldest turns first,
 * then older assistant sql). Returns the trimmed messages + the denormalized
 * source_sql (last assistant sql, the future-alerts keystone) + auto title
 * (first user question). Never throws.
 */
function normalizeShareRecord(pass1: Record<string, unknown>, rawMessages: unknown): {
  messages: Record<string, unknown>[];
  sourceSql: string | null;
  title: string | null;
} {
  const raw = Array.isArray(rawMessages) ? rawMessages : [];
  const messages = (JSON.parse(String(pass1.messages)) as unknown[]).map((m, index) => {
    const rec = m && typeof m === "object" ? (m as Record<string, unknown>) : {};
    const role = rec.role === "assistant" ? "assistant" : "user";
    const out: Record<string, unknown> = { role };
    if (typeof rec.content === "string" && rec.content) out.content = rec.content.slice(0, SHARE_MAX_CONTENT);
    if (typeof rec.sql === "string" && rec.sql) out.sql = rec.sql.slice(0, SHARE_MAX_SQL);
    if (typeof rec.ts === "number" && Number.isFinite(rec.ts)) out.ts = rec.ts;
    const original = raw[index] && typeof raw[index] === "object" && !Array.isArray(raw[index])
      ? raw[index] as Record<string, unknown>
      : {};
    // Reasoning may arrive on the lake-normalized message or only on the raw
    // client payload — keep whichever is present so share/timeline can show
    // the same Thinking disclosure as the live chat.
    const reasoningRaw =
      (typeof rec.reasoning === "string" && rec.reasoning)
      || (typeof original.reasoning === "string" && original.reasoning)
      || "";
    if (reasoningRaw) out.reasoning = reasoningRaw.slice(0, SHARE_MAX_REASONING);
    const result = capShareResult(original.result);
    if (result) out.result = result;
    const chart = capShareChart(original.chart, Array.isArray(result?.columns) ? result.columns as string[] : undefined);
    if (chart) out.chart = chart;
    const desk = capShareDesk(original.desk ?? rec.desk);
    if (desk) out.desk = desk;
    const trades = capShareTrades(original.trades ?? rec.trades);
    if (trades) out.trades = trades;
    if (Array.isArray(rec.tools)) {
      // The schema tolerates tools (future ToolRow capture); v1 client never
      // sends them, so this is a defensive cap, not a UI feature.
      const tools = rec.tools
        .slice(0, SHARE_MAX_TOOLS)
        .map((t) => {
          const tr = t && typeof t === "object" ? (t as Record<string, unknown>) : {};
          const name = str(tr.name)?.slice(0, 80) ?? "";
          if (!name) return null;
          const tool: Record<string, unknown> = { name };
          if (typeof tr.args === "string" && tr.args) tool.args = tr.args.slice(0, SHARE_MAX_TOOL_ARG);
          if (tr.ok === true || tr.ok === false) tool.ok = tr.ok;
          if (tr.summary != null && tr.summary !== "") tool.summary = str(tr.summary)!.slice(0, 500);
          return tool;
        })
        .filter((t): t is Record<string, unknown> => t !== null);
      if (tools.length) out.tools = tools;
    }
    return out;
  });

  // chatRecovery retries land as consecutive assistant rows — collapse before
  // the byte budget so /share does not show stacked empty bubbles.
  const coalesced = coalesceAssistantMessageRecords(messages);
  messages.length = 0;
  messages.push(...coalesced);

  const firstUser = messages.find((m) => m.role === "user" && typeof m.content === "string" && m.content);
  const title = typeof firstUser?.content === "string"
    ? clipTitle(firstUser.content, SHARE_MAX_TITLE)
    : null;

  // Byte budget: drop oldest turns first (a share is judged by its tail),
  // then older sql, then hard-backstop truncation. The tighter per-field caps
  // bound a single message far below the budget, so the loop is deterministic.
  const bytes = () => utf8Bytes(JSON.stringify(messages));
  while (messages.length > 1 && bytes() > SHARE_MESSAGES_MAX_BYTES) messages.shift();
  if (bytes() > SHARE_MESSAGES_MAX_BYTES) {
    for (const m of [...messages].reverse()) delete m.result;
  }
  if (bytes() > SHARE_MESSAGES_MAX_BYTES) {
    for (const m of [...messages].reverse()) delete m.chart;
  }
  if (bytes() > SHARE_MESSAGES_MAX_BYTES) {
    for (const m of [...messages].reverse()) delete m.desk;
  }
  if (bytes() > SHARE_MESSAGES_MAX_BYTES) {
    for (const m of [...messages].reverse()) delete m.trades;
  }
  if (bytes() > SHARE_MESSAGES_MAX_BYTES) {
    for (const m of [...messages].reverse()) delete m.sql; // newest last to lose sql
  }
  if (bytes() > SHARE_MESSAGES_MAX_BYTES) {
    for (const m of [...messages].reverse()) delete m.tools;
  }
  let guard = 0;
  while (bytes() > SHARE_MESSAGES_MAX_BYTES && messages.length > 0 && guard++ < 10_000) {
    const oldest = messages[0];
    const content = String(oldest.content ?? "");
    if (!content) {
      messages.shift(); // already empty — drop the turn
      continue;
    }
    oldest.content = content.slice(0, Math.floor(content.length / 2));
    if (!oldest.content) delete oldest.content;
  }

  const lastAssistant = [...messages].reverse().find(
    (m) => m.role === "assistant" && typeof m.sql === "string" && m.sql,
  );
  return { messages, sourceSql: lastAssistant ? String(lastAssistant.sql) : null, title };
}

/**
 * POST /api/share/chat — mint a public share for a conversation.
 * Body: the full ChatHistoryRecord (the lake capture shape — the pass-1
 * normalizer requires started_at/ended_at ISO timestamps + non-empty messages).
 */
async function createShare(env: Env, req: Request, ctx: ExecutionContext): Promise<Response> {
  // Oversized raw body → 413 before JSON.parse (bounds parse cost, keeps the
  // D1 row budget honest). Content-Length is byte-accurate when present; the
  // text() read re-checks actual UTF-8 byte length regardless.
  const contentLength = Number(req.headers.get("Content-Length") ?? "0");
  if (contentLength > SHARE_RAW_BODY_MAX) return json(env, { error: "payload too large" }, 413);
  let raw: string;
  try {
    raw = await req.text();
  } catch {
    return json(env, { error: "invalid JSON body" }, 400);
  }
  if (utf8Bytes(raw) > SHARE_RAW_BODY_MAX) return json(env, { error: "payload too large" }, 413);
  let body: Record<string, unknown>;
  try {
    body = JSON.parse(raw) as Record<string, unknown>;
  } catch {
    return json(env, { error: "invalid JSON body" }, 400);
  }
  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    return json(env, { error: "invalid JSON body" }, 400);
  }

  // Optional bot run link — admin only. Checked before rate-limit / mint so a
  // retry of an already-shared run returns the existing share (idempotent) and
  // does not consume the per-IP share budget.
  let linkedRunId: string | null = null;
  let botHandleFromRun: string | null = null;
  const runIdRaw = typeof body.run_id === "string" ? body.run_id.trim() : "";
  if (runIdRaw) {
    const admin = await requireBotAdmin(env, req);
    if (!admin.ok) return json(env, { error: "unauthorized" }, 401);
    const run = await getBotRun(env.SCHEMA_DB, runIdRaw);
    const decision = botShareReuseDecision(run);
    if (decision.action === "not_found") return json(env, { error: "run not found" }, 400);
    if (decision.action === "reuse") {
      const existing = await env.SCHEMA_DB.prepare(
        `SELECT share_id, bot_handle FROM shared_chats WHERE share_id = ?1`,
      ).bind(decision.share_id).first<{ share_id: string; bot_handle: string | null }>();
      if (existing) {
        return json(env, {
          share_id: existing.share_id,
          url: "/share/" + existing.share_id,
          can_publish: false,
          on_timeline: Boolean(existing.bot_handle),
          bot_handle: existing.bot_handle,
        });
      }
      // Orphan pointer (share row gone) — fall through and mint a fresh share.
    }
    linkedRunId = run!.run_id;
    botHandleFromRun = run!.handle;
  }

  // Abuse signals set server-side only — never accepted from the body (the
  // exact chat-history capture pattern). CF-Connecting-IP is set by Cloudflare.
  const ip = (req.headers.get("CF-Connecting-IP") ?? "").slice(0, CHAT_HISTORY_MAX_AUX);
  const ua = (req.headers.get("User-Agent") ?? "").slice(0, CHAT_HISTORY_MAX_AUX);

  // Pass 1 — the SHARED lake normalizer, verbatim (identical validation and
  // failure modes to the R2 capture: mode, ISO timestamps, roles, caps).
  let pass1: Record<string, unknown>;
  try {
    pass1 = normalizeChatHistoryRecord(body, ip, ua);
  } catch (e) {
    return json(env, { error: String((e && (e as Error).message) || e) }, 400);
  }

  // Rate check: cheap in-isolate filter first, then the D1-backed COUNT that
  // survives isolate recycling and spans colocations (served by
  // idx_shared_chats_ip). 429 past the threshold — the honest lever for bulk
  // share creation.
  if (shareRateHitLocal(ip)) return json(env, { error: "rate limited" }, 429);
  const recent = await env.SCHEMA_DB.prepare(
    "SELECT COUNT(*) AS n FROM shared_chats WHERE created_ip = ?1 AND created_at > ?2",
  ).bind(ip, Date.now() - SHARE_RATE_WINDOW_MS).first<{ n: number }>();
  if ((recent?.n ?? 0) >= SHARE_RATE_LIMIT) return json(env, { error: "rate limited" }, 429);

  // Pass 2 — share-only tightening + budgets.
  const { messages, sourceSql, title: clippedTitle } = normalizeShareRecord(pass1, body.messages);
  let title = clippedTitle;
  // Cheap-model headline + ticker NER before INSERT so the share lands with a
  // real title (clipTitle remains the fallback if OpenRouter is down).
  if (env.OPEN_ROUTER_KEY?.trim() && env.COPILOT_MODEL?.trim()) {
    try {
      const model = createCopilotModel(
        { OPEN_ROUTER_KEY: env.OPEN_ROUTER_KEY, COPILOT_MODEL: env.COPILOT_MODEL },
        new URL(req.url).origin,
      );
      const meta = await enrichChatMeta(env, String(pass1.chat_id), messages, model);
      if (meta.title) title = meta.title;
    } catch (error) {
      console.warn("share chat-meta enrich failed", error);
    }
  }
  const messagesJson = JSON.stringify(messages);
  // Assembled-row check: messages JSON + source_sql + column overhead must sit
  // under the D1 2 MB row ceiling — a share can never 500 on INSERT.
  const rowBytes = utf8Bytes(messagesJson) + utf8Bytes(sourceSql ?? "") + 512;
  if (rowBytes > SHARE_ROW_MAX_BYTES) return json(env, { error: "payload too large" }, 413);

  const shareId = base62Encode(crypto.getRandomValues(new Uint8Array(SHARE_ID_BYTES)));
  const now = Date.now();
  const user = await getSessionUser(env, req);

  // Optional bot attribution — only product admins (or ADMIN_TOKEN) may stamp
  // an enabled bot handle onto a share. Bot shares appear on the public timeline.
  // When run_id is present, the run's handle is the source of truth (body
  // bot_handle must match if also supplied).
  let botHandle: string | null = null;
  let botModerationReject: string | null = null;
  let pendingBotImprovement: {
    moderation: Awaited<ReturnType<typeof moderateTimelineShare>>;
    model: ReturnType<typeof createCopilotModel> | null;
    handle: string;
    publicOrigin: string;
  } | null = null;
  const botHandleRaw = typeof body.bot_handle === "string" ? body.bot_handle.trim().toLowerCase() : "";
  if (botHandleRaw || botHandleFromRun) {
    if (!runIdRaw) {
      const admin = await requireBotAdmin(env, req);
      if (!admin.ok) return json(env, { error: "unauthorized" }, 401);
    }
    if (botHandleRaw && botHandleFromRun && botHandleRaw !== botHandleFromRun) {
      return json(env, { error: "bot_handle does not match run" }, 400);
    }
    const wantHandle = botHandleRaw || botHandleFromRun!;
    const bot = await getBotProfile(env.SCHEMA_DB, wantHandle);
    if (!bot || !bot.enabled) return json(env, { error: "bot not found" }, 400);
    // Bot shares auto-list on the timeline — run the same quality gate as
    // human publish / headless mint before stamping bot_handle.
    const moderationModel = env.OPEN_ROUTER_KEY?.trim() && env.COPILOT_MODEL?.trim()
      ? createCopilotModel(
        { OPEN_ROUTER_KEY: env.OPEN_ROUTER_KEY, COPILOT_MODEL: env.COPILOT_MODEL },
        new URL(req.url).origin,
      )
      : null;
    const moderation = await moderateTimelineShare(messages, moderationModel);
    if (!moderation.allow) {
      console.info(JSON.stringify({
        timelineModeration: true,
        action: "reject_bot_create_share",
        bot_handle: bot.handle,
        source: moderation.source,
        reason: moderation.reason,
      }));
      // Mint unlisted (no bot_handle) so the run still has a capability URL;
      // withhold timeline listing.
      botHandle = null;
      botModerationReject = moderation.reason;
    } else {
      botHandle = bot.handle;
    }
    const requestOrigin = new URL(req.url).origin;
    pendingBotImprovement = {
      moderation,
      model: moderationModel,
      handle: bot.handle,
      publicOrigin: requestOrigin.includes("api-dev.")
        ? "https://dev.lobster.mp"
        : "https://lobster.mp",
    };
  }

  try {
    await env.SCHEMA_DB.prepare(
      `INSERT INTO shared_chats
         (share_id, chat_id, title, mode, model, messages, source_sql, created_ip, created_ua, created_at, updated_at, bot_handle, run_id)
       VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10, ?11, ?12)`,
    ).bind(
      shareId,
      String(pass1.chat_id),
      title,
      String(pass1.mode),
      pass1.model ? String(pass1.model) : null,
      messagesJson,
      sourceSql,
      ip || null,
      ua || null,
      now,
      botHandle,
      linkedRunId,
    ).run();
    if (pendingBotImprovement) {
      scheduleImprovementReport(
        env,
        pendingBotImprovement.model,
        {
          messages,
          decision: pendingBotImprovement.moderation,
          action: pendingBotImprovement.moderation.allow
            ? "allow_bot_create_share"
            : "reject_bot_create_share",
          shareId,
          runId: linkedRunId,
          botHandle: pendingBotImprovement.handle,
          publicOrigin: pendingBotImprovement.publicOrigin,
        },
        { waitUntil: (p) => ctx.waitUntil(p) },
      );
    }
    if (user && !botHandle && !botModerationReject) await recordShareOwner(env.SCHEMA_DB, shareId, user.id);
    if (linkedRunId) {
      if (botModerationReject) {
        await updateBotRun(env.SCHEMA_DB, linkedRunId, {
          status: "failed",
          share_id: shareId,
          error: `timeline quality: ${botModerationReject}`,
        });
      } else {
        await updateBotRun(env.SCHEMA_DB, linkedRunId, { status: "shared", share_id: shareId });
      }
    }
  } catch (dbErr) {
    // Concurrent share of the same run_id: unique index wins — return the
    // row that landed and keep bot_runs in sync.
    if (linkedRunId && String(dbErr).includes("UNIQUE")) {
      const byRun = await env.SCHEMA_DB.prepare(
        `SELECT share_id, bot_handle FROM shared_chats WHERE run_id = ?1`,
      ).bind(linkedRunId).first<{ share_id: string; bot_handle: string | null }>();
      if (byRun) {
        if (byRun.bot_handle) {
          await updateBotRun(env.SCHEMA_DB, linkedRunId, { status: "shared", share_id: byRun.share_id });
        } else if (botModerationReject) {
          await updateBotRun(env.SCHEMA_DB, linkedRunId, {
            status: "failed",
            share_id: byRun.share_id,
            error: `timeline quality: ${botModerationReject}`,
          });
        } else {
          await updateBotRun(env.SCHEMA_DB, linkedRunId, { status: "shared", share_id: byRun.share_id });
        }
        return json(env, {
          share_id: byRun.share_id,
          url: "/share/" + byRun.share_id,
          can_publish: false,
          on_timeline: Boolean(byRun.bot_handle),
          bot_handle: byRun.bot_handle,
          ...(byRun.bot_handle ? {} : botModerationReject
            ? { moderation_rejected: true, reason: botModerationReject }
            : {}),
        });
      }
    }
    // Explicitly user-requested → fail LOUDLY so they can retry (unlike the
    // best-effort lake capture, where D1-buffering absorbs write failures).
    console.error("share create failed", dbErr);
    return json(env, { error: "storage unavailable" }, 502);
  }
  shareRateRecordLocal(ip);
  const handle = user && !botHandle && !botModerationReject
    ? await getHandle(env.SCHEMA_DB, user.id)
    : null;
  return json(env, {
    share_id: shareId,
    url: "/share/" + shareId,
    can_publish: Boolean(handle),
    on_timeline: Boolean(botHandle),
    bot_handle: botHandle,
    ...(botModerationReject
      ? { moderation_rejected: true, reason: botModerationReject }
      : {}),
  });
}

/**
 * GET /api/share/:id — public read of a shared transcript. No auth (the
 * share_id is the capability); unknown or expired ids are indistinguishable
 * 404s. created_ip / created_ua are never selected — privacy by construction.
 */
async function getSharedChat(env: Env, shareId: string, ctx: ExecutionContext): Promise<Response> {
  if (!SHARE_ID_RE.test(shareId)) return json(env, { error: "not found" }, 404);
  const row = await env.SCHEMA_DB.prepare(
    `SELECT share_id, chat_id, title, mode, model, messages, source_sql, created_at, expires_at, bot_handle
     FROM shared_chats WHERE share_id = ?1`,
  ).bind(shareId).first<{
    share_id: string;
    chat_id: string;
    title: string | null;
    mode: string;
    model: string | null;
    messages: string;
    source_sql: string | null;
    created_at: number;
    expires_at: number | null;
    bot_handle: string | null;
  }>();
  if (!row) return json(env, { error: "not found" }, 404);
  if (row.expires_at && row.expires_at < Date.now()) return json(env, { error: "not found" }, 404);
  let messages: unknown = null;
  try {
    messages = JSON.parse(row.messages);
  } catch {
    /* rows are written by us; tolerate a corrupt one rather than 500 */
  }
  if (Array.isArray(messages)) {
    const healed = coalesceAssistantMessageRecords(messages);
    // Persist DSML / stacked-assistant heals so the timeline and next read
    // see structured desk content without re-parsing markup forever.
    if (JSON.stringify(healed) !== JSON.stringify(messages)) {
      const messagesJson = JSON.stringify(healed);
      ctx.waitUntil(
        env.SCHEMA_DB.prepare(
          `UPDATE shared_chats SET messages = ?1, updated_at = ?2 WHERE share_id = ?3`,
        ).bind(messagesJson, Date.now(), row.share_id).run().then(() => undefined).catch((error) => {
          console.warn("share DSML heal persist failed", error);
        }),
      );
    }
    messages = healed;
  }
  const linked = row.chat_id ? await listChatTickers(env.SCHEMA_DB, row.chat_id) : [];
  let tickers = [...new Set(linked.map((row) => row.ticker.trim().toUpperCase()).filter(Boolean))];
  // Older shares (human + bot) may still carry the prompt as title, or predate
  // NER tags. Enrich on read so /share and the timeline heal without a republish.
  if (
    row.chat_id
    && messages
    && shareNeedsMetaBackfill(row.title, messages)
    && env.OPEN_ROUTER_KEY?.trim()
    && env.COPILOT_MODEL?.trim()
  ) {
    try {
      const model = createCopilotModel(
        { OPEN_ROUTER_KEY: env.OPEN_ROUTER_KEY, COPILOT_MODEL: env.COPILOT_MODEL },
        "https://lobster.mp",
      );
      const meta = await backfillShareMeta(env, {
        chatId: row.chat_id,
        shareId: row.share_id,
        messages,
        storedTitle: row.title,
        model,
      });
      if (meta.title) row.title = meta.title;
      if (meta.tickers.length) {
        tickers = [...new Set([...tickers, ...meta.tickers.map((t) => t.trim().toUpperCase()).filter(Boolean)])];
      }
    } catch (error) {
      console.warn("share meta enrich on read failed", error);
    }
  }
  const author = await getTimelineAuthor(env.SCHEMA_DB, shareId);
  let bot: { handle: string; display_name: string; persona: string } | null = null;
  if (row.bot_handle) {
    const profile = await getBotProfile(env.SCHEMA_DB, row.bot_handle);
    if (profile) {
      bot = { handle: profile.handle, display_name: profile.display_name, persona: profile.persona };
    }
  }
  return json(env, {
    share_id: row.share_id,
    title: shareDisplayTitle(messages, row.title),
    mode: row.mode,
    model: row.model,
    created_at: row.created_at,
    messages,
    source_sql: row.source_sql,
    on_timeline: Boolean(author) || Boolean(bot),
    author,
    bot_handle: bot?.handle ?? null,
    bot,
    /** NER / research_ticker tags linked via chat_tickers. */
    tickers,
  });
}

export class CopilotAgent extends CopilotAgentBase<Env> {
  protected override loadSchema(): Promise<LakeTable[]> {
    return schemaTables(this.env, this.ctx, false);
  }

  protected override executeLakeQuery(sql: string, limit: number) {
    return runQuery(this.env, sql, limit);
  }

  protected override fetchNews(symbol: string, limit: number) {
    return news(this.env, symbol, limit);
  }

  protected override searchWeb(query: string, limit: number) {
    return webSearch(this.env, query, limit);
  }

  protected override fetchEconomicCalendar(days: number) {
    return econCalendar(this.env, days);
  }

  protected override researchTicker(symbol: string, opts?: { force?: boolean; chatId?: string }) {
    return researchTickerForAgent(this.env, symbol, opts);
  }
}

// ---------------------------------------------------------------------------
// Ticker research — lake + D1 brief for chat widget / route (OpenFIGI/Tavily off the HTTP path)
// ---------------------------------------------------------------------------

async function lakeSecurityLookup(env: Env, ticker: string): Promise<LakeSecurityRow | null> {
  try {
    const rows = await r2sql(
      env,
      `SELECT security_id, ticker, figi, composite_figi, isin, name, exchange, currency, sector FROM (` +
        `  SELECT security_id, ticker, figi, composite_figi, isin, name, exchange, currency, sector,` +
        `    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY fetched_at DESC, run_id DESC) rn` +
        `  FROM options.securities WHERE ticker = ${lit(ticker)}` +
        `) WHERE rn = 1`,
      "sec_" + ticker,
      QUERY_TTL_MS,
    );
    return rows[0] ?? null;
  } catch {
    return null;
  }
}

async function loadResearchEarnings(env: Env, ticker: string): Promise<EarningsBrief[]> {
  try {
    const rows = await r2sql(
      env,
      `SELECT earnings_date, time, fiscal_q, eps_forecast, last_year_eps, name FROM (` +
        `  SELECT earnings_date, time, fiscal_q, eps_forecast, last_year_eps, name,` +
        `    ROW_NUMBER() OVER (PARTITION BY symbol, earnings_date ORDER BY fetched_at DESC, run_id DESC) rn` +
        `  FROM options.earnings WHERE symbol = ${lit(ticker)}` +
        `) WHERE rn = 1 ORDER BY earnings_date DESC LIMIT 6`,
      "earn_" + ticker,
      QUERY_TTL_MS,
    );
    return rows.map((r) => ({
      earnings_date: String(r.earnings_date),
      time: strOrNull(r.time),
      fiscal_q: strOrNull(r.fiscal_q),
      eps_forecast: numOrNull(r.eps_forecast),
      last_year_eps: numOrNull(r.last_year_eps),
      name: strOrNull(r.name),
    }));
  } catch {
    return [];
  }
}

export interface SecFilingBrief {
  form_type: string;
  accession: string;
  filed_at: string;
  report_date: string | null;
  description: string | null;
  edgar_url: string;
  kind: "filing" | "prospectus" | string;
}

async function loadResearchFilings(
  env: Env,
  ticker: string,
  limit = 20,
): Promise<SecFilingBrief[]> {
  const lim = Math.max(1, Math.min(50, Math.floor(limit)));
  try {
    const rows = await r2sql(
      env,
      `SELECT form_type, accession, filed_at, report_date, description, edgar_url, kind FROM (` +
        `  SELECT form_type, accession, filed_at, report_date, description, edgar_url, kind,` +
        `    ROW_NUMBER() OVER (PARTITION BY accession ORDER BY fetched_at DESC, run_id DESC) rn` +
        `  FROM options.sec_filings WHERE ticker = ${lit(ticker)}` +
        `) WHERE rn = 1 ORDER BY filed_at DESC LIMIT ${lim}`,
      "filings_" + ticker + "_" + lim,
      QUERY_TTL_MS,
    );
    return rows.map((r) => ({
      form_type: String(r.form_type || ""),
      accession: String(r.accession || ""),
      filed_at: String(r.filed_at || ""),
      report_date: strOrNull(r.report_date),
      description: strOrNull(r.description),
      edgar_url: String(r.edgar_url || ""),
      kind: String(r.kind || "filing"),
    })).filter((r) => r.edgar_url && r.form_type);
  } catch {
    return [];
  }
}

async function loadResearchFundamentals(env: Env, ticker: string): Promise<FundamentalsBrief> {
  try {
    const rows = await r2sql(
      env,
      `SELECT market_cap, enterprise_value, trailing_pe, forward_pe, peg_ratio, price_to_book,` +
        `  total_debt, debt_to_equity, profit_margins, revenue_growth, source` +
        ` FROM options.fundamentals WHERE ticker = ${lit(ticker)}` +
        ` ORDER BY fetched_at DESC LIMIT 1`,
      "fund_" + ticker,
      QUERY_TTL_MS,
    );
    if (!rows.length) return emptyFundamentals();
    const r = rows[0];
    return {
      market_cap: numOrNull(r.market_cap),
      enterprise_value: numOrNull(r.enterprise_value),
      trailing_pe: numOrNull(r.trailing_pe),
      forward_pe: numOrNull(r.forward_pe),
      peg_ratio: numOrNull(r.peg_ratio),
      price_to_book: numOrNull(r.price_to_book),
      total_debt: numOrNull(r.total_debt),
      debt_to_equity: numOrNull(r.debt_to_equity),
      profit_margins: numOrNull(r.profit_margins),
      revenue_growth: numOrNull(r.revenue_growth),
      source: strOrNull(r.source),
    };
  } catch {
    return emptyFundamentals();
  }
}

async function loadResearchShorting(env: Env, ticker: string): Promise<ShortingBrief> {
  const empty = emptyShorting();
  try {
    const [siRows, rsRows] = await Promise.all([
      r2sql(
        env,
        `SELECT settlement_date, short_interest, short_interest_change_pct, days_to_cover, source` +
          ` FROM options.short_interest WHERE symbol = ${lit(ticker)}` +
          ` ORDER BY settlement_date DESC, fetched_at DESC LIMIT 1`,
        "si_" + ticker,
        QUERY_TTL_MS,
      ).catch(() => [] as Row[]),
      r2sql(
        env,
        `SELECT trade_date, short_ratio, source FROM options.reg_sho_daily WHERE symbol = ${lit(ticker)}` +
          ` ORDER BY trade_date DESC, fetched_at DESC LIMIT 1`,
        "regsho_" + ticker,
        QUERY_TTL_MS,
      ).catch(() => [] as Row[]),
    ]);
    const si = siRows[0];
    const rs = rsRows[0];
    if (!si && !rs) return empty;
    return {
      settlement_date: si ? strOrNull(si.settlement_date) : null,
      short_interest: si ? numOrNull(si.short_interest) : null,
      short_interest_change_pct: si ? numOrNull(si.short_interest_change_pct) : null,
      days_to_cover: si ? numOrNull(si.days_to_cover) : null,
      short_ratio: rs ? numOrNull(rs.short_ratio) : null,
      short_ratio_date: rs ? strOrNull(rs.trade_date) : null,
      source: strOrNull(si?.source ?? rs?.source) ?? "finra",
    };
  } catch {
    return empty;
  }
}

async function loadResearchOhlc(env: Env, ticker: string): Promise<OhlcBar[]> {
  try {
    // Bound the scan: technicals need ~90 sessions (~SMA50). Do not window
    // the full OHLC history — that is what made a cold brief miss multi-second.
    const since = new Date(Date.now() - 200 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const rows = await r2sql(
      env,
      `SELECT date, open, high, low, close, volume FROM (` +
        `  SELECT date, open, high, low, close, volume,` +
        `    ROW_NUMBER() OVER (PARTITION BY date ORDER BY fetched_at DESC, run_id DESC) rn` +
        `  FROM options.ohlc WHERE symbol = ${lit(ticker)} AND date >= ${lit(since)} AND close IS NOT NULL` +
        `) WHERE rn = 1 ORDER BY date DESC LIMIT ${RESEARCH_OHLC_LIMIT}`,
      "research_ohlc_v3_" + ticker,
      QUERY_TTL_MS,
    );
    return (rows as Row[]).map((r) => ({
      date: String(r.date),
      open: numOrNull(r.open),
      high: numOrNull(r.high),
      low: numOrNull(r.low),
      close: numOrNull(r.close),
      volume: numOrNull(r.volume),
    })).reverse();
  } catch {
    return [];
  }
}

async function loadResearchRealizedVol(env: Env, ticker: string): Promise<RealizedVolBrief | null> {
  try {
    const rows = await r2sql(
      env,
      `SELECT as_of_date, realized_vol_30d, realized_vol_90d FROM (` +
        `  SELECT as_of_date, realized_vol_30d, realized_vol_90d,` +
        `    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY fetched_at DESC, run_id DESC) rn` +
        `  FROM options.realized_vol WHERE symbol = ${lit(ticker)}` +
        `) WHERE rn = 1`,
      "research_rv_" + ticker,
      QUERY_TTL_MS,
    );
    if (!rows.length) return null;
    return {
      as_of_date: String(rows[0].as_of_date),
      realized_vol_30d: numOrNull(rows[0].realized_vol_30d),
      realized_vol_90d: numOrNull(rows[0].realized_vol_90d),
    };
  } catch {
    return null;
  }
}

async function loadResearchEtfProfile(env: Env, ticker: string): Promise<TickerResearch["etf"]> {
  try {
    const [profileRows, holdingRows] = await Promise.all([
      r2sql(
        env,
        `SELECT name, family, category, asset_class, net_assets, expense_ratio,` +
          ` net_expense_ratio, trailing_yield, inception_date FROM (` +
          `  SELECT name, family, category, asset_class, net_assets, expense_ratio,` +
          `    net_expense_ratio, trailing_yield, inception_date,` +
          `    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY fetched_at DESC, run_id DESC) rn` +
          `  FROM options.etf_profiles WHERE ticker = ${lit(ticker)}` +
          `) WHERE rn = 1`,
        "research_etf_" + ticker,
        QUERY_TTL_MS,
      ),
      r2sql(
        env,
        `SELECT rank, holding_symbol, holding_name, weight FROM (` +
          `  SELECT rank, holding_symbol, holding_name, weight,` +
          `    ROW_NUMBER() OVER (PARTITION BY ticker, rank ORDER BY fetched_at DESC, run_id DESC) rn` +
          `  FROM options.etf_holdings WHERE ticker = ${lit(ticker)}` +
          `) WHERE rn = 1 ORDER BY rank LIMIT 15`,
        "research_etfh_" + ticker,
        QUERY_TTL_MS,
      ),
    ]);
    if (!profileRows.length && !holdingRows.length) return null;
    const p = profileRows[0];
    return {
      name: p ? strOrNull(p.name) : null,
      family: p ? strOrNull(p.family) : null,
      category: p ? strOrNull(p.category) : null,
      asset_class: p ? strOrNull(p.asset_class) : null,
      net_assets: p ? numOrNull(p.net_assets) : null,
      expense_ratio: p ? numOrNull(p.expense_ratio) : null,
      net_expense_ratio: p ? numOrNull(p.net_expense_ratio) : null,
      trailing_yield: p ? numOrNull(p.trailing_yield) : null,
      inception_date: p ? strOrNull(p.inception_date) : null,
      holdings: (holdingRows as Row[]).map((r) => ({
        rank: numOrNull(r.rank),
        holding_symbol: strOrNull(r.holding_symbol),
        holding_name: strOrNull(r.holding_name),
        weight: numOrNull(r.weight),
      })),
    };
  } catch {
    return null;
  }
}

function researchDepsFor(env: Env): ResearchDeps {
  // Slim lake reads for the brief — not enrichSymbol (that also pulls
  // corporate_actions + etf_holdings + 260 OHLC bars for the chart path).
  return {
    lakeLookup: (ticker) => lakeSecurityLookup(env, ticker),
    loadOhlc: (ticker) => loadResearchOhlc(env, ticker),
    loadRealizedVol: (ticker) => loadResearchRealizedVol(env, ticker),
    loadEarnings: (ticker) => loadResearchEarnings(env, ticker),
    loadNews: async (ticker, limit) => {
      const result = await news(env, ticker, limit);
      return {
        items: result.items.map((item) => ({ title: item.title, link: item.link })),
        error: result.error,
      };
    },
    loadEtfProfile: (ticker) => loadResearchEtfProfile(env, ticker),
    loadFundamentals: (ticker) => loadResearchFundamentals(env, ticker),
    loadShorting: (ticker) => loadResearchShorting(env, ticker),
  };
}

async function researchTickerForAgent(
  env: Env,
  symbol: string,
  opts?: { force?: boolean; chatId?: string },
): Promise<{ research?: TickerResearch; summary: string; error?: string; enrolled?: boolean }> {
  try {
    const research = sanitizeResearchCommentary(
      await getOrComputeResearch(env, symbol, researchDepsFor(env), {
        force: opts?.force,
        chatId: opts?.chatId,
        includeNews: true,
        includeSecondary: true,
      }),
    );
    maybeEnrollMissingTicker(env, research, {
      source: "copilot_research",
      requestedBy: opts?.chatId || null,
    });
    let summary = summarizeResearch(research);
    if (research.price.spot == null && isEnrollableEquityTicker(research.identity.ticker)) {
      summary +=
        "\n\nLake data is thin for this ticker — it has been queued for on-demand " +
        "ingest (options chain, OHLC, fundamentals). Retry research shortly.";
    }
    return { research, summary };
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    return { summary: `Research failed: ${message}`, error: message };
  }
}

async function handleResearchGet(env: Env, req: Request, tickerRaw: string, ctx: ExecutionContext): Promise<Response> {
  const ticker = parseTickerParam(tickerRaw);
  if (!ticker) return json(env, { error: "invalid ticker" }, 400, "private");
  const url = new URL(req.url);
  const force = url.searchParams.get("force") === "1" || url.searchParams.get("force") === "true";
  const chatId = url.searchParams.get("chat_id")?.trim() || undefined;
  const started = Date.now();
  try {
    // Secondary lake reads (RV, earnings, ETF profile + holdings) are on for the
    // ticker detail page — ETF fund stats / components are first-class there.
    const research = sanitizeResearchCommentary(
      await getOrComputeResearch(env, ticker, researchDepsFor(env), {
        force,
        chatId,
        includeNews: false,
        includeSecondary: true,
        liveFigi: false,
        waitUntil: (p) => ctx.waitUntil(p),
      }),
    );
    maybeEnrollMissingTicker(env, research, {
      source: "research_api",
      requestedBy: chatId || null,
      waitUntil: (p) => ctx.waitUntil(p),
    });
    const dur = Date.now() - started;
    return new Response(JSON.stringify(research), {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": force ? "private, no-store" : "private, max-age=30, stale-while-revalidate=300",
        "Server-Timing": `app;dur=${dur};desc="${research.cache_hit ? "cache" : "compute"}"`,
      },
    });
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    return json(env, { error: message }, 502, "private");
  }
}

async function handleResearchFilingsGet(env: Env, req: Request, tickerRaw: string): Promise<Response> {
  const ticker = parseTickerParam(tickerRaw);
  if (!ticker) return json(env, { error: "invalid ticker" }, 400, "private");
  const url = new URL(req.url);
  const limit = num(url.searchParams.get("limit") ?? 20);
  const items = await loadResearchFilings(env, ticker, limit);
  return json(
    env,
    { ticker, items, count: items.length },
    200,
    "private",
  );
}

/** Cap warm batch size so a single admin/loader call cannot OOM the isolate. */
const RESEARCH_WARM_MAX_TICKERS = 40;

/**
 * POST /api/research/warm (Bearer ADMIN_TOKEN) — force-recompute briefs into D1
 * for a ticker batch. Used by the loader `research-briefs-daily` job so
 * /research/{ticker} serves from D1 instead of a cold R2 SQL fan-out.
 */
async function handleResearchWarm(env: Env, req: Request): Promise<Response> {
  if (!adminAuthorized(req, env)) return json(env, { error: "unauthorized" }, 401, "private");
  let body: { tickers?: unknown; concurrency?: unknown } = {};
  try {
    body = (await req.json()) as typeof body;
  } catch {
    return json(env, { error: "invalid JSON body" }, 400, "private");
  }
  if (!Array.isArray(body.tickers) || body.tickers.length === 0) {
    return json(env, { error: "tickers must be a non-empty string array" }, 400, "private");
  }
  if (body.tickers.length > RESEARCH_WARM_MAX_TICKERS) {
    return json(
      env,
      { error: `tickers capped at ${RESEARCH_WARM_MAX_TICKERS} per request` },
      400,
      "private",
    );
  }
  const tickers = body.tickers.map((t) => String(t));
  const concurrency =
    typeof body.concurrency === "number" && Number.isFinite(body.concurrency)
      ? Math.floor(body.concurrency)
      : undefined;
  const started = Date.now();
  try {
    const summary = await warmResearchTickers(env, tickers, researchDepsFor(env), {
      concurrency,
      includeNews: false,
      includeSecondary: true,
      liveFigi: false,
    });
    return json(
      env,
      { ...summary, duration_ms: Date.now() - started },
      200,
      "private",
    );
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    return json(env, { error: message }, 502, "private");
  }
}

/**
 * POST /api/symbols/enroll (Bearer ADMIN_TOKEN) — enroll a ticker into the
 * loader's durable on-demand universe so ETL keeps refreshing it. Proxies to
 * loader POST /symbols/enroll (requires LOADER_TOKEN on this Worker).
 */
async function handleSymbolsEnroll(
  env: Env,
  req: Request,
  ctx: ExecutionContext,
): Promise<Response> {
  if (!adminAuthorized(req, env)) return json(env, { error: "unauthorized" }, 401, "private");
  let body: { symbol?: unknown; source?: unknown; notes?: unknown; load_now?: unknown } = {};
  try {
    body = (await req.json()) as typeof body;
  } catch {
    return json(env, { error: "invalid JSON body" }, 400, "private");
  }
  const raw = typeof body.symbol === "string" ? body.symbol : "";
  if (!isEnrollableEquityTicker(raw)) {
    return json(env, { error: `invalid enrollable ticker: ${raw || "(empty)"}` }, 400, "private");
  }
  if (!(env.LOADER_TOKEN || "").trim()) {
    return json(env, { error: "LOADER_TOKEN is not configured on the API Worker" }, 503, "private");
  }
  const result = await enrollTickerWithLoader(env, raw, {
    source: typeof body.source === "string" ? body.source : "admin",
    notes: typeof body.notes === "string" ? body.notes : null,
    loadNow: body.load_now !== false,
  });
  if (!result) {
    return json(env, { error: "enroll unavailable" }, 503, "private");
  }
  if (result.error) {
    return json(env, result, 502, "private");
  }
  // Arm is handled by the loader; nothing else to wait on here.
  ctx.waitUntil(Promise.resolve());
  return json(env, result, 200, "private");
}

async function handleResearchCommentaryGet(env: Env, req: Request, tickerRaw: string): Promise<Response> {
  const ticker = parseTickerParam(tickerRaw);
  if (!ticker) return json(env, { error: "invalid ticker" }, 400, "private");
  const url = new URL(req.url);
  const force = url.searchParams.get("force") === "1" || url.searchParams.get("force") === "true";
  try {
    const origin = new URL(req.url).origin;
    const commentary = await getOrComputeCommentary(env, ticker, {
      ...researchDepsFor(env),
      createModel: () => {
        if (!env.OPEN_ROUTER_KEY?.trim() || !env.COPILOT_MODEL?.trim()) return null;
        return createCopilotModel(
          { OPEN_ROUTER_KEY: env.OPEN_ROUTER_KEY, COPILOT_MODEL: env.COPILOT_MODEL },
          origin,
        );
      },
    }, { force });
    return json(env, commentary, 200, "private");
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    return json(env, { error: message }, 502, "private");
  }
}


// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Continuous-loader pass-through (read-only /loop endpoints)
// ---------------------------------------------------------------------------
const LOADER_BASE_DEFAULT = "https://cboe-to-r2.robertlancer.workers.dev";
// Short in-isolate cache so the UI's polling doesn't hammer the loader DO.
const LOADER_TTL_MS = 5 * 1000;

// Forwards a read-only /loop/* request to the continuous loader worker. The
// loader only Bearer-guards /loop/trigger and /loop/arm, so /loop/status and
// /loop/symbols are safe to proxy without a token.
async function loaderGet(env: Env, path: string, cacheKey: string): Promise<unknown> {
  const base = (env.LOADER_BASE_URL || LOADER_BASE_DEFAULT).replace(/\/+$/, "");
  return cached<unknown>(cacheKey, LOADER_TTL_MS, async () => {
    const r = await fetch(base + path);
    if (!r.ok) throw new Error(`loader ${r.status}: ${(await r.text()).slice(0, 200)}`);
    return r.json();
  });
}

function withCors(env: Env, req: Request, resp: Response): Response {
  const headers = new Headers(resp.headers);
  // `new Headers(resp.headers)` can drop Set-Cookie in some runtimes. The OAuth
  // callback sets the session cookie here; without it, Google returns the user
  // to Pages still signed out.
  if (headers.getSetCookie().length === 0) {
    for (const cookie of resp.headers.getSetCookie()) headers.append("Set-Cookie", cookie);
  }
  const origin = req.headers.get("Origin") ?? "";
  if (origin && isTrustedOrigin(origin)) {
    headers.set("Access-Control-Allow-Origin", origin);
    headers.set("Access-Control-Allow-Credentials", "true");
    headers.append("Vary", "Origin");
  } else {
    headers.set("Access-Control-Allow-Origin", env.CORS_ORIGIN ?? "*");
  }
  headers.set("Access-Control-Allow-Methods", "GET,POST,PATCH,DELETE,OPTIONS");
  // Agent HTTP responses have immutable headers, so clone rather than mutating in place.
  headers.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  return new Response(resp.body, { status: resp.status, statusText: resp.statusText, headers });
}

function json(_env: Env, data: unknown, status = 200, cache: "public" | "private" = "public"): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": cache === "private" ? "private, no-store" : "public, max-age=60",
    },
  });
}

async function requireUser(env: Env, req: Request): Promise<SessionUser | Response> {
  const user = await getSessionUser(env, req);
  if (!user) return json(env, { error: "unauthorized" }, 401, "private");
  return user;
}

async function handleMe(env: Env, req: Request, path: string): Promise<Response | null> {
  if (path === "/api/me/avatar") {
    const user = await requireUser(env, req);
    if (user instanceof Response) return user;
    if (req.method === "POST") {
      const contentType = req.headers.get("Content-Type") || "";
      let bytes: ArrayBuffer;
      let mime: string;
      try {
        if (contentType.includes("multipart/form-data")) {
          const form = await req.formData();
          const file = form.get("avatar") ?? form.get("file");
          if (!(file instanceof File)) {
            return json(env, { error: "avatar file is required (field name: avatar)" }, 400, "private");
          }
          if (file.size > AVATAR_MAX_BYTES) {
            return json(env, { error: "avatar must be 2 MB or smaller" }, 413, "private");
          }
          bytes = await file.arrayBuffer();
          mime = file.type || "application/octet-stream";
        } else {
          const len = Number(req.headers.get("Content-Length") || 0);
          if (len > AVATAR_MAX_BYTES) {
            return json(env, { error: "avatar must be 2 MB or smaller" }, 413, "private");
          }
          bytes = await req.arrayBuffer();
          mime = contentType || "application/octet-stream";
        }
      } catch {
        return json(env, { error: "could not read avatar upload" }, 400, "private");
      }
      const result = await putAvatar(env, user, bytes, mime);
      if (!result.ok) return json(env, { error: result.error }, result.status, "private");
      const row = await getUserProfile(env.SCHEMA_DB, user.id);
      const pub = profilePublicFields(user.id, row, user.name);
      return json(env, { ok: true, ...pub }, 200, "private");
    }
    if (req.method === "DELETE") {
      const result = await clearAvatar(env, user);
      if (!result.ok) return json(env, { error: result.error }, result.status, "private");
      const row = await getUserProfile(env.SCHEMA_DB, user.id);
      const pub = profilePublicFields(user.id, row, user.name);
      return json(env, { ok: true, ...pub }, 200, "private");
    }
    return json(env, { error: "method not allowed" }, 405, "private");
  }

  if (path !== "/api/me") return null;
  const user = await requireUser(env, req);
  if (user instanceof Response) return user;
  if (req.method === "GET") {
    const row = await getUserProfile(env.SCHEMA_DB, user.id);
    const pub = profilePublicFields(user.id, row, user.name);
    return json(env, {
      ok: true,
      id: user.id,
      name: pub.name,
      email: user.email,
      image: user.image ?? null,
      handle: row?.handle ?? null,
      display_name: pub.display_name,
      avatar_url: pub.avatar_url,
      suggested_handle: row?.handle ? null : suggestHandle(user.email, user.name),
      is_admin: isAdminEmail(user.email),
    }, 200, "private");
  }
  if (req.method === "PATCH") {
    let body: Record<string, unknown>;
    try {
      body = await req.json() as Record<string, unknown>;
    } catch {
      return json(env, { error: "invalid JSON body" }, 400, "private");
    }
    const result = await updateProfile(env.SCHEMA_DB, user.id, body, user.name);
    if (!result.ok) return json(env, { error: result.error }, result.status, "private");
    return json(env, {
      ok: true,
      handle: result.handle,
      display_name: result.display_name,
      avatar_url: result.avatar_url,
      name: result.name,
    }, 200, "private");
  }
  return json(env, { error: "method not allowed" }, 405, "private");
}

async function handleAvatarGet(env: Env, req: Request, path: string): Promise<Response | null> {
  const match = path.match(/^\/api\/avatars\/([^/]+)$/);
  if (!match) return null;
  if (req.method !== "GET" && req.method !== "HEAD") {
    return json(env, { error: "method not allowed" }, 405, "private");
  }
  return serveAvatar(env, decodeURIComponent(match[1]));
}

/**
 * Bot profiles — public read of enabled bots; admin CRUD + generate trigger.
 *
 * Generate returns a fresh chat_id + a prompt that has not already been used
 * on a prior run for that bot (unused seed, or LLM invent). The admin UI opens
 * Copilot as that persona and auto-sends. Sharing with bot_handle stamps the
 * public timeline attribution.
 */
async function handleBots(env: Env, req: Request, path: string, ctx: ExecutionContext): Promise<Response | null> {
  if (path === "/api/bots" && req.method === "GET") {
    const items = await listBotProfiles(env.SCHEMA_DB, { enabledOnly: true });
    return json(env, {
      items: items.map((bot) => ({
        handle: bot.handle,
        display_name: bot.display_name,
        persona: bot.persona,
        bio: bot.bio,
      })),
    });
  }

  const publicOne = path.match(/^\/api\/bots\/([^/]+)$/);
  if (publicOne && req.method === "GET") {
    const bot = await getBotProfile(env.SCHEMA_DB, decodeURIComponent(publicOne[1]));
    if (!bot || !bot.enabled) return json(env, { error: "not found" }, 404);
    return json(env, {
      handle: bot.handle,
      display_name: bot.display_name,
      persona: bot.persona,
      bio: bot.bio,
    });
  }

  if (path === "/api/admin/bots" && req.method === "GET") {
    const admin = await requireBotAdmin(env, req);
    if (!admin.ok) return json(env, { error: admin.error }, admin.status, "private");
    return json(env, { items: await listBotProfiles(env.SCHEMA_DB) }, 200, "private");
  }

  if (path === "/api/admin/copilot/capabilities" && req.method === "GET") {
    const admin = await requireBotAdmin(env, req);
    if (!admin.ok) return json(env, { error: admin.error }, admin.status, "private");
    const q = new URL(req.url).searchParams;
    const schemaMode = (q.get("schema") ?? "live").toLowerCase();
    const includeSamples = q.get("samples") === "1" || q.get("samples") === "true";
    const tables = schemaMode === "placeholder"
      ? []
      : await schemaTables(env, ctx, false);
    return json(
      env,
      describeCopilotCapabilities({
        tables,
        includeSamples,
      }),
      200,
      "private",
    );
  }

  if (path === "/api/admin/bots" && req.method === "POST") {
    const admin = await requireBotAdmin(env, req);
    if (!admin.ok) return json(env, { error: admin.error }, admin.status, "private");
    let body: Record<string, unknown>;
    try {
      body = await req.json() as Record<string, unknown>;
    } catch {
      return json(env, { error: "invalid JSON body" }, 400, "private");
    }
    const result = await createBotProfile(env.SCHEMA_DB, body);
    if (!result.ok) return json(env, { error: result.error }, result.status, "private");
    return json(env, { ok: true, bot: result.profile }, 201, "private");
  }

  const adminOne = path.match(/^\/api\/admin\/bots\/([^/]+)$/);
  if (adminOne) {
    const handle = decodeURIComponent(adminOne[1]);
    const admin = await requireBotAdmin(env, req);
    if (!admin.ok) return json(env, { error: admin.error }, admin.status, "private");
    if (req.method === "GET") {
      const bot = await getBotProfile(env.SCHEMA_DB, handle);
      if (!bot) return json(env, { error: "not found" }, 404, "private");
      const runs = await listBotRuns(env.SCHEMA_DB, bot.handle, 20);
      const schedule = await getBotSchedule(env.SCHEMA_DB, bot.handle);
      return json(env, { bot, runs, schedule }, 200, "private");
    }
    if (req.method === "PUT" || req.method === "PATCH") {
      let body: Record<string, unknown>;
      try {
        body = await req.json() as Record<string, unknown>;
      } catch {
        return json(env, { error: "invalid JSON body" }, 400, "private");
      }
      const result = await updateBotProfile(env.SCHEMA_DB, handle, body);
      if (!result.ok) return json(env, { error: result.error }, result.status, "private");
      return json(env, { ok: true, bot: result.profile }, 200, "private");
    }
    if (req.method === "DELETE") {
      const result = await deleteBotProfile(env.SCHEMA_DB, handle);
      if (!result.ok) return json(env, { error: result.error }, result.status, "private");
      return json(env, { ok: true }, 200, "private");
    }
    return json(env, { error: "method not allowed" }, 405, "private");
  }

  const generate = path.match(/^\/api\/admin\/bots\/([^/]+)\/generate$/);
  if (generate && req.method === "POST") {
    const admin = await requireBotAdmin(env, req);
    if (!admin.ok) return json(env, { error: admin.error }, admin.status, "private");
    const bot = await getBotProfile(env.SCHEMA_DB, decodeURIComponent(generate[1]));
    if (!bot) return json(env, { error: "not found" }, 404, "private");
    if (!bot.enabled) return json(env, { error: "bot is disabled" }, 400, "private");
    let body: Record<string, unknown> = {};
    try {
      const text = await req.text();
      if (text.trim()) body = JSON.parse(text) as Record<string, unknown>;
    } catch {
      return json(env, { error: "invalid JSON body" }, 400, "private");
    }
    const promptRaw = typeof body.prompt === "string" ? body.prompt.trim() : "";
    const usedPrompts = await listBotRunPrompts(env.SCHEMA_DB, bot.handle, 100);
    const resolved = resolveBotGeneratePrompt(promptRaw || undefined, bot.seed_prompts, usedPrompts);
    let prompt = resolved.prompt;
    let prompt_source: "requested" | "seed" | "invent" = resolved.source === "invent" ? "invent" : resolved.source;
    if (!prompt) {
      if (!env.OPEN_ROUTER_KEY?.trim() || !env.COPILOT_MODEL?.trim()) {
        return json(env, {
          error: "all seed prompts were already used — add a new seed, pass a unique prompt, or configure Copilot to invent one",
        }, 400, "private");
      }
      const origin = new URL(req.url).origin;
      const model = createCopilotModel(
        { OPEN_ROUTER_KEY: env.OPEN_ROUTER_KEY, COPILOT_MODEL: env.COPILOT_MODEL },
        origin,
      );
      const invented = await inventBotPrompt(bot, usedPrompts, model);
      if (!invented) {
        return json(env, {
          error: "could not invent a unique prompt different from prior chats — try again or pass a new prompt",
        }, 502, "private");
      }
      prompt = invented;
      prompt_source = "invent";
    }
    const chatId = crypto.randomUUID();
    const run = await createBotRun(env.SCHEMA_DB, bot.handle, chatId, prompt);
    await updateBotRun(env.SCHEMA_DB, run.run_id, { status: "running" });
    return json(env, {
      ok: true,
      run_id: run.run_id,
      chat_id: chatId,
      prompt,
      prompt_source,
      bot: {
        handle: bot.handle,
        display_name: bot.display_name,
        persona: bot.persona,
        system_prompt_extra: bot.system_prompt_extra,
        model: bot.model,
        reasoning_effort: bot.reasoning_effort,
      },
      chat_url: `/chat/${chatId}`,
    }, 200, "private");
  }

  const schedulePath = path.match(/^\/api\/admin\/bots\/([^/]+)\/schedule$/);
  if (schedulePath) {
    const admin = await requireBotAdmin(env, req);
    if (!admin.ok) return json(env, { error: admin.error }, admin.status, "private");
    const handle = decodeURIComponent(schedulePath[1]);
    const bot = await getBotProfile(env.SCHEMA_DB, handle);
    if (!bot) return json(env, { error: "not found" }, 404, "private");
    if (req.method === "GET") {
      const schedule = await getBotSchedule(env.SCHEMA_DB, bot.handle);
      return json(env, { schedule }, 200, "private");
    }
    if (req.method === "PUT" || req.method === "PATCH") {
      let body: Record<string, unknown>;
      try {
        body = await req.json() as Record<string, unknown>;
      } catch {
        return json(env, { error: "invalid JSON body" }, 400, "private");
      }
      const result = await upsertBotSchedule(env.SCHEMA_DB, bot.handle, body, env);
      if (!result.ok) return json(env, { error: result.error }, result.status, "private");
      return json(env, { ok: true, schedule: result.schedule }, 200, "private");
    }
    if (req.method === "DELETE") {
      await deleteBotSchedule(env.SCHEMA_DB, bot.handle);
      return json(env, { ok: true }, 200, "private");
    }
    return json(env, { error: "method not allowed" }, 405, "private");
  }

  const scheduleTrigger = path.match(/^\/api\/admin\/bots\/([^/]+)\/schedule\/trigger$/);
  if (scheduleTrigger && req.method === "POST") {
    const admin = await requireBotAdmin(env, req);
    if (!admin.ok) return json(env, { error: admin.error }, admin.status, "private");
    const handle = decodeURIComponent(scheduleTrigger[1]);
    const schedule = await getBotSchedule(env.SCHEMA_DB, handle);
    if (!schedule) return json(env, { error: "schedule not found" }, 404, "private");
    const force = new URL(req.url).searchParams.get("force") === "1";
    const outcome = await runOneBotSchedule(env, schedule, {
      force,
      waitUntil: (p) => ctx.waitUntil(p),
    });
    if (outcome.ok && outcome.deferred) {
      return json(env, {
        ok: true,
        deferred: true,
        reason: outcome.reason,
        next_run_at: outcome.next_run_at,
      }, 200, "private");
    }
    if (!outcome.ok) return json(env, { error: outcome.error }, 400, "private");
    return json(env, {
      ok: true,
      run_id: outcome.run.run_id,
      chat_id: outcome.run.chat_id,
      share_id: outcome.share_id,
      share_url: `/share/${outcome.share_id}`,
    }, 200, "private");
  }

  const schedulesTick = path === "/api/admin/bots/schedules/tick";
  if (schedulesTick && req.method === "POST") {
    const admin = await requireBotAdmin(env, req);
    if (!admin.ok) return json(env, { error: admin.error }, admin.status, "private");
    const summary = await runDueBotSchedules(env, { waitUntil: (p) => ctx.waitUntil(p) });
    return json(env, { ok: true, ...summary }, 200, "private");
  }

  const runPath = path.match(/^\/api\/admin\/bots\/runs\/([^/]+)$/);
  if (runPath) {
    const admin = await requireBotAdmin(env, req);
    if (!admin.ok) return json(env, { error: admin.error }, admin.status, "private");
    const runId = decodeURIComponent(runPath[1]);
    if (req.method === "GET") {
      const run = await getBotRun(env.SCHEMA_DB, runId);
      if (!run) return json(env, { error: "not found" }, 404, "private");
      return json(env, { run }, 200, "private");
    }
    if (req.method === "PATCH") {
      let body: Record<string, unknown>;
      try {
        body = await req.json() as Record<string, unknown>;
      } catch {
        return json(env, { error: "invalid JSON body" }, 400, "private");
      }
      const status = body.status;
      if (status !== "shared" && status !== "failed" && status !== "running" && status !== "queued") {
        return json(env, { error: "status must be queued|running|shared|failed" }, 400, "private");
      }
      const share_id = typeof body.share_id === "string" ? body.share_id : null;
      const error = typeof body.error === "string" ? body.error : null;
      const run = await updateBotRun(env.SCHEMA_DB, runId, { status, share_id, error });
      if (!run) return json(env, { error: "not found" }, 404, "private");
      return json(env, { ok: true, run }, 200, "private");
    }
    return json(env, { error: "method not allowed" }, 405, "private");
  }

  return null;
}

async function handleUserChats(env: Env, req: Request, path: string): Promise<Response | null> {
  if (path === "/api/chats" && req.method === "GET") {
    const user = await requireUser(env, req);
    if (user instanceof Response) return user;
    const items = await listUserChats(env.SCHEMA_DB, user.id);
    return json(env, { ok: true, items }, 200, "private");
  }
  if (path === "/api/chats/claim" && req.method === "POST") {
    const user = await requireUser(env, req);
    if (user instanceof Response) return user;
    let body: Record<string, unknown>;
    try {
      body = await req.json() as Record<string, unknown>;
    } catch {
      return json(env, { error: "invalid JSON body" }, 400, "private");
    }
    const chatId = parseChatId(body.chat_id);
    if (!chatId) return json(env, { error: "chat_id must be a UUID" }, 400, "private");
    const title = titleFromMessages(undefined, typeof body.title === "string" ? body.title : null);
    const result = await claimChat(env.SCHEMA_DB, user.id, chatId, title);
    if (!result.ok) return json(env, { error: result.error }, result.status, "private");
    return json(env, result, 200, "private");
  }
  const tickersMatch = path.match(/^\/api\/chats\/([^/]+)\/tickers$/);
  if (tickersMatch && req.method === "GET") {
    const chatId = parseChatId(decodeURIComponent(tickersMatch[1]));
    if (!chatId) return json(env, { error: "chat_id must be a UUID" }, 400, "private");
    const items = await listChatTickers(env.SCHEMA_DB, chatId);
    return json(env, { chat_id: chatId, items }, 200, "private");
  }
  const railMatch = path.match(/^\/api\/chats\/([^/]+)\/rail$/);
  if (railMatch && req.method === "GET") {
    const chatId = parseChatId(decodeURIComponent(railMatch[1]));
    if (!chatId) return json(env, { error: "chat_id must be a UUID" }, 400, "private");
    return json(
      env,
      await loadChatRail(
        { env, queryLake: (sql, key) => r2sql(env, sql, key) },
        chatId,
      ),
      200,
      "private",
    );
  }
  const transcriptMatch = path.match(/^\/api\/chats\/([^/]+)\/transcript$/);
  if (transcriptMatch && req.method === "GET") {
    const chatId = parseChatId(decodeURIComponent(transcriptMatch[1]));
    if (!chatId) return json(env, { error: "chat_id must be a UUID" }, 400, "private");
    const user = await requireUser(env, req);
    if (user instanceof Response) return user;
    const owner = await ownerOf(env.SCHEMA_DB, chatId);
    if (!owner || owner.user_id !== user.id) {
      return json(env, { error: "forbidden" }, 403, "private");
    }
    const backup = await loadOwnedChatTranscript(env.SCHEMA_DB, chatId);
    return json(env, { ok: true, chat_id: chatId, ...backup }, 200, "private");
  }
  const item = path.match(/^\/api\/chats\/([^/]+)$/);
  if (!item) return null;
  const chatId = parseChatId(decodeURIComponent(item[1]));
  if (!chatId) return json(env, { error: "chat_id must be a UUID" }, 400, "private");
  const user = await requireUser(env, req);
  if (user instanceof Response) return user;
  if (req.method === "PATCH") {
    let body: Record<string, unknown>;
    try {
      body = await req.json() as Record<string, unknown>;
    } catch {
      return json(env, { error: "invalid JSON body" }, 400, "private");
    }
    if (typeof body.title !== "string") return json(env, { error: "title is required" }, 400, "private");
    const result = await renameChat(env.SCHEMA_DB, user.id, chatId, body.title);
    if (!result.ok) return json(env, { error: result.error }, result.status, "private");
    return json(env, result, 200, "private");
  }
  if (req.method === "DELETE") {
    const result = await deleteChat(env.SCHEMA_DB, user.id, chatId);
    if (!result.ok) return json(env, { error: result.error }, result.status, "private");
    return json(env, result, 200, "private");
  }
  return null;
}

async function handle(env: Env, req: Request, ctx: ExecutionContext): Promise<Response> {
  const url = new URL(req.url);
  const path = url.pathname;
  const q = url.searchParams;

  // latest-underlying subquery is reused; precompute nothing here (cached on demand).

  if (path === "/api/health") return json(env, { ok: true, auth: { google: googleConfigured(env) } });

  // Read-only pass-through to the continuous loader's live /loop state.
  if (path === "/loader/status")
    return json(env, await loaderGet(env, "/loop/status", "loader:status"));
  if (path === "/loader/symbols")
    return json(env, await loaderGet(env, "/loop/symbols" + url.search, "loader:symbols:" + url.search));

  if (path === "/api/stats") return json(env, await stats(env));
  if (path === "/api/runs") return json(env, await runs(env, num(q.get("limit") ?? 5)));
  if (path === "/api/sectors") return json(env, await sectors(env));
  if (path === "/api/symbols")
    return json(env, await symbols(env, q.get("q") ?? undefined, num(q.get("limit") ?? 50)));
  if (path === "/api/symbols/enroll" && req.method === "POST") {
    return handleSymbolsEnroll(env, req, ctx);
  }
  if (path === "/api/news")
    return json(env, await news(env, q.get("symbol"), q.get("limit") ? num(q.get("limit")) : NEWS_DEFAULT_LIMIT));
  if (path === "/api/web_search")
    return json(env, await webSearch(env, q.get("q"), q.get("limit") ? num(q.get("limit")) : WEB_SEARCH_DEFAULT_LIMIT));
  if (path === "/api/iv_rank")
    return json(env, await ivRank(env, q.get("symbol"), q.get("days") ? num(q.get("days")) : IV_RANK_DEFAULT_DAYS));
  if (path === "/api/econ_calendar")
    return json(env, await econCalendar(env, q.get("days") ? num(q.get("days")) : ECON_DEFAULT_DAYS));
  if (path === "/api/tables") return json(env, await schemaTables(env, ctx, q.get("force") === "1"));
  // Copilot chat is served exclusively by CopilotAgent at
  // /agents/copilot-agent/:conversation-id.

  if (path === "/api/notebook/premium")
    return json(env, await notebookPremium(env, {
      target_dte: q.get("target_dte") ? num(q.get("target_dte")) : undefined,
      tolerance: q.get("tolerance") ? num(q.get("tolerance")) : undefined,
      moneyness_band: q.get("moneyness_band") ? num(q.get("moneyness_band")) : undefined,
      min_volume: q.get("min_volume") ? num(q.get("min_volume")) : undefined,
      limit: q.get("limit") ? num(q.get("limit")) : undefined,
    }));

  if (path.startsWith("/api/symbol/")) {
    const sym = decodeURIComponent(path.slice("/api/symbol/".length)).toUpperCase();
    const parts = parseSymbolDetailParts(q.get("parts"));
    const expiration = q.get("expiration")?.trim() || undefined;
    const nearRaw = q.get("near_spot");
    const nearSpot = nearRaw != null && nearRaw !== "" ? num(nearRaw) : undefined;
    try {
      return json(env, await symbolDetail(env, sym, { parts, expiration, nearSpot }));
    } catch (e) {
      if (parts === "ohlc_intraday") {
        const message = e instanceof Error ? e.message : String(e);
        return json(env, { error: message, ohlc: [] }, 502, "private");
      }
      throw e;
    }
  }

  if (path === "/api/research/warm" && req.method === "POST") {
    return handleResearchWarm(env, req);
  }

  if (path.startsWith("/api/research/")) {
    const rest = decodeURIComponent(path.slice("/api/research/".length));
    const [tickerPart, sub] = rest.split("/");
    if (sub === "chats" && req.method === "GET") {
      const ticker = parseTickerParam(tickerPart);
      if (!ticker) return json(env, { error: "invalid ticker" }, 400, "private");
      const securityId = securityIdForTicker(ticker);
      const chats = await listSecurityChats(env.SCHEMA_DB, securityId, num(q.get("limit") ?? 20));
      return json(env, { ticker, security_id: securityId, items: chats }, 200, "private");
    }
    if (sub === "commentary" && req.method === "GET") {
      return handleResearchCommentaryGet(env, req, tickerPart);
    }
    if (sub === "filings" && req.method === "GET") {
      return handleResearchFilingsGet(env, req, tickerPart);
    }
    if (!sub && (req.method === "GET" || req.method === "POST")) {
      return handleResearchGet(env, req, tickerPart, ctx);
    }
  }

  if (path === "/api/underlyings")
    return json(env, await underlyings(env, {
      sector: q.get("sector") ?? undefined, q: q.get("q") ?? undefined,
      limit: num(q.get("limit") ?? 50), offset: num(q.get("offset") ?? 0),
    }));

  if (path === "/api/screen")
    return json(env, await screen(env, {
      symbol: q.get("symbol") ?? undefined, type: q.get("type") ?? undefined, sector: q.get("sector") ?? undefined,
      min_strike: q.get("min_strike") ? num(q.get("min_strike")) : undefined,
      max_strike: q.get("max_strike") ? num(q.get("max_strike")) : undefined,
      min_volume: q.get("min_volume") ? num(q.get("min_volume")) : undefined,
      min_open_interest: q.get("min_open_interest") ? num(q.get("min_open_interest")) : undefined,
      min_iv: q.get("min_iv") ? num(q.get("min_iv")) : undefined,
      max_iv: q.get("max_iv") ? num(q.get("max_iv")) : undefined,
      min_delta: q.get("min_delta") ? num(q.get("min_delta")) : undefined,
      max_delta: q.get("max_delta") ? num(q.get("max_delta")) : undefined,
      in_the_money: q.has("in_the_money") ? q.get("in_the_money") === "true" : undefined,
      expiration_before: q.get("expiration_before") ?? undefined,
      expiration_after: q.get("expiration_after") ?? undefined,
      near_spot_strikes: q.has("near_spot_strikes") ? num(q.get("near_spot_strikes")) : 50,
      sort: q.get("sort") ?? undefined, order: q.get("order") ?? undefined,
      limit: num(q.get("limit") ?? 100), offset: num(q.get("offset") ?? 0),
    }));

  if (path === "/api/query" && req.method === "POST") {
    const body = await req.json() as { sql?: string; limit?: number };
    let sql = body.sql ?? "";
    // Private tables (options.chat_history) are admin-only: block any SQL that
    // references them unless the request carries ADMIN_TOKEN.
    if (!adminAuthorized(req, env) && PRIVATE_LAKE_TABLES_RE.test(sql.toLowerCase())) {
      return json(env, { error: "query references a private table" }, 403);
    }
    // Normalize ticker↔symbol against the live DESCRIBE cache so SQL Lab and
    // hand-written queries hit the same synonym rewrite Copilot uses.
    try {
      const tables = await schemaTables(env, ctx, false);
      sql = applyColumnSynonyms(sql, tables).sql;
    } catch (e) {
      console.error("query synonym rewrite skipped", e);
    }
    return json(env, await runQuery(env, sql, body.limit ?? 1000));
  }

  const me = await handleMe(env, req, path);
  if (me) return me;

  const avatar = await handleAvatarGet(env, req, path);
  if (avatar) return avatar;

  const chats = await handleUserChats(env, req, path);
  if (chats) return chats;

  const timeline = await handleTimeline(env, req, path, ctx, (sql, key) => r2sql(env, sql, key));
  if (timeline) return timeline;

  const bots = await handleBots(env, req, path, ctx);
  if (bots) return bots;

  // Signed-up users directory (session admin or ADMIN_TOKEN).
  if (path === "/api/admin/users" && req.method === "GET") {
    const admin = await requireBotAdmin(env, req);
    if (!admin.ok) return json(env, { error: admin.error }, admin.status, "private");
    const limit = Number(q.get("limit") ?? 500);
    return json(env, { items: await listAdminUsers(env.SCHEMA_DB, { limit }) }, 200, "private");
  }

  // Suggested trades from successful suggest_trades tool events (~30d retention).
  if (path === "/api/admin/trades" && req.method === "GET") {
    const admin = await requireBotAdmin(env, req);
    if (!admin.ok) return json(env, { error: admin.error }, admin.status, "private");
    ctx.waitUntil(purgeExpiredToolEvents(env.SCHEMA_DB));
    return json(
      env,
      await listAdminSuggestedTrades(env.SCHEMA_DB, {
        limit: num(q.get("limit") ?? 100),
        before: q.get("before"),
      }),
      200,
      "private",
    );
  }

  // Copilot chat history: capture (open, best-effort) + admin-only read.
  if (path === "/api/chat/history" && req.method === "POST")
    return await saveChatHistory(env, ctx, req);
  if (path === "/api/admin/chat_history" && req.method === "GET") {
    const admin = await requireBotAdmin(env, req);
    if (!admin.ok) return json(env, { error: admin.error }, admin.status, "private");
    return json(
      env,
      await adminChatHistory(env, num(q.get("limit") ?? 100), q.get("before")),
      200,
      "private",
    );
  }
  // Copilot tool-call debug log (D1). Public — payloads are capped tool
  // args/errors/SQL, not transcripts or abuse metadata. Omitting ok defaults
  // to failures; pass ok=all for every outcome. share_id resolves the
  // originating chat so a share URL is enough to debug.
  if ((path === "/api/tool_calls" || path === "/api/admin/tool_calls") && req.method === "GET") {
    const parsed = parseToolEventListQuery(q);
    if (parsed.error) return json(env, { ok: false, error: parsed.error }, 400);
    let chatId = parsed.chat_id ?? null;
    if (parsed.share_id) {
      const fromShare = await chatIdForShare(env.SCHEMA_DB, parsed.share_id);
      if (!fromShare) return json(env, { ok: false, error: "share not found" }, 404);
      // share_id wins when both are supplied — the share is the capability.
      chatId = fromShare;
    }
    ctx.waitUntil(purgeExpiredToolEvents(env.SCHEMA_DB));
    return json(
      env,
      await listToolEvents(env.SCHEMA_DB, {
        chat_id: chatId,
        share_id: parsed.share_id,
        tool: parsed.tool,
        ok: parsed.ok,
        limit: parsed.limit,
        before: parsed.before,
      }),
      200,
      "private",
    );
  }

  // Copilot chat shares: create (open, user-requested) + public read. The id
  // IS the capability — no auth, and unknown ids 404 identically to expired.
  if (path === "/api/share/chat" && req.method === "POST")
    return await createShare(env, req, ctx);
  if (path.startsWith("/api/share/"))
    return await getSharedChat(env, path.slice("/api/share/".length), ctx);


  return json(env, { error: "not found" }, 404);
}

export default {
  async fetch(req: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(req.url);
    if (req.method === "OPTIONS") {
      return withCors(env, req, new Response(null, { status: 204 }));
    }
    if (url.pathname.startsWith("/api/auth/")) {
      // Pages (dev.lobster.mp) and the Worker (api-dev.lobster.mp) are different
      // origins. Better Auth does not emit CORS headers, so a credentialed
      // sign-in fetch is dropped by the browser unless we wrap it like /api/*.
      const auth = createAuth(env, req);
      if (!auth) return withCors(env, req, json(env, { error: "auth is not configured" }, 503, "private"));
      return withCors(env, req, await auth.handler(req));
    }
    try {
      const denied = await authorizeCopilotAgent(env, req);
      if (denied) return withCors(env, req, denied);
      const agentResponse = await routeAgentRequest(req, env);
      if (agentResponse) return agentResponse.status === 101 ? agentResponse : withCors(env, req, agentResponse);
      return withCors(env, req, await handle(env, req, ctx));
    } catch (e) {
      return withCors(env, req, json(env, { error: String(e) }, 500));
    }
  },

  /**
   * Hourly cron — process due bot schedules (market-gated hourly overviews).
   * Actual cadence is per-row on bot_schedules; this tick just wakes the runner.
   */
  async scheduled(_event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(
      runDueBotSchedules(env, { waitUntil: (p) => ctx.waitUntil(p) }).then((summary) => {
        console.log(JSON.stringify({ botSchedules: true, ...summary }));
      }).catch((error) => {
        console.error("bot schedules tick failed", error);
      }),
    );
  },
};
