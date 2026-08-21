/**
 * Public timeline of opted-in chat shares.
 *
 * Unlisted shares (shared_chats) stay a capability URL. A signed-in author
 * with a handle can POST /api/timeline to list that share on GET /api/timeline
 * (the home feed). DELETE removes the listing; the share link still works.
 * Admins may unpublish any human listing or any bot share (clears bot_handle
 * so the share leaves the feed without revoking the link).
 *
 * Cheap-model title + ticker NER runs for every share that still carries the
 * prompt as its title (human posts included — not only bot mint).
 *
 * Before a human listing lands, timeline-moderation judges whether the
 * transcript is a finished, feed-worthy answer (same gate bots use at mint).
 */
import { isAdminEmail } from "./admin";
import { getSessionUser, type AuthEnv } from "./auth";
import { backfillShareMeta, shareNeedsMetaBackfill, type ChatMetaEnv } from "./chat-meta";
import { listTickersForChats } from "./chat-tickers";
import { createCopilotModel } from "./copilot-contract";
import { coalesceAssistantMessageRecords } from "./share-turns";
import { getHandle, parseHandle, publicName } from "./profiles";
import { avatarUrlFor } from "./avatars";
import { shareDisplayTitle } from "./user-chats";
import { loadTimelineRail, type TimelineLakeQuery } from "./timeline-rail";
import {
  moderateTimelineShare,
  TIMELINE_QUALITY_REJECTED_ERROR,
} from "./timeline-moderation";
import { scheduleImprovementReport, type ImprovementReporterEnv } from "./improvement-reporter";

const SHARE_ID_RE = /^[0-9A-Za-z]{1,48}$/;
const LIST_DEFAULT = 30;
const LIST_MAX = 50;
/** Cap sync OpenRouter meta passes per timeline page (rest waitUntil). */
const META_SYNC_MAX = 6;
/** Safety ceiling for a single first-message preview (not a display truncate). */
export const EXCERPT_MAX = 100_000;

export interface TimelineEnv extends AuthEnv, ChatMetaEnv, ImprovementReporterEnv {
  SCHEMA_DB: D1Database;
  OPEN_ROUTER_KEY?: string;
  COPILOT_MODEL?: string;
  TAVILY_API_KEY?: string;
}

function json(data: unknown, status = 200, cache: "public" | "private" = "public"): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": cache === "private" ? "private, no-store" : "public, max-age=15",
    },
  });
}

function messageRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

/**
 * Timeline preview body: the first full assistant answer, else the first user
 * turn, else the title. Keeps paragraph breaks so the feed can render the
 * message expanded; only a large safety ceiling applies.
 */
export function excerptFromMessages(messages: unknown, title: string | null): string {
  // Heal DSML / stacked assistants before picking the public excerpt.
  const rows = coalesceAssistantMessageRecords(messages);
  let text = "";
  for (const row of rows) {
    const rec = messageRecord(row);
    if (rec?.role === "assistant" && typeof rec.content === "string" && rec.content.trim()) {
      text = rec.content;
      break;
    }
  }
  if (!text) {
    for (const row of rows) {
      const rec = messageRecord(row);
      if (rec?.role === "user" && typeof rec.content === "string" && rec.content.trim()) {
        text = rec.content;
        break;
      }
    }
  }
  if (!text && typeof title === "string") text = title;
  // Trim edges and collapse runs of spaces/tabs, but keep newlines so markdown
  // and multi-paragraph answers stay readable on the infinite-scroll feed.
  const normalized = text
    .replace(/[^\S\n]+/g, " ")
    .replace(/ *\n */g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
  if (normalized.length <= EXCERPT_MAX) return normalized;
  return normalized.slice(0, EXCERPT_MAX - 1).trimEnd() + "…";
}

/**
 * Full conversation for the feed: every user/assistant turn, slimmed the same
 * way as a single-turn preview (drop result rows, cap text). Readers stay on
 * the timeline instead of jumping to /share. Falls back to a title-only stub
 * when the share has no renderable turns.
 */
export function previewMessagesFromShare(messages: unknown, title: string | null = null): Record<string, unknown>[] {
  const rows = coalesceAssistantMessageRecords(messages);
  const out: Record<string, unknown>[] = [];
  for (const row of rows) {
    const slim = slimPreviewMessage(row);
    if (slim) out.push(slim);
  }
  if (out.length > 0) return out;
  if (typeof title === "string" && title.trim()) {
    return [{ role: "assistant", content: title.trim() }];
  }
  return [];
}

/** Cap feed payload: keep chat chrome fields, drop bulky result row snapshots. */
function slimPreviewMessage(rec: Record<string, unknown>): Record<string, unknown> | null {
  const role = rec.role === "assistant" ? "assistant" : rec.role === "user" ? "user" : null;
  if (!role) return null;
  const out: Record<string, unknown> = { role };
  if (typeof rec.content === "string" && rec.content) out.content = rec.content.slice(0, EXCERPT_MAX);
  if (typeof rec.reasoning === "string" && rec.reasoning.trim()) {
    out.reasoning = rec.reasoning.slice(0, EXCERPT_MAX);
  }
  if (typeof rec.sql === "string" && rec.sql.trim()) out.sql = rec.sql.slice(0, 20_000);
  if (typeof rec.ts === "number" && Number.isFinite(rec.ts)) out.ts = rec.ts;
  if (rec.chart && typeof rec.chart === "object" && !Array.isArray(rec.chart)) out.chart = rec.chart;
  if (rec.desk && typeof rec.desk === "object" && !Array.isArray(rec.desk)) out.desk = rec.desk;
  if (rec.trades && typeof rec.trades === "object" && !Array.isArray(rec.trades)) out.trades = rec.trades;
  // Omit result rows from the list payload — AssistantMessageBody re-runs SQL
  // when needed, same path as snapshot-less shares.
  if (!out.content && !out.sql && !out.reasoning && !out.chart && !out.desk && !out.trades) return null;
  return out;
}

export function flagsFromMessages(messages: unknown): { has_sql: boolean; has_chart: boolean } {
  const rows = Array.isArray(messages) ? messages : [];
  let has_sql = false;
  let has_chart = false;
  for (const row of rows) {
    const rec = messageRecord(row);
    if (!rec) continue;
    if (typeof rec.sql === "string" && rec.sql.trim()) has_sql = true;
    if (rec.chart && typeof rec.chart === "object" && !Array.isArray(rec.chart)) has_chart = true;
  }
  return { has_sql, has_chart };
}

export type TimelineQuery =
  | { ok: true; limit: number; before: number | null; handle: string | null }
  | { ok: false; status: 400; error: string };

export function parseTimelineQuery(q: URLSearchParams): TimelineQuery {
  const limitRaw = q.get("limit");
  let limit = LIST_DEFAULT;
  if (limitRaw != null && limitRaw !== "") {
    const n = Number(limitRaw);
    if (!Number.isInteger(n) || n < 1) {
      return { ok: false, status: 400, error: "limit must be a positive integer" };
    }
    limit = Math.min(n, LIST_MAX);
  }
  const beforeRaw = q.get("before");
  let before: number | null = null;
  if (beforeRaw != null && beforeRaw !== "") {
    const n = Number(beforeRaw);
    if (!Number.isFinite(n) || n < 1) {
      return { ok: false, status: 400, error: "before must be a timestamp" };
    }
    before = n;
  }
  const handleRaw = q.get("handle");
  let handle: string | null = null;
  if (handleRaw != null && handleRaw !== "") {
    const parsed = parseHandle(handleRaw);
    if (!parsed.ok) return { ok: false, status: 400, error: parsed.error };
    handle = parsed.handle;
  }
  return { ok: true, limit, before, handle };
}

export async function recordShareOwner(db: D1Database, shareId: string, userId: string): Promise<void> {
  await db.prepare(
    `INSERT INTO share_owners (share_id, user_id, created_at)
     VALUES (?1, ?2, ?3)
     ON CONFLICT(share_id) DO NOTHING`,
  ).bind(shareId, userId, Date.now()).run();
}

export async function getTimelineAuthor(
  db: D1Database,
  shareId: string,
): Promise<{
  handle: string;
  name: string;
  is_bot?: boolean;
  persona?: string | null;
  avatar_url?: string | null;
} | null> {
  const human = await db.prepare(
    `SELECT pr.handle AS handle, pr.display_name AS display_name,
            pr.avatar_key AS avatar_key, pr.updated_at AS updated_at,
            u.name AS oauth_name, pr.user_id AS user_id
     FROM timeline_posts p
     JOIN user_profiles pr ON pr.user_id = p.user_id
     JOIN "user" u ON u.id = p.user_id
     WHERE p.share_id = ?1`,
  ).bind(shareId).first<{
    handle: string;
    display_name: string | null;
    avatar_key: string | null;
    updated_at: number;
    oauth_name: string;
    user_id: string;
  }>();
  if (human) {
    return {
      handle: human.handle,
      name: publicName(human.display_name, human.oauth_name),
      is_bot: false,
      avatar_url: avatarUrlFor(human.user_id, human.avatar_key, human.updated_at),
    };
  }
  const bot = await db.prepare(
    `SELECT b.handle AS handle, b.display_name AS name, b.persona AS persona
     FROM shared_chats s
     JOIN bot_profiles b ON b.handle = s.bot_handle AND b.enabled = 1
     WHERE s.share_id = ?1 AND s.bot_handle IS NOT NULL`,
  ).bind(shareId).first<{ handle: string; name: string; persona: string }>();
  if (!bot) return null;
  return { handle: bot.handle, name: bot.name, is_bot: true, persona: bot.persona, avatar_url: null };
}

interface ShareRow {
  share_id: string;
  chat_id: string;
  title: string | null;
  messages: string;
  expires_at: number | null;
}

interface TimelineRow {
  share_id: string;
  chat_id: string;
  excerpt: string | null;
  has_sql: number;
  has_chart: number;
  published_at: number;
  title: string | null;
  model: string | null;
  messages: string | null;
  handle: string;
  name: string;
  user_id: string | null;
  avatar_key: string | null;
  updated_at: number | null;
}

function itemFromRow(row: TimelineRow & { is_bot?: number }, tickers: string[] = []) {
  // Prefer a live slimmed transcript from the share so older posts stored
  // under the old 280-char excerpt cap still render the full conversation.
  let parsed: unknown = [];
  if (row.messages) {
    try {
      parsed = JSON.parse(row.messages);
    } catch {
      parsed = [];
    }
  }
  const messages = previewMessagesFromShare(parsed, row.title);
  let excerpt = row.excerpt ?? "";
  if (messages.length) {
    excerpt = excerptFromMessages(parsed, row.title) || excerpt;
  }
  const isBot = row.is_bot === 1;
  return {
    share_id: row.share_id,
    url: "/share/" + row.share_id,
    title: shareDisplayTitle(parsed, row.title),
    excerpt,
    messages,
    handle: row.handle,
    name: row.name,
    avatar_url: !isBot && row.user_id
      ? avatarUrlFor(row.user_id, row.avatar_key, row.updated_at)
      : null,
    published_at: row.published_at,
    model: row.model,
    has_sql: row.has_sql === 1,
    has_chart: row.has_chart === 1,
    /** OpenFIGI-linked tickers from chat_tickers for the originating chat. */
    tickers,
  };
}

function parseRowMessages(row: TimelineRow): unknown {
  if (!row.messages) return [];
  try {
    return JSON.parse(row.messages);
  } catch {
    return [];
  }
}

/**
 * Heal prompt-as-title (and missing NER) shares on the feed — human posts
 * included. Sync a small batch so the response already carries headlines;
 * anything past the cap heals via waitUntil for the next load.
 */
async function healTimelineMeta(
  env: TimelineEnv,
  ctx: ExecutionContext,
  rows: TimelineRow[],
  tickersByChat: Map<string, string[]>,
  origin: string,
): Promise<void> {
  if (!env.OPEN_ROUTER_KEY?.trim() || !env.COPILOT_MODEL?.trim()) return;
  const needing: { row: TimelineRow; messages: unknown }[] = [];
  for (const row of rows) {
    if (!row.chat_id) continue;
    const messages = parseRowMessages(row);
    if (!shareNeedsMetaBackfill(row.title, messages)) continue;
    needing.push({ row, messages });
  }
  if (!needing.length) return;

  const model = createCopilotModel(
    { OPEN_ROUTER_KEY: env.OPEN_ROUTER_KEY, COPILOT_MODEL: env.COPILOT_MODEL },
    origin,
  );
  const sync = needing.slice(0, META_SYNC_MAX);
  const deferred = needing.slice(META_SYNC_MAX);

  for (const item of deferred) {
    ctx.waitUntil(backfillShareMeta(env, {
      chatId: item.row.chat_id,
      shareId: item.row.share_id,
      messages: item.messages,
      storedTitle: item.row.title,
      model,
    }));
  }

  await Promise.all(sync.map(async ({ row, messages }) => {
    const meta = await backfillShareMeta(env, {
      chatId: row.chat_id,
      shareId: row.share_id,
      messages,
      storedTitle: row.title,
      model,
    });
    if (meta.title) row.title = meta.title;
    if (meta.tickers.length) {
      const merged = [
        ...(tickersByChat.get(row.chat_id) ?? []),
        ...meta.tickers.map((ticker) => ticker.trim().toUpperCase()).filter(Boolean),
      ];
      tickersByChat.set(row.chat_id, [...new Set(merged)]);
    }
  }));
}

async function listTimeline(env: TimelineEnv, req: Request, ctx: ExecutionContext): Promise<Response> {
  const parsed = parseTimelineQuery(new URL(req.url).searchParams);
  if (!parsed.ok) return json({ error: parsed.error }, parsed.status, "private");

  let profile: {
    handle: string;
    name: string;
    is_bot?: boolean;
    persona?: string | null;
    bio?: string | null;
    /** Epoch ms when the public handle (or bot profile) was created. */
    created_at?: number | null;
    avatar_url?: string | null;
  } | null = null;
  if (parsed.handle) {
    const human = await env.SCHEMA_DB.prepare(
      `SELECT pr.handle AS handle, pr.display_name AS display_name,
              pr.avatar_key AS avatar_key, pr.updated_at AS updated_at,
              u.name AS oauth_name, pr.user_id AS user_id, pr.created_at AS created_at
       FROM user_profiles pr
       JOIN "user" u ON u.id = pr.user_id
       WHERE pr.handle = ?1`,
    ).bind(parsed.handle).first<{
      handle: string;
      display_name: string | null;
      avatar_key: string | null;
      updated_at: number;
      oauth_name: string;
      user_id: string;
      created_at: number;
    }>();
    if (human) {
      profile = {
        handle: human.handle,
        name: publicName(human.display_name, human.oauth_name),
        is_bot: false,
        created_at: human.created_at,
        avatar_url: avatarUrlFor(human.user_id, human.avatar_key, human.updated_at),
      };
    } else {
      const bot = await env.SCHEMA_DB.prepare(
        `SELECT handle, display_name AS name, persona, bio, created_at
         FROM bot_profiles WHERE handle = ?1 AND enabled = 1`,
      ).bind(parsed.handle).first<{
        handle: string;
        name: string;
        persona: string;
        bio: string | null;
        created_at: number;
      }>();
      if (!bot) return json({ error: "not found" }, 404);
      profile = {
        handle: bot.handle,
        name: bot.name,
        is_bot: true,
        persona: bot.persona,
        bio: bot.bio,
        created_at: bot.created_at,
        avatar_url: null,
      };
    }
  }

  // Fetch human opt-in posts and always-public bot shares, then merge by
  // published_at. Keeps the query simple (D1 SQLite) and avoids double-counting
  // when a share somehow appears in both sets.
  const humanClauses = ["(s.expires_at IS NULL OR s.expires_at > ?1)"];
  const humanBindings: (string | number)[] = [Date.now()];
  if (parsed.before != null) {
    humanBindings.push(parsed.before);
    humanClauses.push(`p.published_at < ?${humanBindings.length}`);
  }
  if (parsed.handle) {
    humanBindings.push(parsed.handle);
    humanClauses.push(`pr.handle = ?${humanBindings.length}`);
  }
  humanBindings.push(parsed.limit + 1);
  const humanSql =
    `SELECT p.share_id, s.chat_id, p.excerpt, p.has_sql, p.has_chart, p.published_at,
            s.title, s.model, s.messages, pr.handle,
            COALESCE(NULLIF(TRIM(pr.display_name), ''), u.name) AS name,
            pr.user_id AS user_id, pr.avatar_key AS avatar_key, pr.updated_at AS updated_at,
            0 AS is_bot
     FROM timeline_posts p
     JOIN shared_chats s ON s.share_id = p.share_id
     JOIN user_profiles pr ON pr.user_id = p.user_id
     JOIN "user" u ON u.id = p.user_id
     WHERE ${humanClauses.join(" AND ")}
     ORDER BY p.published_at DESC
     LIMIT ?${humanBindings.length}`;

  const botClauses = ["(s.expires_at IS NULL OR s.expires_at > ?1)", "s.bot_handle IS NOT NULL"];
  const botBindings: (string | number)[] = [Date.now()];
  if (parsed.before != null) {
    botBindings.push(parsed.before);
    botClauses.push(`s.created_at < ?${botBindings.length}`);
  }
  if (parsed.handle) {
    botBindings.push(parsed.handle);
    botClauses.push(`b.handle = ?${botBindings.length}`);
  }
  botBindings.push(parsed.limit + 1);
  const botSql =
    `SELECT s.share_id, s.chat_id, NULL AS excerpt,
            0 AS has_sql, 0 AS has_chart, s.created_at AS published_at,
            s.title, s.model, s.messages, b.handle, b.display_name AS name,
            NULL AS user_id, NULL AS avatar_key, NULL AS updated_at, 1 AS is_bot
     FROM shared_chats s
     JOIN bot_profiles b ON b.handle = s.bot_handle AND b.enabled = 1
     WHERE ${botClauses.join(" AND ")}
     ORDER BY s.created_at DESC
     LIMIT ?${botBindings.length}`;

  const [humanRows, botRows] = await Promise.all([
    env.SCHEMA_DB.prepare(humanSql).bind(...humanBindings).all<TimelineRow & { is_bot: number }>(),
    env.SCHEMA_DB.prepare(botSql).bind(...botBindings).all<TimelineRow & { is_bot: number }>(),
  ]);
  const merged = [...(humanRows.results ?? []), ...(botRows.results ?? [])]
    .sort((a, b) => b.published_at - a.published_at || (a.share_id < b.share_id ? 1 : -1));
  const seen = new Set<string>();
  const deduped: Array<TimelineRow & { is_bot: number }> = [];
  for (const row of merged) {
    if (seen.has(row.share_id)) continue;
    seen.add(row.share_id);
    // Recompute flags from messages for bot rows (SQL LIKE is approximate).
    if (row.is_bot === 1 && row.messages) {
      try {
        const flags = flagsFromMessages(JSON.parse(row.messages));
        row.has_sql = flags.has_sql ? 1 : 0;
        row.has_chart = flags.has_chart ? 1 : 0;
        row.excerpt = excerptFromMessages(JSON.parse(row.messages), row.title);
      } catch {
        /* keep defaults */
      }
    }
    deduped.push(row);
  }
  const extra = deduped.length > parsed.limit;
  const page = extra ? deduped.slice(0, parsed.limit) : deduped;
  const next_before = extra ? page[page.length - 1]?.published_at ?? null : null;
  const tickersByChat = await listTickersForChats(
    env.SCHEMA_DB,
    page.map((row) => row.chat_id),
  );
  await healTimelineMeta(env, ctx, page, tickersByChat, new URL(req.url).origin);
  const items = page.map((row) => ({
    ...itemFromRow(row, tickersByChat.get(row.chat_id) ?? []),
    is_bot: row.is_bot === 1,
  }));
  // Feed mutates via publish/unpublish — never serve a stale browser/CDN copy
  // after an admin unlist (public max-age made removals look like no-ops).
  return json({ items, next_before, profile }, 200, "private");
}

async function publishTimeline(
  env: TimelineEnv,
  req: Request,
  ctx: ExecutionContext,
): Promise<Response> {
  const user = await getSessionUser(env, req);
  if (!user) return json({ error: "unauthorized" }, 401, "private");
  const handle = await getHandle(env.SCHEMA_DB, user.id);
  if (!handle) return json({ error: "handle is required" }, 400, "private");

  let body: Record<string, unknown>;
  try {
    body = await req.json() as Record<string, unknown>;
  } catch {
    return json({ error: "invalid JSON body" }, 400, "private");
  }
  const shareId = typeof body.share_id === "string" ? body.share_id.trim() : "";
  if (!SHARE_ID_RE.test(shareId)) return json({ error: "share_id is required" }, 400, "private");

  const share = await env.SCHEMA_DB.prepare(
    `SELECT share_id, chat_id, title, messages, expires_at FROM shared_chats WHERE share_id = ?1`,
  ).bind(shareId).first<ShareRow>();
  if (!share || (share.expires_at && share.expires_at < Date.now())) {
    return json({ error: "not found" }, 404, "private");
  }

  const owner = await env.SCHEMA_DB.prepare(
    "SELECT user_id FROM share_owners WHERE share_id = ?1",
  ).bind(shareId).first<{ user_id: string }>();
  if (!owner) {
    return json({ error: "only the author can post this chat to the timeline" }, 403, "private");
  }
  if (owner.user_id !== user.id) {
    return json({ error: "forbidden" }, 403, "private");
  }

  const existing = await env.SCHEMA_DB.prepare(
    "SELECT published_at FROM timeline_posts WHERE share_id = ?1",
  ).bind(shareId).first<{ published_at: number }>();
  if (existing) {
    return json({ ok: true, share_id: shareId, published_at: existing.published_at }, 200, "private");
  }

  let messages: unknown = [];
  try {
    messages = JSON.parse(share.messages);
  } catch {
    messages = [];
  }

  // Quality gate — refuse cut-off / placeholder / unfinished tool-loop dumps.
  const requestOrigin = new URL(req.url).origin;
  const moderationModel = env.OPEN_ROUTER_KEY?.trim() && env.COPILOT_MODEL?.trim()
    ? createCopilotModel(
      { OPEN_ROUTER_KEY: env.OPEN_ROUTER_KEY, COPILOT_MODEL: env.COPILOT_MODEL },
      requestOrigin,
    )
    : null;
  const moderation = await moderateTimelineShare(messages, moderationModel);
  const publicOrigin = requestOrigin.includes("api-dev.")
    ? "https://dev.lobster.mp"
    : requestOrigin.includes("api.")
      ? "https://lobster.mp"
      : "https://lobster.mp";
  scheduleImprovementReport(
    env,
    moderationModel,
    {
      messages,
      decision: moderation,
      action: moderation.allow ? "allow_publish" : "reject_publish",
      shareId,
      publicOrigin,
    },
    { waitUntil: (p) => ctx.waitUntil(p) },
  );
  if (!moderation.allow) {
    console.info(JSON.stringify({
      timelineModeration: true,
      action: "reject_publish",
      share_id: shareId,
      source: moderation.source,
      reason: moderation.reason,
    }));
    return json({
      error: TIMELINE_QUALITY_REJECTED_ERROR,
      reason: moderation.reason,
    }, 422, "private");
  }

  // Land with a real headline + NER tags — same pass bots get at mint time.
  let title = share.title;
  if (
    share.chat_id
    && shareNeedsMetaBackfill(share.title, messages)
    && env.OPEN_ROUTER_KEY?.trim()
    && env.COPILOT_MODEL?.trim()
  ) {
    try {
      const model = createCopilotModel(
        { OPEN_ROUTER_KEY: env.OPEN_ROUTER_KEY, COPILOT_MODEL: env.COPILOT_MODEL },
        new URL(req.url).origin,
      );
      const meta = await backfillShareMeta(env, {
        chatId: share.chat_id,
        shareId: share.share_id,
        messages,
        storedTitle: share.title,
        model,
      });
      if (meta.title) title = meta.title;
    } catch (error) {
      console.warn("timeline publish meta enrich failed", error);
    }
  }
  const excerpt = excerptFromMessages(messages, title);
  const flags = flagsFromMessages(messages);
  const now = Date.now();
  try {
    await env.SCHEMA_DB.prepare(
      `INSERT INTO timeline_posts
         (share_id, user_id, excerpt, has_sql, has_chart, published_at)
       VALUES (?1, ?2, ?3, ?4, ?5, ?6)`,
    ).bind(shareId, user.id, excerpt || null, flags.has_sql ? 1 : 0, flags.has_chart ? 1 : 0, now).run();
  } catch (error) {
    if (String(error).includes("UNIQUE")) {
      return json({ ok: true, share_id: shareId, published_at: now }, 200, "private");
    }
    console.error("timeline publish failed", error);
    return json({ error: "storage unavailable" }, 502, "private");
  }
  return json({ ok: true, share_id: shareId, published_at: now }, 200, "private");
}

/**
 * Who may unlist a share: the human author of a timeline_posts row, or an
 * admin (who can also clear bot_handle so bot shares leave the feed).
 */
export function resolveUnpublishAccess(input: {
  admin: boolean;
  userId: string;
  postUserId: string | null;
  hasBotHandle: boolean;
}): { unpublishHuman: boolean; unpublishBot: boolean; forbidden: boolean } {
  const hasHuman = input.postUserId != null;
  const unpublishHuman = hasHuman && (input.admin || input.postUserId === input.userId);
  const unpublishBot = input.hasBotHandle && input.admin;
  const forbidden =
    (hasHuman && !unpublishHuman)
    || (!hasHuman && input.hasBotHandle && !unpublishBot);
  return { unpublishHuman, unpublishBot, forbidden };
}

async function unpublishTimeline(env: TimelineEnv, req: Request, shareId: string): Promise<Response> {
  const user = await getSessionUser(env, req);
  if (!user) return json({ error: "unauthorized" }, 401, "private");
  if (!SHARE_ID_RE.test(shareId)) return json({ error: "not found" }, 404, "private");

  const post = await env.SCHEMA_DB.prepare(
    "SELECT user_id FROM timeline_posts WHERE share_id = ?1",
  ).bind(shareId).first<{ user_id: string }>();
  const botShare = await env.SCHEMA_DB.prepare(
    "SELECT bot_handle FROM shared_chats WHERE share_id = ?1 AND bot_handle IS NOT NULL",
  ).bind(shareId).first<{ bot_handle: string }>();

  const access = resolveUnpublishAccess({
    admin: isAdminEmail(user.email),
    userId: user.id,
    postUserId: post?.user_id ?? null,
    hasBotHandle: Boolean(botShare),
  });
  if (access.forbidden) return json({ error: "forbidden" }, 403, "private");

  if (access.unpublishHuman) {
    await env.SCHEMA_DB.prepare(
      "DELETE FROM timeline_posts WHERE share_id = ?1",
    ).bind(shareId).run();
  }
  if (access.unpublishBot) {
    // Bot shares appear on the feed via bot_handle (no timeline_posts row).
    // Clearing attribution unlists them; the unlisted /share/{id} link remains.
    await env.SCHEMA_DB.prepare(
      "UPDATE shared_chats SET bot_handle = NULL, updated_at = ?2 WHERE share_id = ?1",
    ).bind(shareId, Date.now()).run();
  }
  return json({ ok: true, share_id: shareId }, 200, "private");
}

export async function handleTimeline(
  env: TimelineEnv,
  req: Request,
  path: string,
  ctx: ExecutionContext,
  queryLake?: TimelineLakeQuery,
): Promise<Response | null> {
  if (path === "/api/timeline") {
    if (req.method === "GET") return listTimeline(env, req, ctx);
    if (req.method === "POST") return publishTimeline(env, req, ctx);
    return json({ error: "method not allowed" }, 405, "private");
  }
  if (path === "/api/timeline/rail") {
    if (req.method !== "GET") return json({ error: "method not allowed" }, 405, "private");
    return json(await loadTimelineRail({ env, queryLake }));
  }
  const item = path.match(/^\/api\/timeline\/([^/]+)$/);
  if (!item) return null;
  if (req.method === "DELETE") return unpublishTimeline(env, req, decodeURIComponent(item[1]));
  return json({ error: "method not allowed" }, 405, "private");
}
