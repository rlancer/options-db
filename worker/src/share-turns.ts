/**
 * Flatten AI SDK UIMessage parts into share/timeline turns.
 */
import type { UIMessage } from "ai";
import { chartFitsResult, inferChartSpec, wantsChart, type ChartSpec } from "./chart-spec";
import { normalizeDeskBrief, type DeskBrief, type DeskBriefInput } from "./copilot-desk";
import { normalizeSuggestedTrades, type SuggestedTrades } from "./copilot-trades";
import { looksLikeDsmlToolMarkup, parseDsmlToolCalls, stripDsmlToolMarkup } from "./dsml";

export type ShareTurn = {
  role: "user" | "assistant";
  content: string;
  reasoning?: string;
  sql?: string;
  chart?: ChartSpec;
  desk?: DeskBrief;
  trades?: SuggestedTrades;
  ts?: number;
};

export type ShareCapture = {
  sql?: string | null;
  result?: { columns?: string[]; rows?: Record<string, unknown>[]; error?: string } | null;
  chart?: ChartSpec | null;
  desk?: DeskBrief | null;
  trades?: SuggestedTrades | null;
};

/** True when an assistant turn has anything worth showing (or merging). */
export function assistantShareTurnHasSubstance(turn: Pick<ShareTurn, "content" | "reasoning" | "sql" | "chart" | "desk" | "trades">): boolean {
  return Boolean(
    (typeof turn.content === "string" && turn.content.trim())
    || (typeof turn.reasoning === "string" && turn.reasoning.trim())
    || (typeof turn.sql === "string" && turn.sql.trim())
    || turn.chart
    || turn.desk
    || turn.trades,
  );
}

/**
 * Merge consecutive assistant turns from chatRecovery retries into one.
 * Each stalled desk attempt used to land as another empty bubble on /share
 * and in live chat (user → assistant → assistant → assistant).
 */
export function mergeAssistantShareTurns(earlier: ShareTurn, later: ShareTurn): ShareTurn {
  const desk = later.desk ?? earlier.desk;
  const content = (
    desk?.overview
    || (later.content?.trim() ? later.content : "")
    || earlier.content
    || ""
  ).trim();
  const merged: ShareTurn = {
    role: "assistant",
    content: content || ((later.reasoning || earlier.reasoning) ? "(see reasoning)" : ""),
  };
  const reasoning = (later.reasoning?.trim() || earlier.reasoning || "").trim();
  if (reasoning) merged.reasoning = reasoning;
  const sql = later.sql?.trim() || earlier.sql;
  if (sql) merged.sql = sql;
  if (later.chart || earlier.chart) merged.chart = later.chart ?? earlier.chart;
  if (desk) merged.desk = desk;
  if (later.trades || earlier.trades) merged.trades = later.trades ?? earlier.trades;
  if (later.ts != null || earlier.ts != null) merged.ts = later.ts ?? earlier.ts;
  return merged;
}

/**
 * Recover desk / trades / chart when DeepSeek left DSML tool markup in text
 * instead of structured tool parts. Strips the markup and prefers the desk
 * overview as visible content.
 */
export function healShareTurnFromDsml(turn: ShareTurn): ShareTurn {
  if (turn.role !== "assistant") return turn;
  const content = typeof turn.content === "string" ? turn.content : "";
  if (!looksLikeDsmlToolMarkup(content)) return turn;

  const calls = parseDsmlToolCalls(content);
  let desk = turn.desk ?? null;
  let trades = turn.trades ?? null;
  let chart = turn.chart ?? null;
  for (const call of calls) {
    if (call.name === "publish_desk" && !desk) {
      const next = normalizeDeskBrief(call.args as DeskBriefInput);
      if (next) desk = next;
    }
    if (call.name === "suggest_trades" && !trades) {
      const next = normalizeSuggestedTrades(call.args as { trades?: unknown; skip_reason?: unknown });
      if (next) trades = next;
    }
    if (call.name === "render_chart" && !chart) {
      const next = asChartSpec(call.args);
      if (next) chart = next;
    }
  }

  const stripped = stripDsmlToolMarkup(content);
  const nextContent = (desk?.overview || stripped || "").trim()
    || (turn.reasoning ? "(see reasoning)" : "");

  const out: ShareTurn = { ...turn, content: nextContent };
  if (desk) out.desk = desk;
  if (trades) out.trades = trades;
  if (chart) out.chart = chart;
  return out;
}

function shareTurnFromRecord(rec: Record<string, unknown>): ShareTurn {
  return {
    role: "assistant",
    content: typeof rec.content === "string" ? rec.content : "",
    ...(typeof rec.reasoning === "string" ? { reasoning: rec.reasoning } : {}),
    ...(typeof rec.sql === "string" ? { sql: rec.sql } : {}),
    ...(rec.chart && typeof rec.chart === "object" ? { chart: rec.chart as ChartSpec } : {}),
    ...(rec.desk && typeof rec.desk === "object" ? { desk: rec.desk as DeskBrief } : {}),
    ...(rec.trades && typeof rec.trades === "object" ? { trades: rec.trades as SuggestedTrades } : {}),
    ...(typeof rec.ts === "number" ? { ts: rec.ts } : {}),
  };
}

function recordFromShareTurn(turn: ShareTurn, result?: unknown): Record<string, unknown> {
  const next: Record<string, unknown> = { role: "assistant", content: turn.content };
  if (turn.reasoning) next.reasoning = turn.reasoning;
  if (turn.sql) next.sql = turn.sql;
  if (turn.chart) next.chart = turn.chart;
  if (turn.desk) next.desk = turn.desk;
  if (turn.trades) next.trades = turn.trades;
  if (turn.ts != null) next.ts = turn.ts;
  if (result != null) next.result = result;
  return next;
}

/** Collapse recovery debris: consecutive assistants → one turn; drop empty shells. */
export function coalesceAssistantShareTurns(turns: ShareTurn[]): ShareTurn[] {
  const out: ShareTurn[] = [];
  for (const raw of turns) {
    const turn = raw.role === "assistant" ? healShareTurnFromDsml(raw) : raw;
    if (turn.role !== "assistant") {
      out.push(turn);
      continue;
    }
    const prev = out[out.length - 1];
    if (prev?.role === "assistant") {
      out[out.length - 1] = mergeAssistantShareTurns(prev, turn);
      continue;
    }
    if (!assistantShareTurnHasSubstance(turn)) continue;
    out.push(turn);
  }
  return out;
}

/**
 * Same coalesce for stored share/timeline JSON rows ({role, content, …}).
 * Used on share write + public read so existing multi-bubble shares heal.
 * Also recovers DeepSeek DSML tool markup left in assistant content.
 */
export function coalesceAssistantMessageRecords(messages: unknown): Record<string, unknown>[] {
  if (!Array.isArray(messages)) return [];
  const out: Record<string, unknown>[] = [];
  for (const raw of messages) {
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) continue;
    const rec = { ...(raw as Record<string, unknown>) };
    const role = rec.role === "assistant" ? "assistant" : rec.role === "user" ? "user" : null;
    if (!role) continue;
    rec.role = role;
    if (role !== "assistant") {
      out.push(rec);
      continue;
    }
    const asTurn = healShareTurnFromDsml(shareTurnFromRecord(rec));
    // Write healed desk/content back onto the record before merge / push.
    Object.assign(rec, recordFromShareTurn(asTurn, rec.result));
    const prev = out[out.length - 1];
    if (prev?.role === "assistant") {
      const earlier = healShareTurnFromDsml(shareTurnFromRecord(prev));
      const merged = mergeAssistantShareTurns(earlier, asTurn);
      const next = recordFromShareTurn(
        merged,
        rec.result != null ? rec.result : prev.result,
      );
      out[out.length - 1] = next;
      continue;
    }
    if (!assistantShareTurnHasSubstance(asTurn) && rec.result == null) continue;
    out.push(rec);
  }
  return out;
}
type ToolPayload = {
  sql?: unknown;
  result?: { columns?: unknown; rows?: unknown } | null;
  chart?: unknown;
  desk?: unknown;
  trades?: unknown;
};

function asChartSpec(value: unknown): ChartSpec | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const rec = value as Record<string, unknown>;
  const kind = rec.kind;
  if (kind !== "line" && kind !== "area" && kind !== "scatter" && kind !== "bar") return null;
  if (typeof rec.x !== "string" || !rec.x.trim()) return null;
  if (typeof rec.y !== "string" || !rec.y.trim()) return null;
  const chart: ChartSpec = { kind, x: rec.x.trim(), y: rec.y.trim() };
  if (typeof rec.title === "string" && rec.title.trim()) chart.title = rec.title.trim();
  if (typeof rec.series === "string" && rec.series.trim()) chart.series = rec.series.trim();
  if (typeof rec.xLabel === "string" && rec.xLabel.trim()) chart.xLabel = rec.xLabel.trim();
  if (typeof rec.yLabel === "string" && rec.yLabel.trim()) chart.yLabel = rec.yLabel.trim();
  return chart;
}

function asQueryResult(value: unknown): { columns: string[]; rows: Record<string, unknown>[] } | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const rec = value as { columns?: unknown; rows?: unknown; error?: unknown };
  if (typeof rec.error === "string" && rec.error.trim()) return null;
  if (!Array.isArray(rec.columns) || !rec.columns.every((c) => typeof c === "string")) return null;
  if (!Array.isArray(rec.rows)) return null;
  return {
    columns: rec.columns as string[],
    rows: rec.rows.filter((row): row is Record<string, unknown> => Boolean(row) && typeof row === "object" && !Array.isArray(row)),
  };
}

function toolPartName(part: { type?: unknown }): string {
  if (typeof part.type !== "string") return "";
  // AI SDK tool UI parts: "tool-render_chart" / "dynamic-tool" with toolName.
  if (part.type.startsWith("tool-")) return part.type.slice("tool-".length);
  return part.type;
}

/**
 * Stamp the last assistant turn with the DO turn-budget capture.
 * Message parts sometimes omit tool outputs after headless runs / mid-turn
 * recovery; capture_json is the authoritative sql/result/chart/desk/trades from the
 * completed turn (publish_desk / suggest_trades included).
 */
export function applyCaptureToShareTurns(
  turns: ShareTurn[],
  capture: ShareCapture | null | undefined,
  question = "",
): ShareTurn[] {
  if (!capture || !turns.length) return turns;
  const out = turns.map((turn) => ({ ...turn }));
  let assistantIdx = -1;
  for (let i = out.length - 1; i >= 0; i--) {
    if (out[i].role === "assistant") {
      assistantIdx = i;
      break;
    }
  }
  if (assistantIdx < 0) return out;
  const turn = out[assistantIdx];
  const captureSql = typeof capture.sql === "string" && capture.sql.trim() ? capture.sql.trim() : null;
  if (captureSql) turn.sql = captureSql;
  let chart = turn.chart ?? asChartSpec(capture.chart);
  const result = asQueryResult(capture.result);
  if (chart && result && !chartFitsResult(chart, result.columns)) chart = null;
  if (!chart && result && wantsChart(question)) {
    chart = inferChartSpec(result.columns, result.rows);
  }
  if (chart) turn.chart = chart;
  if (!turn.desk && capture.desk) {
    const desk = normalizeDeskBrief(capture.desk);
    if (desk) {
      turn.desk = desk;
      // Desk overview is the canonical visible answer; mid-turn "Let me…"
      // narration must not win when capture recovered the desk.
      if (desk.overview) turn.content = desk.overview;
    }
  }
  if (!turn.trades && capture.trades) {
    const trades = normalizeSuggestedTrades(capture.trades);
    if (trades) turn.trades = trades;
  }
  out[assistantIdx] = turn;
  return out;
}

/** Flatten UIMessage parts into share/timeline turns (text + optional reasoning/sql/chart). */
export function extractShareTurns(messages: UIMessage[]): ShareTurn[] {
  const out: ShareTurn[] = [];
  let lastUserQuestion = "";
  for (const message of messages) {
    if (message.role !== "user" && message.role !== "assistant") continue;
    const content = message.parts
      .filter((part): part is { type: "text"; text: string } => part.type === "text" && typeof (part as { text?: string }).text === "string")
      .map((part) => part.text)
      .join("")
      .trim();
    const reasoning = message.parts
      .filter((part): part is { type: "reasoning"; text: string } => part.type === "reasoning" && typeof (part as { text?: string }).text === "string")
      .map((part) => part.text)
      .join("")
      .trim();

    let sql: string | undefined;
    let chart: ChartSpec | null = null;
    let result: { columns: string[]; rows: Record<string, unknown>[] } | null = null;
    let desk: DeskBrief | null = null;
    let trades: SuggestedTrades | null = null;
    for (const part of message.parts) {
      const name = toolPartName(part as { type?: unknown });
      const input = "input" in part ? (part as { input?: unknown }).input : undefined;
      // render_chart args are themselves a ChartSpec — keep them even when output was stripped.
      if (name === "render_chart") {
        const fromInput = asChartSpec(input);
        if (fromInput) chart = fromInput;
      }
      if (name === "publish_desk" && input && typeof input === "object") {
        const fromInput = normalizeDeskBrief(input as DeskBriefInput);
        if (fromInput) desk = fromInput;
      }
      if (name === "suggest_trades" && input && typeof input === "object") {
        const fromInput = normalizeSuggestedTrades(input as { trades?: unknown; skip_reason?: unknown });
        if (fromInput) trades = fromInput;
      }
      if (!("output" in part) || !part.output || typeof part.output !== "object") continue;
      const output = part.output as ToolPayload;
      if (typeof output.sql === "string" && output.sql.trim()) sql = output.sql.trim();
      const nextResult = asQueryResult(output.result);
      if (nextResult) {
        result = nextResult;
        if (chart && !chartFitsResult(chart, result.columns)) chart = null;
      }
      const nextChart = asChartSpec(output.chart) ?? (name === "render_chart" ? asChartSpec(input) : null);
      if (nextChart) {
        chart = nextChart;
        if (result && !chartFitsResult(chart, result.columns)) chart = null;
      }
      if (output.desk && typeof output.desk === "object") {
        const fromOutput = normalizeDeskBrief(output.desk as DeskBriefInput);
        if (fromOutput) desk = fromOutput;
      }
      if (output.trades && typeof output.trades === "object") {
        const fromOutput = normalizeSuggestedTrades(output.trades as { trades?: unknown; skip_reason?: unknown });
        if (fromOutput) trades = fromOutput;
      }
    }

    const meta = message.metadata as {
      createdAt?: number;
      sql?: string;
      chart?: unknown;
      desk?: unknown;
      trades?: unknown;
    } | undefined;
    if (!sql && typeof meta?.sql === "string" && meta.sql.trim()) sql = meta.sql.trim();
    if (!chart) {
      const metaChart = asChartSpec(meta?.chart);
      if (metaChart && (!result || chartFitsResult(metaChart, result.columns))) chart = metaChart;
    }
    if (!desk && meta?.desk && typeof meta.desk === "object") {
      const fromMeta = normalizeDeskBrief(meta.desk as DeskBriefInput);
      if (fromMeta) desk = fromMeta;
    }
    if (!trades && meta?.trades && typeof meta.trades === "object") {
      const fromMeta = normalizeSuggestedTrades(meta.trades as { trades?: unknown; skip_reason?: unknown });
      if (fromMeta) trades = fromMeta;
    }

    // Live chat falls back to inferChartSpec when the model skips render_chart;
    // mirror that for headless bot shares so timeline posts keep a figure.
    if (!chart && result && wantsChart(lastUserQuestion)) {
      chart = inferChartSpec(result.columns, result.rows);
    }

    if (message.role === "user" && content) lastUserQuestion = content;
    if (!content && !reasoning) continue;
    const turn: ShareTurn = {
      role: message.role,
      content: content || (reasoning ? "(see reasoning)" : ""),
    };
    if (reasoning) turn.reasoning = reasoning;
    if (sql) turn.sql = sql;
    if (chart) turn.chart = chart;
    if (desk) turn.desk = desk;
    if (trades) turn.trades = trades;
    if (typeof meta?.createdAt === "number" && Number.isFinite(meta.createdAt)) turn.ts = meta.createdAt;
    out.push(turn);
  }
  return coalesceAssistantShareTurns(out);
}
