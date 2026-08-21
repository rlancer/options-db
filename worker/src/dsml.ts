/**
 * DeepSeek V4 DSML (DeepSeek Markup Language) tool-call markup.
 *
 * When OpenRouter/AI SDK fail to promote native DSML into structured tool_calls,
 * the model writes the invoke block into assistant text. Shares and timeline
 * posts then store raw `<｜DSML｜tool_calls>…` instead of desk/trades/chart.
 *
 * Parse and strip that markup so transcripts stay structured.
 */

/** Fullwidth vertical line U+FF5C — DeepSeek's DSML delimiter token. */
const DSML = "\uFF5CDSML\uFF5C";

const TOOL_CALLS_RE = new RegExp(
  `<${DSML}tool_calls>([\\s\\S]*?)</${DSML}tool_calls>`,
  "g",
);
const INVOKE_RE = new RegExp(
  `<${DSML}invoke\\s+name="([^"]+)">([\\s\\S]*?)</${DSML}invoke>`,
  "g",
);
const PARAM_RE = new RegExp(
  `<${DSML}parameter\\s+name="([^"]+)"(?:\\s+string="(true|false)")?>([\\s\\S]*?)</${DSML}parameter>`,
  "g",
);

export type DsmlToolCall = {
  name: string;
  args: Record<string, unknown>;
};

/** True when text contains a DSML tool_calls block (or a truncated open tag). */
export function looksLikeDsmlToolMarkup(text: string): boolean {
  if (!text) return false;
  return text.includes(`<${DSML}tool_calls>`) || text.includes(`<${DSML}invoke`);
}

function parseParamValue(raw: string, stringAttr: string | undefined): unknown {
  const value = raw.trim();
  if (stringAttr === "true") return value;
  if (stringAttr === "false") {
    if (!value) return null;
    try {
      return JSON.parse(value) as unknown;
    } catch {
      return value;
    }
  }
  // Attribute omitted — try JSON, else keep the literal string.
  if (/^(?:true|false|null|-?\d+(?:\.\d+)?)$/.test(value) || value.startsWith("{") || value.startsWith("[")) {
    try {
      return JSON.parse(value) as unknown;
    } catch {
      /* fall through */
    }
  }
  return value;
}

function parseInvokeBody(body: string): Record<string, unknown> {
  const args: Record<string, unknown> = {};
  PARAM_RE.lastIndex = 0;
  let match: RegExpExecArray | null;
  while ((match = PARAM_RE.exec(body)) !== null) {
    const name = match[1]?.trim();
    if (!name) continue;
    args[name] = parseParamValue(match[3] ?? "", match[2]);
  }
  return args;
}

/** Extract structured tool calls from DSML markup embedded in model text. */
export function parseDsmlToolCalls(text: string): DsmlToolCall[] {
  if (!looksLikeDsmlToolMarkup(text)) return [];
  const out: DsmlToolCall[] = [];
  TOOL_CALLS_RE.lastIndex = 0;
  let block: RegExpExecArray | null;
  const bodies: string[] = [];
  while ((block = TOOL_CALLS_RE.exec(text)) !== null) {
    bodies.push(block[1] ?? "");
  }
  // Truncated / unclosed tool_calls (share content caps) — still parse invokes.
  if (!bodies.length) bodies.push(text);

  for (const body of bodies) {
    INVOKE_RE.lastIndex = 0;
    let invoke: RegExpExecArray | null;
    while ((invoke = INVOKE_RE.exec(body)) !== null) {
      const name = invoke[1]?.trim();
      if (!name) continue;
      out.push({ name, args: parseInvokeBody(invoke[2] ?? "") });
    }
  }
  return out;
}

/**
 * Remove DSML tool_calls / orphan invoke blocks from text.
 * Returns trimmed remainder (may be empty when the turn was tool-only markup).
 */
export function stripDsmlToolMarkup(text: string): string {
  if (!looksLikeDsmlToolMarkup(text)) return text;
  let out = text.replace(TOOL_CALLS_RE, "");
  // Orphan invoke left after a truncated outer wrapper.
  out = out.replace(
    new RegExp(`<${DSML}invoke\\s+name="[^"]+">[\\s\\S]*?</${DSML}invoke>`, "g"),
    "",
  );
  // Stray open/close tags if the share content was byte-capped mid-block.
  out = out.replace(new RegExp(`</?${DSML}[^>]*>`, "g"), "");
  return out.replace(/\n{3,}/g, "\n\n").trim();
}
