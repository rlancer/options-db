import assert from "node:assert/strict";
import { test } from "node:test";
import { looksLikeDsmlToolMarkup, parseDsmlToolCalls, stripDsmlToolMarkup } from "../src/dsml.ts";
import {
  coalesceAssistantMessageRecords,
  extractShareTurns,
  healShareTurnFromDsml,
  type ShareTurn,
} from "../src/share-turns.ts";
import type { UIMessage } from "ai";
import { excerptFromMessages, previewMessagesFromShare } from "../src/timeline.ts";

const DSML = "\uFF5CDSML\uFF5C";

/** Regression fixture: share 1QP3jquD3GEOwXE7rDdvaoX2M (@nowlobster) stored raw DSML. */
function samplePublishDeskDsml(): string {
  return [
    `<${DSML}tool_calls>`,
    `<${DSML}invoke name="publish_desk">`,
    `<${DSML}parameter name="fundamental" string="true">MRNA is the marquee catalyst (+12% today) while semis lag.</${DSML}parameter>`,
    `<${DSML}parameter name="macro" string="true">Rates add no inversion spark: 2s 4.19%, 10s 4.65%, curve steep.</${DSML}parameter>`,
    `<${DSML}parameter name="options" string="true">NVDA 220 line prints heavy two-sided flow; VSAT carries 411K OI.</${DSML}parameter>`,
    `<${DSML}parameter name="overview" string="true">The index fade is a sector rotation, not broad de-grossing: SPY soft, QQQ lagging on semis/AI, idiosyncratic longs carrying the tape.</${DSML}parameter>`,
    `<${DSML}parameter name="technical" string="true">Three-session fade in SPY/QQQ with damage concentrated in large-cap tech.</${DSML}parameter>`,
    `</${DSML}invoke>`,
    `</${DSML}tool_calls>`,
  ].join("\n");
}

test("parseDsmlToolCalls recovers publish_desk string parameters", () => {
  const calls = parseDsmlToolCalls(samplePublishDeskDsml());
  assert.equal(calls.length, 1);
  assert.equal(calls[0].name, "publish_desk");
  assert.equal(calls[0].args.fundamental, "MRNA is the marquee catalyst (+12% today) while semis lag.");
  assert.match(String(calls[0].args.overview), /sector rotation/);
  assert.match(String(calls[0].args.technical), /Three-session fade/);
});

test("parseDsmlToolCalls parses JSON parameters when string=false", () => {
  const text = [
    `<${DSML}tool_calls>`,
    `<${DSML}invoke name="render_chart">`,
    `<${DSML}parameter name="kind" string="true">line</${DSML}parameter>`,
    `<${DSML}parameter name="x" string="true">date</${DSML}parameter>`,
    `<${DSML}parameter name="y" string="true">close</${DSML}parameter>`,
    `<${DSML}parameter name="series" string="true">symbol</${DSML}parameter>`,
    `<${DSML}parameter name="title" string="true">Index posture</${DSML}parameter>`,
    `</${DSML}invoke>`,
    `<${DSML}invoke name="suggest_trades">`,
    `<${DSML}parameter name="trades" string="false">[{"ticker":"NVDA","bias":"bearish","conviction":"medium","structure":"put debit","legs":[{"right":"put","side":"buy","strike":220,"expiration":"2026-09-18","dte":28}],"rationale":"Semis fade with two-sided 220 flow"}]</${DSML}parameter>`,
    `</${DSML}invoke>`,
    `</${DSML}tool_calls>`,
  ].join("\n");
  const calls = parseDsmlToolCalls(text);
  assert.equal(calls.length, 2);
  assert.equal(calls[0].name, "render_chart");
  assert.equal(calls[0].args.kind, "line");
  assert.equal(calls[1].name, "suggest_trades");
  assert.ok(Array.isArray(calls[1].args.trades));
  assert.equal((calls[1].args.trades as { ticker: string }[])[0].ticker, "NVDA");
});

test("stripDsmlToolMarkup removes tool blocks", () => {
  const mixed = `Tape check.\n${samplePublishDeskDsml()}\nDone.`;
  assert.equal(stripDsmlToolMarkup(mixed), "Tape check.\n\nDone.");
  assert.equal(stripDsmlToolMarkup(samplePublishDeskDsml()), "");
  assert.equal(looksLikeDsmlToolMarkup(samplePublishDeskDsml()), true);
  assert.equal(looksLikeDsmlToolMarkup("plain prose"), false);
});

test("healShareTurnFromDsml builds desk and replaces content with overview", () => {
  const turn: ShareTurn = {
    role: "assistant",
    content: samplePublishDeskDsml(),
    sql: "SELECT 1",
  };
  const healed = healShareTurnFromDsml(turn);
  assert.ok(healed.desk);
  assert.match(healed.desk!.overview, /sector rotation/);
  assert.match(healed.desk!.fundamental!, /MRNA/);
  assert.match(healed.desk!.options!, /VSAT/);
  assert.equal(healed.content, healed.desk!.overview);
  assert.equal(healed.sql, "SELECT 1");
  assert.equal(looksLikeDsmlToolMarkup(healed.content), false);
});

test("coalesceAssistantMessageRecords heals stored DSML shares", () => {
  const out = coalesceAssistantMessageRecords([
    { role: "user", content: "Hourly market overview" },
    {
      role: "assistant",
      content: samplePublishDeskDsml(),
      sql: "SELECT symbol, date, close FROM options.ohlc LIMIT 10",
      chart: { kind: "line", x: "date", y: "close", series: "symbol" },
    },
  ]);
  assert.equal(out.length, 2);
  const assistant = out[1];
  assert.ok(assistant.desk && typeof assistant.desk === "object");
  assert.match(String((assistant.desk as { overview: string }).overview), /sector rotation/);
  assert.equal(assistant.content, (assistant.desk as { overview: string }).overview);
  assert.equal(looksLikeDsmlToolMarkup(String(assistant.content)), false);
});

test("extractShareTurns recovers DSML publish_desk from text parts", () => {
  const messages = [
    {
      id: "1",
      role: "user",
      parts: [{ type: "text", text: "Hourly market overview" }],
    },
    {
      id: "2",
      role: "assistant",
      parts: [{ type: "text", text: samplePublishDeskDsml() }],
    },
  ] as UIMessage[];
  const turns = extractShareTurns(messages);
  assert.equal(turns.length, 2);
  assert.ok(turns[1].desk);
  assert.match(turns[1].desk!.overview, /sector rotation/);
  assert.equal(turns[1].content, turns[1].desk!.overview);
});

test("timeline preview and excerpt hide DSML after heal", () => {
  const messages = [
    { role: "user", content: "Hourly market overview" },
    { role: "assistant", content: samplePublishDeskDsml() },
  ];
  const preview = previewMessagesFromShare(messages);
  assert.equal(preview.length, 2);
  assert.ok(preview[1].desk);
  assert.equal(looksLikeDsmlToolMarkup(String(preview[1].content)), false);
  const excerpt = excerptFromMessages(messages, null);
  assert.equal(looksLikeDsmlToolMarkup(excerpt), false);
  assert.match(excerpt, /sector rotation/);
});
