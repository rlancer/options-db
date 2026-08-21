# Plan: Twitter / X → Lobster

> **Transient plan** (see `AGENTS.md` docs policy). Lives on the feature branch while
> this is in flight. Before merge to production behavior: fold still-true facts into
> `README.md` / package `AGENTS.md` and delete this file.

**Outcome:** people `@` the lobster on X; it replies with a short answer + link; the
full Q&A lands on lobster.mp (share + optional timeline).

**Product shape:** thin social front door — not a full Copilot port into tweets.
Reuse `shared_chats` / timeline under a system `@lobster` identity; do not invent a
third public feed.

---

## Phase 0 — Product decisions

**Owner: you (manual).** Decide before any build.

| Decision | Why it matters |
| --- | --- |
| Account handle (`@lobster_mp` vs similar) | Brand, searchability |
| Reply style: one tweet vs short thread | Latency + abuse surface |
| Always link to `/share/{id}`? | Keeps richness on-site |
| Public timeline auto-post vs share-only | Noise vs discovery |
| Who can trigger: anyone vs followers-only vs allowlist | Cost control |
| Languages / non-options spam policy | Support burden |

**Automate later:** none of this.

---

## Phase 1 — X account + API access

**Owner: you (manual / owner-only).**

1. Create the X account and verify email/phone.
2. Apply for **X Developer** access (project + app).
3. Enable **read + write + mentions** (and webhooks if that route is chosen).
4. Generate API keys / OAuth tokens; store them as **Worker secrets** (never in git).
5. Set bio, avatar, pinned tweet pointing at lobster.mp + “ask by mentioning.”

**Can be automated after secrets exist:** posting, mention polling/webhooks, reply
formatting — once credentials are in the Worker.

**You keep forever:** renewing tokens if X rotates them, handling app suspension /
policy changes, paying the X API tier.

---

## Phase 2 — Cost & abuse gates

**Do not ship without this.** Policy knobs are yours; enforcement is automated.

**You set policy:**

- Max mentions processed / hour / day
- Max question length
- Reject obvious spam / “ignore me” patterns
- Whether non-followers are ignored
- Model + max tool steps for the Twitter path (cheaper/shorter than web Copilot)

**Can be automated:**

- Rate counters in D1 / Durable Object
- Deduping the same tweet
- Ignoring retweets / self-replies / bot reply loops
- Canned “rate limited, try the site” tweet

---

## Phase 3 — Core pipeline

Happy path:

`mention → create chat → constrained Copilot turn → short reply tweet → mint share → (optional) timeline post`

| Piece | Owner |
| --- | --- |
| Ingress (webhook or poller) | Automate |
| Drive Copilot server-side (no browser WebSocket) | Automate |
| Tweet-sized system prompt + truncated tools | Automate (you review copy) |
| `shared_chats` snapshot + `/share/{id}` link | Automate (reuse existing) |
| Post under a fixed `@lobster` UI identity | Automate after you claim/create that handle |
| Secrets, X app, handle claim | **You** |

**You still do:** first live smoke tests on a private/test account; watch OpenRouter
spend for a few days.

**Architecture notes (not a full design):**

- There is no HTTP “ask Copilot” API today — product path is Agents WebSocket. Bot
  needs a server-side one-shot runner of the same agent.
- Copilot burns the site OpenRouter key with thin per-turn rate limiting; mention
  quotas are a prerequisite.
- Answer shape mismatch: tools produce SQL/tables/charts; X needs a one-liner +
  link. Separate social system prompt + truncated tool budget.
- Latency: multi-step lake turns vs X expectations — prefer ack + follow-up, or a
  hard short tool budget for social.
- Mentions are not Better Auth users — keep system-owned shares (`source=twitter`),
  do not claim `user_chats`.

---

## Phase 4 — UX on lobster.mp

Minimal:

- Shares from X tagged `source=twitter` (or equivalent)
- Optional filter / badge on timeline: “via X”
- Share page shows original tweet URL when present

**You decide:** whether Twitter-sourced posts appear on the main `/` timeline by
default, or only under `/u/lobster` (or a dedicated feed).

**Recommendation:** system-handle timeline first; main home feed later once quality
looks good.

---

## Phase 5 — Ops & failure modes

| Situation | You | Automate |
| --- | --- | --- |
| Lake / API down | Monitor / decide kill switch | Auto-reply “try again later” + skip share |
| OpenRouter blowup | Raise/lower budgets, pause bot | Hard daily spend / mention caps |
| Bad / wrong answers going viral | Mute / delete tweets, pause feature | Log every turn; link to share for audit |
| X API outage / 429 | Wait / upgrade tier | Backoff, queue, don’t double-reply |
| Legal / ToS / impersonation | Policy, account ownership | N/A |

Add a **kill switch** (Worker env / flag) you can flip without a full feature
redeploy if possible.

---

## Suggested ship order

1. **You:** account + API + secrets + product knobs (Phase 0–1).
2. **Build:** dry-run mode — process mentions, write shares, **don’t tweet** yet.
3. **You:** review 20–50 dry-run shares for quality / cost.
4. **Build:** enable replies on a private test account.
5. **You:** go public; pin instructions; watch spend 1–2 weeks.
6. **Later:** auto-timeline on `/`, richer threads, follower-gated mode — only if
   Phase 5 is calm.

---

## You vs automate (summary)

**Only you can / should do**

- Create & own the X account and developer app
- Put API secrets in Cloudflare
- Claim the public lobster handle on lobster.mp
- Set spend / abuse policy and kill switch
- Approve “go public” after dry-run review
- Handle X policy, billing tier, and account reputation

**Safe to automate**

- Mention intake, dedupe, rate limits
- Constrained Copilot runs
- Short reply + share link
- Recording into `shared_chats` / timeline
- Retries, backoff, canned failure tweets
- Logging for cost and content audit

---

## Out of scope for v1

- DMs as sessions
- Full tool dumps / charts in tweets
- Logged-in X identity mapping to Better Auth users
- Real-time debate threads with unlimited follow-ups

---

## Done when

Someone can `@` the account with a market question, get a short reply with a working
`/share/...` link, see the full answer on the site, and daily OpenRouter + X costs
stay inside the caps you set — with a one-flag way to pause replies.
