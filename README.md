# herdr-mupot-bridge v0.3.0

Mupot record <-> herdr board bridge. Spec: `docs/herdr/herdr-mupot-bridge-plugin.md`.
Flight semantics contract: Loom (spec §5.5).

## Capabilities (v0)
- **flight-board** — polls `flight_list(squad-core)`, classifies per Loom's rules
  (PHANTOM ≤0.006 / GATE HOLD 0.006–0.5 with gate_reason / STUCK-RUNNING cost=0 age>30m),
  prints the board + change deltas. `--once` for a single check.
- **presence-report** — observed herdr states (`herdr agent list`) -> `state/board.json`.
- **inbox-deliver** — v1 deliver hop: mupot inbox -> seat pane. Peek (never consumes by accident); delivers to the seat's OWN pane (prime-agent seats via pane send-text, claude/hermes via agent prompt); dedupes delivered seqs (`state/delivered-<seat>.seq`); logs every delivery. Per-seat: `BRIDGE_SEAT`, `BRIDGE_TOKEN_FILE`.

## Usage
```bash
herdr plugin link /mnt/HC_Volume_104325311/mumega.com/agents/river/herdr-mupot-bridge   # register in herdr
herdr plugin action invoke herdr-mupot-bridge flight-board --once                       # single flight check
bash bridge/flight-board.sh --once                                                      # or direct
cat state/flight_board.out                                                               # watch loop output
```

## Config
- `HERDR_MUPOT_TOKEN_FILE` — mupot member token (default `~/.fleet/agents/river.token`, today-minted)
- `BRIDGE_SQUAD_ID` — squad to monitor (default `squad-core`)

## Safety
- Read-only mupot calls, enforced by an allowlist in `mupot_call` (structural gate): `flight_list, inbox, peers, status, resolve_agent, flight_get, task_list, project_list, presence_list` — anything else is REFUSED and logged. No mutations, no send, no dispatch.
- Token: today-minted member token only; never relayed.
- State + logs under `state/`.

## Workflow (this build)
1. Built by River · 2. Tested by Loom (flight-controller) · 3. Gated by Kasra (Author≠Gate).

## bridge/seatlink.mjs v0.3.0 (added 2026-09-03 — this repo is now its canonical home)

`bridge/inbox-deliver.sh` above was v1's poll-every-N-seconds deliver hop. It was superseded
on the live host by an event-driven Node rewrite (`seatlink.mjs`, subscribes to Herdr's own
event socket instead of polling) that was **hand-deployed to
`~/.local/share/mupot-seatlink/bridge/seatlink.mjs` and never committed anywhere** — no git
history, no tests, no review trail, exactly the un-versioned "SSH-patched" state the rest of
this repo's workflow exists to avoid. `bridge/seatlink.mjs` in this commit is that live script,
reproduced verbatim (2026-09-03), plus a fix for one measured defect
([mumega-com#1179](https://github.com/Mumega-com/mumega-com/issues/1179)):

**The defect.** seatlink defers mupot mail for a busy Herdr seat (queued in memory, not
delivered) and drains the queue unconditionally once the seat goes idle. Between defer and
drain the seat can consume that same message through a different channel entirely — a direct
`inbox`/`inbox_ack` MCP call mid-turn, or the Stop hook between turns — and the deferred queue
has no way to learn that happened. Drain then re-offers already-settled mail, with wording that
pushes the reader to re-act on work that is already done. Measured on host `muvps`, seat
`muvps_kasra`: 13+ replayed offers in one flight, every one for a message already consumed
before the offer arrived.

**What was ruled out.** The obvious first suspects — re-delivery of the same delivery id, and
SSE cursor rollback — were both measured and dead (see the issue). A third candidate, "a fresh
SSE reconnect with a stale cursor re-surfaces an already-acked row", was tested directly against
mupot's real schema (`readAgentInboxForReader`'s `read_at IS NULL` predicate) in the sibling
mupot PR (`kasra/ack-terminal-marker-and-seatlink-ack-reconcile`) and does **not** reproduce —
the server-side query correctly excludes already-acked rows regardless of how stale the client's
cursor is. The deferred-queue path here is the one place in the bridge that holds a snapshot
across unbounded elapsed time without ever re-validating it, which is why it is the fix target.

**The fix.** `drainDeferred()` now reconciles its queue against the pot's *current* unread state
(`fetchUnreadIds`, a plain `GET /api/inbox?peek=1&limit=100`) before delivering anything, via
`bridge/lib/reconcile.mjs`. Reconciliation fails OPEN: a network/parse error delivers the queue
unchanged, because dropping a message that is genuinely still unread is silent message loss —
strictly worse than the false-work this fix removes. A second, smaller fix in the same commit:
`formatPotMail()` used to say "send the required correlated ACK" on every offer unconditionally;
it now adopts `mupot#1278` / `#1280` (`expects_reply` and `reply_basis === "request_id_field"`).
Terminal `kind:"ack"` messages retain `request_id` solely as a replay-once idempotency key
under migration 0032, but report `expects_reply: false`. Messages carrying quoted request IDs
(`reply_basis: "body_token"`) or `expects_reply: false` resolve to "No ACK is required", terminating
ACK loops cleanly while preserving deduplication idempotency.

**Tests.** `npm test` (vitest) — `tests/reconcile.test.mjs` covers the pure filter/fail-open
policy; `tests/seatlink.test.mjs` covers the wiring (`fetchUnreadIds`'s HTTP call,
`formatPotMail`'s conditional wording, `drainDeferred`'s re-entrancy guard and stale-drop
behavior). Every guard in both files was mutation-tested by hand (invert/weaken/delete the
condition, confirm the suite fails) before being trusted.

**Deploying this is NOT part of this change.** The live host still runs the untracked copy at
`~/.local/share/mupot-seatlink/bridge/seatlink.mjs` under `mupot-seatlink.service`. Bringing the
host in line with this repo means: `rsync bridge/ ~/.local/share/mupot-seatlink/bridge/` (or
equivalent) and `systemctl --user restart mupot-seatlink`. That is an operator action, gated the
same way any daemon restart is — it is deliberately not automated by landing this PR.
