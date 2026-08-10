# herdr-mupot-bridge v0.1.0

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
