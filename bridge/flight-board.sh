#!/usr/bin/env bash
# flight-board.sh — mupot flight monitor per Loom's semantics (spec 5.5).
# Usage: flight-board.sh [--once]   (default: watch loop every 60s)
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$DIR/lib.sh"

SNAPSHOT="${STATE_DIR}/flights_snapshot.json"
SQUAD_ID="${BRIDGE_SQUAD_ID:-squad-core}"

check_once() {
  local resp; resp="$(mupot_call flight_list "{\"squad_id\":\"${SQUAD_ID}\",\"limit\":100}")" || return 1
  python3 - "$resp" "${SQUAD_ID}" "${STATE_DIR}" <<'PYEOF'
import sys, json, time, os
resp = sys.argv[1]
squad = sys.argv[2] if len(sys.argv) > 2 else "squad-core"
snap_path = os.path.join(sys.argv[3] if len(sys.argv) > 3 else ".", "flights_snapshot.json")
prev = ""
try:
    with open(snap_path) as fh:  # prev is read BEFORE the new snapshot is written
        prev = fh.read()
except Exception:
    pass
try:
    d = json.loads(resp)
    payload = json.loads(d["result"]["content"][0]["text"])
    flights = payload["result"]["flights"]
except Exception as e:
    print(f"flight_board parse error: {e}"); sys.exit(0)

now = time.time()
lines = []
changed = []
try:
    _p = json.loads(prev)
    _pp = json.loads(_p["result"]["content"][0]["text"])
    prev_flights = {f.get("id"): f for f in _pp["result"]["flights"]} if prev else {}
except Exception:
    prev_flights = {}

for f in flights:
    fid = f.get("id", "?")
    sid = fid[:8]
    st = f.get("status", "?")
    gv = f.get("gate_verdict", "-")
    score = f.get("score", 0.0) or 0.0
    age = ""
    if f.get("created_at"):
        try:
            age = f"{int((now - f['created_at']/1000)/60)}m" if f["created_at"] > 1e12 else f"{(f['created_at'] or '')[:10]}"
        except Exception:
            age = ""
    line = f"{sid} {st} gate={gv} score={score:.4f}"
    if st == "held":
        if score <= 0.006:
            line += " [PHANTOM: empty signals]"
        elif score < 0.5:
            line += f" [GATE HOLD: {f.get('gate_reason','?')}]"
        else:
            line += " [held]"
    elif st == "running":
        cost = f.get("cost_micro_usd", 0) or 0
        if cost == 0 and f.get("started_at"):
            try:
                if now - f["started_at"]/1000 > 1800:
                    line += " [STUCK-RUNNING: cost=0, age>30m]"
            except Exception:
                pass
        line += f" age={age}"
    elif st in ("landed", "failed", "waiting", "sleeping"):
        line += f" age={age}"
    lines.append(line)
    if fid in prev_flights and (prev_flights[fid].get("status") != st or prev_flights[fid].get("gate_verdict") != gv):
        changed.append(f"CHANGED: {line}")

out = "\n".join(lines)
print(f"FLIGHT BOARD ({squad}) — {len(flights)} flights")
print(out)
if changed:
    print("\nCHANGED SINCE LAST POLL:")
    for c in changed:
        print(c)
# persist the new snapshot ONLY after classification (so prev stays meaningful)
with open(snap_path, "w") as fh:
    fh.write(resp)
PYEOF
}

if [ "${1:-}" = "--once" ]; then
  check_once
  exit 0
fi

log "flight-board watch started (60s)"
while true; do
  check_once | tee "${STATE_DIR}/flight_board.out"
  sleep 60
done
