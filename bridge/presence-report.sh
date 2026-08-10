#!/usr/bin/env bash
# presence-report.sh — observed herdr states -> board report (v0: report)
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$DIR/lib.sh"

OUT="${STATE_DIR}/board.json"
herdr_agents_json > "$OUT" 2>/dev/null
python3 - "$OUT" <<'PYEOF'
import sys, json
try:
    with open(sys.argv[1]) as fh:
        agents = [json.loads(l) for l in fh if l.strip()]
except Exception:
    agents = []
print("BOARD — %d agents" % len(agents))
for a in agents:
    print("  %-16s %-12s %-10s %s" % (a.get("name") or a.get("agent","-"), a.get("agent","-"), a.get("agent_status","?"), a.get("pane_id","-")))
PYEOF
