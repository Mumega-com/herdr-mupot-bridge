#!/usr/bin/env bash
# lib.sh — shared helpers for herdr-mupot-bridge
set -uo pipefail

BRIDGE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${BRIDGE_DIR}/state"
LOG="${STATE_DIR}/bridge.log"
MUPOT_URL="https://mupot.mumega.com/mcp"
TOKEN_FILE="${HERDR_MUPOT_TOKEN_FILE:-/home/mumega/.fleet/agents/river-agent-bound.token}"
HERDR_BIN="${HERDR_BIN_PATH:-/home/mumega/.local/bin/herdr}"

mkdir -p "$STATE_DIR"

log() { printf '%s %s\n' "$(date -Iseconds)" "$*" >> "$LOG"; }

# mupot_call <tool> <json-args>  -> prints the result JSON on stdout
mupot_call() {
  local tool="$1" args="$2"
  [ -z "${args:-}" ] && args="{}"
  # WARN 4: read-only allowlist — the plugin's selling point is structural, not conventional
  case "$tool" in
    flight_list|inbox|peers|status|resolve_agent|flight_get|task_list|project_list|presence_list)
      ;;
    *)
      log "REFUSED non-readonly tool: $tool"
      return 1
      ;;
  esac
  if [ ! -f "$TOKEN_FILE" ]; then
    log "ERROR: token file missing: $TOKEN_FILE"
    return 1
  fi
  local tok; tok="$(cat "$TOKEN_FILE")"
  curl -sS -X POST "$MUPOT_URL" \
    -H "Authorization: Bearer ${tok}" \
    -H "Content-Type: application/json" \
    -d "{\"jsonrpc\":\"2.0\",\"id\":\"bridge\",\"method\":\"tools/call\",\"params\":{\"name\":\"${tool}\",\"arguments\":${args}}}"
}


# mupot_ack <to> <in_reply_to> <body> — the plugin's ONLY write: a delivery ack.
# Restricted by construction: kind=ack + in_reply_to are hard-coded; no free-form sends.
mupot_ack() {
  local to="$1" in_reply_to="$2" body="$3"
  [ -n "$in_reply_to" ] || { log "ack REFUSED: missing in_reply_to"; return 1; }
  [ -f "$TOKEN_FILE" ] || { log "ack REFUSED: no token"; return 1; }
  local tok; tok="$(cat "$TOKEN_FILE")"
  local payload
  payload="$(python3 -c '
import json,sys
print(json.dumps({"jsonrpc":"2.0","id":"bridge-ack","method":"tools/call","params":{"name":"send","arguments":{"to":sys.argv[1],"body":sys.argv[2],"kind":"ack","in_reply_to":sys.argv[3]}}}))
' "$to" "$body" "$in_reply_to")"
  curl -sS -X POST "$MUPOT_URL" -H "Authorization: Bearer ${tok}" -H "Content-Type: application/json" -d "$payload"     | python3 -c 'import sys,json
try:
    d=json.load(sys.stdin)
    r=d["result"]["content"][0]["text"]
    print(r)
except Exception as e:
    print("ack-error", e)' 2>/dev/null || echo "ack-error"
  log "ack sent to=$to in_reply_to=$in_reply_to"
}

# herdr agents as JSON array (jq-free: python3)
herdr_agents_json() {
  "$HERDR_BIN" agent list 2>/dev/null | python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)
    for a in d.get("result", {}).get("agents", []):
        print(json.dumps(a))
except Exception:
    pass'
}

