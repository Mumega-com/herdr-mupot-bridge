#!/usr/bin/env bash
# inbox-deliver.sh — mupot inbox -> seat pane (v1.1: deliver hop + receipts + ack).
# Per-seat: BRIDGE_SEAT (default river), BRIDGE_TOKEN_FILE (default river-agent-bound.token).
# Peek (never consumes by accident); deliver to the seat's OWN pane; write a delivery receipt;
# send an ACK (kind=ack, in_reply_to) — the plugin's ONLY write, ack-only by construction.
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$DIR/lib.sh"

SEAT="${BRIDGE_SEAT:-river}"
NAME="muvps_${SEAT}"
DONE_FILE="${STATE_DIR}/delivered-${SEAT}.seq"
RECEIPTS_FILE="${STATE_DIR}/receipts-${SEAT}.jsonl"
TMP_OUT="${STATE_DIR}/deliver-out-${SEAT}.txt"

# locate the seat's pane + kind from herdr (observed, not assumed)
pane=""; kind=""
while IFS= read -r line; do
  nm="$(printf '%s' "$line" | python3 -c 'import sys,json
try: print(json.load(sys.stdin).get("name") or "")
except Exception: print("")' 2>/dev/null)"
  if [ "$nm" = "$NAME" ]; then
    pane="$(printf '%s' "$line" | python3 -c 'import sys,json
try: print(json.load(sys.stdin).get("pane_id") or "")
except Exception: print("")' 2>/dev/null)"
    kind="$(printf '%s' "$line" | python3 -c 'import sys,json
try: print(json.load(sys.stdin).get("agent") or "")
except Exception: print("")' 2>/dev/null)"
  fi
done < <(herdr_agents_json)

if [ -z "$pane" ]; then
  log "deliver: no live pane for seat $SEAT ($NAME) — message stays queued"
  echo "NO-PANE: $SEAT has no live herdr pane; nothing delivered"
  exit 0
fi

# peek the seat's inbox (read-only; does NOT consume)
resp="$(mupot_call inbox "{\"limit\":5,\"peek\":true}")"

# classify + deliver + write receipts (stdout -> temp file; no command-substitution/heredoc interplay)
python3 - "$resp" "$pane" "$kind" "$SEAT" "$NAME" "${DONE_FILE}" "${RECEIPTS_FILE}" <<'PYEOF' > "$TMP_OUT"
import sys, json, subprocess, time, os, datetime
H = os.environ.get("HERDR_BIN") or "herdr"
resp, pane, kind, seat, name, done_file, receipts_file = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7]
try:
    d = json.loads(resp)
    text = d["result"]["content"][0]["text"]
    msgs = json.loads(text)["result"]["messages"]
except Exception as e:
    print(f"inbox: unparsed ({e})"); sys.exit(0)

done = set()
try:
    with open(done_file) as fh:
        done = {int(x) for x in fh.read().split()}
except Exception:
    pass

if not msgs:
    print(f"INBOX({seat}): 0 pending — nothing to deliver")
    sys.exit(0)

print(f"INBOX({seat}): {len(msgs)} pending -> delivering to {name} ({pane}, {kind})")
for m in msgs:
    seq = m.get("seq")
    if seq in done:
        print(f"  seq={seq} -> already delivered (skip)")
        continue
    body = (m.get("body") or "").strip().replace("\n", " ")
    body = body[:600]
    prefix = f"[bridge-delivery seq={seq} from={str(m.get('from_agent',''))[:8]}] "
    if kind in ("claude", "hermes"):
        r = subprocess.run([H, "agent", "prompt", name, prefix + body, "--wait", "--timeout", "60000"], capture_output=True, text=True)
        ok = r.returncode == 0
    else:
        r1 = subprocess.run([H, "pane", "send-text", pane, prefix + body], capture_output=True, text=True)
        time.sleep(0.5)
        r2 = subprocess.run([H, "pane", "send-keys", pane, "enter"], capture_output=True, text=True)
        ok = r1.returncode == 0 and r2.returncode == 0
    print(f"  seq={seq} -> {'DELIVERED' if ok else 'FAILED'} ({kind})")
    if ok:
        with open(done_file, "a") as fh:
            fh.write(f"{seq}\n")
        rec = {"ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "msg_id": m.get("id"), "seq": seq, "from_agent": m.get("from_agent"),
               "delivered_to_pane": pane, "via": kind, "seat": seat}
        with open(receipts_file, "a") as fh:
            fh.write(json.dumps(rec) + "\n")
        print(f"  receipt: {rec['msg_id'][:8]} seq={seq} pane={pane}")
        print(f"ACK {m.get('from_agent','')} {m.get('id','')}")
PYEOF

cat "$TMP_OUT"

# ACK receipts on mupot (the plugin's ONLY write: kind=ack, in_reply_to — never free-form)
grep '^ACK ' "$TMP_OUT" | while read -r _ from mid; do
  mupot_ack "$from" "$mid" "delivered via herdr bridge to pane $pane (seat $SEAT)"
done

log "inbox-deliver: seat=$SEAT pane=$pane kind=$kind receipts=$RECEIPTS_FILE"
