#!/usr/bin/env bash
# start.sh — startup hook: launch the watch loops (flight board + presence report)
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$DIR/lib.sh"

log "bridge start"
nohup bash "$DIR/flight-board.sh" >/dev/null 2>&1 &
nohup bash -c 'while true; do bash "'$DIR'/presence-report.sh" >/dev/null 2>&1; sleep 60; done' >/dev/null 2>&1 &
log "loops launched (flight-board, presence-report)"
