#!/usr/bin/env bash
# Does probe-origin.sh reach the right conclusion?
#
#   bash tests/probe-origin.test.sh
#
# The script's entire value is its verdict, so that is what gets asserted here: an origin that
# caps each connection has to produce "parallel", one that caps the pipe has to produce "302",
# and the failure shapes that used to produce confident nonsense have to produce no verdict at
# all. tests/origin-sim.py supplies each shape; nothing external is needed.
#
# Ranges are small and the caps are generous so the whole thing runs in well under a minute.
# That is fine here - what is under test is the decision, not the measurement floor.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
PROBE="$HERE/../probe-origin.sh"
SIM="$HERE/origin-sim.py"
PORT="${PORT:-18099}"
PY="${PYTHON:-python3}"

[ -f "$PROBE" ] || { echo "x $PROBE not found"; exit 2; }
command -v "$PY" >/dev/null 2>&1 || { echo "x $PY not found"; exit 2; }

PASS=0; FAIL=0; PID=""
OUT="$(mktemp "${TMPDIR:-/tmp}/probe-test.XXXXXX")"
cleanup() { [ -n "$PID" ] && kill "$PID" 2>/dev/null; rm -f "$OUT"; }
trap cleanup EXIT INT TERM

start_sim() {   # $1 = MODE, $2 = RATE
  [ -n "$PID" ] && { kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null; PID=""; }
  PORT=$(( PORT + 1 ))
  MODE="$1" RATE="$2" SIZE=201326592 "$PY" "$SIM" "$PORT" >/dev/null 2>&1 &
  PID=$!
  i=0
  while [ "$i" -lt 50 ]; do
    [ "$(curl -s -o /dev/null --max-time 1 -w '%{http_code}' "http://127.0.0.1:$PORT/ping")" = "204" ] && return 0
    i=$(( i + 1 )); sleep 0.2
  done
  echo "x simulator did not come up on port $PORT"; return 1
}

check() {   # $1 = name, $2 = expected substring, $3.. = env for the probe
  name="$1"; want="$2"; shift 2
  if grep -qF "$want" "$OUT"; then
    echo "  ok   $name"
    PASS=$(( PASS + 1 ))
  else
    echo "  FAIL $name"
    echo "       expected to find: $want"
    sed 's/^/       | /' "$OUT" | tail -12
    FAIL=$(( FAIL + 1 ))
  fi
}

run_probe() {   # remaining args go to probe-origin.sh
  PROBE_MIB=8 PROBE_COOLDOWN=1 PROBE_MAXTIME=60 \
    bash "$PROBE" "http://127.0.0.1:$PORT/f.mkv" "$@" > "$OUT" 2>&1
}

echo "probe-origin.sh verdicts"

# 4 MiB/s on every connection, nothing shared: throughput has to scale with the count.
start_sim perconn 4194304 || exit 1
run_probe 4 8
check "per-connection cap -> parallel" "cap is PER CONNECTION"

# 16 MiB/s for the server as a whole: adding connections just divides the same pipe.
start_sim total 16777216 || exit 1
run_probe 4 8
check "shared pipe cap -> 302" "cap is THE PIPE"

# The historic trap: every request fails, and an unchecked benchmark times error pages and
# calls it throughput.
start_sim 403 4194304 || exit 1
run_probe 4
check "all requests 403 -> no verdict" "HTTP 403 for a 1 KiB Range request"

# No Range support at all, which is what a plain `python3 -m http.server` does.
start_sim norange 4194304 || exit 1
run_probe 4
check "origin ignores Range -> no verdict" "this origin ignores Range headers"

# Unthrottled loopback: fast enough that the ratio between rounds is pure overhead noise.
start_sim perconn 1073741824 || exit 1
PROBE_MIB=8 PROBE_COOLDOWN=1 PROBE_CEILING_MBPS=1000 \
  bash "$PROBE" "http://127.0.0.1:$PORT/f.mkv" 4 > "$OUT" 2>&1
check "implausibly fast -> no verdict" "plausibility ceiling"

# Argument handling: a bare connection count must not be mistaken for a url.
start_sim perconn 4194304 || exit 1
PROBE_URL="http://127.0.0.1:$PORT/f.mkv" PROBE_MIB=8 PROBE_COOLDOWN=1 PROBE_RECHECK=0 \
  bash "$PROBE" 4 > "$OUT" 2>&1
check "PROBE_URL + positional count" "cap is PER CONNECTION"

bash "$PROBE" > "$OUT" 2>&1
check "no url -> usage" "usage: probe-origin.sh"

echo
echo "  $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] || exit 1
