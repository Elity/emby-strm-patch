#!/usr/bin/env bash
# Does your origin throttle each CONNECTION, or the whole PIPE?
#
#   usage: probe-origin.sh <url> [parallel-counts...]
#          PROBE_URL='https://...' probe-origin.sh
#
#   probe-origin.sh 'https://pan.example.com/movie.mkv'          # 1 vs 4 vs 8
#   probe-origin.sh 'https://pan.example.com/movie.mkv' 4        # 1 vs 4 only
#   PROBE_MIB=64 probe-origin.sh 'https://...' 4 8 16
#
# This is the FIRST of the two probes and it needs no Emby, no patch and no NAS - just a url
# to a large file on the origin you are considering. It answers the one question that decides
# between the two delivery modes:
#
#   throughput scales with connections  ->  the cap is per connection  ->  use mode 'parallel'
#   throughput stays flat               ->  the cap is the pipe        ->  use mode '302'
#
# 302 hands the transfer to the client. It takes the server out of the path; it does not
# create bandwidth. Against a per-connection cap the client inherits the very same limit and
# the file still stutters - the bottleneck merely moved. Only bother tuning ramp-seconds
# (`run-tests.sh seeks 25`, see that script's header) if this one says 'parallel'.
#
# Three things this deliberately does not trust:
#
#   * The HTTP status AND the byte count of every single request are checked. An earlier
#     hand-rolled version of this test proudly reported "6 MB in 146 ms"; every request had
#     in fact been a 403 and it was timing error pages. Unchecked concurrency benchmarks
#     produce confident nonsense.
#   * The single-connection baseline is measured twice, before and after. If the origin
#     degraded, or a CDN started serving us from cache, the two disagree and the verdict is
#     withheld rather than guessed. PROBE_RECHECK=0 skips that second baseline.
#   * Any round faster than PROBE_CEILING_MBPS is refused outright. Past roughly a gigabit
#     the clock is measuring request overhead, not the origin, and the ratio between rounds
#     becomes noise that still looks like a result. A cache hit or a loopback stand-in reads
#     exactly like this - and this project has already lost time to it once, when a mount
#     benchmarked at 23.8 MB/s and then delivered 320-650 KB/s from four cold offsets.
#
# Ranges are >= 32 MiB by default because anything smaller measures TCP slow-start rather
# than the origin, and every request in the whole run gets its own disjoint slice of the file
# so no round can be inflated by a cache an earlier round warmed.
#
# The verdict logic has a test: tests/probe-origin.test.sh drives this script against
# tests/origin-sim.py, which can impose either a per-connection cap or a shared one, and
# asserts that each shape produces the matching recommendation.
#
# Environment:
#   PROBE_URL        url, if not given as $1
#   PROBE_MIB        MiB per request           (default 32; smaller is warned about)
#   PROBE_MAXTIME    per-request seconds       (default 180)
#   PROBE_COOLDOWN   seconds between rounds    (default 5)
#   PROBE_RECHECK    repeat the 1-conn baseline at the end, 1/0 (default 1)
#   PROBE_CEILING_MBPS  refuse to judge above this rate (default 1000)
#   PROBE_SIZE       file size in bytes, if the origin will not report one
#
# Read-only: nothing is written anywhere but a temp dir that is removed on exit.
set -uo pipefail

usage() {
  sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'
  echo "  (the header of $0 explains what the numbers mean)"
}

is_num() { case "${1:-}" in ''|*[!0-9]*) return 1 ;; *) return 0 ;; esac; }

# $1 is the url only if it looks like one. Otherwise fall back to PROBE_URL and leave every
# positional argument to be read as a connection count - so `PROBE_URL=... probe-origin.sh 4`
# means "4 connections", not "connect to the host named 4".
URL=""
case "${1:-}" in
  *://*) URL="$1"; shift ;;
  *)     URL="${PROBE_URL:-}" ;;
esac
if [ -z "$URL" ]; then
  echo "x no url given (a url has to contain '://')" >&2
  usage >&2
  exit 2
fi

CONNS="$*"
[ -z "$CONNS" ] && CONNS="4 8"
for n in $CONNS; do
  is_num "$n" || { echo "x '$n' is not a connection count" >&2; usage >&2; exit 2; }
  if [ "$n" -lt 2 ] || [ "$n" -gt 64 ]; then
    echo "x connection count out of range (2..64): $n" >&2
    echo "  1 is always measured as the baseline; list only the parallel counts." >&2
    exit 2
  fi
done

MIB="${PROBE_MIB:-32}"
MAXTIME="${PROBE_MAXTIME:-180}"
COOLDOWN="${PROBE_COOLDOWN:-5}"
RECHECK="${PROBE_RECHECK:-1}"
# Above this, a "measurement" is almost certainly a cache hit or a loopback round trip rather
# than an origin. See the note next to the check itself.
CEILING="${PROBE_CEILING_MBPS:-1000}"
for v in "$MIB" "$MAXTIME" "$COOLDOWN" "$CEILING"; do
  is_num "$v" || { echo "x PROBE_MIB / PROBE_MAXTIME / PROBE_COOLDOWN / PROBE_CEILING_MBPS must be integers" >&2; exit 2; }
done
[ "$MIB" -lt 1 ] && { echo "x PROBE_MIB must be at least 1" >&2; exit 2; }
BYTES=$(( MIB * 1024 * 1024 ))

command -v curl >/dev/null 2>&1 || { echo "x curl not found" >&2; exit 2; }

TMP="$(mktemp -d "${TMPDIR:-/tmp}/probe-origin.XXXXXX")" || exit 2
trap 'rm -rf "$TMP"' EXIT INT TERM HUP

# Print the origin without its signature: signed urls are credentials and this output gets pasted.
SAFE=$(printf '%s' "$URL" | sed 's/?.*/?.../')
echo "origin : $SAFE"
echo "plan   : ${MIB} MiB per request; rounds 1 $CONNS$( [ "$RECHECK" = 1 ] && echo ' 1(recheck)')"
[ "$MIB" -lt 32 ] && echo "  ! ${MIB} MiB is under the 32 MiB floor - short transfers measure slow-start, not the origin"

########## preflight: is this url usable at all, and how big is the file ##########

PRE=$(curl -sS -L -D "$TMP/head" -o /dev/null --max-time 60 -r 0-1023 \
      -w '%{http_code} %{size_download}' "$URL" 2>"$TMP/preerr")
PRE_CODE=$(printf '%s' "$PRE" | cut -d' ' -f1)
PRE_GOT=$(printf '%s' "$PRE" | cut -d' ' -f2)

if [ -z "$PRE_CODE" ] || [ "$PRE_CODE" = "000" ]; then
  echo "x the origin could not be reached: $(head -1 "$TMP/preerr" 2>/dev/null)"
  exit 1
fi
if [ "$PRE_CODE" = "200" ]; then
  echo 'x HTTP 200 for a Range request: this origin ignores Range headers.'
  echo '  Multi-connection fetching cannot work against it, so mode parallel is off the table.'
  echo '  302 is the only mode that can help here, and only if the pipe rather than the'
  echo '  origin is what limits you.'
  exit 1
fi
if [ "$PRE_CODE" != "206" ]; then
  echo "x HTTP $PRE_CODE for a 1 KiB Range request - nothing was measured."
  echo "  An expired signed url, or a token bound to a different client ip, looks exactly like this."
  echo "  Get a fresh url and retry. Do NOT read a run of failed requests as a slow origin."
  exit 1
fi
if [ "$PRE_GOT" != "1024" ]; then
  echo "x 206 but $PRE_GOT bytes instead of 1024 - the origin is not honouring the range it acknowledged."
  exit 1
fi

SIZE="${PROBE_SIZE:-}"
if [ -z "$SIZE" ]; then
  SIZE=$(grep -ai '^content-range:' "$TMP/head" | tail -1 | tr -d '\r' | sed 's|.*/||' | tr -cd '0-9')
fi
if ! is_num "$SIZE" || [ "$SIZE" -lt 1 ]; then
  echo "x could not learn the file size (no usable Content-Range header). Pass PROBE_SIZE=<bytes>."
  exit 1
fi

# Every request in the run gets its own slice, so no round can be inflated by a cache the
# previous round warmed. Start a little way in: byte 0 is what every player reads first and
# is the likeliest thing to be sitting in a CDN edge already.
TOTAL_REQ=1
for n in $CONNS; do TOTAL_REQ=$(( TOTAL_REQ + n )); done
[ "$RECHECK" = 1 ] && TOTAL_REQ=$(( TOTAL_REQ + 1 ))
LO=$(( SIZE / 20 ))
NEED=$(( LO + TOTAL_REQ * BYTES ))

printf 'file   : %s bytes (%s MiB); the run needs %s MiB for %s disjoint requests\n' \
  "$SIZE" "$(( SIZE / 1048576 ))" "$(( NEED / 1048576 ))" "$TOTAL_REQ"

if [ "$SIZE" -lt "$NEED" ]; then
  echo "x that file is too small for this plan."
  echo "  Use a larger file, drop a round (e.g. '$0 <url> 4'), or lower PROBE_MIB"
  echo "  (keep it >= 32 or the measurement stops meaning anything)."
  exit 1
fi
echo

CURSOR="$LO"
export TMP
export PROBE_MAXTIME="$MAXTIME"

########## one round of N simultaneous range fetches ##########

round() {   # $1 = connections, $2 = label ; appends "n mbps bytes secs" to $TMP/agg.$SLOT
  n="$1"; label="$2"
  rm -f "$TMP"/r.* "$TMP"/e.*

  i=0
  while [ "$i" -lt "$n" ]; do
    off="$CURSOR"
    CURSOR=$(( CURSOR + BYTES ))
    end=$(( off + BYTES - 1 ))
    (
      curl -sS -L --max-time "$MAXTIME" -o /dev/null -r "$off-$end" \
           -w '%{http_code} %{size_download} %{time_total} %{speed_download}' \
           "$URL" 2>"$TMP/e.$i"
      printf ' %s\n' "$?"
    ) > "$TMP/r.$i" &
    i=$(( i + 1 ))
  done
  wait

  i=0
  while [ "$i" -lt "$n" ]; do
    cat "$TMP/r.$i" 2>/dev/null
    i=$(( i + 1 ))
  done > "$TMP/round"

  echo "--- $label: $n connection(s) x ${MIB} MiB ---"
  awk -v want="$BYTES" -v n="$n" '
    {
      code=$1; got=$2+0; t=$3+0; sp=$4+0; ec=$5+0
      printf "  #%d  HTTP=%-4s %8.1f MiB  %6.1fs  %8.2f Mbps%s\n", \
             NR-1, code, got/1048576, t, sp*8/1e6, \
             (code != "206" ? "   <- not 206" : (got < want ? "   <- short" : ""))
      bytes += got; sum += sp
      if (t > max) max = t
      if (code != "206") bad++
      if (ec != 0 && ec != 28) { err++; errcode = ec }
      if (got < want) short++
    }
    END {
      if (NR != n)    { print "  x only " NR " of " n " requests reported back"; exit 3 }
      if (bad)        { print "  x " bad "/" n " requests were not 206 - this round timed error pages, not transfers"; exit 3 }
      if (err)        { print "  x " err "/" n " requests failed at the transport level (curl exit " errcode ")"; exit 3 }
      if (bytes <= 0) { print "  x zero bytes transferred"; exit 3 }
      if (max <= 0)   { print "  x no usable timing"; exit 3 }
      if (short)      { printf "  ! %d request(s) came up short (hit --max-time %s?) - the rate below is still bytes/elapsed\n", short, ENVIRON["PROBE_MAXTIME"] }
      printf "  aggregate: %.1f MiB in %.1f s -> %.2f Mbps   (sum of per-connection rates: %.2f Mbps)\n", \
             bytes/1048576, max, bytes*8/1e6/max, sum*8/1e6
      printf "%s %.4f %d %.3f\n", n, bytes*8/1e6/max, bytes, max \
             > (ENVIRON["TMP"] "/agg." ENVIRON["SLOT"])
    }' "$TMP/round"
  rc=$?
  if [ "$rc" != 0 ]; then
    head -1 "$TMP/e.0" 2>/dev/null | sed 's/^/  curl: /'
    return 1
  fi
  return 0
}

FAILED=0
BASE_SLOT=0
RECHECK_SLOT=0

# A round faster than the ceiling is not a measurement of an origin. Withhold, do not guess.
implausible() {   # $1 = Mbps, $2 = which round
  echo
  echo "########## no verdict ##########"
  printf '  %s came out at %.0f Mbps, past the %s Mbps plausibility ceiling.\n' "$2" "$1" "$CEILING"
  echo "  At that rate the elapsed time is dominated by request overhead rather than by the"
  echo "  origin, so the ratio between rounds is noise and any verdict from it is arbitrary."
  echo "  What gets measured instead is normally one of:"
  echo "    * a cache hit - a CDN edge, a proxy, or a local mount that already held the bytes"
  echo "    * loopback, or a LAN host standing in for the origin"
  echo "  This project has been burned by precisely that: a mount benchmarked at 23.8 MB/s,"
  echo "  then re-measured at four cold offsets for 320-650 KB/s. The fast number was cache."
  echo "  Point this at the real origin - or raise PROBE_CEILING_MBPS if your link truly is"
  echo "  that fast and you know the bytes are cold."
}

SLOT=0; export SLOT
round 1 "baseline" || FAILED=1

# Checked here rather than at the end so an obviously bogus target costs one round, not all of them.
if [ "$FAILED" = 0 ]; then
  B0=$(cut -d' ' -f2 "$TMP/agg.$BASE_SLOT")
  if awk -v m="$B0" -v c="$CEILING" 'BEGIN{ exit !(m > c) }'; then
    implausible "$B0" "the single-connection baseline"
    exit 1
  fi
fi

if [ "$FAILED" = 0 ]; then
  for n in $CONNS; do
    echo "  (cooling down ${COOLDOWN}s)"
    sleep "$COOLDOWN"
    SLOT=$(( SLOT + 1 )); export SLOT
    round "$n" "parallel" || { FAILED=1; break; }
  done
fi

if [ "$FAILED" = 0 ] && [ "$RECHECK" = 1 ]; then
  echo "  (cooling down ${COOLDOWN}s)"
  sleep "$COOLDOWN"
  SLOT=$(( SLOT + 1 )); export SLOT
  RECHECK_SLOT="$SLOT"
  round 1 "baseline again" || FAILED=1
fi

echo
if [ "$FAILED" != 0 ]; then
  echo "########## no verdict ##########"
  echo "  A round failed, so there is nothing to compare. Fix the request above and re-run."
  echo "  By far the most common cause: an expired signed url."
  exit 1
fi

########## verdict ##########

# The baseline was checked early; a parallel round can still come back implausible on its own
# (one slice sitting in a cache is enough), and that poisons the ratio just as thoroughly.
CS=0
while [ "$CS" -le "$SLOT" ]; do
  v=$(cut -d' ' -f2 "$TMP/agg.$CS" 2>/dev/null)
  c=$(cut -d' ' -f1 "$TMP/agg.$CS" 2>/dev/null)
  if [ -n "$v" ] && awk -v m="$v" -v c="$CEILING" 'BEGIN{ exit !(m > c) }'; then
    implausible "$v" "the ${c}-connection round"
    exit 1
  fi
  CS=$(( CS + 1 ))
done

BASE=$(cut -d' ' -f2 "$TMP/agg.$BASE_SLOT")

if [ "$RECHECK" = 1 ]; then
  BASE2=$(cut -d' ' -f2 "$TMP/agg.$RECHECK_SLOT")
  DRIFT=$(awk -v a="$BASE" -v b="$BASE2" 'BEGIN{ printf "%.2f", (a > b ? a/b : b/a) }')
  echo "########## baseline stability ##########"
  printf '  1 connection: %.2f Mbps before, %.2f Mbps after   (ratio %sx)\n' "$BASE" "$BASE2" "$DRIFT"
  if awk -v d="$DRIFT" 'BEGIN{ exit !(d > 1.35) }'; then
    echo "  x the origin did not behave the same at the start and at the end of the run."
    echo "    Everything measured in between is unreliable: a degraded origin makes every"
    echo "    setting look equally bad, a warm cache makes every setting look equally good."
    echo "    Wait a few minutes and run it again. No verdict."
    exit 1
  fi
  echo "  ok - stable enough to compare"
  # Average the two baselines: it is the better estimate across the whole window.
  BASE=$(awk -v a="$BASE" -v b="$BASE2" 'BEGIN{ printf "%.4f", (a + b) / 2 }')
  echo
fi

echo "########## result ##########"
printf '  %-8s %14s %12s\n' conns Mbps 'vs 1 conn'
printf '  %-8s %14.2f %11sx\n' 1 "$BASE" '1.00'
BEST_N=""; BEST_MBPS=""; BEST_SPEEDUP=""
SC_N=""; SC_MBPS=""; SC_SPEEDUP=""
SCALES=0
S=0
for n in $CONNS; do
  S=$(( S + 1 ))
  m=$(cut -d' ' -f2 "$TMP/agg.$S")
  sp=$(awk -v m="$m" -v b="$BASE" 'BEGIN{ printf "%.2f", (b > 0 ? m / b : 0) }')
  printf '  %-8s %14.2f %11sx\n' "$n" "$m" "$sp"
  # "Scales" is judged per round against 0.6 x linear. The factor is deliberately loose: a
  # per-connection cap never scales perfectly - the pipe, the proxy and the origin's own
  # ceilings all take a cut - and the decision only needs the shape, not the coefficient.
  # The verdict quotes whichever round actually passed this test, not the best round overall:
  # 8 connections at 3.0x fails it while 4 connections at 2.5x passes, and quoting the 8 would
  # print a number that contradicts the sentence around it.
  if awk -v s="$sp" -v n="$n" 'BEGIN{ exit !(s >= 0.6 * n) }'; then
    SCALES=1
    if [ -z "$SC_SPEEDUP" ] || awk -v a="$sp" -v b="$SC_SPEEDUP" 'BEGIN{ exit !(a > b) }'; then
      SC_N="$n"; SC_MBPS="$m"; SC_SPEEDUP="$sp"
    fi
  fi
  # Best is always one of the parallel rounds, never the baseline: every verdict below is a
  # statement about what concurrency did.
  if [ -z "$BEST_SPEEDUP" ] || awk -v a="$sp" -v b="$BEST_SPEEDUP" 'BEGIN{ exit !(a > b) }'; then
    BEST_N="$n"; BEST_MBPS="$m"; BEST_SPEEDUP="$sp"
  fi
done

echo
echo "########## verdict ##########"
if [ "$SCALES" = 1 ]; then
  printf '  %s connections gave %sx the throughput of one (%.2f -> %.2f Mbps).\n' \
    "$SC_N" "$SC_SPEEDUP" "$BASE" "$SC_MBPS"
  echo "  => the cap is PER CONNECTION. Use mode 'parallel' for this origin."
  echo "     302 would hand the client the same single-connection limit you just measured."
  echo "     Next step: sweep ramp-seconds for this origin with 'run-tests.sh seeks 25'."
  exit 0
fi
if awk -v s="$BEST_SPEEDUP" 'BEGIN{ exit !(s < 0.8) }'; then
  printf '  %s connections were SLOWER than one (%sx: %.2f -> %.2f Mbps).\n' \
    "$BEST_N" "$BEST_SPEEDUP" "$BASE" "$BEST_MBPS"
  echo "  => concurrency actively costs you here. Use mode '302'."
  echo "     Origins do this by rate-limiting per client rather than per connection, or by"
  echo "     answering the extra requests with 503s. Either way parallel fetching is a"
  echo "     pure loss. There is nothing to tune - do not sweep ramp-seconds."
  exit 0
fi
if awk -v s="$BEST_SPEEDUP" 'BEGIN{ exit !(s <= 1.4) }'; then
  printf '  %s connections gave %sx - flat (%.2f -> %.2f Mbps).\n' \
    "$BEST_N" "$BEST_SPEEDUP" "$BASE" "$BEST_MBPS"
  echo "  => the cap is THE PIPE, not the connection. Use mode '302' for this origin."
  echo "     'parallel' would push the same bytes through the same bottleneck and add a"
  echo "     detour through the server's own bandwidth and memory. Nothing to tune here."
  exit 0
fi
printf '  %s connections gave %sx - between flat and linear (%.2f -> %.2f Mbps).\n' \
  "$BEST_N" "$BEST_SPEEDUP" "$BASE" "$BEST_MBPS"
echo "  => partial scaling. Something caps you short of linear, but concurrency still buys"
echo "     real throughput. Decide on the absolute number rather than the ratio: if"
echo "     $BEST_MBPS Mbps clears the bitrate of the files you actually play, 'parallel' earns"
echo "     its cost; if the 1-connection number already clears it, take the simpler mode."
echo "     Worth one re-run - a momentarily busy origin lands in this band by accident."
exit 0
