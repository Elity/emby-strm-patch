#!/usr/bin/env bash
# EmbyStrmParallel test runner.
#
#   ./run-tests.sh              mock + config + live correctness + throughput  (~3 min)
#   ./run-tests.sh mock         loopback correctness/faults/memory only, no network (~40 s)
#   ./run-tests.sh config       IsMatch configuration + self-arming log (instant)
#   ./run-tests.sh logging      EMBY_STRM_LOG diagnostics only
#   ./run-tests.sh live         live correctness + throughput
#   ./run-tests.sh throughput   1 / 4 / 8 connection throughput only
#   ./run-tests.sh tune         connection-count and chunk-size sweep (~6 min)
#   ./run-tests.sh ramp         chunk-size startup ramp, A/B interleaved
#   ./run-tests.sh soak 512     sustained live transfer of N MiB with memory sampling
#   ./run-tests.sh seeks 25     8 open-ended requests, each abandoned after N s  <-- see below
#   ./run-tests.sh measure 8 8 192 2900000000     <conns> <chunkMiB> <wantMiB> <offset>
#
# Live modes read the url from TEST_URL.txt (found by walking up from the build output).
#
# ---------------------------------------------------------------------------
# TUNING EMBY_STRM_RAMP_SECONDS  (the one setting that is origin-specific)
# ---------------------------------------------------------------------------
# Abandoning a stream leaves its connections lingering at the origin; enough of them and the
# origin degrades sharply. Connection slow-start bounds how many a short-lived stream can
# leave behind. The right interval depends on YOUR origin, and it has a cliff.
#
# Reproduce the sweep (each run needs a ~100 s cooldown or the numbers are meaningless -
# a degraded origin makes every setting look equally bad):
#
#   for r in 6 3 2 1; do
#     sleep 100
#     echo "=== RAMP=$r ==="
#     EMBY_STRM_RAMP_SECONDS=$r ./run-tests.sh seeks 25
#   done
#
# Measured against one reference origin that throttles per connection (per-seek Mbps, then a
# separate continuous read). Your numbers will differ; the SHAPE is the transferable part:
#
#   RAMP  per-seek Mbps                                        avg    sustained
#    6    2.35  5.52 10.21  9.63 10.04  7.53 10.21  9.96        8.2     25.02
#    3   12.81 12.90  4.59 12.90  3.17 10.30 12.73  7.61        9.6     27.47
#    2    7.61 18.27 12.90 15.33 18.18  4.93 18.10 15.67       13.9     28.93
#    2   10.13 12.98 12.98 18.10 18.27 15.67 15.67 15.66       14.9     25.91
#    1   23.55 11.14  0.08  0.15  0.48  0.15  0.08  0.15    COLLAPSE     0.23
#
# Read that table twice before changing anything:
#
#  * 2 beats 6 on BOTH axes - seeks and sustained. There is no trade-off in that range.
#  * 1 is a cliff, not a slope, and it takes the CONTINUOUS case down with it (0.23 Mbps on a
#    plain sequential read). That is worse than the bug slow-start fixes, because continuous
#    playback is the dominant workload. 2 sits one notch from that edge.
#
# The shipped default is 6 for margin. Lower it only with your own numbers in hand.
set -euo pipefail
cd "$(dirname "$0")/tests/EmbyStrmParallel.Tests"
dotnet build -v q --nologo
exec dotnet run --no-build -c Debug -- "${@:-all}"
