#!/usr/bin/env bash
# End-to-end test with no Emby binaries: patch two synthetic assemblies that have the same
# shape as Emby's, then invoke the patched methods and check the results.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
OUT=$ROOT/work/test-out
rm -rf "$OUT"; mkdir -p "$OUT"

echo "== build =="
dotnet build -c Release -v q --nologo template/template.csproj
dotnet build -c Release -v q --nologo patcher/patcher.csproj
dotnet build -c Release -v q --nologo rtcheck/rtcheck.csproj
dotnet build -c Release -v q --nologo tests/behaviour/behaviour.csproj
dotnet build -c Release -v q --nologo tests/fixture-redirect/fixture.csproj
dotnet build -c Release -v q --nologo tests/fixture-notranscode/fixture.csproj

TPL=$ROOT/template/bin/Release/net8.0/StrmDirectTemplate.dll
FR=$ROOT/tests/fixture-redirect/bin/Release/net8.0
FN=$ROOT/tests/fixture-notranscode/bin/Release/net8.0

echo
echo "== patch the fixtures =="
dotnet run -c Release --no-build --project patcher -- \
  "$FR/Emby.Server.Implementations.dll" "$OUT/Emby.Server.Implementations.dll" "$FR" "$TPL"
dotnet run -c Release --no-build --project patcher -- \
  "$FN/Emby.Server.MediaEncoding.dll" "$OUT/Emby.Server.MediaEncoding.dll" "$FN" "$TPL"

echo
echo "== idempotency: patching a patched assembly must be refused =="
# The patcher exits non-zero here on purpose, so step around set -e / pipefail.
set +e
IDEM=$(dotnet run -c Release --no-build --project patcher -- \
         "$OUT/Emby.Server.Implementations.dll" "$OUT/twice.dll" "$OUT" "$TPL" 2>&1)
IDEM_RC=$?
set -e
if [ "$IDEM_RC" -ne 0 ] && printf '%s' "$IDEM" | grep -q "Already patched"; then
  echo "  [PASS] refused (exit $IDEM_RC)"
else
  echo "  [FAIL] a second patch was not refused (exit $IDEM_RC)"; printf '%s\n' "$IDEM"; exit 1
fi
[ -e "$OUT/twice.dll" ] && { echo "  [FAIL] output was written despite refusal"; exit 1; }
echo "  [PASS] no output written"

echo
echo "== matcher runtime checks =="
dotnet run -c Release --no-build --project rtcheck -- "$OUT/Emby.Server.Implementations.dll" "$FR"
dotnet run -c Release --no-build --project rtcheck -- "$OUT/Emby.Server.MediaEncoding.dll" "$FN"

echo
echo "== behavioural checks on the patched methods =="
dotnet run -c Release --no-build --project tests/behaviour -- \
  "$OUT/Emby.Server.Implementations.dll" "$OUT/Emby.Server.MediaEncoding.dll"

echo
echo "== check-config =="
# The fixtures carry the real type names and get the real marker fields, so `embypatch check`
# reads them exactly as it would read a live install. $OUT stands in for system/: patch A and B
# are present, C is not, which is the escape-hatch shape.
PD=$OUT/programdata; mkdir -p "$PD/config"
CFG=$PD/config/strm-routing.txt
CHECK_FAIL=0
# Hermetic: layer 1 and 2 of the lookup would otherwise inherit whatever the caller's shell has.
unset EMBY_STRM_PREFIXES EMBY_STRM_CONFIG
check_case() {   # <name> <expected-exit> <expected-substring>
  local name=$1 want=$2 needle=$3 out rc
  set +e
  out=$(dotnet run -c Release --no-build --project "$ROOT/patcher" -- check "$PD" "$OUT" 2>&1)
  rc=$?
  set -e
  if [ "$rc" = "$want" ] && printf '%s' "$out" | grep -q "$needle"; then
    echo "  [PASS] $name (exit $rc)"
  else
    echo "  [FAIL] $name: exit $rc (want $want), looking for '$needle'"; printf '%s\n' "$out"
    CHECK_FAIL=1
  fi
}

printf '# plain url lines\nhttps://plain.example/\nhttps://two.example/d/  302\nramp-seconds = 3\n' > "$CFG"
check_case "plain 302 config is satisfiable" 0 "OK   2 route(s)"

printf 'https://par.example/  parallel\n' > "$CFG"
check_case "parallel without patch C is unsatisfiable" 1 "UNSATISFIABLE"

printf 'https://typo.example/  paralell\n' > "$CFG"
check_case "misspelled mode is reported with its line number" 1 "line 1: unknown mode 'paralell'"

printf 'https://pan.example/d/\xe9\x98\xbf\xe9\x87\x8c/\n' > "$CFG"
check_case "decoded (non-ASCII) prefix raises the percent-encoding warning" 0 "non-ASCII"

printf 'chunck-mb = 16\n' > "$CFG"
check_case "misspelled setting key is not silently ignored" 1 "unknown setting 'chunck-mb'"

[ "$CHECK_FAIL" = 0 ] || exit 1

echo
echo "all tests passed"
