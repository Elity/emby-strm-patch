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
dotnet build -c Release -v q --nologo parallel/src/EmbyStrmParallel.csproj
dotnet build -c Release -v q --nologo tests/behaviour/behaviour.csproj
dotnet build -c Release -v q --nologo tests/fixture-redirect/fixture.csproj
dotnet build -c Release -v q --nologo tests/fixture-notranscode/fixture.csproj

TPL=$ROOT/template/bin/Release/net8.0/StrmDirectTemplate.dll
HELPER=$ROOT/parallel/src/bin/Release/net8.0/EmbyStrmParallel.dll
FR=$ROOT/tests/fixture-redirect/bin/Release/net8.0
FN=$ROOT/tests/fixture-notranscode/bin/Release/net8.0

echo
echo "== patch the fixtures =="
# --parallel on purpose: without it patch C's IL is never executed by any test, and a helper
# that works in isolation proves nothing about whether Emby ever calls it. See run.sh's
# behaviour step, section 7.
dotnet run -c Release --no-build --project patcher -- \
  "$FR/Emby.Server.Implementations.dll" "$OUT/Emby.Server.Implementations.dll" "$FR" "$TPL" \
  --parallel "$HELPER"
dotnet run -c Release --no-build --project patcher -- \
  "$FN/Emby.Server.MediaEncoding.dll" "$OUT/Emby.Server.MediaEncoding.dll" "$FN" "$TPL"

# The patched assembly now references EmbyStrmParallel; on a real install that is the deps.json
# step, here it just has to sit beside the assembly that loads it.
cp "$HELPER" "$OUT/"

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
# reads them exactly as it would read a live install. $OUT stands in for system/: A, B and C are
# all patched in, but EmbyServer.deps.json is absent - which is the shape of the mistake that
# actually happens, a helper dropped next to the assembly without being registered.
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
check_case "parallel without a deps.json entry is unsatisfiable" 1 "not registered in EmbyServer.deps.json"

# Registering the helper is the step people forget: dropping the DLL into system/ is not enough,
# because the assembly manifest decides what loads. With the entry AND the dependency edge in
# place the same config becomes satisfiable, which is what makes the message above actionable
# rather than just a complaint.
cat > "$OUT/EmbyServer.deps.json" <<'DEPSEOF'
{
  "runtimeTarget": { "name": ".NETCoreApp,Version=v8.0" },
  "targets": {
    ".NETCoreApp,Version=v8.0": {
      "Emby.Server.Implementations/1.0.0": {
        "dependencies": { "EmbyStrmParallel": "1.0.0" },
        "runtime": { "Emby.Server.Implementations.dll": {} }
      },
      "EmbyStrmParallel/1.0.0": { "runtime": { "EmbyStrmParallel.dll": {} } }
    }
  },
  "libraries": {}
}
DEPSEOF
check_case "parallel becomes satisfiable once the helper is registered" 0 "OK   1 route(s)"
rm -f "$OUT/EmbyServer.deps.json"

printf 'https://typo.example/  paralell\n' > "$CFG"
check_case "misspelled mode is reported with its line number" 1 "line 1: unknown mode 'paralell'"

printf 'https://pan.example/d/\xe9\x98\xbf\xe9\x87\x8c/\n' > "$CFG"
check_case "decoded (non-ASCII) prefix raises the percent-encoding warning" 0 "non-ASCII"

printf 'chunck-mb = 16\n' > "$CFG"
check_case "misspelled setting key is not silently ignored" 1 "unknown setting 'chunck-mb'"

[ "$CHECK_FAIL" = 0 ] || exit 1

echo
echo "all tests passed"
