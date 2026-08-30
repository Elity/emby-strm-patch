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
echo "all tests passed"
