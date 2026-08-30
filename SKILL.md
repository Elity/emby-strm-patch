---
name: emby-strm-follow-302
description: Patch an Emby Server install so that .strm media (remote http/https direct links) is answered with a 302 redirect and never transcoded, instead of the server relaying every byte. Use this whenever someone mentions .strm playback being slow or stuttering, Emby proxying/relaying remote media, 206 responses on /Videos/{id}/stream or /original.*, unwanted transcoding of cloud-drive media, saturated server upload while a .strm plays, patching Emby assemblies, adding a new URL prefix to the direct-play list, re-applying the patch after an Emby upgrade, or rolling the patch back. Reach for this skill even when the request is phrased as a vague performance complaint about network-drive libraries.
---

# Emby strm follow-302

Injects two short IL sequences into Emby's own assemblies so that media whose
`MediaSourceInfo.Path` matches a configured URL prefix is handed to the client directly.

| Patch | Assembly · method | Effect |
|---|---|---|
| **A** | `Emby.Server.MediaEncoding.dll`<br>`MediaInfoService.SetDeviceSpecificData` | Sets `SupportsTranscoding = false` for matched sources. Emby's own `ForceDirectPlay` path then keeps `SupportsDirectPlay` true and never builds a `TranscodingUrl`. |
| **B** | `Emby.Server.Implementations.dll`<br>`HttpResultFactory.GetStaticFileResult` | Matched sources return **302** to the origin URL, so `/Videos/{id}/stream` and `/Videos/{id}/original.*` stop relaying bytes through the server. |

Neither patch modifies an existing instruction. Both prepend to the method body with every
branch target set to the original first instruction, so a non-matching path executes exactly
as stock. Prefixes are **not** baked into the IL — they are read from configuration at runtime,
and with no configuration present the patch is inert.

The two patches are independent; either can be applied or rolled back alone.

## What the patcher looks for

The patcher locates its targets by type and method **signature**, never by file offset, so it
normally works across Emby versions without changes. If it reports a target it cannot find, the
names have moved — these are the semantics to look for, and `references/internals.md` walks
through re-deriving them:

**Patch A** — the method that finalises playback capabilities for **one** `MediaSourceInfo`
during PlaybackInfo. Recognisable by: it takes a single `MediaSourceInfo` (the sibling overload
takes the whole response and loops), it constructs a `StreamBuilder` and options object, and it
contains Emby's own lever — `!SupportsTranscoding` drives `ForceDirectPlay` / `ForceDirectStream`
and an early `return` placed **before** `TranscodingUrl` is assigned. Goal: make matched sources
report that they do not support transcoding, and let Emby's existing code do the rest.

**Patch B** — the convergence point for static/progressive file responses, where the path being
served is `MediaSourceInfo.Path` verbatim. Recognisable by: it takes an options object exposing a
`Path` property, and its declaring type also exposes a factory that builds a redirect result.
Goal: for matched paths, return that redirect instead of streaming the bytes.

If two or more identifying traits no longer hold, stop and re-read the code rather than forcing
the patch through.

## Step 0 — Locate the Emby installation (required, do not skip)

**Stop here and establish the paths before touching anything else.** Emby's layout differs per
platform and per packaging, and guessing wrong means patching an install nobody runs or writing
config where nothing reads it.

Ask the user for the Emby Server location. Offer to search first — that is usually faster than
making them dig:

```bash
# Linux / macOS
find / -name "Emby.Server.Implementations.dll" -not -path "*/proc/*" 2>/dev/null
```
```powershell
# Windows
Get-ChildItem -Path C:\ -Filter Emby.Server.Implementations.dll -Recurse -ErrorAction SilentlyContinue
```

Present the candidates and have the user confirm. Then record these four facts and repeat them
back before proceeding:

| | What | How to confirm |
|---|---|---|
| `SYSTEM_DIR` | Directory holding `EmbyServer.dll`, `Emby.Server.Implementations.dll`, `Emby.Server.MediaEncoding.dll` — usually `<install>/system` | all three files present |
| `PROGRAMDATA_DIR` | Emby's data directory, holding `config/`, `logs/`, `data/` | `config/system.xml` exists |
| Start/stop method | service unit, init script, container, or app | the user states it, or you read the packaging |
| Elevation | whether writing into `SYSTEM_DIR` needs admin/root | try a `touch` in it as the current user |

Also record the Emby version (`PROGRAMDATA_DIR/config/system.xml`, or the About page) and the
**sha256 of both stock assemblies** — you need them for the rollback record and to tell later
whether an upgrade has reverted the patch.

This skill assumes the patcher runs on the **same machine** as Emby.

## Step 1 — Prerequisites

- **.NET SDK 8.0+** (`dotnet --version`). Needed to build the patcher and the template.
- Write access to `SYSTEM_DIR` (possibly elevated) and permission to stop/start the server.
- Enough downtime for a restart. The swap itself takes seconds; the restart dominates.

## Step 2 — Build the patched assemblies

`SYSTEM_DIR` doubles as the reference directory — Cecil needs the sibling assemblies to resolve
types. Read from it, **write somewhere else**; never write output into `SYSTEM_DIR` while the
server is running.

```bash
mkdir -p work/out
(cd template && dotnet build -c Release)

TPL=$PWD/template/bin/Release/net8.0/StrmDirectTemplate.dll
for d in Emby.Server.Implementations Emby.Server.MediaEncoding; do
  (cd patcher && dotnet run -- "$SYSTEM_DIR/$d.dll" "$PWD/../work/out/$d.dll" "$SYSTEM_DIR" "$TPL")
done
```

Expected output per assembly:

```
matcher  : cloned Emby.Server.StrmDirect.StrmDirect (2 fields / 3 methods)
patch    : B - 302 redirect from the streaming endpoint
           IL before: 111
           IL after: 122  (+11)
OK       : wrote .../Emby.Server.Implementations.dll
```

The `IL before` count varies by Emby version — ignore it. What must hold is the `matcher` line,
the right patch being selected, the delta (**+11** for B, **+9** for A), and the `OK` line.

Failure modes:

- `x No known target type in this assembly` — wrong input file.
- `x ... not found` (a specific type or method) — Emby changed the shape. Re-read *What the
  patcher looks for* above, then `references/internals.md` → *Re-deriving on a new Emby version*.
  Do not improvise.
- `x Already patched` — the input was a patched assembly, not stock.
- `x Template references another type in its own assembly` — only happens if you edited
  `template/StrmDirect.cs`; read the constraints at the top of that file.

Record the sha256 of both outputs. Managed IL cannot be patched in place, so the file size
always changes — that is metadata rebuild, not lost content.

## Step 3 — Verify offline (both checks, before any downtime)

**Runtime check — this is the one that matters.** Decompiling to readable C# does not prove the
IL executes. Load the patched assembly and actually call the cloned matcher:

```bash
cd rtcheck
for d in Emby.Server.Implementations Emby.Server.MediaEncoding; do
  dotnet run -- "../work/out/$d.dll" "$SYSTEM_DIR"
done
# expect: ALL PASS   pass=18 fail=0
```

18 assertions cover: no config means no match, env-var prefixes, config file with comments and
blank lines, case-insensitivity, unrelated http sources staying untouched, env beating file, and
missing/garbage config not throwing.

**Optional static check** — decompile and read the injection back. With
`dotnet tool install -g ilspycmd --version 8.2.0.7535` and `DOTNET_ROLL_FORWARD=Major`:

```bash
ilspycmd -t Emby.Server.Implementations.HttpServer.HttpResultFactory work/out/Emby.Server.Implementations.dll | grep -A7 'GetStaticFileResult(IRequest'
ilspycmd -t Emby.Server.MediaEncoding.Api.MediaInfoService       work/out/Emby.Server.MediaEncoding.dll   | grep -A5 'SetDeviceSpecificData(long'
```

You should see `if (StrmDirect.IsMatch(path))` at the top of each method. Use `-l c` to count
types (patched should have exactly one more than stock); `-l c,i,s,e,d` returns nothing and is
not evidence of anything.

## Step 4 — Write the configuration

Copy `strm-direct.txt.example` to `PROGRAMDATA_DIR/config/strm-direct.txt` and put the real URL
prefixes in it, one per line:

```
https://pan.example.com/
```

The prefix must match the **beginning of the URL stored inside your .strm files** — read one to
confirm. Matching is `StartsWith`, case-insensitive.

Keep prefixes narrow. Anything else Emby serves over http — Live TV channels in particular —
must not match, or it loses its transcoding fallback. Check what your .strm files actually
contain versus what else in the library has an http `Path`.

Alternatives, in priority order: `EMBY_STRM_PREFIXES` (semicolon-separated values),
`EMBY_STRM_CONFIG` (absolute path to the file), then the file lookup
(`-programdata` argument → `<system>/strm-direct.txt` → `<system>/../programdata/config/strm-direct.txt`).
Environment variables need a server restart to take effect; the file is re-read every 30 seconds.

**Write the config before swapping the assemblies**, so there is never a window where the patch
is live with nothing configured.

## Step 5 — Deploy

The flow is the same everywhere; the commands are platform-specific and you should use whatever
is right for this install.

1. **Back up first, while the server is still up.** Copy each stock assembly to
   `_stock_<name>_<version>.bak` beside it. Guard the copy with an existence check
   (`[ -f "$BAK" ] || cp ...`) so a second run never overwrites the stock backup with a patched
   file. Verify the backup's sha256 matches the stock sha you recorded in Step 0.
2. **Stop Emby** and wait for the process to actually exit.
3. **Swap.** Copy each patched file in as `<name>.dll.new`, fix ownership and permissions to
   match the original, then rename over the target. Never write directly onto the live file —
   it is memory-mapped while the process runs.
4. **Start Emby.** If the start command runs in the foreground and streams the log, background
   it (`>/dev/null 2>&1 &` or the platform equivalent); otherwise your shell hangs until it
   times out.
5. **Confirm** the on-disk sha256 of both assemblies matches what you built.

## Step 6 — Verify on the running server

Do all six. Checks 3 and 5 are the regression tests, and they are the ones that catch a patch
that is too broad.

| # | Check | Expect |
|---|---|---|
| 1 | Startup log for `TypeLoadException`, `MissingMethodException`, `BadImageFormatException`, `InvalidProgramException`, `MissingFieldException`, `FileNotFoundException` | zero hits |
| 2 | `GET /Videos/<strmId>/stream?MediaSourceId=mediasource_<strmId>&Static=true&api_key=<key>` | **302** to your configured prefix |
| 3 | Same request against a **local library** item, with `Range: bytes=0-1` | **206** plus `Content-Range` |
| 4 | `POST /Items/<strmId>/PlaybackInfo` with a DeviceProfile whose `MaxStreamingBitrate` is deliberately **below** the source bitrate | `SupportsTranscoding:false`, `SupportsDirectPlay:true`, no `TranscodingUrl` |
| 5 | Same low-bitrate PlaybackInfo against a **local library** item | `SupportsTranscoding:true`, `TranscodingUrl` present — the fallback must survive |
| 6 | `/System/Info`, a poster image, `/web/index.html` | all 200 |

Optional but worth the 70 seconds — **prove the config is live**: point the prefix at something
that cannot match, wait ~32 seconds, confirm check 2 no longer returns 302, then restore and
confirm it returns 302 again. This exercises the whole configuration path without a restart.

Two things that look like failures and are not:

- A probe **without** a `Range` header returns **200**, not 206, when the prefix does not match.
  The signal is "not a 302", not "must be 206".
- A health-check poll issued while Emby is still loading logs a `ServiceUnavailableException`
  and a 503. Harmless.

Judge results from request logs and byte counters. A client's self-reported play method cannot
distinguish "client to origin" from "client to server to origin".

## Rollback

**Soft rollback first.** Empty `strm-direct.txt` (or comment out every prefix). Within 30 seconds
the patch is inert and the server behaves exactly like stock — no restart, no downtime. Use this
to bisect anything you suspect the patch caused.

**Hard rollback**: stop the server, copy the `_stock_*.bak` files back over the targets, start.
Roll back one assembly or both; they are independent.

## After an Emby upgrade

An upgrade replaces the whole application directory, so **both patched assemblies revert to
stock**. `PROGRAMDATA_DIR` is untouched, so `strm-direct.txt` survives — only the assemblies
need redoing.

Symptom: .strm playback starts stuttering or transcoding again, with no configuration change.

1. sha256 both assemblies. If they match stock, the upgrade reverted the patch.
2. Re-run Steps 2 → 3 → 5 → 6 against the new version. Record the new sha values.
3. The patcher locates its targets by type and method **signature**, not by offset, so a minor
   upgrade usually just works. If it reports a target it cannot find, re-read *What the patcher
   looks for* near the top, then `references/internals.md` → *Re-deriving on a new Emby version*.

## Changing the patcher itself

`bash tests/run.sh` patches two synthetic assemblies shaped like Emby's, then invokes the patched
methods and checks the results — no Emby binaries involved. Run it after touching anything in
`patcher/` or `template/`. CI runs the same script on Linux, Windows and macOS.

## Adding or changing a URL prefix

Edit `strm-direct.txt`. Takes effect within 30 seconds. No rebuild, no restart, no downtime.

## Pitfalls

- **Never overwrite a running assembly.** Stop the server, write a temp file, rename over the
  target. Ownership and permissions must match the original or Emby will not start.
- **Keep stock and patched in separate directories.** Copying a stock directory over your output
  silently reverts it, and the two are indistinguishable afterwards without a sha check.
- **Guard the backup step.** Without an existence check, a second run backs up the already-patched
  file as "stock" and destroys your rollback.
- **Run the regression checks (Step 6, #3 and #5).** The patched methods are global; only the
  prefix match keeps local-library and Live TV behaviour intact.
- **Emby's log obfuscates sensitive values with zero-width characters.** A pattern like
  `[a-f0-9]{32}` will not match an API key in the log; strip non-hex bytes first
  (`tr -cd 'a-f0-9'`). Most tokens in the log are non-admin and answer 403 — try several.
- **Editing `template/StrmDirect.cs`?** Read the constraint list at the top of the file first.
  Array literals in particular get lowered into a `<PrivateImplementationDetails>` RVA field that
  cannot be cloned; the patcher will refuse rather than emit a dangling reference.

## Files

- `references/internals.md` — how each injection point was chosen, the IL, and how to re-derive
  the targets when Emby changes shape
- `patcher/` — target detection, both injections, idempotency markers
- `patcher/TypeCloner.cs` — deep-copies the matcher type into the target assembly
- `template/StrmDirect.cs` — the prefix matcher, ordinary C#; constraints documented in-file
- `rtcheck/` — loads a patched assembly and executes the matcher, 18 assertions
- `tests/run.sh` — full offline test: patches synthetic assemblies and checks real behaviour
- `strm-direct.txt.example` — annotated configuration template
