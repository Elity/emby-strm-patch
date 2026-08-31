---
name: emby-strm-patch
description: Patch an Emby Server install so that .strm media (remote http/https direct links) is never transcoded and is delivered either as a 302 redirect to the origin or over several concurrent connections, instead of the server relaying every byte down one connection. Use this whenever someone mentions .strm playback being slow or stuttering, Emby proxying/relaying remote media, 206 responses on /Videos/{id}/stream or /original.*, unwanted transcoding of cloud-drive media, saturated server upload while a .strm plays, an origin that throttles per connection, patching Emby assemblies, adding a URL prefix to the routing list, switching a prefix between 302 and parallel mode, checking why a configured prefix is not taking effect, re-applying the patch after an Emby upgrade, or rolling the patch back. Reach for this skill even when the request is phrased as a vague performance complaint about network-drive libraries.
---

# Emby strm patch

Injects short IL sequences into Emby's own assemblies so that media whose `MediaSourceInfo.Path`
matches a configured URL prefix is delivered the way that origin actually needs.

| Patch | Assembly · method | Effect | Fires for |
|---|---|---|---|
| **A** | `Emby.Server.MediaEncoding.dll`<br>`MediaInfoService.SetDeviceSpecificData` | Sets `SupportsTranscoding = false`. Emby's own `ForceDirectPlay` path then keeps `SupportsDirectPlay` true and never builds a `TranscodingUrl`. | **any** configured prefix, in either mode |
| **B** | `Emby.Server.Implementations.dll`<br>`HttpResultFactory.GetStaticFileResult` | Matched sources return **302** to the origin, so `/Videos/{id}/stream` and `/original.*` stop relaying bytes. | mode `302` |
| **C** | `Emby.Server.Implementations.dll`<br>`HttpResultFactory.GetContent` | Matched sources are fetched over several concurrent Range requests and relayed. Optional, needs a helper assembly. | mode `parallel` |

No patch modifies an existing instruction. All prepend to the method body with every branch
target set to the original first instruction, so a non-matching path executes exactly as stock.
Nothing is baked into the IL — prefixes and modes are read from configuration at runtime, and
with no configuration present every patch is inert.

## The one decision: 302 or parallel

**302 removes the server from the transfer. It does not add bandwidth.** If the origin caps
*each connection* rather than total throughput, a 302 hands the client that same cap and a
high-bitrate file still stutters — the bottleneck moved, it did not go away. Only fetching over
several connections at once helps there, and that is patch C.

Which applies is a property of the **origin**, not of the machine, so it is configured **per URL
prefix**. One Emby install can legitimately have both.

**Do not guess this.** `bash parallel/probe-origin.sh '<url to a large file on that origin>'`
measures one connection against several and returns a verdict. It needs no Emby and no patch, so
it can be run before anything is installed. It takes 4–5 minutes (its runtime is set by the
origin) and it **withholds a verdict** rather than guessing when requests fail, when the baseline
drifts, or when a round comes back implausibly fast — a CDN cache hit reads exactly like a fast
origin.

Only if it says `parallel` is patch C worth installing. Default to `302`, which is also what an
omitted mode token means.

## B and C live in one assembly

They target **different methods of the same class** and carry independent marker fields, so one
patcher run installs both and the choice moves entirely into `strm-routing.txt`. Nothing enforces
mutual exclusion because nothing needs to: when B answers a redirect it returns from
`GetStaticFileResult`, so no `FileWriter` is constructed and `GetContent` is never reached.

Omitting `--parallel` is the escape hatch: only B is injected, and a 302-only install never needs
the helper assembly or a `deps.json` edit.

Design rationale: `references/mode-routing.md`. IL and injection mechanics:
`references/internals.md`.

## What the patcher looks for

Targets are located by type and method **signature**, never by file offset, so it normally works
across Emby versions unchanged. If it reports a target it cannot find, the names have moved —
these are the semantics to look for, and `references/internals.md` walks through re-deriving them:

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

**Patch C** — the method on that same type that builds the stream Emby is about to relay, taking
the same options object plus an offset, a length and a cancellation token, and returning
`Task<T>` where `T` is the stream handler. It is `async`, so its body is only the state-machine
kick-off stub and returning before the first instruction cannot strand a started machine.
Goal: for matched paths, hand back a stream that fetches over several connections.

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

- **.NET SDK 8.0+** (`dotnet --version`). Needed to build the patcher, the template and the helper.
- Write access to `SYSTEM_DIR` (possibly elevated) and permission to stop/start the server.
- Enough downtime for a restart. The swap itself takes seconds; the restart dominates.
- **Python 3** — only for mode `parallel`, to register the helper in `EmbyServer.deps.json`.

## Step 2 — Decide the mode per origin

Read one `.strm` file from each library to get the exact URL prefix, then run the probe against a
large file on each distinct origin:

```bash
bash parallel/probe-origin.sh 'https://origin.example.com/large-file.mkv'
```

Record, for each prefix, the verdict. If every origin says `302`, skip building the helper —
Steps 3–6 get simpler and nothing in `parallel/` is needed.

## Step 3 — Build the patched assemblies

`SYSTEM_DIR` doubles as the reference directory — Cecil needs the sibling assemblies to resolve
types. Read from it, **write somewhere else**; never write output into `SYSTEM_DIR` while the
server is running.

```bash
mkdir -p work/out
dotnet build -c Release template/template.csproj
dotnet build -c Release patcher/patcher.csproj
dotnet build -c Release parallel/src/EmbyStrmParallel.csproj    # only if any origin needs parallel

TPL=$PWD/template/bin/Release/net8.0/StrmDirectTemplate.dll
HLP=$PWD/parallel/src/bin/Release/net8.0/EmbyStrmParallel.dll
P=patcher/bin/Release/net8.0/embypatch.dll

# A
dotnet "$P" "$SYSTEM_DIR/Emby.Server.MediaEncoding.dll" work/out/Emby.Server.MediaEncoding.dll "$SYSTEM_DIR" "$TPL"

# B, plus C when --parallel is given. Drop the flag for a 302-only install.
dotnet "$P" "$SYSTEM_DIR/Emby.Server.Implementations.dll" work/out/Emby.Server.Implementations.dll "$SYSTEM_DIR" "$TPL" --parallel "$HLP"
```

Expected output per assembly:

```
matcher  : cloned Emby.Server.StrmDirect.StrmDirect (12 fields / 29 methods)
patch    : B - 302 redirect from the streaming endpoint
           IL before: 111
           IL after: 122  (+11)
patch    : C - parallel chunked fetch
           IL before: 29
           IL after: 59  (+30)
OK       : wrote .../Emby.Server.Implementations.dll
```

The `IL before` counts vary by Emby version — ignore them. What must hold is the `matcher` line,
the right patches being selected, and the **deltas**: **+9** for A, **+11** for B, **+30** for C.

Failure modes:

- `x No known target type in this assembly` — wrong input file.
- `x ... not found` (a specific type or method) — Emby changed shape. Re-read *What the patcher
  looks for* above, then `references/internals.md` → *Re-deriving on a new Emby version*.
  Do not improvise.
- `x Already patched` — the input was a patched assembly, not stock.
- `x Template references another type in its own assembly` — only happens if you edited
  `shared/RoutingConfig.cs`; read the constraints at the top of that file.

⚠️ **Changing the config filename or the parser means patch A must be rebuilt too.** A clones the
same matcher; an old clone still looks for the old filename and silently matches nothing. The
symptom is transcoding mysteriously coming back.

Record the sha256 of every output. Managed IL cannot be patched in place, so the file size always
changes — that is metadata rebuild, not lost content.

## Step 4 — Verify offline (before any downtime)

**Runtime check — this is the one that matters.** Decompiling to readable C# does not prove the
IL executes. Load the patched assembly and actually call the cloned matcher:

```bash
cd rtcheck
for d in Emby.Server.Implementations Emby.Server.MediaEncoding; do
  dotnet run -- "../work/out/$d.dll" "$SYSTEM_DIR"
done
# expect: ALL PASS   pass=79 fail=0
```

79 assertions: no config means no match, env-var prefixes, config files with comments and blank
lines, case-insensitivity, mode tokens and their absence, unrelated http sources staying
untouched, env beating file, missing/garbage config not throwing — **plus 61 shared parser
vectors**. Those 61 are asserted against the *same* table the helper assembly is tested with, so
a failure there means the cloned copy and the helper's copy have diverged, not that an injection
is wrong.

Also run the full offline suite if you changed anything in the repo:

```bash
bash tests/run.sh                          # patcher + injections + check-config on synthetic assemblies
bash parallel/run-tests.sh mock            # the fetcher: correctness, faults, memory  (~45 s)
bash parallel/run-tests.sh config          # config parsing, settings wiring, diagnostics
bash parallel/tests/probe-origin.test.sh   # the probe's verdicts against a simulated origin
```

**Optional static check** — decompile and read the injection back. With
`dotnet tool install -g ilspycmd --version 8.2.0.7535` and `DOTNET_ROLL_FORWARD=Major`:

```bash
ilspycmd -t Emby.Server.Implementations.HttpServer.HttpResultFactory work/out/Emby.Server.Implementations.dll | grep -A7 'GetStaticFileResult(IRequest'
ilspycmd -t Emby.Server.MediaEncoding.Api.MediaInfoService       work/out/Emby.Server.MediaEncoding.dll   | grep -A5 'SetDeviceSpecificData(long'
```

You should see `StrmDirect.IsRedirect(path)` at the top of B and `StrmDirect.IsMatch(...)` at the
top of A. Use `-l c` to count types (patched should have exactly one more than stock);
`-l c,i,s,e,d` returns nothing and is not evidence of anything.

## Step 5 — Write the configuration

Copy `strm-routing.txt.example` to `PROGRAMDATA_DIR/config/strm-routing.txt` and put the real
prefixes in it, one per line, each with the mode Step 2 established:

```
https://pan.example.com/          parallel
https://fast-cdn.example.net/     302
https://other.example/                       # omitted = 302

ramp-seconds = 6
```

- Matching is `StartsWith`, case-insensitive, against the URL **exactly as stored in the .strm
  file**. If it contains non-ASCII characters, Emby keeps it **percent-encoded** and does not
  decode it — so write the prefix percent-encoded too. A decoded prefix matches nothing, and the
  failure is completely silent.
- A **misspelled mode voids the whole line** and is reported; it is never downgraded to 302.
  Same for a misspelled setting key.
- Settings are global and only affect `parallel`. Priority is env > file > built-in default.
  Put tuning **here**, not in exported environment variables — an upgrade rewrites the launcher
  script those live in, and tuning that vanishes on upgrade resurfaces as "it started stuttering
  again" months later.

Alternatives, in priority order: `EMBY_STRM_PREFIXES` (semicolon-separated entries, same syntax
including the mode token), `EMBY_STRM_CONFIG` (absolute path), then the file lookup
(`-programdata` argument → `<system>/strm-routing.txt` → `<system>/../programdata/config/strm-routing.txt`).
Environment variables need a restart; the file is re-read every 30 seconds.

Keep prefixes narrow. Anything else Emby serves over http — Live TV channels in particular —
must not match, or it loses its transcoding fallback.

**Write the config before swapping the assemblies**, so there is never a window where the patch
is live with nothing configured.

## Step 6 — Deploy

The flow is the same everywhere; the commands are platform-specific.

1. **Back up first, while the server is still up.** Copy each stock assembly to
   `_stock_<name>_<version>.bak` beside it, and `EmbyServer.deps.json` too if you are installing
   patch C. Guard every copy with an existence check (`[ -f "$BAK" ] || cp ...`) so a second run
   never overwrites the stock backup with a patched file. Verify each backup's sha256 against
   what you recorded in Step 0.
2. **Stop Emby** and wait for the process to actually exit.
3. **Swap.** Copy each patched file in as `<name>.dll.new`, fix ownership and permissions to
   match the original, then rename over the target. Never write directly onto the live file —
   it is memory-mapped while the process runs.
4. **Only for parallel, and only the first time:** copy `EmbyStrmParallel.dll` into `SYSTEM_DIR`
   and register it:
   ```bash
   python3 parallel/deps_patch.py add "$SYSTEM_DIR/EmbyServer.deps.json" EmbyStrmParallel 1.0.0
   ```
   Emby is a framework-dependent .NET app: the host builds its assembly list from `deps.json`, so
   a DLL merely dropped into `system/` **will not load**. Verify with `deps_patch.py check`.
5. **Start Emby.** If the start command runs in the foreground and streams the log, background it
   (`>/dev/null 2>&1 &` or the platform equivalent); otherwise your shell hangs.
6. **Confirm** the on-disk sha256 of every file matches what you built.

## Step 7 — Verify on the running server

First, the offline report:

```bash
dotnet "$P" check "$PROGRAMDATA_DIR" "$SYSTEM_DIR"
```

It prints which patches are installed, whether the helper is present and registered, every prefix
→ mode → whether that mode is **satisfiable**, every setting's value **and its source**, and every
rejected line with its number. Exit 0 means clean. Note it reads the launcher script's `export`
lines as the top priority layer, because a separate process cannot inherit the server's own
environment — a report that ignored them would confidently state the wrong effective value.

Then, against the running server. Do all of these; 3 and 5 are the regressions and they are what
catch a patch that is too broad.

| # | Check | Expect |
|---|---|---|
| 1 | Startup log for `TypeLoadException`, `MissingMethodException`, `BadImageFormatException`, `InvalidProgramException`, `MissingFieldException`, `FileNotFoundException` | zero hits |
| 2 | `GET /Videos/<strmId>/stream?MediaSourceId=mediasource_<strmId>&Static=true&api_key=<key>` on a **`302`-mode** item | **302** to the origin URL |
| 2b | Same, on a **`parallel`-mode** item, `Range: bytes=<offset>-<offset+100663295>` | **206**, and throughput well above the single-connection baseline |
| 3 | Same request against a **local library** item, with `Range: bytes=0-1` | **206** plus `Content-Range` |
| 4 | `POST /Items/<strmId>/PlaybackInfo` with a DeviceProfile whose `MaxStreamingBitrate` is deliberately **below** the source bitrate | `SupportsTranscoding:false`, `SupportsDirectPlay:true`, no `TranscodingUrl` |
| 5 | Same low-bitrate PlaybackInfo against a **local library** item | `SupportsTranscoding:true`, `TranscodingUrl` present — the fallback must survive |
| 6 | `/System/Info`, a poster image, `/web/index.html` | all 200 |
| 7 | *(parallel)* 6–8 open-ended requests each abandoned after ~25 s | throughput stays flat, does not collapse |
| 8 | *(parallel)* server RSS | bounded — roughly 96 MB per concurrent stream on top of baseline |

⚠️ **Do not measure parallel throughput with a small range.** The chunk size is 8 MiB and one
chunk is one connection, so anything under ~96 MB measures connection slow-start and reports the
single-connection rate no matter how well the patch works.

If both modes are configured, the single most valuable check is **2 and 2b in the same session**:
one prefix answering 302 and another answering 206 at multi-connection throughput, from the same
binary in the same process, proves the whole routing design. Nothing else does.

Set `log = <path>` in the config to get a per-stream summary line from the fetcher; it is the
only place a stream that silently degraded shows up.

Two things that look like failures and are not:

- A probe **without** a `Range` header returns **200**, not 206, when the prefix does not match.
  The signal is "not a 302", not "must be 206".
- A health-check poll issued while Emby is still loading logs a `ServiceUnavailableException`
  and a 503. Harmless.

Judge results from request logs and byte counters. A client's self-reported play method cannot
distinguish "client to origin" from "client to server to origin". And throughput numbers do not
measure "does it stutter" — have someone actually watch something before calling it done.

## Tuning `ramp-seconds` (parallel only)

The fetcher opens 2 connections and adds one more every `ramp-seconds` up to `connections`.
It exists because abandoning a stream leaves its connections lingering at the origin, and past
some count the origin degrades sharply. The right value is **origin-specific and has a cliff** —
one notch too low and even ordinary sequential playback collapses, worse than the original bug.

The shipped default of **6** is conservative. To find your own value, sweep it with
`parallel/run-tests.sh seeks 25` — each round needs ~100 s of cooldown or you are just measuring
a degraded origin — and read that script's header, which documents the method and a measured
table. Change it in `strm-routing.txt`; it goes live within 30 seconds, no restart.

## Rollback

**Switch mode** (lightest, and usually the right first move). Change a prefix's token between
`302` and `parallel`. Live in 30 seconds, no restart, no assembly swap.

**Soft rollback.** Empty `strm-routing.txt` (or comment out every line). Within 30 seconds all
three patches are inert and the server behaves exactly like stock — no restart, no downtime. Use
this to bisect anything you suspect the patch caused.

**Hard rollback.** Stop the server, copy the `_stock_*.bak` files back over the targets, and if
patch C was installed, `python3 parallel/deps_patch.py remove <deps.json> EmbyStrmParallel` and
restore `EmbyServer.deps.json`. Start. Roll back any patch alone; they are independent.

## After an Emby upgrade

An upgrade replaces the whole application directory, so **everything in it reverts to stock**:
both patched assemblies, and for parallel also `EmbyStrmParallel.dll` and its `deps.json` entry.

`PROGRAMDATA_DIR` is untouched, so `strm-routing.txt` survives — prefixes, modes **and tuning**.
Only the binaries need redoing; there is nothing to reconstruct by hand.

Symptom: .strm playback starts stuttering or transcoding again, with no configuration change.

1. sha256 the assemblies, or just run `embypatch check` — it names what went missing.
2. Re-run Steps 3 → 4 → 6 → 7 against the new version. Record the new sha values.
3. Targets are located by signature, not offset, so a minor upgrade usually just works. If a
   target cannot be found, re-read *What the patcher looks for*, then
   `references/internals.md` → *Re-deriving on a new Emby version*.

## Changing the patcher itself

```bash
bash tests/run.sh
bash parallel/run-tests.sh mock
bash parallel/run-tests.sh config
bash parallel/tests/probe-origin.test.sh
```

All four are offline — no Emby binaries, no network. Run them after touching anything in
`patcher/`, `shared/`, `template/` or `parallel/`. `tests/run.sh` is the important one: it really
clones the matcher into a synthetic assembly and calls it, which is what exposes "cloned fine,
matches nothing at runtime". CI runs the timing-independent suites on Linux, Windows and macOS
and the full set on Linux.

## Pitfalls

- **Never overwrite a running assembly.** Stop the server, write a temp file, rename over the
  target. Ownership and permissions must match the original or Emby will not start.
- **Registering the helper is not optional for parallel.** A DLL in `system/` that is not in
  `EmbyServer.deps.json` is simply never loaded, and the symptom is patch C appearing to do
  nothing at all.
- **Prefixes must be percent-encoded** if the URL has non-ASCII characters. Emby stores the
  `.strm` URL verbatim and matching is a literal `StartsWith`, so a decoded prefix never matches
  and never logs anything. `check` warns about this.
- **Rebuild patch A whenever the parser or config filename changes.** It carries its own clone of
  the matcher.
- **Do not put tuning in the launcher script's environment.** It is overwritten by upgrades, and
  `check` — running as a different process — cannot see it, so its report would be wrong.
- **Measure parallel throughput with ≥ 96 MB ranges.** Smaller ranges measure slow-start.
- **Keep stock and patched in separate directories.** Copying a stock directory over your output
  silently reverts it, and the two are indistinguishable afterwards without a sha check.
- **Guard the backup step.** Without an existence check, a second run backs up the already-patched
  file as "stock" and destroys your rollback.
- **Run the regression checks (Step 7, #3 and #5).** The patched methods are global; only the
  prefix match keeps local-library and Live TV behaviour intact.
- **Emby's log obfuscates sensitive values with zero-width characters.** A pattern like
  `[a-f0-9]{32}` will not match an API key in the log; strip non-hex bytes first
  (`tr -cd 'a-f0-9'`). Most tokens in the log are non-admin and answer 403 — try several.
- **Editing `shared/RoutingConfig.cs`?** Read the constraint list at the top of the file first.
  Array literals and string `switch` get lowered into a `<PrivateImplementationDetails>` RVA field
  that cannot be cloned, and a static constructor is not cloned at all — which leaves fields null
  and makes the patch silently match nothing.

## Files

- `references/mode-routing.md` — why the mode is runtime configuration, the config grammar, and
  what was deliberately left out
- `references/internals.md` — how each injection point was chosen, the IL, and how to re-derive
  the targets when Emby changes shape
- `patcher/` — target detection, all three injections, idempotency markers
- `patcher/TypeCloner.cs` — deep-copies the matcher type into the target assembly
- `patcher/CheckConfig.cs` — the `check` subcommand
- `shared/RoutingConfig.cs` — the routing parser, compiled into **both** the cloneable template
  and the helper assembly; constraints documented in-file
- `shared/RoutingVectors.cs` — parse vectors both compiled copies are asserted against
- `parallel/src/` — the multi-connection fetcher (patch C's helper assembly)
- `parallel/probe-origin.sh` — does this origin throttle per connection? Run before anything else
- `parallel/deps_patch.py` — add/remove/check the helper's `EmbyServer.deps.json` entry
- `parallel/run-tests.sh` — the fetcher's suites; its header documents the `ramp-seconds` sweep
- `rtcheck/` — loads a patched assembly and executes the matcher, 79 assertions
- `tests/run.sh` — full offline test: patches synthetic assemblies and checks real behaviour
- `strm-routing.txt.example` — annotated configuration template
