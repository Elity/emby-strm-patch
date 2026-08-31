# Internals

How the three injection points were chosen, what the IL does, and how to re-derive the targets
when Emby changes shape.

The *why* — why the delivery mode is per-prefix runtime configuration rather than a build-time
choice — is in `mode-routing.md`. This file is the mechanics.

## The problem

A `.strm` file contains a single http(s) URL. Emby resolves it into a remote
`MediaProtocol.Http` source and stores that URL in `MediaSourceInfo.Path`.

Two of Emby's defaults for remote sources assume the **server** is closer to the origin than the
client is. When the origin is a public endpoint that the client can reach just as well, both
defaults become pure overhead:

```
[1] PlaybackInfo
    client reports MaxStreamingBitrate below the source bitrate
      -> ContainerBitrateExceedsLimit
      -> SupportsDirectPlay:false + TranscodingUrl
      -> client pulls .ts segments; the server runs ffmpeg
         (it downloads the full-bitrate source *and* re-encodes it — cost on both sides)

[2] Streaming endpoint
    GET /Videos/{id}/stream  or  /Videos/{id}/original.*
      -> Emby fetches the remote URL itself and forwards the bytes
      -> client sees 206; every byte crosses the server
```

The goal is one 302 and no server-side bytes — or, when the origin throttles each connection so
hard that a single connection cannot carry the file, several concurrent connections and a relay
that is still faster than one direct connection would be. Which of the two applies is a property
of the origin, so it is configuration; see `mode-routing.md` §1.

## Patch A — `MediaInfoService.SetDeviceSpecificData`

```
assembly : Emby.Server.MediaEncoding.dll
type     : Emby.Server.MediaEncoding.Api.MediaInfoService
method   : void SetDeviceSpecificData(long itemId, string mediaType, MediaSourceInfo mediaSource, ...)
```

Two overloads share the name. One takes a `PlaybackInfoResponse` and loops; the other takes a
single `MediaSourceInfo`. The patcher selects the one that has a `MediaSourceInfo` parameter and
reads that parameter's index for the `ldarg`.

The lever is already in Emby's own code, and its switch is `SupportsTranscoding`:

```csharp
if (mediaSource.SupportsDirectPlay) {
    if (!mediaSource.SupportsTranscoding) { videoOptions2.ForceDirectPlay = true; }   // bypasses profile limits
    var streamInfo = streamBuilder.BuildVideoItem(videoOptions2);
    if (streamInfo == null || !streamInfo.IsDirectStream) mediaSource.SupportsDirectPlay = false;
}
if (mediaSource.SupportsDirectStream) {
    if (!mediaSource.SupportsTranscoding) { videoOptions2.ForceDirectStream = true; }
}
if (!mediaSource.SupportsTranscoding) { return; }      // TranscodingUrl is assigned after this line
```

Emby wrote that for the case where a user policy forbids transcoding. The semantics are exactly
what is needed here, so the injection is a single assignment:

```csharp
if (mediaSource != null && StrmDirect.IsMatch(mediaSource.Path))
    mediaSource.SupportsTranscoding = false;
// original body follows unchanged
```

Everything else follows from Emby's own control flow: `ForceDirectPlay` keeps
`SupportsDirectPlay` true, `ForceDirectStream` does the same for direct stream, and the early
`return` means `TranscodingUrl` is never built.

IL (9 instructions):

```
ldarg mediaSource ; brfalse -> original first instruction
ldarg mediaSource ; callvirt get_Path() ; call StrmDirect.IsMatch ; brfalse -> original first instruction
ldarg mediaSource ; ldc.i4.0 ; callvirt set_SupportsTranscoding
```

`MediaSourceInfo.Path` and `SupportsTranscoding` are auto-properties on
`MediaBrowser.Model.Dto.MediaSourceInfo`; both default to a value that makes the un-patched
behaviour the permissive one (`SupportsTranscoding = true`, `SupportsDirectPlay = true`).

## Patch B — `HttpResultFactory.GetStaticFileResult`

```
assembly : Emby.Server.Implementations.dll
type     : Emby.Server.Implementations.HttpServer.HttpResultFactory
method   : Task<object> GetStaticFileResult(IRequest requestContext, StaticFileResultOptions options)
```

The obvious place to look is whatever handles `/Videos/{Id}/stream`, but that is not where the
paths converge. Every static-file response — including `/Videos/{id}/stream` and
`/Videos/{id}/original.*` — funnels through this method, and `options.Path` is the
`MediaSourceInfo.Path`: an http URL for .strm media, a filesystem path for local media. One test
separates them.

The same class already exposes `GetRedirectResult(string)`, which builds the 302 response object,
so nothing new has to be constructed.

```csharp
string path = options.Path;
if (StrmDirect.IsRedirect(path))
    return Task.FromResult(GetRedirectResult(path));
// original body follows unchanged
```

Note the predicate: **`IsRedirect`, not `IsMatch`.** Patch A wants "is this source configured at
all, in any mode"; B wants "is this source configured *for 302*". That one-word difference is what
lets B and C live in the same binary and be arbitrated by configuration (`mode-routing.md` §4).

IL (11 instructions):

```
ldarg options ; callvirt get_Path() ; stloc path
ldloc path ; call StrmDirect.IsRedirect ; brfalse -> original first instruction
ldarg.0 ; ldloc path ; call GetRedirectResult ; call Task.FromResult<object> ; ret
```

Choosing this convergence point covers several endpoints at once. The cost is that it also
serves local files and static assets, which is why the regression checks in SKILL.md Step 6
(#3 and #5) are not optional.

## Patch C — `HttpResultFactory.GetContent`

```
assembly : Emby.Server.Implementations.dll
type     : Emby.Server.Implementations.HttpServer.HttpResultFactory
method   : Task<StreamHandler> GetContent(StaticFileResultOptions options, long offset,
                                          long length, CancellationToken ct)
```

Optional — injected only when the patcher is given `--parallel <helper.dll>`. Where B removes the
server from the transfer, C keeps it there but makes it fetch over several concurrent Range
requests, which is the only thing that helps when the origin's cap is per connection.

`GetContent` is where Emby builds the stream it is about to relay. It is an `async` method, so its
body is nothing but the state-machine kick-off stub (29 IL on 4.9.3.0, one local, no exception
handlers). Returning before the first instruction is therefore safe in the strongest sense: the
state machine is never started.

```csharp
if (ParallelFetch.IsMatch(options.Path)) {
    var sh = new StreamHandler();
    sh.Stream = ParallelFetch.Open(path, offset, length, out var total, out var clen, ct);
    if (sh.Stream != null) {            // null => helper declined; fall through to stock
        sh.TotalLength = total;
        sh.Length = clen;
        return Task.FromResult(sh);
    }
}
// original body follows unchanged
```

`ParallelFetch.IsMatch` forwards to `StrmDirect.IsParallel`, so C fires only for prefixes whose
mode is `parallel`.

Two things make this different from A and B:

- **`StreamHandler` is never named.** Its type is read off the return type (`Task<T>`), and the
  ctor plus the `Stream` / `TotalLength` / `Length` setters are located on it. A rename of that
  class does not break the patcher.
- **The helper lives in a separate assembly**, which is the one thing this project otherwise
  avoids (see *Why not a helper DLL* below). The fetcher needs `HttpClient`, generics, `async`
  and a few thousand instructions; cloning that in is not on the table. The price is a
  `EmbyServer.deps.json` registration — `parallel/deps_patch.py` — because a DLL merely dropped
  into `system/` will not be loaded by a framework-dependent .NET app.

`Open` returning `null` means "I decline", and control falls through to the stock body. That is
the failure path for anything the helper cannot handle, and it degrades to ordinary Emby
behaviour rather than to an error.

### B and C in one assembly

They are injected into **different methods of the same type**, with independent marker fields, so
one patcher run installs both. No interlock is needed: when B answers a redirect it returns from
`GetStaticFileResult`, so no `FileWriter` is constructed and `GetContent` is never reached.
Mutual exclusion is a consequence of control flow. See `mode-routing.md` §2.

## Injection technique

All three sequences are inserted **before the original first instruction**, and every `brfalse`
target is that same original first instruction. Consequences:

- Non-matching paths execute a bounds-free null check and a `StartsWith` loop, then continue into
  code whose control flow is byte-identical to stock.
- No existing instruction is modified, no signature changes, no exception handler is touched.
- Failure is contained: if patch A somehow leaves `SupportsDirectPlay = false`, the client falls
  back to `DirectStreamUrl` (`/original.*`), which patch B redirects. The degraded mode is still
  a direct connection.

Each patch adds a private static marker field to the target type — `__strm302_patched` (B),
`__strm_notranscode_patched` (A), `__strm_parallel_patched` (C). The patcher checks for it
**before** cloning anything, so a rejected run cannot leave a half-modified assembly. Because the
three markers are independent, B and C coexist on `HttpResultFactory` without either run being
able to mistake the other's work for its own.

## Why Mono.Cecil and not byte patching

These are **managed IL** assemblies. Locating a target requires metadata awareness (type and
method signatures), and inserting instructions necessarily changes method body length and the
metadata tables. Cecil rewrites the whole assembly, so the output size and hash always differ
from stock — expected, not a symptom.

Do not round-trip through `ilasm`/`ildasm`-style tools for this. Fidelity on modern .NET
assemblies is not guaranteed, and a silently dropped attribute costs you the whole assembly.

Signature-based location is also why a minor Emby upgrade usually needs nothing but a re-run.

## The routing matcher and the type cloner

Reading configuration needs `try`/`catch`, file IO and string handling — a few hundred IL
instructions, which is not something to hand-assemble. So the matcher is written as ordinary C#
in `shared/RoutingConfig.cs`, compiled by `template/template.csproj`, and deep-copied into the
target module by `patcher/TypeCloner.cs` (landing at `Emby.Server.StrmDirect.StrmDirect`). Patch A
then just `call`s `IsMatch` and patch B `IsRedirect`.

**That same file is also compiled into the parallel helper assembly**, by
`parallel/src/EmbyStrmParallel.csproj`, via a second `<Compile Include="../../shared/…" />`. One
source file, two independent build outputs, no runtime coupling between them — which is what
keeps A and B working when the helper is not installed at all. `shared/RoutingVectors.cs` holds
the input/expected table that **both** copies are asserted against, so a divergence is a test
failure rather than a field report. The reasoning is in `mode-routing.md` §4.1.

The cloner handles: field and method shells created before bodies (they reference each other),
local variable tables, `ImportReference` for every operand, a second pass to rebind branch
targets, and exception handler boundary remapping.

Two alternatives were rejected **for the matcher**:

- **Shipping it as a separate helper DLL next to Emby's assemblies.** The .NET host resolves
  assemblies through `EmbyServer.deps.json`; anything not listed there will not load, so this
  would mean editing that file too — one more fragile moving part, and one that a 302-only user
  should not have to accept.
- **Hand-writing the IL.** See above.

Patch C does pay that price, deliberately and only for itself: a multi-connection fetcher needs
`HttpClient`, generics and `async`, none of which the cloner supports. Cloning it is not an
option, so the helper is a real assembly with a real `deps.json` entry — and it is optional, so
nobody who only wants 302 ever meets it.

### Cloner constraints

`shared/RoutingConfig.cs` documents them in-file. The one that actually bit:

```csharp
char[] seps = new char[] { ';', '\n', '\r' };   // Roslyn lowers this to
                                                //   RuntimeHelpers.InitializeArray(arr, <PrivateImplementationDetails> RVA field)
char[] seps = new char[3];                      // emits plain stelem — clonable
seps[0] = ';'; seps[1] = '\n'; seps[2] = '\r';
```

That RVA field lives in the **template** assembly's compiler-generated
`<PrivateImplementationDetails>` type and does not come across. The cloned code then carries a
dangling reference that throws at runtime — inside `Load()`'s `try/catch`, which swallows it.
The visible symptom is "configuration is never read", with nothing in the log.

It decompiles to readable-looking C# (`ILSpy` shows
`RuntimeHelpers.InitializeArray(array, (RuntimeFieldHandle)/*OpCode not supported*/)` if you look
closely), which is precisely why `rtcheck` exists: **static inspection does not prove the IL
runs.** `TypeCloner.GuardNotTemplateInternal` now refuses to emit such a reference at all.

Same reasoning excludes generics (`List<T>`, LINQ), static constructors, lambdas, iterators,
`async`, and `switch` jump tables (also lowered into `<PrivateImplementationDetails>`).

The no-static-constructor rule deserves its own warning, because it fails *silently in the other
direction*: a `.cctor` is simply not cloned, so `static readonly string[] Empty = new string[0];`
stays `null` forever in the injected copy, the resulting `NullReferenceException` is swallowed by
the same fail-soft `catch`, and the patch just never matches anything.

### Configuration resolution

The file is `strm-routing.txt`. First hit wins:

1. `EMBY_STRM_PREFIXES` — semicolon-separated entries, works on every platform
2. `EMBY_STRM_CONFIG` — absolute path to the file, works on every platform
3. `-programdata <p>` from the process command line → `<p>/config/strm-routing.txt`
4. `AppContext.BaseDirectory` → `<system>/strm-routing.txt`
5. `AppContext.BaseDirectory`'s parent → `<parent>/programdata/config/strm-routing.txt`

Each entry is either a **prefix line** (`<url-prefix> [302|parallel]`) or a **setting line**
(`key = value`); comments run to end of line. The `://` test decides which, and it is applied
*before* the `=` test so that a prefix carrying a signed query string is not read as a setting.
An unparseable line is skipped and recorded rather than voiding the file, and a *misspelled* mode
or setting key voids its own line instead of quietly falling back to a default. The full grammar
and the reasoning behind each rule are in `mode-routing.md` §3.

The result is cached for 30 seconds, so edits apply without a restart.

Any exception during loading yields an empty table. An empty table means every predicate returns
false, which means all three injections fall straight through — **no configuration produces stock
behaviour**. That is what makes the patched binary safe to hand to someone else, and it also
turns "empty the config file" into a zero-downtime rollback switch.

## Re-deriving on a new Emby version

The patcher matches by name and signature, so start by simply re-running it. If it reports a
missing target, work through these in order.

1. **Type renamed** (`x No known target type in this assembly`)
   ```bash
   ilspycmd -l c <system>/Emby.Server.Implementations.dll | grep -i resultfactory
   ilspycmd -l c <system>/Emby.Server.MediaEncoding.dll   | grep -iE "mediainfo|playbackinfo"
   ```
   Update `RedirectType` / `NoTranscodeType` in `patcher/Program.cs`. Patch C targets the same
   type as B but declares it separately as `HostType` in `patcher/PatchParallel.cs` — update both.

2. **Method signature changed**
   - B matches: name `GetStaticFileResult`, 2 parameters, second parameter type named
     `StaticFileResultOptions`.
   - A matches: name `SetDeviceSpecificData`, any parameter typed `MediaSourceInfo`.
   - C matches: name `GetContent`, 4 parameters, first typed `StaticFileResultOptions`, return
     type a generic instance (`Task<StreamHandler>`).
   ```bash
   ilspycmd -t <full type name> <assembly> | grep -n '<method name>'
   ```

3. **`GetRedirectResult` is gone** (B). Find whatever now builds a 302 in that class (search for
   `302` or `Location`). It must be a method **on the same type**, otherwise the
   `il.Create(OpCodes.Call, getRedirect)` needs `module.ImportReference(...)`.

4. **The `ForceDirectPlay` lever changed** (A) — the one to watch most closely.
   ```bash
   ilspycmd -t Emby.Server.MediaEncoding.Api.MediaInfoService <assembly> \
     | grep -nE "ForceDirectPlay|ForceDirectStream|SupportsTranscoding"
   ```
   You must still see `!SupportsTranscoding -> ForceDirectPlay = true` **and**
   `if (!mediaSource.SupportsTranscoding) return;`. If either is gone, a single assignment is no
   longer equivalent to patch A, and you need a different approach — injecting
   `SupportsDirectPlay = true; TranscodingUrl = null;` before every `ret`, which is a
   significantly more invasive change.

5. **`MediaSourceInfo.Path` / `SupportsTranscoding` renamed.** `FindAccessor` walks the base
   chain; update the property name constants.

6. **`StreamHandler` changed shape** (C). The patcher never names the type — it reads it off
   `Task<T>` — but it does require a parameterless constructor plus settable `Stream`,
   `TotalLength` and `Length`. If `GetContent` stopped being `async`, re-check the assumption in
   `PatchParallel.cs` that returning before the first instruction cannot strand a started state
   machine.

7. **Verify the result**, statically and at runtime. `rtcheck` must pass 79/79 — 61 of those are
   the shared parser vectors, so a failure there means the cloned copy and the helper's copy have
   diverged, not that an injection is wrong — and the decompiled method must show the injected
   `if` at the top.

**Stop rule:** if two or more of items 1–6 fail to match, Emby has restructured this path. Redo
the decompilation study rather than forcing the patch through. Shipping an uncertain patch to a
running server costs far more than another half hour of reading.
