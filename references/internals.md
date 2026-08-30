# Internals

How the two injection points were chosen, what the IL does, and how to re-derive the targets
when Emby changes shape.

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

The goal is one 302 and no server-side bytes.

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
if (StrmDirect.IsMatch(path))
    return Task.FromResult(GetRedirectResult(path));
// original body follows unchanged
```

IL (11 instructions):

```
ldarg options ; callvirt get_Path() ; stloc path
ldloc path ; call StrmDirect.IsMatch ; brfalse -> original first instruction
ldarg.0 ; ldloc path ; call GetRedirectResult ; call Task.FromResult<object> ; ret
```

Choosing this convergence point covers several endpoints at once. The cost is that it also
serves local files and static assets, which is why the regression checks in SKILL.md Step 6
(#3 and #5) are not optional.

## Injection technique

Both sequences are inserted **before the original first instruction**, and both `brfalse` targets
are that same original first instruction. Consequences:

- Non-matching paths execute a bounds-free null check and a `StartsWith` loop, then continue into
  code whose control flow is byte-identical to stock.
- No existing instruction is modified, no signature changes, no exception handler is touched.
- Failure is contained: if patch A somehow leaves `SupportsDirectPlay = false`, the client falls
  back to `DirectStreamUrl` (`/original.*`), which patch B redirects. The degraded mode is still
  a direct connection.

Each patch adds a private static marker field (`__strm302_patched`,
`__strm_notranscode_patched`) to the target type. The patcher checks for it **before** cloning
anything, so a rejected run cannot leave a half-modified assembly.

## Why Mono.Cecil and not byte patching

These are **managed IL** assemblies. Locating a target requires metadata awareness (type and
method signatures), and inserting instructions necessarily changes method body length and the
metadata tables. Cecil rewrites the whole assembly, so the output size and hash always differ
from stock — expected, not a symptom.

Do not round-trip through `ilasm`/`ildasm`-style tools for this. Fidelity on modern .NET
assemblies is not guaranteed, and a silently dropped attribute costs you the whole assembly.

Signature-based location is also why a minor Emby upgrade usually needs nothing but a re-run.

## The prefix matcher and the type cloner

Reading configuration needs `try`/`catch`, file IO and string handling — a few hundred IL
instructions, which is not something to hand-assemble. So the matcher is written as ordinary C#
in `template/StrmDirect.cs`, compiled, and deep-copied into the target module by
`patcher/TypeCloner.cs` (landing at `Emby.Server.StrmDirect.StrmDirect`). Both injection points
then just `call IsMatch`.

The cloner handles: field and method shells created before bodies (they reference each other),
local variable tables, `ImportReference` for every operand, a second pass to rebind branch
targets, and exception handler boundary remapping.

Two alternatives were rejected:

- **Shipping a separate helper DLL next to Emby's assemblies.** The .NET host resolves assemblies
  through `EmbyServer.deps.json`; anything not listed there will not load, so this would mean
  editing that file too — one more fragile moving part.
- **Hand-writing the IL.** See above.

### Template constraints

`template/StrmDirect.cs` documents them in-file. The one that actually bit:

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

### Configuration resolution

First hit wins:

1. `EMBY_STRM_PREFIXES` — semicolon-separated values, works on every platform
2. `EMBY_STRM_CONFIG` — absolute path to the file, works on every platform
3. `-programdata <p>` from the process command line → `<p>/config/strm-direct.txt`
4. `AppContext.BaseDirectory` → `<system>/strm-direct.txt`
5. `AppContext.BaseDirectory`'s parent → `<parent>/programdata/config/strm-direct.txt`

Values are split on `;`, newline and carriage return; entries are trimmed; entries starting with
`#` are dropped. The result is cached for 30 seconds, so edits apply without a restart.

Any exception during loading yields an empty list. Empty list means `IsMatch` always returns
false, which means both injections fall straight through — **no configuration produces stock
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
   Update `RedirectType` / `NoTranscodeType` in `patcher/Program.cs`.

2. **Method signature changed**
   - B matches: name `GetStaticFileResult`, 2 parameters, second parameter type named
     `StaticFileResultOptions`.
   - A matches: name `SetDeviceSpecificData`, any parameter typed `MediaSourceInfo`.
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

6. **Verify the result**, statically and at runtime. `rtcheck` must pass 18/18, and the
   decompiled method must show the injected `if` at the top.

**Stop rule:** if two or more of items 1–5 fail to match, Emby has restructured this path. Redo
the decompilation study rather than forcing the patch through. Shipping an uncertain patch to a
running server costs far more than another half hour of reading.
