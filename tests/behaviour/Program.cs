using System.Reflection;
using System.Net;
using System.Text;

// Behavioural test: patch two synthetic assemblies that have the same shape as Emby's, then
// actually invoke the patched methods and check what they return. No Emby binaries required,
// so this runs in CI.
//
// usage: dotnet run -- <patchedRedirect.dll> <patchedNoTranscode.dll>

string redirectDll = Path.GetFullPath(args[0]);
string noTranscodeDll = Path.GetFullPath(args[1]);

int pass = 0, fail = 0;
void Check(string name, object? got, object? want)
{
    bool ok = Equals(got, want);
    if (ok) pass++; else fail++;
    Console.WriteLine($"  [{(ok ? "PASS" : "FAIL")}] {name}");
    if (!ok) Console.WriteLine($"         got  {got ?? "<null>"}\n         want {want ?? "<null>"}");
}

// --- redirect fixture -------------------------------------------------------

var rAsm = Assembly.LoadFrom(redirectDll);
var factoryType = rAsm.GetType("Emby.Server.Implementations.HttpServer.HttpResultFactory", true)!;
var optionsType = rAsm.GetType("MediaBrowser.Controller.Net.StaticFileResultOptions", true)!;
var factory = Activator.CreateInstance(factoryType)!;
var getStatic = factoryType.GetMethod("GetStaticFileResult")!;

// Present only when the assembly was patched with --parallel. Resolved through the patched
// assembly's own reference so this reports "not injected" rather than "file missing" - those
// are different failures and only one of them is a bug in the patcher.
Assembly? parallelHelper = null;
if (rAsm.GetReferencedAssemblies().Any(a => a.Name == "EmbyStrmParallel"))
{
    string sideBySide = Path.Combine(Path.GetDirectoryName(redirectDll)!, "EmbyStrmParallel.dll");
    if (!File.Exists(sideBySide))
        throw new FileNotFoundException(
            "the patched assembly references EmbyStrmParallel but the helper is not beside it; " +
            "on a real install this is the deps.json step (see deps_patch.py)", sideBySide);
    parallelHelper = Assembly.LoadFrom(sideBySide);
}

string? Serve(string? path)
{
    var opts = Activator.CreateInstance(optionsType)!;
    optionsType.GetProperty("Path")!.SetValue(opts, path);
    var task = (Task<object>)getStatic.Invoke(factory, new object?[] { null, opts })!;
    return (string?)task.GetAwaiter().GetResult();
}

// --- no-transcode fixture ---------------------------------------------------

var nAsm = Assembly.LoadFrom(noTranscodeDll);
var svcType = nAsm.GetType("Emby.Server.MediaEncoding.Api.MediaInfoService", true)!;
var msiType = nAsm.GetType("MediaBrowser.Model.Dto.MediaSourceInfo", true)!;
var svc = Activator.CreateInstance(svcType)!;
var setData = svcType.GetMethod("SetDeviceSpecificData")!;

bool SupportsTranscodingAfter(string? path)
{
    var msi = Activator.CreateInstance(msiType)!;
    msiType.GetProperty("Path")!.SetValue(msi, path);
    setData.Invoke(svc, new object?[] { 1L, "Video", msi, true, true });
    return (bool)msiType.GetProperty("SupportsTranscoding")!.GetValue(msi)!;
}

void NullSourceDoesNotThrow()
{
    setData.Invoke(svc, new object?[] { 1L, "Video", null, true, true });
}

// --- cache control ----------------------------------------------------------

void ResetMatchers()
{
    foreach (var a in new[] { rAsm, nAsm })
    {
        var t = a.GetType("Emby.Server.StrmDirect.StrmDirect", true)!;
        t.GetMethod("InvalidateCache", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, null);
    }
}

// --- assertions -------------------------------------------------------------

Console.WriteLine("1) no configuration - both patches must be inert");
Environment.SetEnvironmentVariable("EMBY_STRM_PREFIXES", null);
Environment.SetEnvironmentVariable("EMBY_STRM_CONFIG", "/no/such/file");
ResetMatchers();
Check("remote url is still relayed", Serve("https://pan.example.com/d/a.mkv"), "STREAM:https://pan.example.com/d/a.mkv");
Check("local path is still relayed", Serve("/media/movies/a.mkv"), "STREAM:/media/movies/a.mkv");
Check("transcoding stays available", SupportsTranscodingAfter("https://pan.example.com/d/a.mkv"), true);

Console.WriteLine("\n2) prefix configured - patch B redirects");
Environment.SetEnvironmentVariable("EMBY_STRM_PREFIXES", "https://pan.example.com/;https://alist.example.org/d/");
ResetMatchers();
Check("matched url redirects", Serve("https://pan.example.com/d/a.mkv"), "REDIRECT:https://pan.example.com/d/a.mkv");
Check("second prefix redirects", Serve("https://alist.example.org/d/b.mkv"), "REDIRECT:https://alist.example.org/d/b.mkv");
Check("case insensitive", Serve("HTTPS://PAN.EXAMPLE.COM/d/a.mkv"), "REDIRECT:HTTPS://PAN.EXAMPLE.COM/d/a.mkv");
Check("local path untouched", Serve("/media/movies/a.mkv"), "STREAM:/media/movies/a.mkv");
Check("unrelated http source untouched", Serve("https://cdn.example.net/live/ch.m3u8"), "STREAM:https://cdn.example.net/live/ch.m3u8");
Check("null path does not throw", Serve(null), "STREAM:");

Console.WriteLine("\n3) prefix configured - patch A disables transcoding");
Check("matched source loses transcoding", SupportsTranscodingAfter("https://pan.example.com/d/a.mkv"), false);
Check("local source keeps transcoding", SupportsTranscodingAfter("/media/movies/a.mkv"), true);
Check("unrelated http source keeps transcoding", SupportsTranscodingAfter("https://cdn.example.net/live/ch.m3u8"), true);
Check("null path keeps transcoding", SupportsTranscodingAfter(null), true);
try { NullSourceDoesNotThrow(); Check("null MediaSourceInfo does not throw", true, true); }
catch (Exception e) { Check("null MediaSourceInfo does not throw: " + (e.InnerException?.GetType().Name ?? e.GetType().Name), false, true); }

Console.WriteLine("\n4) config file is honoured");
Environment.SetEnvironmentVariable("EMBY_STRM_PREFIXES", null);
var cfg = Path.Combine(Path.GetTempPath(), "strm-routing-behaviour.txt");
File.WriteAllText(cfg, "# comment\n\n  https://from-file.example/  \n");
Environment.SetEnvironmentVariable("EMBY_STRM_CONFIG", cfg);
ResetMatchers();
Check("file prefix redirects", Serve("https://from-file.example/x.mkv"), "REDIRECT:https://from-file.example/x.mkv");
Check("file prefix disables transcoding", SupportsTranscodingAfter("https://from-file.example/x.mkv"), false);
Check("other url unaffected", Serve("https://pan.example.com/d/a.mkv"), "STREAM:https://pan.example.com/d/a.mkv");

Console.WriteLine("\n5) patch A covers every mode - transcoding would defeat both of them");
File.WriteAllText(cfg, "https://par.example/   parallel\nhttps://typo.example/  paralell\n");
ResetMatchers();
Check("a parallel prefix still loses transcoding", SupportsTranscodingAfter("https://par.example/a.mkv"), false);
Check("a line with a misspelled mode is void, not a match", SupportsTranscodingAfter("https://typo.example/a.mkv"), true);

// Patch B asks IsRedirect, not IsMatch (references/mode-routing.md 4). This is what makes one binary
// able to carry B and C at once: a prefix routed to parallel must reach GetContent, and it only
// gets there if GetStaticFileResult declines to redirect it. If B ever regressed to IsMatch,
// every parallel prefix would silently become a 302 - the mode switch would look configured and
// do nothing, which is the failure this whole design exists to prevent.
Console.WriteLine("\n6) patch B redirects only the 302 mode");
File.WriteAllText(cfg, "https://redir.example/    302\nhttps://par.example/      parallel\nhttps://plain.example/\nhttps://typo.example/     paralell\n");
ResetMatchers();
Check("explicit 302 prefix redirects", Serve("https://redir.example/a.mkv"), "REDIRECT:https://redir.example/a.mkv");
Check("bare prefix defaults to 302 and redirects", Serve("https://plain.example/a.mkv"), "REDIRECT:https://plain.example/a.mkv");
Check("parallel prefix is NOT redirected", Serve("https://par.example/a.mkv"), "STREAM:https://par.example/a.mkv");
Check("misspelled mode is not redirected either", Serve("https://typo.example/a.mkv"), "STREAM:https://typo.example/a.mkv");

// --- patch C: the injected call site itself ------------------------------------------------
//
// Everything above tests patches A and B. Patch C was only ever tested by calling the helper
// directly, which cannot see the part that is easiest to get wrong: the IL. A helper that works
// perfectly proves nothing if Emby never calls it, if the null-means-fallback branch is wired
// backwards, or if TotalLength and Length are assigned to the wrong properties.
//
// This section invokes the *patched* GetContent through reflection, against a real loopback
// origin, and checks which path ran by looking at what came back.

if (parallelHelper == null)
{
    Console.WriteLine("\n7) patch C injection - SKIPPED (assembly was patched without --parallel)");
}
else
{
    Console.WriteLine("\n7) patch C injection - the host actually reaches the helper");

    var shType = rAsm.GetType("MediaBrowser.Model.IO.StreamHandler", true)!;
    var getContent = factoryType.GetMethod("GetContent")!;

    // The helper carries its own copy of the routing parser (one source file, two binaries), so
    // it has its own 30s cache to invalidate. Missing this would make every case below read a
    // stale config and pass or fail for the wrong reason.
    var helperStrm = parallelHelper.GetType("StrmDirect", true)!;
    var helperInvalidate = helperStrm.GetMethod("InvalidateCache", BindingFlags.Static | BindingFlags.NonPublic)!;

    void Configure(string contents)
    {
        File.WriteAllText(cfg, contents);
        ResetMatchers();
        helperInvalidate.Invoke(null, null);
    }

    // (path, offset, length) -> (stream contents or marker, totalLength, length)
    (string Body, long? Total, long? Len) Content(string path, long offset, long length)
    {
        var opts = Activator.CreateInstance(optionsType)!;
        optionsType.GetProperty("Path")!.SetValue(opts, path);
        var task = (Task)getContent.Invoke(factory, new object?[] { opts, offset, length, CancellationToken.None })!;
        task.GetAwaiter().GetResult();
        var handler = task.GetType().GetProperty("Result")!.GetValue(task)!;
        var stream = (Stream)shType.GetProperty("Stream")!.GetValue(handler)!;
        var total = (long?)shType.GetProperty("TotalLength")!.GetValue(handler);
        var len = (long?)shType.GetProperty("Length")!.GetValue(handler);
        using var ms = new MemoryStream();
        stream.CopyTo(ms);
        stream.Dispose();
        return (Encoding.ASCII.GetString(ms.ToArray()), total, len);
    }

    using var origin = new TinyOrigin(4096);
    string good = origin.Url;

    // (a) not routed to parallel -> Emby's own path, untouched.
    Configure($"{origin.Prefix}  302\n");
    Check("a 302-mode prefix never reaches the helper", Content(good, 0, 0).Body, "HOST-PATH:" + good);

    // (b) routed to parallel -> the helper runs and its results land on the right properties.
    Configure($"{origin.Prefix}  parallel\n");
    var got = Content(good, 100, 500);
    Check("parallel prefix is served by the helper", got.Body, origin.Expect(100, 500));
    Check("TotalLength carries the RESOURCE size", got.Total, (long?)4096);
    Check("Length carries the RANGE size", got.Len, (long?)500);

    var whole = Content(good, 0, 0);
    Check("open-ended request delivers the whole resource", whole.Body.Length, 4096);
    Check("open-ended TotalLength", whole.Total, (long?)4096);

    // (c) the helper declines -> the injected brfalse must fall through to Emby, not fail the
    //     request. A 404 from the origin is the cheapest way to force a decline.
    string dead = origin.Prefix + "missing.mkv";
    Configure($"{origin.Prefix}  parallel\n");
    Check("an origin that cannot serve the request falls back to the host path",
          Content(dead, 0, 0).Body.StartsWith("HOST-PATH:"), true);

    // (d) an origin with no complete-length must decline rather than let Emby publish this
    //     stream's range length as the file length. This is the case direct helper tests could
    //     never see, because it is the HOST's `TotalLength ?? Stream.Length` that does the damage.
    using var unknownTotal = new TinyOrigin(4096) { OmitTotal = true };
    Configure($"{unknownTotal.Prefix}  parallel\n");
    Check("an origin with an unknown total falls back instead of inventing one",
          Content(unknownTotal.Url, 100, 500).Body.StartsWith("HOST-PATH:"), true);
}

File.Delete(cfg);

Console.WriteLine($"\n{(fail == 0 ? "ALL PASS" : "FAILURES")}   pass={pass} fail={fail}");
return fail == 0 ? 0 : 1;

/// <summary>
/// A loopback HTTP origin that speaks just enough of RFC 7233 for patch C's helper: 206 with a
/// Content-Range, honest byte content, and an option to withhold the complete-length.
///
/// Content is 'A'..'Z' repeating by absolute offset, so a body delivered from the wrong offset
/// is visible in the assertion message rather than showing up as a length mismatch.
/// </summary>
sealed class TinyOrigin : IDisposable
{
    private readonly HttpListener _listener = new();
    private readonly long _size;

    public string Prefix { get; }
    public string Url => Prefix + "file.mkv";

    /// <summary>Answer "bytes X-Y/*" instead of a real total.</summary>
    public bool OmitTotal { get; init; }

    public TinyOrigin(long size)
    {
        _size = size;
        var probe = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        int port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        Prefix = $"http://127.0.0.1:{port}/";
        _listener.Prefixes.Add(Prefix);
        _listener.Start();
        _ = Task.Run(Loop);
    }

    public string Expect(long offset, long length)
    {
        long end = length > 0 ? Math.Min(offset + length, _size) : _size;
        var sb = new StringBuilder();
        for (long i = offset; i < end; i++) sb.Append((char)('A' + (int)(i % 26)));
        return sb.ToString();
    }

    private async Task Loop()
    {
        while (_listener.IsListening)
        {
            HttpListenerContext ctx;
            try { ctx = await _listener.GetContextAsync(); } catch { return; }
            _ = Task.Run(async () =>
            {
                try
                {
                    // Exactly one resource exists; anything else stands in for an origin that
                    // cannot serve the request at all.
                    if (ctx.Request.Url!.AbsolutePath != "/file.mkv")
                    {
                        ctx.Response.StatusCode = 404;
                        ctx.Response.Close();
                        return;
                    }

                    long from = 0, to = _size - 1;
                    string? range = ctx.Request.Headers["Range"];
                    if (range != null && range.StartsWith("bytes="))
                    {
                        var parts = range[6..].Split('-');
                        from = long.Parse(parts[0]);
                        if (parts.Length > 1 && parts[1].Length > 0) to = Math.Min(long.Parse(parts[1]), _size - 1);
                    }

                    ctx.Response.StatusCode = 206;
                    ctx.Response.Headers["Content-Range"] =
                        $"bytes {from}-{to}/" + (OmitTotal ? "*" : _size.ToString());

                    var body = Encoding.ASCII.GetBytes(Expect(from, to - from + 1));
                    ctx.Response.ContentLength64 = body.Length;
                    await ctx.Response.OutputStream.WriteAsync(body);
                    ctx.Response.Close();
                }
                catch { try { ctx.Response.Abort(); } catch { } }
            });
        }
    }

    public void Dispose()
    {
        try { _listener.Stop(); } catch { }
        try { _listener.Close(); } catch { }
    }
}
