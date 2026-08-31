using System.Reflection;

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
File.Delete(cfg);

Console.WriteLine($"\n{(fail == 0 ? "ALL PASS" : "FAILURES")}   pass={pass} fail={fail}");
return fail == 0 ? 0 : 1;
