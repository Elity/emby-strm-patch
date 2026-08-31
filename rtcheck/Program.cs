using System.Reflection;

// Actually execute the cloned matcher. Decompiling to readable C# does not prove the IL runs —
// a dangling <PrivateImplementationDetails> reference decompiles fine and throws at runtime,
// where a try/catch swallows it and the patch silently does nothing. This catches that.
//
// usage: dotnet run -- <patched.dll> [dependencyDir]

string dll = args.Length > 0 ? args[0] : throw new ArgumentException("pass the patched assembly path");
string probeDir = args.Length > 1 ? Path.GetFullPath(args[1]) : Path.GetDirectoryName(Path.GetFullPath(dll))!;

AppDomain.CurrentDomain.AssemblyResolve += (_, e) => {
    var p = Path.Combine(probeDir, new AssemblyName(e.Name).Name + ".dll");
    return File.Exists(p) ? Assembly.LoadFrom(p) : null;
};

var asm = Assembly.LoadFrom(Path.GetFullPath(dll));
var t = asm.GetType("Emby.Server.StrmDirect.StrmDirect", throwOnError: true)!;
const BindingFlags Any = BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public;
var isMatch = t.GetMethod("IsMatch", Any)!;
var isRedirect = t.GetMethod("IsRedirect", Any)!;
var isParallel = t.GetMethod("IsParallel", Any)!;
var getSetting = t.GetMethod("GetSetting", Any)!;
var getSettingSource = t.GetMethod("GetSettingSource", Any)!;
var getErrors = t.GetMethod("GetErrors", Any)!;
var getRoutes = t.GetMethod("GetRoutes", Any)!;
var getSettings = t.GetMethod("GetSettings", Any)!;
var invalidate = t.GetMethod("InvalidateCache", Any)!;

bool Ask(MethodInfo m, string? s) {
    try { return (bool)m.Invoke(null, new object?[] { s })!; }
    catch (TargetInvocationException e) { Console.WriteLine("    !! threw: " + e.InnerException); return false; }
}
bool M(string? s) => Ask(isMatch, s);
string? Setting(MethodInfo m, string key) {
    try { return (string?)m.Invoke(null, new object?[] { key }); }
    catch (TargetInvocationException e) { Console.WriteLine("    !! threw: " + e.InnerException); return null; }
}
void Reset() { invalidate.Invoke(null, null); }   // bust the 30s cache

int pass = 0, fail = 0;
void Check(string name, bool got, bool want) {
    bool ok = got == want; if (ok) pass++; else fail++;
    Console.WriteLine($"  [{(ok ? "PASS" : "FAIL")}] {name}  (got {got}, want {want})");
}

Console.WriteLine($"assembly: {Path.GetFileName(dll)}\n");

Console.WriteLine("1) no configuration at all - must behave exactly like stock Emby");
Environment.SetEnvironmentVariable("EMBY_STRM_PREFIXES", null);
Environment.SetEnvironmentVariable("EMBY_STRM_CONFIG", null);
Reset();
Check("remote url does not match", M("https://pan.example.com/d/x.mkv"), false);
Check("local path does not match", M("/media/movies/a.mkv"), false);
Check("null does not throw", M(null), false);
Check("empty string does not throw", M(""), false);

Console.WriteLine("\n2) EMBY_STRM_PREFIXES (semicolon separated)");
Environment.SetEnvironmentVariable("EMBY_STRM_PREFIXES", "https://pan.example.com/;https://alist.example.org/d/");
Reset();
Check("first prefix matches", M("https://pan.example.com/d/a.mkv"), true);
Check("second prefix matches", M("https://alist.example.org/d/b.mkv"), true);
Check("case insensitive", M("HTTPS://PAN.EXAMPLE.COM/d/a.mkv"), true);
Check("unrelated http source is untouched", M("https://cdn.example.net/live/channel.m3u8"), false);
Check("local path does not match", M("/media/movies/a.mkv"), false);
Check("any path under the prefix matches", M("https://pan.example.com/other"), true);

Console.WriteLine("\n3) config file via EMBY_STRM_CONFIG (comments, blank lines, whitespace)");
Environment.SetEnvironmentVariable("EMBY_STRM_PREFIXES", null);
var f = Path.Combine(Path.GetTempPath(), "strm-routing-test.txt");
File.WriteAllText(f, "# prefixes\n\n  https://pan.example.com/  \n\n# another comment\nhttps://two.example/d/\n");
Environment.SetEnvironmentVariable("EMBY_STRM_CONFIG", f);
Reset();
Check("first prefix read and trimmed", M("https://pan.example.com/d/a.mkv"), true);
Check("second prefix read", M("https://two.example/d/b.mkv"), true);
Check("comment line is not a prefix", M("# prefixes and more"), false);
Check("blank line did not become a match-everything prefix", M("arbitrary string"), false);

Console.WriteLine("\n4) environment variable wins over the file");
Environment.SetEnvironmentVariable("EMBY_STRM_PREFIXES", "https://env.only/");
Reset();
Check("env prefix is used", M("https://env.only/x"), true);
Check("file prefix is ignored", M("https://pan.example.com/d/a.mkv"), false);

Console.WriteLine("\n5) broken or missing configuration must not take the server down");
Environment.SetEnvironmentVariable("EMBY_STRM_PREFIXES", null);
Environment.SetEnvironmentVariable("EMBY_STRM_CONFIG", "/no/such/path/xxx.txt");
Reset();
Check("missing file -> false, no throw", M("https://pan.example.com/d/a.mkv"), false);
File.WriteAllText(f, "\0\0\0\n;;;;\n   \n");
Environment.SetEnvironmentVariable("EMBY_STRM_CONFIG", f);
Reset();
Check("garbage content -> no throw", M("https://pan.example.com/d/a.mkv"), false);
File.Delete(f);

// 6) The shared vector table, run against the CLONED copy of the parser. The same table runs
//    against the helper's copy in EmbyStrmParallel.Tests, which is what stops check-config and
//    the runtime from ever drifting apart (references/mode-routing.md 4.1).
Console.WriteLine("\n6) shared parse vectors (identical table runs against the helper build)");
var vectorFile = Path.Combine(Path.GetTempPath(), "strm-routing-vectors-" + Guid.NewGuid().ToString("N") + ".txt");
File.WriteAllText(vectorFile, RoutingVectors.Text());
try
{
    RoutingVectors.ResetEnvironment(vectorFile);
    Reset();
    int n = RoutingVectors.Run(
        s => Ask(isMatch, s),
        s => Ask(isRedirect, s),
        s => Ask(isParallel, s),
        k => Setting(getSetting, k),
        k => Setting(getSettingSource, k),
        () => (string[])getErrors.Invoke(null, null)!,
        () => (string[])getRoutes.Invoke(null, null)!,
        () => (string[])getSettings.Invoke(null, null)!,
        (name, ok) => Check(name, ok, true));
    Console.WriteLine($"   ({n} vector checks)");
}
finally
{
    File.Delete(vectorFile);
    foreach (var v in RoutingVectors.EnvVars()) Environment.SetEnvironmentVariable(v, null);
    Reset();
}

Console.WriteLine($"\n{(fail == 0 ? "ALL PASS" : "FAILURES")}   pass={pass} fail={fail}");
return fail == 0 ? 0 : 1;
