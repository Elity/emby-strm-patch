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
var isMatch = t.GetMethod("IsMatch", BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)!;
var fPrefixes = t.GetField("_prefixes", BindingFlags.Static | BindingFlags.NonPublic)!;
var fNext = t.GetField("_nextReload", BindingFlags.Static | BindingFlags.NonPublic)!;

bool M(string? s) {
    try { return (bool)isMatch.Invoke(null, new object?[] { s })!; }
    catch (TargetInvocationException e) { Console.WriteLine("    !! threw: " + e.InnerException); return false; }
}
void Reset() { fPrefixes.SetValue(null, null); fNext.SetValue(null, 0L); }   // bust the 30s cache

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
var f = Path.Combine(Path.GetTempPath(), "strm-direct-test.txt");
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

Console.WriteLine($"\n{(fail == 0 ? "ALL PASS" : "FAILURES")}   pass={pass} fail={fail}");
return fail == 0 ? 0 : 1;
