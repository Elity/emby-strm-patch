using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using Mono.Cecil;

// `embypatch check <programdata-dir> <emby-system-dir>`
//
// Answers one question: would this installation actually do what strm-routing.txt asks for?
//
// It lives inside the patcher, not in a shell script, for two reasons that are not negotiable
// (references/mode-routing.md §5):
//
//   · "is patch C installed" is a marker field inside a DLL, which is Cecil's job
//   · the config must be parsed by THE SAME parser the server runs. shared/RoutingConfig.cs is
//     compiled into this executable exactly like it is compiled into the template and into the
//     helper, so a line this report accepts is a line the server accepts. A second, hand-rolled
//     parser here would eventually disagree with the runtime, and "the checker says it is fine"
//     is the worst possible starting point for debugging.
//
// It never repairs anything. A prefix asking for a mode whose patch is missing is reported and
// the exit code goes to 1; it is not quietly downgraded. Silent behaviour changes are the exact
// failure mode this whole project is built to avoid (mode-routing.md §5, §11).
internal static class CheckConfig
{
    // Names of the DLLs and the deps entry, all relative to <emby-system-dir>.
    private const string ImplDll   = "Emby.Server.Implementations.dll";
    private const string MencDll   = "Emby.Server.MediaEncoding.dll";
    private const string HelperDll = "EmbyStrmParallel.dll";
    private const string DepsJson  = "EmbyServer.deps.json";
    private const string HelperAsm = "EmbyStrmParallel";

    // Built-in defaults, for the "nothing overrides this" case. Source of truth is
    // parallel/src/ParallelFetchOptions.cs; these are only ever printed, never applied.
    private static (string Key, string Default)[] SettingKeys()
    {
        return new[]
        {
            ("ramp-seconds",        "6"),
            ("connections",         "8"),
            ("chunk-mb",            "8"),
            ("buffer-mb",           "128"),
            ("initial-connections", "2"),
            ("log",                 "(off)"),
        };
    }

    private enum Tri { Yes, No, Unknown }

    public static int Run(string[] args)
    {
        if (args.Length < 2)
        {
            Console.Error.WriteLine("usage: embypatch check <programdata-dir> <emby-system-dir>");
            Console.Error.WriteLine("  programdata-dir  the directory holding config/ and logs/  (Emby's -programdata)");
            Console.Error.WriteLine("  emby-system-dir  the directory holding the patched DLLs   (Emby's system/)");
            return 2;
        }

        string programData = Path.GetFullPath(args[0]);
        string systemDir   = Path.GetFullPath(args[1]);

        var errors = new List<string>();
        var warns  = new List<string>();

        Console.WriteLine("== embypatch check ==");
        Console.WriteLine($"programdata : {programData}");
        Console.WriteLine($"system      : {systemDir}");
        if (!Directory.Exists(programData)) errors.Add($"programdata directory does not exist: {programData}");
        if (!Directory.Exists(systemDir))   errors.Add($"system directory does not exist: {systemDir}");

        // ---- 1. the three patches -------------------------------------------------------
        string implPath = Path.Combine(systemDir, ImplDll);
        string mencPath = Path.Combine(systemDir, MencDll);

        string noteA, noteB, noteC;
        Tri a = HasField(mencPath, Program.NoTranscodeType, Program.NoTranscodeMarker, out noteA);
        Tri b = HasField(implPath, Program.RedirectType,    Program.RedirectMarker,    out noteB);
        Tri c = HasField(implPath, Program.RedirectType,    PatchParallel.Marker,      out noteC);

        Console.WriteLine();
        Console.WriteLine("-- patches --");
        Line("A  no transcoding", a, MencDll, Program.NoTranscodeMarker, noteA);
        Line("B  302 redirect  ", b, ImplDll, Program.RedirectMarker,    noteB);
        Line("C  parallel relay", c, ImplDll, PatchParallel.Marker,      noteC);

        if (a == Tri.Unknown) errors.Add("cannot determine patch A: " + noteA);
        if (b == Tri.Unknown) errors.Add("cannot determine patch B: " + noteB);

        // ---- 2. the helper assembly -----------------------------------------------------
        string helperPath = Path.Combine(systemDir, HelperDll);
        string depsPath   = Path.Combine(systemDir, DepsJson);
        bool helperPresent = File.Exists(helperPath);

        string depsNote;
        Tri depsOk = DepsRegisters(depsPath, HelperAsm, out depsNote);

        Console.WriteLine();
        Console.WriteLine("-- helper assembly (patch C only) --");
        Console.WriteLine($"  {HelperDll,-28} {(helperPresent ? "present" : "ABSENT")}" +
                          (helperPresent ? $"   {new FileInfo(helperPath).Length} bytes" : ""));
        Console.WriteLine($"  {DepsJson,-28} {(depsOk == Tri.Yes ? "registers " + HelperAsm : depsOk == Tri.No ? "NOT REGISTERED" : "?")}" +
                          (depsNote == null ? "" : $"   {depsNote}"));
        if (depsOk == Tri.No || depsOk == Tri.Unknown)
            Console.WriteLine("     (.NET builds its trusted-assembly list from deps.json; an unregistered DLL never loads)");

        // ---- 3. routing configuration ---------------------------------------------------
        //
        // Emby's own environment is set by `export` lines inside bin/emby-server, which this
        // process does not inherit. Reading them and pushing them into this process is what makes
        // the "env" column mean what the server sees rather than what this shell happens to have.
        string exportsFile;
        var exports = ServerExports(systemDir, out exportsFile);
        foreach (var kv in exports) Environment.SetEnvironmentVariable(kv.Key, kv.Value);

        // Snapshot the two lookup variables BEFORE pinning the winner below, or the ladder would
        // report its own bookkeeping as if the operator had set it.
        string envPrefixes = Environment.GetEnvironmentVariable("EMBY_STRM_PREFIXES");
        string envConfig   = Environment.GetEnvironmentVariable("EMBY_STRM_CONFIG");
        bool inlineEnv     = NonEmpty(envPrefixes);

        string configPath = ResolveConfig(programData, systemDir);
        if (!inlineEnv && configPath != null)
            Environment.SetEnvironmentVariable("EMBY_STRM_CONFIG", configPath);   // pin the winner

        StrmDirect.InvalidateCache();
        string[] parseErrors = StrmDirect.GetErrors();      // also forces the parse
        string[] routes = Routes();

        Console.WriteLine();
        Console.WriteLine("-- configuration --");
        PrintLadder(programData, systemDir, configPath, inlineEnv, envPrefixes, envConfig);
        if (exportsFile != null && exports.Count > 0)
        {
            Console.WriteLine($"  exports in {exportsFile}:");
            foreach (var kv in exports.OrderBy(k => k.Key))
            {
                Console.WriteLine($"     {kv.Key}={kv.Value}");
                warns.Add($"{kv.Key} is exported from bin/emby-server; an Emby upgrade rewrites that file " +
                          "and the setting vanishes silently. Move it into strm-routing.txt (mode-routing.md §3.3).");
            }
        }

        // ---- 4. routes: mode, and whether anything can honour it ------------------------
        Console.WriteLine();
        int routeCount = routes.Length / 2;
        Console.WriteLine($"-- routes ({routeCount}) --");
        if (routeCount == 0)
        {
            Console.WriteLine("  none — every patch is inert and the server behaves exactly like stock Emby");
        }
        else
        {
            Console.WriteLine("   #  mode      status         prefix");
            for (int i = 0, n = 1; i + 1 < routes.Length; i += 2, n++)
            {
                string prefix = routes[i];
                string mode   = routes[i + 1];

                string why = Unsatisfiable(mode, b, c, helperPresent, depsOk);
                string status = why == null ? "ok" : "UNSATISFIABLE";
                Console.WriteLine($"  {n,2}  {mode,-8}  {status,-13}  {prefix}");
                if (why != null)
                {
                    Console.WriteLine($"      -> {why}");
                    errors.Add($"route #{n} ({mode}) {prefix}: {why}");
                }

                // mode-routing.md §9. strm files hold percent-encoded URLs and Emby stores them
                // verbatim in MediaSourceInfo.Path — confirmed by inspecting a live item, not
                // assumed. Matching is a literal StartsWith, so a prefix typed in its decoded
                // form can never match — and the miss is completely silent. This warning is the
                // only thing standing between a user and an unfindable fault.
                string nonAscii = NonAsciiSample(prefix);
                if (nonAscii != null)
                {
                    Console.WriteLine($"      -> WARN non-ASCII characters ({nonAscii}) — see below");
                    warns.Add($"route #{n} contains non-ASCII characters ({nonAscii}): {prefix}\n" +
                              "     .strm files hold PERCENT-ENCODED URLs and Emby stores them undecoded, so a\n" +
                              "     decoded prefix matches nothing and fails silently. Write it as, for example,\n" +
                              "     https://pan.example.com/d/%E9%98%BF%E9%87%8C.../  (spec appendix B).");
                }
            }

            if (a == Tri.No)
                errors.Add("patch A is not installed: matched sources can still be transcoded, " +
                           "which defeats 302 and parallel alike");
        }

        // ---- 5. settings ----------------------------------------------------------------
        Console.WriteLine();
        Console.WriteLine("-- settings (env > file > default) --");
        Console.WriteLine("   key                   value                          source");
        foreach (var s in SettingKeys())
        {
            string value  = StrmDirect.GetSetting(s.Key);
            string source = StrmDirect.GetSettingSource(s.Key);
            if (source == null) { value = s.Default; source = "default"; }
            else if (source == "env" && exports.ContainsKey(EnvNameOf(s.Key))) source = "bin/emby-server";
            Console.WriteLine($"   {s.Key,-21} {value,-30} {source}");
        }

        // Unknown setting keys used to be caught here, against a list this file kept of its own.
        // That list was a second definition of "a legal key" — exactly the duplication §4.1 exists
        // to prevent — so the check moved into the parser, which validates against EnvNameFor and
        // voids the line. An unknown key therefore never reaches GetSettings() at all; it arrives
        // through GetErrors() and is printed by the rejected-lines section below, with its line
        // number and the legal set.

        // ---- 6. rejected lines ----------------------------------------------------------
        Console.WriteLine();
        Console.WriteLine($"-- rejected lines ({parseErrors.Length / 2}) --");
        if (parseErrors.Length == 0)
        {
            Console.WriteLine("  none");
        }
        else
        {
            for (int i = 0; i + 1 < parseErrors.Length; i += 2)
            {
                Console.WriteLine($"  line {parseErrors[i]}: {parseErrors[i + 1]}");
                errors.Add($"line {parseErrors[i]}: {parseErrors[i + 1]}");
            }
            Console.WriteLine("  (a rejected line is skipped whole — it is never downgraded to the default mode)");
        }

        // ---- 7. verdict ------------------------------------------------------------------
        Console.WriteLine();
        if (warns.Count > 0)
        {
            Console.WriteLine($"-- warnings ({warns.Count}) --");
            foreach (var w in warns) Console.WriteLine("  ! " + w);
            Console.WriteLine();
        }
        if (errors.Count > 0)
        {
            Console.WriteLine($"-- errors ({errors.Count}) --");
            foreach (var e in errors) Console.WriteLine("  x " + e);
            Console.WriteLine();
        }
        Console.WriteLine(errors.Count == 0
            ? $"OK   {routeCount} route(s), {warns.Count} warning(s)"
            : $"FAIL {errors.Count} error(s), {warns.Count} warning(s)");

        // Warnings deliberately do not fail the run: the non-ASCII test is a heuristic, and an
        // exit code that cannot be overridden would block a legitimate configuration.
        return errors.Count == 0 ? 0 : 1;
    }

    // ---------------- patches ----------------

    private static void Line(string label, Tri state, string dll, string marker, string note)
    {
        string s = state == Tri.Yes ? "INSTALLED" : state == Tri.No ? "missing  " : "UNKNOWN  ";
        Console.WriteLine($"  {label}  {s}  {dll}  ({marker})");
        if (note != null) Console.WriteLine($"       {note}");
    }

    /// True when <paramref name="typeFullName"/> in that assembly carries the marker field the
    /// patcher stamps on. Reading metadata is what makes this trustworthy: a file hash would go
    /// stale on every Emby release, and the marker is written by the same code that injects.
    private static Tri HasField(string dll, string typeFullName, string field, out string note)
    {
        note = null;
        if (!File.Exists(dll)) { note = "not found: " + dll; return Tri.Unknown; }
        try
        {
            using var asm = AssemblyDefinition.ReadAssembly(dll);
            var t = asm.MainModule.GetType(typeFullName);
            if (t == null) { note = "type not present: " + typeFullName; return Tri.Unknown; }
            return t.Fields.Any(f => f.Name == field) ? Tri.Yes : Tri.No;
        }
        catch (Exception ex)
        {
            note = "unreadable: " + ex.Message;
            return Tri.Unknown;
        }
    }

    /// Mirrors what parallel/deps_patch.py writes: a `targets/<rid>/<Name>/<ver>` entry plus a
    /// dependency edge from Emby.Server.Implementations so the host cannot prune it.
    private static Tri DepsRegisters(string depsPath, string name, out string note)
    {
        note = null;
        if (!File.Exists(depsPath)) { note = "not found"; return Tri.Unknown; }
        try
        {
            using var doc = JsonDocument.Parse(File.ReadAllText(depsPath));
            if (!doc.RootElement.TryGetProperty("targets", out var targets)) { note = "no targets section"; return Tri.Unknown; }

            bool entry = false, edge = false;
            foreach (var target in targets.EnumerateObject())
            {
                foreach (var lib in target.Value.EnumerateObject())
                {
                    if (lib.Name.Split('/')[0] == name) entry = true;
                    if (lib.Name.Split('/')[0] == "Emby.Server.Implementations" &&
                        lib.Value.TryGetProperty("dependencies", out var deps) &&
                        deps.EnumerateObject().Any(d => d.Name == name))
                        edge = true;
                }
            }
            if (entry && edge) return Tri.Yes;
            if (!entry && !edge) return Tri.No;
            note = entry ? "entry present but Emby.Server.Implementations does not depend on it (host may prune it)"
                         : "dependency edge present but the library entry is missing";
            return Tri.No;
        }
        catch (Exception ex)
        {
            note = "unreadable: " + ex.Message;
            return Tri.Unknown;
        }
    }

    private static string Unsatisfiable(string mode, Tri b, Tri c, bool helperPresent, Tri depsOk)
    {
        if (mode == StrmDirect.Mode302)
            return b == Tri.Yes ? null : "patch B (302) is not installed in " + ImplDll;

        var missing = new List<string>();
        if (c != Tri.Yes) missing.Add("patch C is not installed in " + ImplDll);
        if (!helperPresent) missing.Add(HelperDll + " is not in system/");
        if (depsOk != Tri.Yes) missing.Add(HelperAsm + " is not registered in " + DepsJson);
        return missing.Count == 0 ? null : string.Join("; ", missing);
    }

    // ---------------- configuration ----------------

    /// The five-layer lookup from mode-routing.md §3.1, with the two directories supplied explicitly.
    ///
    /// StrmDirect.FindFile cannot be reused verbatim: layer 3 reads `-programdata` off the
    /// server's command line and layers 4-5 hang off AppContext.BaseDirectory, and in this
    /// process both point at the patcher, not at Emby. Only the ladder is restated here — the
    /// file's CONTENTS still go through the shared parser, which is where drift would actually
    /// hurt.
    private static string ResolveConfig(string programData, string systemDir)
    {
        string fromEnv = Environment.GetEnvironmentVariable("EMBY_STRM_CONFIG");
        if (NonEmpty(fromEnv) && File.Exists(fromEnv.Trim())) return fromEnv.Trim();

        string l3 = Path.Combine(programData, "config", StrmDirect.FileName);
        if (File.Exists(l3)) return l3;

        string l4 = Path.Combine(systemDir, StrmDirect.FileName);
        if (File.Exists(l4)) return l4;

        string l5 = Layer5(systemDir);
        if (File.Exists(l5)) return l5;

        return null;
    }

    private static string Layer5(string systemDir)
    {
        string parent = Path.GetDirectoryName(systemDir.TrimEnd(Path.DirectorySeparatorChar)) ?? systemDir;
        return Path.Combine(parent, "programdata", "config", StrmDirect.FileName);
    }

    private static void PrintLadder(string programData, string systemDir, string configPath, bool inlineEnv,
                                    string envPrefixes, string envConfig)
    {
        string l3 = Path.Combine(programData, "config", StrmDirect.FileName);
        string l4 = Path.Combine(systemDir, StrmDirect.FileName);
        string l5 = Layer5(systemDir);

        // The winner is marked; the losers are printed too, because "my file is being ignored" is
        // otherwise answered only by reading the source.
        bool taken = false;
        Rung(1, "env EMBY_STRM_PREFIXES", NonEmpty(envPrefixes) ? envPrefixes : null, inlineEnv, ref taken);
        Rung(2, "env EMBY_STRM_CONFIG",   NonEmpty(envConfig) ? envConfig : null,
             !inlineEnv && configPath != null && configPath == envConfig?.Trim(), ref taken);
        Rung(3, "<programdata>/config",   l3, !inlineEnv && configPath == l3, ref taken);
        Rung(4, "<system>",               l4, !inlineEnv && configPath == l4, ref taken);
        Rung(5, "<system>/../programdata", l5, !inlineEnv && configPath == l5, ref taken);
        if (!taken)
            Console.WriteLine("  => nothing configured");
    }

    private static void Rung(int n, string what, string value, bool winner, ref bool taken)
    {
        // First hit wins, and only the first is marked. In several common packaging layouts
        // layers 3 and 5 legitimately resolve to the same file, and two arrows would read as
        // two separate sources.
        bool mark = winner && !taken;
        if (mark) taken = true;
        Console.WriteLine($"  {(mark ? "=>" : "  ")} {n} {what,-24} {(value == null ? "unset" : value)}");
    }

    /// Reads `export EMBY_STRM_*=...` out of <system>/../bin/emby-server.
    ///
    /// That file is where the running server's environment actually comes from, and it is also
    /// one of the five things an Emby upgrade overwrites (SKILL.md). Ignoring it would make this
    /// report claim a value comes from the config file while the server is really using another.
    private static Dictionary<string, string> ServerExports(string systemDir, out string path)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        path = null;
        try
        {
            string parent = Path.GetDirectoryName(systemDir.TrimEnd(Path.DirectorySeparatorChar));
            if (parent == null) return result;
            string p = Path.Combine(parent, "bin", "emby-server");
            if (!File.Exists(p)) return result;
            path = p;

            foreach (var raw in File.ReadAllLines(p))
            {
                string line = raw.Trim();
                if (!line.StartsWith("export ", StringComparison.Ordinal)) continue;
                line = line.Substring("export ".Length).Trim();
                int eq = line.IndexOf('=');
                if (eq <= 0) continue;
                string key = line.Substring(0, eq).Trim();
                if (!key.StartsWith("EMBY_STRM_", StringComparison.Ordinal)) continue;
                string value = line.Substring(eq + 1).Trim().Trim('"', '\'');
                result[key] = value;
            }
        }
        catch
        {
            // A report must never be the thing that fails; an unreadable launcher just means the
            // env column falls back to this process's own environment.
        }
        return result;
    }

    private static string EnvNameOf(string key)
    {
        if (key == "ramp-seconds")        return "EMBY_STRM_RAMP_SECONDS";
        if (key == "connections")         return "EMBY_STRM_CONNECTIONS";
        if (key == "chunk-mb")            return "EMBY_STRM_CHUNK_MB";
        if (key == "buffer-mb")           return "EMBY_STRM_BUFFER_MB";
        if (key == "initial-connections") return "EMBY_STRM_INITIAL_CONNECTIONS";
        if (key == "log")                 return "EMBY_STRM_LOG";
        return "";
    }

    /// The parsed { key, value, ... } / { prefix, mode, ... } tables.
    ///
    /// shared/RoutingConfig.cs keeps them in private fields because nothing on Emby's hot path
    /// enumerates them — only this report does. Reaching for them reflectively is the lesser
    /// evil: re-tokenising the file here would create the second parser that mode-routing.md §4.1 exists to
    /// prevent. Picks up a GetRoutes()/GetSettings() accessor automatically if either is added.
    private static string[] Pairs(string accessor, string field)
    {
        const BindingFlags Any = BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public;
        var t = typeof(StrmDirect);

        var m = t.GetMethod(accessor, Any, null, Type.EmptyTypes, null);
        if (m != null && m.ReturnType == typeof(string[]))
            return (string[])m.Invoke(null, null) ?? new string[0];

        var f = t.GetField(field, Any);
        if (f == null)
            throw new InvalidOperationException(
                $"shared/RoutingConfig.cs exposes neither {accessor}() nor the {field} field; " +
                "check-config cannot enumerate the configuration.");
        return (string[])f.GetValue(null) ?? new string[0];
    }

    private static string[] Routes()
    {
        return Pairs("GetRoutes", "_routes");
    }

    private static string[] SettingsRead()
    {
        return Pairs("GetSettings", "_settings");
    }

    private static string NonAsciiSample(string s)
    {
        if (s == null) return null;
        var found = new List<string>();
        foreach (char ch in s)
            if (ch > 127 && !found.Contains(ch.ToString()))
            {
                found.Add(ch.ToString());
                if (found.Count == 4) break;
            }
        return found.Count == 0 ? null : string.Join(" ", found);
    }

    private static bool NonEmpty(string s)
    {
        return s != null && s.Trim().Length > 0;
    }
}
