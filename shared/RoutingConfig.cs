using System;
using System.IO;

// Routing configuration: which URL prefixes get special treatment, and which treatment.
//
// ONE SOURCE, TWO BINARIES. This file is compiled twice, by two projects that never reference
// each other at runtime:
//
//   · template/template.csproj              -> patcher/TypeCloner.cs deep-copies this type into
//                                              Emby.Server.MediaEncoding.dll   (patch A)
//                                              Emby.Server.Implementations.dll (patch B)
//   · parallel/src/EmbyStrmParallel.csproj  -> the side-loaded helper that patch C calls into
//
// Two hand-written parsers for one syntax is how "check-config says it is fine, the runtime
// disagrees" bugs are born, so there is exactly one parser and both products compile it
// verbatim. A+B therefore still work with no helper assembly installed at all.
//
// Three questions, one file (references/mode-routing.md §4):
//   IsMatch     any configured prefix, whatever its mode  -> patch A (never offer transcoding)
//   IsRedirect  prefix whose mode is 302                  -> patch B (answer 302)
//   IsParallel  prefix whose mode is parallel             -> patch C (chunked relay)
//
// ---------------------------------------------------------------------------------------
// CLONER CONSTRAINTS — the cloner is deliberately simple. Every one of these is load-bearing:
//
//   · static fields and static methods only; no instance members, no nested types
//   · no generics (no List<T>, no LINQ); arrays only
//   · NO STATIC CONSTRUCTOR. A `.cctor` is not cloned, so a field initialiser such as
//     `static readonly string[] Empty = new string[0];` would stay null forever in the cloned
//     copy — and fail-soft would swallow the NullReferenceException, so the patch would just
//     silently never match. Initialise lazily or return a fresh value from a method instead.
//   · no lambdas / iterators / async — they emit compiler-generated closure types
//   · no array literals: `new char[] { 'a', 'b' }` is lowered into RuntimeHelpers.InitializeArray
//     plus an RVA field in <PrivateImplementationDetails>, which cannot be cloned. Allocate and
//     assign element by element instead.
//   · no `switch` over strings: lowered into a hash lookup in that same generated type. Use
//     an if/else chain.
//   · reference nothing outside the BCL
//
// The type NAME is fixed at "StrmDirect": patcher/Program.cs looks the source type up by that
// name and both injection sites bind to methods on it. Renaming the type means editing the
// patcher too.
// ---------------------------------------------------------------------------------------
//
// Contract: this runs on a media server's hot path. Nothing here may throw. Every failure —
// file missing, permission denied, syntax error — degrades to "no match", which makes the
// patches inert and leaves the server behaving exactly like stock Emby. A single bad line is
// skipped and recorded (see GetErrors) rather than voiding the whole file.
internal static class StrmDirect
{
    /// <summary>Config file name. Looked for in five places; see FindFile.</summary>
    internal const string FileName = "strm-routing.txt";

    /// <summary>Canonical mode tokens. Omitting the token on a prefix line means Mode302.</summary>
    internal const string Mode302 = "302";
    internal const string ModeParallel = "parallel";

    private const string EnvPrefixes = "EMBY_STRM_PREFIXES";
    private const string EnvConfig = "EMBY_STRM_CONFIG";

    // Edits take effect without restarting Emby.
    private const long CacheMillis = 30000L;

    // Flat interleaved arrays, so a whole parse result becomes visible with ONE reference
    // assignment. Parallel arrays (prefixes[] + modes[]) could be read across two generations
    // and go out of step; a single array cannot.
    //   _routes   { prefix, mode, prefix, mode, ... }   mode is Mode302 or ModeParallel
    //   _settings { key, value, key, value, ... }       key already normalised
    //   _errors   { line, reason, line, reason, ... }   line is 1-based, decimal
    private static string[] _routes;
    private static string[] _settings;
    private static string[] _errors;
    private static string _sourcePath;
    private static long _reloadAfter;

    // ---------------- matching ----------------

    /// <summary>Any configured prefix matches, whatever its mode. Patch A asks this.</summary>
    internal static bool IsMatch(string url)
    {
        return Matches(url, null);
    }

    /// <summary>Matches and the mode is 302. Patch B asks this.</summary>
    internal static bool IsRedirect(string url)
    {
        return Matches(url, Mode302);
    }

    /// <summary>Matches and the mode is parallel. Patch C asks this.</summary>
    internal static bool IsParallel(string url)
    {
        return Matches(url, ModeParallel);
    }

    // ---------------- settings ----------------

    /// <summary>
    /// Effective value of a global setting as a raw string, or null when nothing sets it.
    /// Priority is env &gt; file &gt; (caller's built-in default). Callers parse; keeping the
    /// conversion out here is what lets one parser serve int, path and flag settings alike.
    /// Key lookup is case-insensitive and treats '_' and '-' as the same character.
    /// </summary>
    internal static string GetSetting(string key)
    {
        string k = NormaliseKey(key);
        if (k == null || k.Length == 0) return null;
        string fromEnv = EnvValue(k);
        if (fromEnv != null) return fromEnv;
        return FileSetting(k);
    }

    /// <summary>
    /// Where GetSetting's answer came from: "env", "file", or null when neither supplied one.
    /// check-config has to print the provenance, and deriving it outside this file would mean
    /// duplicating the key-to-environment-variable table.
    /// </summary>
    internal static string GetSettingSource(string key)
    {
        string k = NormaliseKey(key);
        if (k == null || k.Length == 0) return null;
        if (EnvValue(k) != null) return "env";
        return FileSetting(k) != null ? "file" : null;
    }

    // ---------------- diagnostics ----------------

    /// <summary>
    /// Everything the parser accepted as a route, interleaved { prefix, mode, prefix, mode, ... }.
    /// Exists so check-config can list the table without reflecting over private fields — a
    /// reader that reaches in here breaks the moment this type is refactored.
    /// </summary>
    internal static string[] GetRoutes()
    {
        Load();
        string[] r = _routes;
        return r == null ? NoStrings() : r;
    }

    /// <summary>
    /// Everything the parser accepted as a setting, interleaved { key, value, key, value, ... },
    /// keys already normalised. File contents only — GetSetting is what applies the environment
    /// override on top. Same reason as GetRoutes: no reflection at the call site.
    /// </summary>
    internal static string[] GetSettings()
    {
        Load();
        string[] s = _settings;
        return s == null ? NoStrings() : s;
    }

    /// <summary>
    /// Lines the parser rejected, interleaved { line, reason, line, reason, ... } with 1-based
    /// decimal line numbers. Empty when the configuration is clean. Nothing in the hot path
    /// reads this; it exists so check-config can report what was skipped.
    /// </summary>
    internal static string[] GetErrors()
    {
        Load();
        string[] e = _errors;
        return e == null ? NoStrings() : e;
    }

    /// <summary>
    /// The config file actually in use, or null when the configuration came from
    /// EMBY_STRM_PREFIXES or no source was found. check-config prints this; resolving it a
    /// second time elsewhere would fork the five-layer lookup.
    /// </summary>
    internal static string GetSourcePath()
    {
        Load();
        return _sourcePath;
    }

    /// <summary>Test hook: drops the cache so the next call re-reads the configuration.</summary>
    internal static void InvalidateCache()
    {
        _routes = null;
        _reloadAfter = long.MinValue;
    }

    // ---------------- internals ----------------

    private static bool Matches(string url, string mode)
    {
        if (url == null || url.Length == 0) return false;

        Load();
        string[] r = _routes;
        if (r == null) return false;

        for (int i = 0; i + 1 < r.Length; i += 2)
        {
            string prefix = r[i];
            // An empty prefix would StartsWith-match every string on earth.
            if (prefix == null || prefix.Length == 0) continue;
            if (mode != null && !string.Equals(r[i + 1], mode, StringComparison.Ordinal)) continue;
            if (url.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)) return true;
        }
        return false;
    }

    private static string FileSetting(string key)
    {
        Load();
        string[] s = _settings;
        if (s == null) return null;

        string found = null;
        for (int i = 0; i + 1 < s.Length; i += 2)
        {
            if (string.Equals(s[i], key, StringComparison.Ordinal)) found = s[i + 1];   // last wins
        }
        return found;
    }

    private static string EnvValue(string key)
    {
        string name = EnvNameFor(key);
        if (name == null) return null;
        string v = ReadEnv(name);
        return v != null && v.Length > 0 ? v : null;
    }

    // The key -> environment variable table from the spec (§3.3). It doubles as the closed set
    // of legal setting keys: anything it does not know is rejected at parse time. An if/else
    // chain rather than a switch — a string switch is lowered into
    // <PrivateImplementationDetails> and stops cloning.
    private static string EnvNameFor(string key)
    {
        if (string.Equals(key, "ramp-seconds", StringComparison.Ordinal)) return "EMBY_STRM_RAMP_SECONDS";
        if (string.Equals(key, "connections", StringComparison.Ordinal)) return "EMBY_STRM_CONNECTIONS";
        if (string.Equals(key, "chunk-mb", StringComparison.Ordinal)) return "EMBY_STRM_CHUNK_MB";
        if (string.Equals(key, "buffer-mb", StringComparison.Ordinal)) return "EMBY_STRM_BUFFER_MB";
        if (string.Equals(key, "initial-connections", StringComparison.Ordinal)) return "EMBY_STRM_INITIAL_CONNECTIONS";
        if (string.Equals(key, "log", StringComparison.Ordinal)) return "EMBY_STRM_LOG";
        return null;
    }

    // Only ever rendered into an error message; EnvNameFor above is what actually decides.
    // Keep the two in step.
    private const string KnownSettingKeys =
        "ramp-seconds, connections, chunk-mb, buffer-mb, initial-connections, log";

    /// <summary>
    /// Reads and parses, at most once every CacheMillis. Deliberately lock-free: the parse is
    /// built entirely in locals and published with plain reference assignments, so a concurrent
    /// caller sees either the previous snapshot or the next one, never a half-built one. Two
    /// threads racing at the 30 s boundary both parse and one result wins, which costs a few
    /// microseconds and cannot deadlock a media server thread. (A lock would need a gate object,
    /// and a `static readonly` gate needs a .cctor, which the cloner drops — see the header.)
    /// </summary>
    private static void Load()
    {
        long now = Environment.TickCount64;
        if (_routes != null && now < _reloadAfter) return;

        string source = null;
        string[] routes = NoStrings();
        string[] settings = NoStrings();
        string[] errors = NoStrings();

        try
        {
            string[] lines = null;

            // Layer 1: the inline environment variable. Each ';'-separated item uses the same
            // syntax as a file line, mode token included.
            string inline = ReadEnv(EnvPrefixes);
            if (inline != null && inline.Length > 0)
            {
                char[] semi = new char[1];
                semi[0] = ';';
                lines = inline.Split(semi);
            }
            else
            {
                source = FindFile();
                if (source != null)
                {
                    char[] nl = new char[1];
                    nl[0] = '\n';
                    lines = File.ReadAllText(source).Split(nl);   // '\r' is removed by Trim below
                }
            }

            if (lines != null)
            {
                // Worst case each line contributes one pair to one of the three arrays.
                string[] r = new string[lines.Length * 2];
                string[] s = new string[lines.Length * 2];
                string[] e = new string[lines.Length * 2];
                int rn = 0, sn = 0, en = 0;

                for (int i = 0; i < lines.Length; i++)
                {
                    string line = StripComment(lines[i]);
                    if (line.Length == 0) continue;
                    string at = (i + 1).ToString();

                    // ORDER IS LOAD-BEARING. "does it contain ://" is decided before "does it
                    // contain =", because a prefix may carry a signed query string
                    // (…/d/file.mkv?sign=abc) that would otherwise read as a settings line.
                    if (line.IndexOf("://", StringComparison.Ordinal) >= 0)
                    {
                        string[] tok = SplitTokens(line);
                        if (tok.Length == 0)
                        {
                            en = Add(e, en, at, "empty url prefix");
                            continue;
                        }
                        if (tok.Length > 2)
                        {
                            en = Add(e, en, at, "expected '<url-prefix> [302|parallel]' but found " +
                                                tok.Length + " tokens");
                            continue;
                        }

                        string mode = tok.Length == 2 ? NormaliseMode(tok[1]) : Mode302;
                        if (mode == null)
                        {
                            // Never fall back to the default here. A typo that silently means 302
                            // is indistinguishable from parallel being on and mysteriously not
                            // helping, which is the worst kind of bug to chase.
                            en = Add(e, en, at, "unknown mode '" + tok[1] + "'; expected 302 or parallel");
                            continue;
                        }

                        rn = Add(r, rn, tok[0], mode);
                        continue;
                    }

                    int eq = line.IndexOf('=');
                    if (eq > 0)
                    {
                        string rawKey = line.Substring(0, eq).Trim();
                        string key = NormaliseKey(rawKey);
                        if (key == null || key.Length == 0)
                        {
                            en = Add(e, en, at, "setting has an empty key");
                            continue;
                        }
                        if (EnvNameFor(key) == null)
                        {
                            // Same reasoning as an unknown mode token: a misspelled key that is
                            // quietly accepted and then never read looks exactly like a setting
                            // that had no effect, and 'ramp-second = 2' would make a ramp sweep
                            // report four identical rows without ever saying why.
                            en = Add(e, en, at, "unknown setting '" + rawKey + "'; expected one of " + KnownSettingKeys);
                            continue;
                        }
                        sn = Add(s, sn, key, line.Substring(eq + 1).Trim());
                        continue;
                    }

                    en = Add(e, en, at, "neither a url prefix (no '://') nor a 'key = value' setting");
                }

                routes = Shrink(r, rn);
                settings = Shrink(s, sn);
                errors = Shrink(e, en);
            }
        }
        catch
        {
            // A broken config must never take the server down: fall back to "nothing configured",
            // which is exactly stock Emby behaviour.
            source = null;
            routes = NoStrings();
            settings = NoStrings();
            errors = NoStrings();
        }

        _settings = settings;
        _errors = errors;
        _sourcePath = source;
        _routes = routes;                                    // published last: it gates the cache
        _reloadAfter = Environment.TickCount64 + CacheMillis;
    }

    // Layered lookup, first hit wins. The two environment variables work on every platform; the
    // remaining layers cover the usual Emby directory layouts. Only programdata/ survives an
    // Emby upgrade, which is why the tuning knobs live in the file rather than in bin/emby-server.
    private static string FindFile()
    {
        // Layer 2: an explicit absolute path.
        string explicitPath = ReadEnv(EnvConfig);
        if (explicitPath != null && explicitPath.Length > 0 && Exists(explicitPath)) return explicitPath;

        // Layer 3: -programdata <p>  ->  <p>/config/strm-routing.txt
        string programData = ProgramDataFromCommandLine();
        if (programData != null)
        {
            string c = Combine(Combine(programData, "config"), FileName);
            if (Exists(c)) return c;
        }

        string bd = BaseDirectory();
        if (bd != null)
        {
            // Layer 4: next to the assemblies, <system>/strm-routing.txt
            string c1 = Combine(bd, FileName);
            if (Exists(c1)) return c1;

            // Layer 5: <system>/../programdata/config/strm-routing.txt
            string parent = ParentOf(bd);
            if (parent != null)
            {
                string c2 = Combine(Combine(Combine(parent, "programdata"), "config"), FileName);
                if (Exists(c2)) return c2;
            }
        }
        return null;
    }

    private static string ProgramDataFromCommandLine()
    {
        try
        {
            string[] args = Environment.GetCommandLineArgs();
            for (int i = 0; i + 1 < args.Length; i++)
            {
                if (string.Equals(args[i], "-programdata", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(args[i], "--programdata", StringComparison.OrdinalIgnoreCase))
                {
                    string v = args[i + 1];
                    if (v != null)
                    {
                        v = v.Trim();
                        if (v.Length > 0) return v;
                    }
                }
            }
        }
        catch
        {
        }
        return null;
    }

    // ---------------- small helpers ----------------

    /// <summary>Appends one pair to a pre-sized array and returns the new length.</summary>
    private static int Add(string[] pairs, int n, string first, string second)
    {
        pairs[n] = first;
        pairs[n + 1] = second;
        return n + 2;
    }

    private static string[] Shrink(string[] pairs, int n)
    {
        if (n == pairs.Length) return pairs;
        string[] r = new string[n];
        for (int i = 0; i < n; i++) r[i] = pairs[i];
        return r;
    }

    // A method, not a `static readonly` field: a field initialiser needs a .cctor and the
    // cloner does not copy one, which would leave the field null in the injected copy.
    private static string[] NoStrings()
    {
        return new string[0];
    }

    private static string StripComment(string line)
    {
        if (line == null) return "";
        int hash = line.IndexOf('#');
        if (hash >= 0) line = line.Substring(0, hash);
        return line.Trim();
    }

    private static string[] SplitTokens(string line)
    {
        // Element-wise on purpose: an array literal would become an RVA blob. See the header.
        char[] ws = new char[2];
        ws[0] = ' ';
        ws[1] = '\t';
        return line.Split(ws, StringSplitOptions.RemoveEmptyEntries);
    }

    private static string NormaliseMode(string token)
    {
        if (string.Equals(token, Mode302, StringComparison.OrdinalIgnoreCase)) return Mode302;
        if (string.Equals(token, ModeParallel, StringComparison.OrdinalIgnoreCase)) return ModeParallel;
        return null;
    }

    private static string NormaliseKey(string key)
    {
        if (key == null) return null;
        return key.Trim().ToLowerInvariant().Replace('_', '-');
    }

    private static string ReadEnv(string name)
    {
        try
        {
            string v = Environment.GetEnvironmentVariable(name);
            return v == null ? null : v.Trim();
        }
        catch
        {
            return null;
        }
    }

    private static bool Exists(string path)
    {
        try
        {
            return path != null && path.Length > 0 && File.Exists(path);
        }
        catch
        {
            return false;
        }
    }

    private static string BaseDirectory()
    {
        try
        {
            string d = AppContext.BaseDirectory;
            return d != null && d.Length > 0 ? d : null;
        }
        catch
        {
            return null;
        }
    }

    private static string ParentOf(string dir)
    {
        try
        {
            char[] seps = new char[2];
            seps[0] = Path.DirectorySeparatorChar;
            seps[1] = Path.AltDirectorySeparatorChar;
            string p = Path.GetDirectoryName(dir.TrimEnd(seps));
            return p != null && p.Length > 0 ? p : null;
        }
        catch
        {
            return null;
        }
    }

    private static string Combine(string a, string b)
    {
        try
        {
            if (a == null || b == null) return null;
            return Path.Combine(a, b);
        }
        catch
        {
            return null;
        }
    }
}
