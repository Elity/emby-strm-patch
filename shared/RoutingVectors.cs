#nullable disable
using System;

// Shared parse vectors: ONE table, run against BOTH compiled copies of shared/RoutingConfig.cs.
//
//   · rtcheck                 drives the copy TypeCloner injected into a patched assembly,
//                             through reflection — so it tests the cloned IL, not the source
//   · EmbyStrmParallel.Tests  drives the copy compiled into the helper assembly directly
//
// references/mode-routing.md §4.1 requires exactly this. The failure it prevents is the
// checker and the runtime quietly disagreeing about what a line means.
//
// Unlike shared/RoutingConfig.cs this file is never cloned, so ordinary C# is fine here.
internal static class RoutingVectors
{
    /// <summary>The one config text both products must agree on. Line numbers are asserted below.</summary>
    internal static string[] Lines()
    {
        return new string[]
        {
            /*  1 */ "# shared routing vectors - see references/mode-routing.md 4.1",
            /*  2 */ "",
            /*  3 */ "https://plain.example/",
            /*  4 */ "https://redir.example/   302",
            /*  5 */ "https://par.example/     parallel",
            /*  6 */ "   https://Mixed.Example/PATH/   PaRaLLeL    # mixed case, indented, comment",
            /*  7 */ "https://sign.example/d/?sign=abc123=",
            /*  8 */ "https://typo.example/    paralell",
            /*  9 */ "https://extra.example/   302   junk",
            /* 10 */ "ramp-seconds = 2",
            /* 11 */ "Chunk_MB=8",
            /* 12 */ "log = /tmp/strm-routing.log",
            /* 13 */ "this line is neither",
            /* 14 */ "ramp-second = 2",
        };
    }

    internal static string Text()
    {
        return string.Join("\n", Lines());
    }

    /// <summary>Every environment variable the parser reads, so a test can start from a clean slate.</summary>
    internal static string[] EnvVars()
    {
        return new string[]
        {
            "EMBY_STRM_PREFIXES", "EMBY_STRM_CONFIG", "EMBY_STRM_RAMP_SECONDS",
            "EMBY_STRM_CONNECTIONS", "EMBY_STRM_CHUNK_MB", "EMBY_STRM_BUFFER_MB",
            "EMBY_STRM_INITIAL_CONNECTIONS", "EMBY_STRM_LOG",
        };
    }

    /// <summary>Clears every EMBY_STRM_* variable, then points the parser at a config file.</summary>
    internal static void ResetEnvironment(string configPath)
    {
        foreach (string v in EnvVars()) Environment.SetEnvironmentVariable(v, null);
        Environment.SetEnvironmentVariable("EMBY_STRM_CONFIG", configPath);
    }

    /// <summary>
    /// Runs the whole table. The caller supplies one adapter per API so the same assertions can
    /// drive a reflected type or a directly referenced one. Returns the number of checks made.
    /// </summary>
    internal static int Run(Func<string, bool> isMatch,
                            Func<string, bool> isRedirect,
                            Func<string, bool> isParallel,
                            Func<string, string> getSetting,
                            Func<string, string> getSettingSource,
                            Func<string[]> getErrors,
                            Func<string[]> getRoutes,
                            Func<string[]> getSettings,
                            Action<string, bool> check)
    {
        int n = 0;

        // ---- prefix lines ------------------------------------------------------------
        n += Probe(check, isMatch, isRedirect, isParallel,
                   "bare url line defaults to 302", "https://plain.example/a.mkv", true, true, false);
        n += Probe(check, isMatch, isRedirect, isParallel,
                   "explicit 302 token", "https://redir.example/a.mkv", true, true, false);
        n += Probe(check, isMatch, isRedirect, isParallel,
                   "explicit parallel token", "https://par.example/a.mkv", true, false, true);
        n += Probe(check, isMatch, isRedirect, isParallel,
                   "mode token and prefix are both case-insensitive",
                   "https://mixed.example/path/a.mkv", true, false, true);
        n += Probe(check, isMatch, isRedirect, isParallel,
                   "query string containing '=' stays a prefix line",
                   "https://sign.example/d/?sign=abc123=&t=9", true, true, false);

        // ---- lines that must be voided, never downgraded -----------------------------
        n += Probe(check, isMatch, isRedirect, isParallel,
                   "misspelled mode voids the line (never silently 302)",
                   "https://typo.example/a.mkv", false, false, false);
        n += Probe(check, isMatch, isRedirect, isParallel,
                   "three tokens void the line", "https://extra.example/a.mkv", false, false, false);

        // ---- things that must never become prefixes ----------------------------------
        n += Probe(check, isMatch, isRedirect, isParallel,
                   "unconfigured host", "https://other.example/a.mkv", false, false, false);
        n += Probe(check, isMatch, isRedirect, isParallel,
                   "comment text", "# shared routing vectors", false, false, false);
        n += Probe(check, isMatch, isRedirect, isParallel,
                   "settings line", "ramp-seconds = 2", false, false, false);
        n += Probe(check, isMatch, isRedirect, isParallel,
                   "unparseable line", "this line is neither", false, false, false);
        n += Probe(check, isMatch, isRedirect, isParallel,
                   "empty url", "", false, false, false);
        n += Probe(check, isMatch, isRedirect, isParallel,
                   "null url", null, false, false, false);

        // ---- settings ----------------------------------------------------------------
        n += One(check, "setting: ramp-seconds = 2", getSetting("ramp-seconds") == "2");
        n += One(check, "setting key: '_' and '-' are the same character", getSetting("ramp_seconds") == "2");
        n += One(check, "setting key: lookup is case-insensitive", getSetting("RAMP-SECONDS") == "2");
        n += One(check, "setting: 'Chunk_MB=8' with no spaces -> chunk-mb = 8", getSetting("chunk-mb") == "8");
        n += One(check, "setting value keeps its case and slashes",
                 getSetting("log") == "/tmp/strm-routing.log");
        n += One(check, "absent setting is null, not empty", getSetting("connections") == null);
        n += One(check, "a misspelled key is rejected, not silently stored",
                 getSetting("ramp-second") == null);
        n += One(check, "source of a file setting is \"file\"", getSettingSource("ramp-seconds") == "file");
        n += One(check, "source of an absent setting is null", getSettingSource("connections") == null);

        // env > file > default, checked on the live product rather than assumed
        Environment.SetEnvironmentVariable("EMBY_STRM_RAMP_SECONDS", "9");
        n += One(check, "env overrides the file", getSetting("ramp-seconds") == "9");
        n += One(check, "source of an overridden setting is \"env\"", getSettingSource("ramp-seconds") == "env");
        Environment.SetEnvironmentVariable("EMBY_STRM_RAMP_SECONDS", null);
        n += One(check, "file value returns once the env var is gone", getSetting("ramp-seconds") == "2");

        // ---- error report -------------------------------------------------------------
        string[] errors = getErrors();
        n += One(check, "exactly four lines are rejected", errors.Length == 8);
        n += One(check, "rejected line numbers are 8, 9, 13 and 14",
                 errors.Length == 8 && errors[0] == "8" && errors[2] == "9" &&
                 errors[4] == "13" && errors[6] == "14");
        n += One(check, "the misspelled mode is quoted back",
                 errors.Length >= 2 && errors[1].IndexOf("paralell", StringComparison.Ordinal) >= 0);
        n += One(check, "the extra-token line says how many it found",
                 errors.Length >= 4 && errors[3].IndexOf("3 tokens", StringComparison.Ordinal) >= 0);
        n += One(check, "the misspelled setting key is quoted back",
                 errors.Length >= 8 && errors[7].IndexOf("ramp-second'", StringComparison.Ordinal) >= 0);

        // ---- enumeration, so check-config never has to reflect over private fields ----
        string[] routes = getRoutes();
        n += One(check, "five routes survive parsing", routes.Length == 10);
        n += One(check, "routes are interleaved { prefix, mode }",
                 routes.Length == 10 && routes[0] == "https://plain.example/" && routes[1] == "302" &&
                 routes[4] == "https://par.example/" && routes[5] == "parallel");
        n += One(check, "a prefix keeps the case it was written in",
                 routes.Length == 10 && routes[6] == "https://Mixed.Example/PATH/" && routes[7] == "parallel");

        string[] settings = getSettings();
        n += One(check, "three settings survive parsing", settings.Length == 6);
        n += One(check, "settings are interleaved { key, value } with normalised keys",
                 settings.Length == 6 && settings[0] == "ramp-seconds" && settings[1] == "2" &&
                 settings[2] == "chunk-mb" && settings[3] == "8" &&
                 settings[4] == "log" && settings[5] == "/tmp/strm-routing.log");

        return n;
    }

    private static int Probe(Action<string, bool> check,
                             Func<string, bool> isMatch, Func<string, bool> isRedirect, Func<string, bool> isParallel,
                             string what, string url, bool wantMatch, bool wantRedirect, bool wantParallel)
    {
        check(what + " -> IsMatch " + wantMatch, isMatch(url) == wantMatch);
        check(what + " -> IsRedirect " + wantRedirect, isRedirect(url) == wantRedirect);
        check(what + " -> IsParallel " + wantParallel, isParallel(url) == wantParallel);
        return 3;
    }

    private static int One(Action<string, bool> check, string what, bool ok)
    {
        check(what, ok);
        return 1;
    }
}
