using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace EmbyStrmParallel.Tests
{
    /// <summary>
    /// Routing configuration as the helper sees it. ParallelFetch.IsMatch is deliberately the
    /// narrow question — "is this prefix routed to parallel mode?" — not "is it configured at
    /// all". A prefix left at the default 302 belongs to patch B, which takes the server out of
    /// the transfer path entirely, so the chunked fetch must not claim it.
    /// </summary>
    internal static class ConfigTests
    {
        private const string PrefixVar = "EMBY_STRM_PREFIXES";
        private const string ConfigVar = "EMBY_STRM_CONFIG";

        private static void ClearEnv()
        {
            // The shared list, so a new setting can never be forgotten here.
            foreach (string v in RoutingVectors.EnvVars()) Environment.SetEnvironmentVariable(v, null);
            StrmDirect.InvalidateCache();
        }

        private static string WriteConfig(string dir, string body)
        {
            Directory.CreateDirectory(dir);
            string file = Path.Combine(dir, StrmDirect.FileName);
            File.WriteAllText(file, body);
            Environment.SetEnvironmentVariable(ConfigVar, file);
            StrmDirect.InvalidateCache();
            return file;
        }

        internal static async Task RunAsync()
        {
            Harness.Section("routing configuration");

            await Harness.RunAsync("no configuration anywhere -> never matches", async () =>
            {
                ClearEnv();
                string stray = Path.Combine(AppContext.BaseDirectory, StrmDirect.FileName);
                if (File.Exists(stray)) File.Delete(stray);
                StrmDirect.InvalidateCache();

                Harness.Assert(!ParallelFetch.IsMatch("https://anything.example.com/x"), "matched with no config");
                Harness.Assert(!ParallelFetch.IsMatch(""), "matched empty");
                Harness.Assert(!ParallelFetch.IsMatch(null), "matched null");
                await Task.CompletedTask;
                return "no match, no throw";
            }).ConfigureAwait(false);

            await Harness.RunAsync("EMBY_STRM_PREFIXES, semicolon separated, case-insensitive", async () =>
            {
                ClearEnv();
                Environment.SetEnvironmentVariable(PrefixVar,
                    " https://a.example.com/ parallel ; http://B.EXAMPLE.org/x PARALLEL ; ");
                StrmDirect.InvalidateCache();

                Harness.Assert(ParallelFetch.IsMatch("https://a.example.com/movie.mkv"), "first prefix");
                Harness.Assert(ParallelFetch.IsMatch("HTTPS://A.EXAMPLE.COM/movie.mkv"), "url upper-cased");
                Harness.Assert(ParallelFetch.IsMatch("http://b.example.org/x/y"), "prefix upper-cased");
                Harness.Assert(!ParallelFetch.IsMatch("https://other.example.com/movie.mkv"), "unrelated host matched");
                Harness.Assert(!ParallelFetch.IsMatch("http://b.example.org/z"), "wrong path matched");
                await Task.CompletedTask;
                return "3 checks positive, 2 negative";
            }).ConfigureAwait(false);

            await Harness.RunAsync("a 302 prefix belongs to patch B, not to the parallel fetch", async () =>
            {
                ClearEnv();
                Environment.SetEnvironmentVariable(PrefixVar,
                    "https://redir.example/;https://redir2.example/ 302;https://par.example/ parallel");
                StrmDirect.InvalidateCache();

                Harness.Assert(!ParallelFetch.IsMatch("https://redir.example/a.mkv"), "implicit 302 claimed by parallel");
                Harness.Assert(!ParallelFetch.IsMatch("https://redir2.example/a.mkv"), "explicit 302 claimed by parallel");
                Harness.Assert(ParallelFetch.IsMatch("https://par.example/a.mkv"), "parallel prefix not claimed");

                // Patch A takes the wide question, and must still see all three.
                Harness.Assert(StrmDirect.IsMatch("https://redir.example/a.mkv"), "IsMatch missed an implicit 302");
                Harness.Assert(StrmDirect.IsMatch("https://par.example/a.mkv"), "IsMatch missed a parallel prefix");
                Harness.Assert(StrmDirect.IsRedirect("https://redir2.example/a.mkv"), "IsRedirect missed an explicit 302");
                Harness.Assert(!StrmDirect.IsRedirect("https://par.example/a.mkv"), "IsRedirect claimed a parallel prefix");
                await Task.CompletedTask;
                return "302 / parallel / any are three different questions";
            }).ConfigureAwait(false);

            await Harness.RunAsync("EMBY_STRM_CONFIG file: comments, blanks, trimming", async () =>
            {
                ClearEnv();
                string dir = Path.Combine(Path.GetTempPath(), "strmcfg-" + Guid.NewGuid().ToString("N"));
                Directory.CreateDirectory(dir);
                string file = Path.Combine(dir, StrmDirect.FileName);
                File.WriteAllText(file,
                    "# a comment\n" +
                    "\n" +
                    "   https://cfg.example.com/media/   parallel   \n" +
                    "https://second.example.com/ parallel  # trailing comment\n" +
                    "   # indented comment\n");
                Environment.SetEnvironmentVariable(ConfigVar, file);
                StrmDirect.InvalidateCache();
                try
                {
                    Harness.Assert(ParallelFetch.IsMatch("https://cfg.example.com/media/a.mkv"), "trimmed prefix");
                    Harness.Assert(ParallelFetch.IsMatch("https://second.example.com/b.mkv"), "trailing-comment prefix");
                    Harness.Assert(!ParallelFetch.IsMatch("https://cfg.example.com/other"), "wrong path matched");
                    Harness.Assert(!ParallelFetch.IsMatch("# a comment"), "comment became a prefix");
                    Harness.Assert(StrmDirect.GetErrors().Length == 0, "clean file reported errors");
                    await Task.CompletedTask;
                    return "parsed 2 prefixes, 0 errors";
                }
                finally { try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("a bad line is skipped and reported, the rest still works", async () =>
            {
                ClearEnv();
                string dir = Path.Combine(Path.GetTempPath(), "strmcfg-" + Guid.NewGuid().ToString("N"));
                Directory.CreateDirectory(dir);
                string file = Path.Combine(dir, StrmDirect.FileName);
                File.WriteAllText(file,
                    "https://typo.example/ paralell\n" +      // line 1: unknown mode -> void
                    "https://good.example/ parallel\n");      // line 2: unaffected
                Environment.SetEnvironmentVariable(ConfigVar, file);
                StrmDirect.InvalidateCache();
                try
                {
                    Harness.Assert(ParallelFetch.IsMatch("https://good.example/a.mkv"), "good line was dropped too");
                    Harness.Assert(!StrmDirect.IsMatch("https://typo.example/a.mkv"), "typo line silently became 302");

                    string[] errors = StrmDirect.GetErrors();
                    Harness.Assert(errors.Length == 2, "expected exactly one error pair, got " + errors.Length / 2);
                    Harness.Assert(errors[0] == "1", "wrong line number: " + errors[0]);
                    Harness.Assert(errors[1].IndexOf("paralell", StringComparison.Ordinal) >= 0,
                                   "reason does not quote the token: " + errors[1]);
                    Harness.Assert(StrmDirect.GetSourcePath() == file, "source path not reported");
                    await Task.CompletedTask;
                    return "fail-soft: line 1 rejected, line 2 live";
                }
                finally { try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("unreadable / missing config degrades to no-match", async () =>
            {
                ClearEnv();
                Environment.SetEnvironmentVariable(ConfigVar, "/definitely/not/here/\0bad-path");
                StrmDirect.InvalidateCache();
                Harness.Assert(!ParallelFetch.IsMatch("https://a.example.com/x"), "matched despite broken config");
                await Task.CompletedTask;
                return "degraded silently";
            }).ConfigureAwait(false);

            await Harness.RunAsync("AppContext.BaseDirectory/strm-routing.txt is discovered", async () =>
            {
                ClearEnv();
                string file = Path.Combine(AppContext.BaseDirectory, StrmDirect.FileName);
                File.WriteAllText(file, "https://basedir.example.com/ parallel\n");
                StrmDirect.InvalidateCache();
                try
                {
                    Harness.Assert(ParallelFetch.IsMatch("https://basedir.example.com/a"), "base dir file not used");
                    Harness.Assert(!ParallelFetch.IsMatch("https://elsewhere.example.com/a"), "false positive");
                    await Task.CompletedTask;
                    return "discovered next to the assembly";
                }
                finally { try { File.Delete(file); } catch { } StrmDirect.InvalidateCache(); }
            }).ConfigureAwait(false);

            await Harness.RunAsync("env prefixes take priority over a config file", async () =>
            {
                ClearEnv();
                string dir = Path.Combine(Path.GetTempPath(), "strmcfg-" + Guid.NewGuid().ToString("N"));
                Directory.CreateDirectory(dir);
                string file = Path.Combine(dir, StrmDirect.FileName);
                File.WriteAllText(file, "https://fromfile.example.com/ parallel\n");
                Environment.SetEnvironmentVariable(ConfigVar, file);
                Environment.SetEnvironmentVariable(PrefixVar, "https://fromenv.example.com/ parallel");
                StrmDirect.InvalidateCache();
                try
                {
                    Harness.Assert(ParallelFetch.IsMatch("https://fromenv.example.com/a"), "env prefix ignored");
                    Harness.Assert(!ParallelFetch.IsMatch("https://fromfile.example.com/a"), "file should have been shadowed");
                    await Task.CompletedTask;
                    return "priority 1 beats priority 2";
                }
                finally { try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("result is cached (~30s) and refreshes after invalidation", async () =>
            {
                ClearEnv();
                string dir = Path.Combine(Path.GetTempPath(), "strmcfg-" + Guid.NewGuid().ToString("N"));
                Directory.CreateDirectory(dir);
                string file = Path.Combine(dir, StrmDirect.FileName);
                File.WriteAllText(file, "https://v1.example.com/ parallel\n");
                Environment.SetEnvironmentVariable(ConfigVar, file);
                StrmDirect.InvalidateCache();
                try
                {
                    Harness.Assert(ParallelFetch.IsMatch("https://v1.example.com/a"), "v1 not loaded");
                    File.WriteAllText(file, "https://v2.example.com/ parallel\n");
                    Harness.Assert(ParallelFetch.IsMatch("https://v1.example.com/a"), "cache did not hold the old value");
                    Harness.Assert(!ParallelFetch.IsMatch("https://v2.example.com/a"), "cache was bypassed");
                    StrmDirect.InvalidateCache();
                    Harness.Assert(ParallelFetch.IsMatch("https://v2.example.com/a"), "reload did not pick up v2");
                    await Task.CompletedTask;
                    return "stale within window, fresh after";
                }
                finally { try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("-programdata <p>/config/strm-routing.txt (child process)", async () =>
            {
                string dir = Path.Combine(Path.GetTempPath(), "strmpd-" + Guid.NewGuid().ToString("N"));
                Directory.CreateDirectory(Path.Combine(dir, "config"));
                File.WriteAllText(Path.Combine(dir, "config", StrmDirect.FileName), "https://pd.example.com/ parallel\n");
                try
                {
                    int exit = await RunChildAsync(new string[] { "programdata-child", "-programdata", dir }).ConfigureAwait(false);
                    Harness.Assert(exit == 0, "child process exited " + exit + " (0 = prefix resolved from -programdata)");
                    return "resolved via command line";
                }
                finally { try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("shared parse vectors (same table as the cloned template)", async () =>
            {
                ClearEnv();
                string file = Path.Combine(Path.GetTempPath(), "strm-routing-vectors-" + Guid.NewGuid().ToString("N") + ".txt");
                File.WriteAllText(file, RoutingVectors.Text());
                try
                {
                    RoutingVectors.ResetEnvironment(file);
                    StrmDirect.InvalidateCache();

                    int failures = 0;
                    string firstFailure = null;
                    int n = RoutingVectors.Run(
                        StrmDirect.IsMatch,
                        StrmDirect.IsRedirect,
                        StrmDirect.IsParallel,
                        StrmDirect.GetSetting,
                        StrmDirect.GetSettingSource,
                        StrmDirect.GetErrors,
                        StrmDirect.GetRoutes,
                        StrmDirect.GetSettings,
                        delegate (string name, bool ok)
                        {
                            if (ok) return;
                            failures++;
                            if (firstFailure == null) firstFailure = name;
                        });

                    Harness.Assert(failures == 0, failures + "/" + n + " vector(s) failed, first: " + firstFailure);
                    await Task.CompletedTask;
                    return n + " vectors agree with the cloned build";
                }
                finally
                {
                    try { File.Delete(file); } catch { }
                    ClearEnv();
                }
            }).ConfigureAwait(false);

            ClearEnv();

            await TuningAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// The settings half of the spec, end to end: parser -> ParallelFetchOptions / FetchLog.
        /// The whole point of moving the knobs out of bin/emby-server is that an Emby upgrade
        /// rewrites that file, so if these are not wired the migration silently loses the tuning
        /// and the symptom is "it started stuttering again" months later.
        /// </summary>
        private static async Task TuningAsync()
        {
            Harness.Section("tuning settings (config file -> ParallelFetchOptions / FetchLog)");

            await Harness.RunAsync("the config file alone drives every tuning knob", async () =>
            {
                ClearEnv();
                string dir = Path.Combine(Path.GetTempPath(), "strmtune-" + Guid.NewGuid().ToString("N"));
                try
                {
                    WriteConfig(dir,
                        "https://par.example/ parallel\n" +
                        "connections = 5\n" +
                        "chunk-mb = 4\n" +
                        "buffer-mb = 64\n" +
                        "initial-connections = 3\n" +
                        "max-origin-connections = 9\n" +
                        "ramp-seconds = 2\n");

                    ParallelFetchOptions o = ParallelFetchOptions.FromConfiguration();
                    Harness.AssertEqual(5, o.Connections, "connections");
                    Harness.AssertEqual(4L * 1024 * 1024, o.ChunkSize, "chunk-mb");
                    Harness.AssertEqual(64L * 1024 * 1024, o.MaxBufferBytes, "buffer-mb");
                    Harness.AssertEqual(3, o.InitialConnections, "initial-connections");
                    // The only test of this key's READ path. Every budget test sets the property
                    // in code, and `embypatch check` reads the setting through StrmDirect, so one
                    // wrong letter in the string here would ship a knob that displays the
                    // configured value while the fetcher silently runs on the default of 12.
                    Harness.AssertEqual(9, o.MaxOriginConnections, "max-origin-connections");
                    Harness.AssertEqual(2, (long)o.ConnectionRampInterval.TotalSeconds, "ramp-seconds");
                    await Task.CompletedTask;
                    return "6 knobs read from the file, no env involved";
                }
                finally { ClearEnv(); try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("a knob absent from the file keeps its built-in default", async () =>
            {
                ClearEnv();
                string dir = Path.Combine(Path.GetTempPath(), "strmtune-" + Guid.NewGuid().ToString("N"));
                try
                {
                    WriteConfig(dir, "ramp-seconds = 2\n");
                    ParallelFetchOptions o = ParallelFetchOptions.FromConfiguration();
                    Harness.AssertEqual(2, (long)o.ConnectionRampInterval.TotalSeconds, "ramp-seconds");
                    Harness.AssertEqual(6, o.Connections, "connections should still be the default");
                    Harness.AssertEqual(12, o.MaxOriginConnections, "max-origin-connections should still be the default");
                    Harness.AssertEqual(8L * 1024 * 1024, o.ChunkSize, "chunk-mb should still be the default");
                    await Task.CompletedTask;
                    return "file wins where set, default elsewhere";
                }
                finally { ClearEnv(); try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("every settable key is covered by the test-isolation list", async () =>
            {
                // A new key has to be added in four places (the option reader, StrmDirect's
                // key -> env table, CheckConfig's copy of it, and RoutingVectors.EnvVars). Only
                // the last one breaks nothing when forgotten - until a machine happens to export
                // the variable, and then every test that "starts from a clean slate" starts
                // dirty. `max-origin-connections` was forgotten there exactly once already.
                //
                // The parser's own rejection message is the closed set, so this derives the list
                // from the product rather than restating it, and the naming rule is mechanical.
                ClearEnv();
                string dir = Path.Combine(Path.GetTempPath(), "strmkeys-" + Guid.NewGuid().ToString("N"));
                try
                {
                    WriteConfig(dir, "definitely-not-a-setting = 1\n");
                    string[] errors = StrmDirect.GetErrors();
                    Harness.Assert(errors.Length == 2, "expected exactly one rejected line, got " + errors.Length / 2);

                    const string Marker = "expected one of ";
                    int at = errors[1].IndexOf(Marker, StringComparison.Ordinal);
                    Harness.Assert(at >= 0, "the parser no longer lists its known keys: " + errors[1]);

                    string[] isolation = RoutingVectors.EnvVars();
                    string[] keys = errors[1].Substring(at + Marker.Length).Split(',');
                    int seen = 0;
                    foreach (string raw in keys)
                    {
                        string key = raw.Trim();
                        if (key.Length == 0) continue;
                        string env = "EMBY_STRM_" + key.ToUpperInvariant().Replace('-', '_');
                        Harness.Assert(Array.IndexOf(isolation, env) >= 0,
                            "RoutingVectors.EnvVars() is missing " + env + " for setting '" + key + "'");
                        seen++;
                    }
                    Harness.Assert(seen >= 7, "only " + seen + " keys parsed out of: " + errors[1]);
                    await Task.CompletedTask;
                    return seen + " settable keys, every one clearable";
                }
                finally { ClearEnv(); try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("a misspelled key is rejected loudly, not applied quietly", async () =>
            {
                ClearEnv();
                string dir = Path.Combine(Path.GetTempPath(), "strmtune-" + Guid.NewGuid().ToString("N"));
                try
                {
                    WriteConfig(dir, "ramp-second = 2\nconnections = 5\n");   // line 1 is a typo

                    ParallelFetchOptions o = ParallelFetchOptions.FromConfiguration();
                    Harness.AssertEqual(6, (long)o.ConnectionRampInterval.TotalSeconds,
                                        "the typo must not have moved ramp-seconds off its default");
                    Harness.AssertEqual(5, o.Connections, "the valid line beside it must still apply");

                    string[] errors = StrmDirect.GetErrors();
                    Harness.Assert(errors.Length == 2, "expected exactly one rejected line, got " + errors.Length / 2);
                    Harness.Assert(errors[0] == "1", "wrong line number: " + errors[0]);
                    Harness.Assert(errors[1].IndexOf("ramp-second'", StringComparison.Ordinal) >= 0,
                                   "reason does not quote the key: " + errors[1]);
                    Harness.Assert(StrmDirect.GetSetting("ramp-second") == null, "the typo key was stored anyway");
                    await Task.CompletedTask;
                    return "rejected at line 1, default intact, neighbour unaffected";
                }
                finally { ClearEnv(); try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("env still overrides the config file", async () =>
            {
                ClearEnv();
                string dir = Path.Combine(Path.GetTempPath(), "strmtune-" + Guid.NewGuid().ToString("N"));
                try
                {
                    WriteConfig(dir, "ramp-seconds = 2\nconnections = 5\n");
                    Environment.SetEnvironmentVariable("EMBY_STRM_RAMP_SECONDS", "9");

                    ParallelFetchOptions o = ParallelFetchOptions.FromConfiguration();
                    Harness.AssertEqual(9, (long)o.ConnectionRampInterval.TotalSeconds, "env should have won");
                    Harness.AssertEqual(5, o.Connections, "the un-overridden key should still come from the file");
                    await Task.CompletedTask;
                    return "env beats file, per key";
                }
                finally { ClearEnv(); try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("editing the file goes live on the next reload (ramp sweep premise)", async () =>
            {
                ClearEnv();
                string dir = Path.Combine(Path.GetTempPath(), "strmtune-" + Guid.NewGuid().ToString("N"));
                try
                {
                    string file = WriteConfig(dir, "ramp-seconds = 2\n");
                    Harness.AssertEqual(2, (long)ParallelFetchOptions.FromConfiguration().ConnectionRampInterval.TotalSeconds,
                                        "first read");

                    // Rewrite without invalidating: the 30 s window must still hold the old value,
                    // otherwise every request would re-read the file from disk.
                    File.WriteAllText(file, "ramp-seconds = 5\n");
                    Harness.AssertEqual(2, (long)ParallelFetchOptions.FromConfiguration().ConnectionRampInterval.TotalSeconds,
                                        "cache did not hold the old value");

                    // ...and once the window passes, the new value is live with no restart.
                    StrmDirect.InvalidateCache();
                    Harness.AssertEqual(5, (long)ParallelFetchOptions.FromConfiguration().ConnectionRampInterval.TotalSeconds,
                                        "reload did not pick up the edit");
                    await Task.CompletedTask;
                    return "2 -> (stale 2) -> 5 without a restart";
                }
                finally { ClearEnv(); try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("FetchLog takes its path from the config file, env overrides it", async () =>
            {
                ClearEnv();
                string dir = Path.Combine(Path.GetTempPath(), "strmtune-" + Guid.NewGuid().ToString("N"));
                try
                {
                    string fromFile = Path.Combine(dir, "from-file.log");
                    WriteConfig(dir, "log = " + fromFile + "\n");
                    FetchLog.ResetForTests();

                    Harness.Assert(FetchLog.IsEnabled, "the file's log setting did not arm the sink");
                    FetchLog.Write("hello from the config file");
                    Harness.Assert(File.Exists(fromFile), "nothing was written to the path from the file");
                    Harness.Assert(File.ReadAllText(fromFile).IndexOf("hello from the config file",
                                   StringComparison.Ordinal) >= 0, "log line missing");

                    // env wins, exactly as for every other setting
                    string fromEnv = Path.Combine(dir, "from-env.log");
                    Environment.SetEnvironmentVariable(FetchLog.PathVariable, fromEnv);
                    FetchLog.ResetForTests();
                    FetchLog.Write("hello from the environment");
                    Harness.Assert(File.Exists(fromEnv), "env override did not redirect the sink");
                    Harness.Assert(File.ReadAllText(fromFile).IndexOf("hello from the environment",
                                   StringComparison.Ordinal) < 0, "the file path kept receiving lines");
                    await Task.CompletedTask;
                    return "file sink armed from strm-routing.txt, env redirects it";
                }
                finally
                {
                    ClearEnv();
                    FetchLog.ResetForTests();
                    try { Directory.Delete(dir, true); } catch { }
                }
            }).ConfigureAwait(false);

            ClearEnv();
            FetchLog.ResetForTests();
        }

        internal static int ProgramDataChild()
        {
            Environment.SetEnvironmentVariable(PrefixVar, null);
            Environment.SetEnvironmentVariable(ConfigVar, null);
            StrmDirect.InvalidateCache();
            bool ok = ParallelFetch.IsMatch("https://pd.example.com/movie.mkv") &&
                      !ParallelFetch.IsMatch("https://nope.example.com/movie.mkv");
            return ok ? 0 : 3;
        }

        private static async Task<int> RunChildAsync(string[] args)
        {
            string exe = Environment.ProcessPath;
            ProcessStartInfo psi = new ProcessStartInfo();
            bool viaDotnet = exe == null || Path.GetFileNameWithoutExtension(exe).Equals("dotnet", StringComparison.OrdinalIgnoreCase);
            if (viaDotnet)
            {
                psi.FileName = exe ?? "dotnet";
                psi.ArgumentList.Add(Path.Combine(AppContext.BaseDirectory, "EmbyStrmParallel.Tests.dll"));
            }
            else
            {
                psi.FileName = exe;
            }
            foreach (string a in args) psi.ArgumentList.Add(a);
            psi.RedirectStandardOutput = true;
            psi.RedirectStandardError = true;
            psi.Environment["EMBY_STRM_PREFIXES"] = "";
            psi.Environment["EMBY_STRM_CONFIG"] = "";

            using (Process p = Process.Start(psi))
            {
                Task<string> so = p.StandardOutput.ReadToEndAsync();
                Task<string> se = p.StandardError.ReadToEndAsync();
                await p.WaitForExitAsync().ConfigureAwait(false);
                await Task.WhenAll(so, se).ConfigureAwait(false);
                return p.ExitCode;
            }
        }
    }
}
