using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace EmbyStrmParallel.Tests
{
    /// <summary>
    /// The call site in production is hand-written CIL that never assigns ParallelFetch.Logger,
    /// so these tests all exercise the self-arming file sink, not the delegate.
    /// </summary>
    internal static class LoggingTests
    {
        private const long FileSize = 4L * 1024 * 1024;

        private static ParallelFetchOptions Small()
        {
            return new ParallelFetchOptions
            {
                Connections = 4,
                ConnectionRampInterval = TimeSpan.Zero,
                ChunkSize = 64 * 1024,
                FirstChunkSize = 64 * 1024,
                BlockSize = 16 * 1024,
                MaxBufferBytes = 8L * 64 * 1024,
                MaxAttempts = 6,
                RetryBaseDelayMs = 10,
                RetryMaxDelayMs = 50,
                ResponseHeadersTimeout = TimeSpan.FromSeconds(10),
                ReadIdleTimeout = TimeSpan.FromSeconds(10)
            };
        }

        private static string NewTempDir()
        {
            string dir = Path.Combine(Path.GetTempPath(), "strmlog-" + Guid.NewGuid().ToString("N"));
            return dir;
        }

        private static void Arm(string path)
        {
            Environment.SetEnvironmentVariable(FetchLog.PathVariable, path);
            FetchLog.ResetForTests();
        }

        private static void Disarm()
        {
            Environment.SetEnvironmentVariable(FetchLog.PathVariable, null);
            FetchLog.ResetForTests();
        }

        internal static async Task RunAsync(CancellationToken ct)
        {
            Harness.Section("self-arming diagnostics (EMBY_STRM_LOG)");

            await Harness.RunAsync("unset -> nothing is written and nothing throws", async () =>
            {
                Disarm();
                string dir = NewTempDir();
                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    long? t, cl;
                    using (Stream s = ParallelFetch.OpenWith(srv.Url, 0, 100000, Small(), out t, out cl, ct))
                    {
                        await Harness.ReadAllAsync(s, 32 * 1024, ct).ConfigureAwait(false);
                    }
                }
                Harness.Assert(!Directory.Exists(dir), "a directory was created with logging disabled");
                Harness.Assert(!FetchLog.IsEnabled, "IsEnabled should be false when unset");
                return "silent by default";
            }).ConfigureAwait(false);

            await Harness.RunAsync("successful Open logs one line with url tail, offset, length, conn", async () =>
            {
                string dir = NewTempDir();
                // Deliberately two levels deep: the parent directory must be created for us.
                string logPath = Path.Combine(dir, "nested", "parallel.log");
                Arm(logPath);
                try
                {
                    using (MockServer srv = new MockServer(FileSize, fastContent: false))
                    {
                        long? t, cl;
                        using (Stream s = ParallelFetch.OpenWith(srv.Url, 4096, 200000, Small(), out t, out cl, ct))
                        {
                            await Harness.ReadAllAsync(s, 32 * 1024, ct).ConfigureAwait(false);
                        }
                    }
                    Harness.Assert(File.Exists(logPath), "log file was not created at " + logPath);
                    string text = File.ReadAllText(logPath);
                    Harness.Assert(text.Contains("path=parallel"), "no parallel-path line: " + text);
                    Harness.Assert(text.Contains("offset=4096"), "offset missing: " + text);
                    Harness.Assert(text.Contains("length=200000"), "length missing: " + text);
                    Harness.Assert(text.Contains("conn="), "connection count missing: " + text);
                    Harness.Assert(text.Contains("url="), "url missing: " + text);
                    Harness.Assert(!text.Contains("chunk=0MiB") && !text.Contains("memCeiling=0MiB"),
                                   "sizes rounded to zero: " + text);
                    Harness.Assert(text.Contains("chunk=64KiB"), "chunk size wrong unit: " + text);
                    Harness.Assert(text.Contains("[ParallelFetch]"), "prefix missing: " + text);
                    // timestamped
                    Harness.Assert(text.Length > 20 && char.IsDigit(text[0]), "line is not timestamped: " + text);
                    return "line: " + text.Trim().Split('\n')[0].Substring(0, Math.Min(90, text.Trim().Length));
                }
                finally { Disarm(); try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("query string (where the signature lives) never reaches the log", async () =>
            {
                string dir = NewTempDir();
                string logPath = Path.Combine(dir, "parallel.log");
                Arm(logPath);
                try
                {
                    using (MockServer srv = new MockServer(FileSize, fastContent: false))
                    {
                        // Mimic the real origin: a long signature carried in the query string.
                        string secret = "SIGNATUREMUSTNOTAPPEAR0123456789abcdefghijklmnop";
                        string url = srv.Url + "?sig=" + secret;
                        long? t, cl;
                        using (Stream s = ParallelFetch.OpenWith(url, 0, 100000, Small(), out t, out cl, ct))
                        {
                            await Harness.ReadAllAsync(s, 32 * 1024, ct).ConfigureAwait(false);
                        }
                        string text = File.ReadAllText(logPath);
                        Harness.Assert(!text.Contains(secret), "the signature leaked into the log");
                        Harness.Assert(!text.Contains("sig="), "the query string leaked into the log");
                        Harness.Assert(text.Contains("?<redacted>"), "the query should be marked as redacted: " + text);
                        Harness.Assert(text.Contains("/file"), "the path should still identify the request: " + text);

                        // A *short* signed url must not slip through whole either.
                        string shortSigned = "http://h.co/f?s=" + secret;
                        Harness.Assert(!FetchLog.Tail(shortSigned).Contains(secret),
                                       "short signed url leaked: " + FetchLog.Tail(shortSigned));

                        // A long path is truncated to its trailing 40 characters.
                        string longPath = "https://cdn.example.com/" + new string('p', 200) + "/movie-id-abcdef.mkv";
                        string tail = FetchLog.Tail(longPath);
                        Harness.Assert(tail.StartsWith("...") && tail.Length <= 44,
                                       "long path not truncated: " + tail);
                        Harness.Assert(tail.EndsWith("movie-id-abcdef.mkv"), "truncation lost the identifying tail: " + tail);
                        return "query redacted, path tail kept";
                    }
                }
                finally { Disarm(); try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("TryOpen fallback is logged with a reason", async () =>
            {
                string dir = NewTempDir();
                string logPath = Path.Combine(dir, "parallel.log");
                Arm(logPath);
                try
                {
                    long? t, cl;
                    // Nothing is listening on this port, so the probe cannot succeed.
                    Stream s = ParallelFetch.TryOpen("http://127.0.0.1:9/never", 0, 1000, out t, out cl, ct);
                    Harness.Assert(s == null, "TryOpen should return null on failure");
                    Harness.Assert(t == null && cl == null, "out params must be null on failure");
                    Harness.Assert(File.Exists(logPath), "no log file was written for the fallback");
                    string text = File.ReadAllText(logPath);
                    Harness.Assert(text.Contains("FALLBACK to host path"), "fallback not logged: " + text);
                    Harness.Assert(text.Contains("reason="), "no reason recorded: " + text);
                    Harness.Assert(text.Contains("offset=0"), "offset missing: " + text);
                    await Task.CompletedTask;
                    return text.Trim().Substring(24, Math.Min(80, text.Trim().Length - 24));
                }
                finally { Disarm(); try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("chunk retries are logged with the HTTP status", async () =>
            {
                string dir = NewTempDir();
                string logPath = Path.Combine(dir, "parallel.log");
                Arm(logPath);
                try
                {
                    using (MockServer srv = new MockServer(FileSize, fastContent: false))
                    {
                        System.Collections.Concurrent.ConcurrentDictionary<long, int> seen =
                            new System.Collections.Concurrent.ConcurrentDictionary<long, int>();
                        srv.FaultHook = (seq, from, to) =>
                            seen.AddOrUpdate(from, 1, (k, v) => v + 1) == 1 ? MockFault.Status403 : MockFault.None;

                        long? t, cl;
                        using (Stream s = ParallelFetch.OpenWith(srv.Url, 0, 500000, Small(), out t, out cl, ct))
                        {
                            byte[] got = await Harness.ReadAllAsync(s, 32 * 1024, ct).ConfigureAwait(false);
                            Harness.AssertBytesEqual(Pattern.Range(0, 500000), got, "bytes still exact");
                        }
                        string text = File.ReadAllText(logPath);
                        Harness.Assert(text.Contains("HTTP 403"), "403 status not recorded: " + text);
                        Harness.Assert(text.Contains("retry"), "no retry line: " + text);
                        Harness.Assert(text.Contains("resuming at"), "resume offset not recorded: " + text);
                        int lines = text.Split('\n').Length;
                        return lines + " lines, 403s attributed";
                    }
                }
                finally { Disarm(); try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("every stream logs a close summary (the collapse was silent)", async () =>
            {
                string dir = NewTempDir();
                string logPath = Path.Combine(dir, "parallel.log");
                Arm(logPath);
                try
                {
                    using (MockServer srv = new MockServer(FileSize, fastContent: false))
                    {
                        // 1. a stream read to completion
                        long? t, cl;
                        using (Stream s = ParallelFetch.OpenWith(srv.Url, 0, 400000, Small(), out t, out cl, ct))
                        {
                            await Harness.ReadAllAsync(s, 32 * 1024, ct).ConfigureAwait(false);
                        }
                        // 2. a stream abandoned after a few bytes
                        long? t2, cl2;
                        Stream s2 = ParallelFetch.OpenWith(srv.Url, 0, 2 * 1024 * 1024, Small(), out t2, out cl2, ct);
                        byte[] tiny = new byte[4096];
                        await s2.ReadAsync(tiny.AsMemory(0, tiny.Length), ct).ConfigureAwait(false);
                        s2.Dispose();

                        string text = File.ReadAllText(logPath);
                        Harness.Assert(text.Contains("closed complete"), "no completion summary: " + text);
                        Harness.Assert(text.Contains("closed ABANDONED"), "no abandonment summary: " + text);
                        foreach (string field in new string[] { "delivered=", "elapsed=", "rate=", "chunks=", "retries=", "slow=" })
                        {
                            Harness.Assert(text.Contains(field), "summary is missing " + field + ": " + text);
                        }
                        string[] lines = text.Trim().Split('\n');
                        string summary = null;
                        foreach (string l in lines) if (l.Contains("closed ABANDONED")) summary = l;
                        return summary == null ? "ok" : summary.Substring(Math.Min(24, summary.Length));
                    }
                }
                finally { Disarm(); try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("unwritable log path never breaks a request", async () =>
            {
                // A path whose parent cannot be created: an existing *file* used as a directory.
                string dir = NewTempDir();
                Directory.CreateDirectory(dir);
                string blocker = Path.Combine(dir, "blocker");
                File.WriteAllText(blocker, "not a directory");
                Arm(Path.Combine(blocker, "sub", "parallel.log"));
                try
                {
                    using (MockServer srv = new MockServer(FileSize, fastContent: false))
                    {
                        long? t, cl;
                        byte[] got;
                        using (Stream s = ParallelFetch.OpenWith(srv.Url, 1000, 300000, Small(), out t, out cl, ct))
                        {
                            got = await Harness.ReadAllAsync(s, 32 * 1024, ct).ConfigureAwait(false);
                        }
                        Harness.AssertBytesEqual(Pattern.Range(1000, 300000), got, "bytes");
                    }
                    return "logging failed silently, fetch unaffected";
                }
                finally { Disarm(); try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("host-assigned Logger still works alongside the file sink", async () =>
            {
                string dir = NewTempDir();
                string logPath = Path.Combine(dir, "parallel.log");
                Arm(logPath);
                System.Collections.Concurrent.ConcurrentQueue<string> seen =
                    new System.Collections.Concurrent.ConcurrentQueue<string>();
                ParallelFetch.Logger = m => seen.Enqueue(m);
                try
                {
                    using (MockServer srv = new MockServer(FileSize, fastContent: false))
                    {
                        long? t, cl;
                        using (Stream s = ParallelFetch.OpenWith(srv.Url, 0, 100000, Small(), out t, out cl, ct))
                        {
                            await Harness.ReadAllAsync(s, 32 * 1024, ct).ConfigureAwait(false);
                        }
                    }
                    Harness.Assert(seen.Count > 0, "delegate sink received nothing");
                    Harness.Assert(File.Exists(logPath), "file sink wrote nothing");
                    return "both sinks fired (" + seen.Count + " delegate lines)";
                }
                finally { ParallelFetch.Logger = null; Disarm(); try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("a throwing host Logger cannot break a request", async () =>
            {
                Disarm();
                ParallelFetch.Logger = m => { throw new InvalidOperationException("host logger is broken"); };
                try
                {
                    using (MockServer srv = new MockServer(FileSize, fastContent: false))
                    {
                        long? t, cl;
                        byte[] got;
                        using (Stream s = ParallelFetch.OpenWith(srv.Url, 0, 100000, Small(), out t, out cl, ct))
                        {
                            got = await Harness.ReadAllAsync(s, 32 * 1024, ct).ConfigureAwait(false);
                        }
                        Harness.AssertBytesEqual(Pattern.Range(0, 100000), got, "bytes");
                    }
                    return "exception swallowed";
                }
                finally { ParallelFetch.Logger = null; }
            }).ConfigureAwait(false);

            Disarm();
        }
    }
}
