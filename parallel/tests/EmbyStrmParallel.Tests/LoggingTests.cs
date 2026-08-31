using System;
using System.Diagnostics;
using System.Globalization;
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

        /// <summary>
        /// Reads the log while the writer may still have the file open.
        ///
        /// `File.ReadAllText` asks for exclusive-ish sharing, which Windows enforces and Unix does
        /// not - so a test that reads the log promptly after tearing a stream down passed on
        /// macOS and Linux and failed on Windows only. The fetcher keeps writing as its workers
        /// unwind after Dispose (that is exactly what the budget diagnostics are for), so the
        /// race is inherent to reading a live log rather than a quirk of one test.
        /// </summary>
        private static string ReadLog(string path)
        {
            for (int attempt = 0; ; attempt++)
            {
                try
                {
                    using (FileStream fs = new FileStream(path, FileMode.Open, FileAccess.Read,
                                                          FileShare.ReadWrite | FileShare.Delete))
                    using (StreamReader r = new StreamReader(fs))
                    {
                        return r.ReadToEnd();
                    }
                }
                catch (IOException) when (attempt < 20)
                {
                    Thread.Sleep(50);
                }
            }
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
                    string text = ReadLog(logPath);
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
                        string text = ReadLog(logPath);
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
                    //
                    // The two assertions below are about COST, not content, and they exist because
                    // this test once absorbed a 2.0s -> 30.0s regression without going red. Open()
                    // is synchronous by construction, so every second spent here is an Emby request
                    // thread pinned on an origin that is simply gone.
                    Stopwatch sw = Stopwatch.StartNew();
                    Stream s = ParallelFetch.TryOpen("http://127.0.0.1:9/never", 0, 1000, out t, out cl, ct);
                    sw.Stop();
                    Harness.Assert(s == null, "TryOpen should return null on failure");
                    // 20s, not 12. The bound being defended is "not 30s of pinned host thread";
                    // 12 left only ~2s of headroom over a measured 9.9s on a CPU-loaded machine,
                    // and the target is a J4125. 20 keeps 10s of separation on BOTH sides.
                    Harness.Assert(sw.Elapsed.TotalSeconds < 20,
                        "a dead origin pinned the calling thread for " + sw.Elapsed.TotalSeconds.ToString("0.0") +
                        "s before falling back; the probe budget is meant to cap this near 8s");
                    // Lower bound, guarding the opposite failure: a probe so impatient that a
                    // short wobble forces a fallback. That is where this whole change started -
                    // on 2026-08-31 the probe bailed after ~1.9s, four times, over DNS wobbles
                    // that were done inside 2.25s, handing a 13 Mbps film to a 4.1 Mbps single
                    // connection each time. A shrunken probe window measures 2.0s, so 4 separates
                    // them 2x while sitting well under the measured spread (6.2s idle to 7.9s under 8x CPU load -
                    // and load pushes this UP, toward the 8s budget, so the risk is not on this side).
                    Harness.Assert(sw.Elapsed.TotalSeconds > 4,
                        "the probe gave up after only " + sw.Elapsed.TotalSeconds.ToString("0.0") +
                        "s; a short origin wobble would be enough to force a fallback");
                    Harness.Assert(t == null && cl == null, "out params must be null on failure");
                    Harness.Assert(File.Exists(logPath), "no log file was written for the fallback");
                    string text = ReadLog(logPath);
                    Harness.Assert(text.Contains("FALLBACK to host path"), "fallback not logged: " + text);
                    Harness.Assert(text.Contains("reason="), "no reason recorded: " + text);
                    // The reason has to be the PROBE's diagnosis, not whatever exception happened
                    // to be in flight. Two of the three give-up paths used to rethrow the inner
                    // one, so the commonest shape of all - origin accepts, never replies - wrote
                    // `reason=TaskCanceledException: The operation was canceled.` into the only
                    // record a running Emby keeps of why a stream went single-connection.
                    Harness.Assert(text.Contains("Probe gave up on"),
                        "the fallback reason is not the probe's own diagnosis: " + text);
                    Harness.Assert(text.Contains("attempts,") && text.Contains("answered)"),
                        "the reason does not say how many attempts were made or answered: " + text);
                    Harness.Assert(text.Contains("offset=0"), "offset missing: " + text);

                    // Upper bound on retries, which is what actually pins the backoff arithmetic.
                    // Feeding the ANSWERED counter to BackoffMs instead of the attempt counter
                    // computes `base << -1`; the shift masks to 63, the result is 0, the clamp
                    // turns it into no delay at all, and the loop becomes a connect storm -
                    // measured at 39,253 attempts in 30s. Every content assertion above stayed
                    // green through that. The probe caps its backoff at a quarter of its budget,
                    // so 250/500/1000/2000/2000ms fits ~6 attempts into 8s.
                    int probeRetries = 0;
                    foreach (string l in text.Split('\n')) if (l.Contains("probe retry")) probeRetries++;
                    Harness.Assert(probeRetries <= 15,
                        "the probe made " + probeRetries + " retries against a dead origin; the backoff is " +
                        "not growing, so this is a hot loop rather than a retry policy");

                    // There is deliberately NO lower bound on the count, and the reason is worth
                    // recording because two attempts at one were wrong in different ways.
                    //
                    // The property it kept reaching for - the probe caps its backoff at a quarter
                    // of its own budget rather than inheriting RetryMaxDelayMs - separates correct
                    // from broken by exactly ONE attempt (6 vs 5) on this machine. A stopwatch
                    // cannot resolve that (CPU contention costs the CORRECT shape an attempt too:
                    // 4 failures in 21 loaded runs), and neither can the count, because the count
                    // is not portable: on Windows a connect to a refused port costs real time
                    // rather than microseconds, so the same correct code fits 3 cycles into the
                    // budget where Linux and macOS fit 6. CI found that; six local review rounds
                    // did not.
                    //
                    // So the arithmetic is pinned where it is actually arithmetic - ConfigTests,
                    // against ParallelFetch.ProbeMaxDelayMs, no clock and no origin - and what is
                    // left here is only what an end-to-end run can honestly claim: the loop ran,
                    // it was not a hot loop, and it spent about the budget it was given.

                    await Task.CompletedTask;
                    return probeRetries + " retries, gave up in " + sw.Elapsed.TotalSeconds.ToString("0.0") + "s";
                }
                finally { Disarm(); try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("an error retrying cannot fix is not retried", async () =>
            {
                // The other side of moving unanswered attempts off MaxAttempts. Those attempts are
                // now bounded only by the probe's wall clock, so an exception that CANNOT succeed
                // no matter how often it is repeated - a malformed prefix in strm-routing.txt, an
                // unsupported scheme, an OOM - would burn the entire budget every time, on a
                // synchronously-blocked Emby request thread, for every request to that prefix.
                // Before the change MaxAttempts capped it at four tries in ~1.75s; the gate that
                // replaced it is HttpRangeHelper.IsRetryable, shared with the chunk loop.
                string dir = NewTempDir();
                string logPath = Path.Combine(dir, "parallel.log");
                Arm(logPath);
                try
                {
                    long? t, cl;
                    Stopwatch sw = Stopwatch.StartNew();
                    Stream s = ParallelFetch.TryOpen("http://[not-a-uri/never", 0, 1000, out t, out cl, ct);
                    sw.Stop();
                    Harness.Assert(s == null, "a malformed url should not produce a stream");
                    Harness.Assert(sw.Elapsed.TotalSeconds < 2,
                        "a malformed url took " + sw.Elapsed.TotalSeconds.ToString("0.0") +
                        "s to reject; retrying it cannot ever succeed, so this is pure blocked host thread");

                    string text = ReadLog(logPath);
                    int retries = 0;
                    foreach (string l in text.Split('\n')) if (l.Contains("probe retry")) retries++;
                    Harness.AssertEqual(0, retries, "a malformed url was retried " + retries + " time(s)");
                    await Task.CompletedTask;
                    return "rejected in " + sw.Elapsed.TotalSeconds.ToString("0.00") + "s with 0 retries";
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
                        string text = ReadLog(logPath);
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

                        string text = ReadLog(logPath);
                        Harness.Assert(text.Contains("closed complete"), "no completion summary: " + text);
                        Harness.Assert(text.Contains("closed ABANDONED"), "no abandonment summary: " + text);
                        foreach (string field in new string[] { "delivered=", "elapsed=", "rate=", "chunks=", "retries=", "slow=", "permitWait=" })
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

            await Harness.RunAsync("a stream throttled by the origin budget says so in permitWait", async () =>
            {
                // The counter that has to work on the day it matters.
                //
                // In production a stream spent eleven minutes with half its workers frozen on the
                // origin budget and reported `permitWait=0.0s` - because the accumulation sat
                // AFTER the await, so a wait that ended in cancellation contributed nothing.
                // The number read zero precisely when the budget was the culprit, which is worse
                // than having no counter at all: it actively pointed the diagnosis elsewhere.
                //
                // Asserted on the rendered line rather than on the field, because the rendering is
                // the part an operator actually reads.
                string dir = NewTempDir();
                string logPath = Path.Combine(dir, "parallel.log");
                Arm(logPath);
                try
                {
                    using (MockServer srv = new MockServer(32L * 1024 * 1024, fastContent: true))
                    {
                        srv.ThrottleBytesPerSec = 256 * 1024;
                        OriginBudget.ResetForTests();
                        string key = OriginBudget.KeyFor(srv.Url);

                        ParallelFetchOptions o = Small();
                        o.Connections = 4;
                        o.MaxOriginConnections = 4;
                        o.MaxBufferBytes = 64L * 64 * 1024;

                        // Hold most of the budget from outside, so this stream's workers have to
                        // queue for real rather than sail through.
                        OriginBudget.Permit[] held = new OriginBudget.Permit[3];
                        for (int i = 0; i < held.Length; i++)
                            held[i] = await OriginBudget.TryAcquireAsync(key, 4, TimeSpan.FromSeconds(2), ct)
                                                        .ConfigureAwait(false);
                        Harness.Assert(held[2] != null, "could not tie up the budget");

                        long? t, cl;
                        Stream s = ParallelFetch.OpenWith(srv.Url, 0, 4 * 1024 * 1024, o, out t, out cl, ct);
                        byte[] buf = new byte[32 * 1024];
                        await s.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false);
                        await Task.Delay(1200, ct).ConfigureAwait(false);   // let the queued workers accumulate
                        for (int i = 0; i < held.Length; i++) held[i].Dispose();
                        s.Dispose();

                        for (int i = 0; i < 60 && OriginBudget.InUse(key) != 0; i++)
                            await Task.Delay(50, ct).ConfigureAwait(false);

                        string text = ReadLog(logPath);
                        string summary = null;
                        foreach (string l in text.Trim().Split('\n')) if (l.Contains(" closed ")) summary = l;
                        Harness.Assert(summary != null, "no close summary at all: " + text);
                        int at = summary.IndexOf("permitWait=", StringComparison.Ordinal);
                        Harness.Assert(at >= 0, "no permitWait field: " + summary);
                        string value = summary.Substring(at + "permitWait=".Length).Split('s')[0];
                        double seconds;
                        Harness.Assert(double.TryParse(value, System.Globalization.NumberStyles.Float,
                                                       System.Globalization.CultureInfo.InvariantCulture, out seconds),
                                       "permitWait is not a number: " + summary);
                        Harness.Assert(seconds > 0,
                            "permitWait read " + value + "s while the budget was deliberately tied up; " +
                            "the one number that identifies the budget as the culprit is dead: " + summary);
                        return "permitWait=" + value + "s under a tied-up budget";
                    }
                }
                finally { Disarm(); try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("a stream closing with starved workers reports blockedOnBudget", async () =>
            {
                // The production shape, exactly. A stream is abandoned while some of its workers
                // are still queued on the origin budget, and the summary is written from the
                // disposing thread BEFORE those workers unwind - so no amount of fixing where the
                // elapsed time is accumulated can make it visible. The waiter COUNT is raised
                // before the wait starts, which is why it survives that ordering.
                //
                // Without it the log said `permitWait=0.0s` for the one stream that had spent
                // eleven minutes starved, and the diagnosis went looking at the origin instead.
                string dir = NewTempDir();
                string logPath = Path.Combine(dir, "parallel.log");
                Arm(logPath);
                OriginBudget.Permit[] held = new OriginBudget.Permit[3];
                try
                {
                    using (MockServer srv = new MockServer(32L * 1024 * 1024, fastContent: true))
                    {
                        srv.ThrottleBytesPerSec = 256 * 1024;
                        OriginBudget.ResetForTests();
                        string key = OriginBudget.KeyFor(srv.Url);

                        ParallelFetchOptions o = Small();
                        o.Connections = 4;
                        o.MaxOriginConnections = 4;
                        o.MaxBufferBytes = 64L * 64 * 1024;

                        // Leave exactly one permit free: enough for the probe (and so chunk 0),
                        // not enough for anyone else. Never released - the workers are still
                        // waiting when the stream is thrown away.
                        for (int i = 0; i < held.Length; i++)
                            held[i] = await OriginBudget.TryAcquireAsync(key, 4, TimeSpan.FromSeconds(2), ct)
                                                        .ConfigureAwait(false);
                        Harness.Assert(held[2] != null, "could not tie up the budget");

                        long? t, cl;
                        Stream s = ParallelFetch.OpenWith(srv.Url, 0, 4 * 1024 * 1024, o, out t, out cl, ct);
                        byte[] buf = new byte[32 * 1024];
                        await s.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false);
                        await Task.Delay(800, ct).ConfigureAwait(false);
                        s.Dispose();                    // budget still tied up: workers are queued

                        string text = ReadLog(logPath);
                        string summary = null;
                        foreach (string l in text.Trim().Split('\n')) if (l.Contains(" closed ")) summary = l;
                        Harness.Assert(summary != null, "no close summary at all: " + text);
                        Harness.Assert(summary.Contains("blockedOnBudget="),
                            "the stream closed with workers queued on the budget and did not say so: " + summary);
                        return summary.Substring(summary.IndexOf("blockedOnBudget=", StringComparison.Ordinal));
                    }
                }
                finally
                {
                    for (int i = 0; i < held.Length; i++) if (held[i] != null) held[i].Dispose();
                    Disarm();
                    try { Directory.Delete(dir, true); } catch { }
                }
            }).ConfigureAwait(false);

            await Harness.RunAsync("the budget and its clamp are on the open line, rendered", async () =>
            {
                // "budget below connections clamps, LOGS, and still serves" asserted the flag and
                // never the log. A flag nobody renders is not a diagnostic, and the clamp is
                // precisely the case where an operator is looking at the line asking why they are
                // not getting the connection count they configured.
                string dir = NewTempDir();
                string logPath = Path.Combine(dir, "parallel.log");
                Arm(logPath);
                try
                {
                    using (MockServer srv = new MockServer(4L * 1024 * 1024, fastContent: true))
                    {
                        OriginBudget.ResetForTests();
                        ParallelFetchOptions o = Small();
                        o.Connections = 6;
                        o.MaxOriginConnections = 2;      // deliberately contradictory

                        long? t, cl;
                        using (Stream s = ParallelFetch.OpenWith(srv.Url, 0, 300000, o, out t, out cl, ct))
                        {
                            byte[] got = await Harness.ReadAllAsync(s, 32 * 1024, ct).ConfigureAwait(false);
                            Harness.AssertBytesEqual(Pattern.Range(0, 300000), got, "bytes under a clamped config");
                        }

                        string text = ReadLog(logPath);
                        string open = null;
                        foreach (string l in text.Trim().Split('\n')) if (l.Contains("open path=parallel")) open = l;
                        Harness.Assert(open != null, "no open line at all: " + text);
                        Harness.Assert(open.Contains("originBudget=2"),
                            "the open line does not report the budget in force: " + open);
                        Harness.Assert(open.Contains("conn=2(clamped by max-origin-connections=2)"),
                            "the clamp is recorded in a flag but never rendered: " + open);
                        return "conn=2(clamped by max-origin-connections=2) originBudget=2";
                    }
                }
                finally { Disarm(); try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("abandoning a stream logs no FAILED lines, only the summary", async () =>
            {
                // A seek cancels every chunk still in flight. That is the design, not a fault,
                // and on 2026-08-31 it produced 231 "FAILED" lines against 12 real ones - so the
                // real failures could only be found with a grep -v. The closing line already says
                // ABANDONED and chunks=A/B; the per-chunk lines added nothing but volume.
                string dir = NewTempDir();
                string logPath = Path.Combine(dir, "parallel.log");
                Arm(logPath);
                try
                {
                    using (MockServer srv = new MockServer(FileSize, fastContent: false))
                    {
                        srv.ThrottleBytesPerSec = 256 * 1024;   // keep several chunks in flight at dispose
                        long? t, cl;
                        Stream s2 = ParallelFetch.OpenWith(srv.Url, 0, 3 * 1024 * 1024, Small(), out t, out cl, ct);
                        byte[] tiny = new byte[4096];
                        await s2.ReadAsync(tiny.AsMemory(0, tiny.Length), ct).ConfigureAwait(false);
                        await Task.Delay(250, ct).ConfigureAwait(false);
                        s2.Dispose();

                        // Workers unwind after Dispose returns, so wait for the summary line rather
                        // than for a fixed duration: a fixed sleep that is too short reads a log
                        // missing the very lines this test counts, and reports 0 for the wrong reason.
                        string text = "";
                        for (int i = 0; i < 60; i++)
                        {
                            text = ReadLog(logPath);
                            if (text.Contains("closed ABANDONED")) break;
                            await Task.Delay(50, ct).ConfigureAwait(false);
                        }
                        await Task.Delay(200, ct).ConfigureAwait(false);   // let any straggler log land
                        text = ReadLog(logPath);
                        int failedLines = 0;
                        foreach (string l in text.Split('\n')) if (l.Contains("FAILED:")) failedLines++;
                        Harness.Assert(text.Contains("closed ABANDONED"),
                            "the close summary went missing along with the noise: " + text);
                        Harness.AssertEqual(0, failedLines,
                            "teardown logged " + failedLines + " FAILED line(s); a cancelled chunk is not a failure");
                        return "abandoned mid-stream, 0 FAILED lines, summary intact";
                    }
                }
                finally { Disarm(); try { Directory.Delete(dir, true); } catch { } }
            }).ConfigureAwait(false);

            await Harness.RunAsync("a chunk that really fails is still logged as FAILED", async () =>
            {
                // The other half of the filter: silencing teardown must not silence anything that
                // failed on its own merits.
                string dir = NewTempDir();
                string logPath = Path.Combine(dir, "parallel.log");
                Arm(logPath);
                try
                {
                    using (MockServer srv = new MockServer(FileSize, fastContent: false))
                    {
                        // 403 on every attempt for one chunk: answered every time, so MaxAttempts
                        // governs and the chunk gives up for real.
                        srv.FaultHook = (seq, f, t2) => f >= 64 * 1024 ? MockFault.Status403 : MockFault.None;
                        ParallelFetchOptions o = Small();
                        o.MaxAttempts = 2;
                        try
                        {
                            long? t, cl;
                            using (Stream s3 = ParallelFetch.OpenWith(srv.Url, 0, 400000, o, out t, out cl, ct))
                            {
                                await Harness.ReadAllAsync(s3, 32 * 1024, ct).ConfigureAwait(false);
                            }
                            throw new Exception("the doomed stream completed; this test no longer provokes a failure");
                        }
                        catch (IOException) { /* expected */ }
                        finally { srv.FaultHook = null; }

                        string text = ReadLog(logPath);
                        Harness.Assert(text.Contains("FAILED:"),
                            "a genuine chunk failure was silenced along with the teardown noise: " + text);
                        Harness.Assert(text.Contains("failed after"),
                            "the give-up reason is missing: " + text);
                        return "real failure still reported";
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
