using System;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace EmbyStrmParallel.Tests
{
    /// <summary>
    /// Tests against the real origin. The url is read from TEST_URL.txt at runtime and is
    /// never printed, logged or included in any message this file produces.
    /// </summary>
    internal static class LiveTests
    {
        private static string _url;

        internal static bool TryLoadUrl(out string reason)
        {
            reason = null;
            try
            {
                string dir = AppContext.BaseDirectory;
                for (int i = 0; i < 8 && dir != null; i++)
                {
                    string candidate = Path.Combine(dir, "TEST_URL.txt");
                    if (File.Exists(candidate))
                    {
                        _url = File.ReadAllText(candidate).Trim();
                        if (_url.Length == 0) { reason = "TEST_URL.txt is empty"; return false; }
                        return true;
                    }
                    DirectoryInfo parent = Directory.GetParent(dir.TrimEnd(Path.DirectorySeparatorChar));
                    dir = parent == null ? null : parent.FullName;
                }
                reason = "TEST_URL.txt not found walking up from " + AppContext.BaseDirectory;
                return false;
            }
            catch (Exception ex)
            {
                reason = ex.GetType().Name;
                return false;
            }
        }

        private static ParallelFetchOptions LiveSmall()
        {
            return new ParallelFetchOptions
            {
                Connections = 4,
                ChunkSize = 128 * 1024,
                BlockSize = 32 * 1024,
                MaxBufferBytes = 8L * 128 * 1024,
                MaxAttempts = 5,
                ResponseHeadersTimeout = TimeSpan.FromSeconds(60),
                ReadIdleTimeout = TimeSpan.FromSeconds(60)
            };
        }

        private static string Sha(byte[] b)
        {
            return Convert.ToHexString(SHA256.HashData(b)).Substring(0, 16);
        }

        private static async Task<byte[]> SingleConnectionAsync(long offset, long length, CancellationToken ct)
        {
            using (SocketsHttpHandler h = new SocketsHttpHandler { AllowAutoRedirect = true, AutomaticDecompression = System.Net.DecompressionMethods.None })
            using (HttpClient c = new HttpClient(h))
            {
                c.Timeout = TimeSpan.FromMinutes(15);
                HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Get, _url);
                req.Headers.Range = length > 0
                    ? new System.Net.Http.Headers.RangeHeaderValue(offset, offset + length - 1)
                    : new System.Net.Http.Headers.RangeHeaderValue(offset, null);
                using (HttpResponseMessage r = await c.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false))
                {
                    if (!r.IsSuccessStatusCode) throw new IOException("reference fetch returned " + (int)r.StatusCode);
                    // A range request that comes back 200 is the origin ignoring Range, and a
                    // short body is usually an error page. Either would surface later as a
                    // "byte mismatch" and send someone hunting a corruption bug that is not
                    // there - so fail loudly here instead, naming the real cause.
                    if (r.StatusCode != System.Net.HttpStatusCode.PartialContent)
                    {
                        throw new IOException("reference fetch returned " + (int)r.StatusCode +
                                              " instead of 206 - the origin ignored Range, so this is NOT a comparable control");
                    }
                    byte[] body = await r.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false);
                    long expected = length > 0 ? length : body.LongLength;
                    if (length > 0 && body.LongLength != length)
                    {
                        throw new IOException("reference fetch returned " + body.Length + " bytes for a " + expected +
                                              " byte range - the control itself is wrong (error page?), not the component");
                    }
                    return body;
                }
            }
        }

        internal static async Task CorrectnessAsync(CancellationToken ct)
        {
            Harness.Section("live origin: byte-exactness vs a single-connection fetch");

            long total = 0;
            await Harness.RunAsync("probe reports a plausible total length", async () =>
            {
                long? t, cl;
                using (Stream s = ParallelFetch.OpenWith(_url, 0, 65536, LiveSmall(), out t, out cl, ct))
                {
                    Harness.Assert(t.HasValue && t.Value > 0, "no totalLength reported");
                    Harness.AssertEqual(65536, cl.Value, "contentLength");
                    total = t.Value;
                    await Task.CompletedTask;
                }
                return "total = " + total + " bytes (" + (total / 1024.0 / 1024 / 1024).ToString("0.00") + " GiB)";
            }).ConfigureAwait(false);

            if (total == 0) return;

            await LiveRange("mid-file 256 KiB", 1_500_000_000L, 256 * 1024, total, ct).ConfigureAwait(false);
            await LiveRange("single byte", 5_000_000_000L, 1, total, ct).ConfigureAwait(false);
            await LiveRange("range ending exactly at EOF", total - 300_000, 300_000, total, ct).ConfigureAwait(false);
            await LiveRange("length=0 near EOF", total - 200_000, 0, total, ct).ConfigureAwait(false);

            await Harness.RunAsync("length=0 at offset 0 reports whole-resource length", async () =>
            {
                long? t, cl;
                using (Stream s = ParallelFetch.OpenWith(_url, 0, 0, LiveSmall(), out t, out cl, ct))
                {
                    Harness.AssertEqual(total, t.Value, "totalLength");
                    Harness.AssertEqual(total, cl.Value, "contentLength");
                    Harness.AssertEqual(total, s.Length, "Stream.Length");
                    await Task.CompletedTask;
                }
                return "whole file = " + total;
            }).ConfigureAwait(false);
        }

        private static async Task LiveRange(string name, long offset, long length, long total, CancellationToken ct)
        {
            await Harness.RunAsync(name, async () =>
            {
                long expected = length > 0 ? Math.Min(length, total - offset) : total - offset;

                long? t, cl;
                byte[] parallel;
                using (Stream s = ParallelFetch.OpenWith(_url, offset, length, LiveSmall(), out t, out cl, ct))
                {
                    parallel = await Harness.ReadAllAsync(s, 81920, ct).ConfigureAwait(false);
                }
                Harness.AssertEqual(total, t.Value, "totalLength");
                Harness.AssertEqual(expected, cl.Value, "contentLength");
                Harness.AssertEqual(expected, parallel.Length, "delivered length");

                byte[] reference = await SingleConnectionAsync(offset, length, ct).ConfigureAwait(false);
                Harness.AssertBytesEqual(reference, parallel, "parallel vs single-connection");
                return expected + " bytes, sha256/16 " + Sha(parallel);
            }).ConfigureAwait(false);
        }

        internal static async Task ThroughputAsync(CancellationToken ct)
        {
            Harness.Section("live origin: throughput scaling (production chunk size)");

            long[] offsets = { 1_000_000_000L, 3_000_000_000L, 6_000_000_000L };
            int[] conns = { 1, 4, 8 };

            for (int i = 0; i < conns.Length; i++)
            {
                int n = conns[i];
                await MeasureAsync(n + " connection(s)", n, 8 * 1024 * 1024,
                                   (long)n * 8 * 1024 * 1024 * 2, offsets[i], ct).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Evidence for the chosen defaults: does adding connections past 8 keep paying, and
        /// does chunk size matter once per-request latency is amortised?
        /// </summary>
        internal static async Task TuneAsync(CancellationToken ct)
        {
            Harness.Section("live origin: does scaling continue past 8 connections?");
            await MeasureAsync("2 conn  x 8 MiB chunk", 2, 8 * 1024 * 1024, 32L * 1024 * 1024, 400_000_000L, ct).ConfigureAwait(false);
            await MeasureAsync("12 conn x 8 MiB chunk", 12, 8 * 1024 * 1024, 192L * 1024 * 1024, 1_400_000_000L, ct).ConfigureAwait(false);
            await MeasureAsync("16 conn x 8 MiB chunk", 16, 8 * 1024 * 1024, 256L * 1024 * 1024, 2_400_000_000L, ct).ConfigureAwait(false);

            Harness.Section("live origin: chunk size at 8 connections (fixed 96 MiB transfer)");
            await MeasureAsync("8 conn x 2 MiB chunk", 8, 2 * 1024 * 1024, 96L * 1024 * 1024, 3_400_000_000L, ct).ConfigureAwait(false);
            await MeasureAsync("8 conn x 4 MiB chunk", 8, 4 * 1024 * 1024, 96L * 1024 * 1024, 4_400_000_000L, ct).ConfigureAwait(false);
            await MeasureAsync("8 conn x 8 MiB chunk", 8, 8 * 1024 * 1024, 96L * 1024 * 1024, 5_400_000_000L, ct).ConfigureAwait(false);
            await MeasureAsync("8 conn x 16 MiB chunk", 8, 16 * 1024 * 1024, 96L * 1024 * 1024, 7_400_000_000L, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Chunk 0 reaches the consumer at single-connection speed until it completes, so chunk 0's
        /// size sets how long playback runs slow before parallelism takes over. A/B interleaved,
        /// because this origin's available bandwidth drifts enough to invert a naive comparison.
        /// </summary>
        internal static async Task RampAsync(CancellationToken ct)
        {
            Harness.Section("live origin: startup ramp, first chunk 8 MiB (off) vs 1 MiB (on)");
            long[] offsets = { 600_000_000L, 2_600_000_000L, 4_600_000_000L, 7_600_000_000L };
            bool[] rampOn = { false, true, false, true };
            for (int i = 0; i < offsets.Length; i++)
            {
                bool on = rampOn[i];
                long offset = offsets[i];
                await Harness.RunAsync("ramp " + (on ? "ON  (first 1 MiB)" : "OFF (first 8 MiB)") + ": MiB delivered by t=", async () =>
                {
                    ParallelFetchOptions o = new ParallelFetchOptions
                    {
                        Connections = 8,
                        ChunkSize = 8 * 1024 * 1024,
                        FirstChunkSize = on ? 1024 * 1024 : 8 * 1024 * 1024,
                        BlockSize = 64 * 1024,
                        MaxBufferBytes = 96L * 1024 * 1024,
                        ResponseHeadersTimeout = TimeSpan.FromSeconds(60),
                        ReadIdleTimeout = TimeSpan.FromSeconds(60)
                    };
                    long want = 192L * 1024 * 1024;
                    double[] marks = { 2, 5, 10, 20, 30 };
                    long[] at = new long[marks.Length];

                    Stopwatch sw = Stopwatch.StartNew();
                    long? t, cl;
                    Stream s = ParallelFetch.OpenWith(_url, offset, want, o, out t, out cl, ct);
                    byte[] buf = new byte[81920];
                    long read = 0;
                    int next = 0;
                    using (s)
                    {
                        while (next < marks.Length)
                        {
                            int got = await s.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false);
                            if (got <= 0) break;
                            read += got;
                            double el = sw.Elapsed.TotalSeconds;
                            while (next < marks.Length && el >= marks[next]) { at[next] = read; next++; }
                        }
                    }
                    sw.Stop();
                    while (next < marks.Length) { at[next] = read; next++; }

                    System.Text.StringBuilder sb = new System.Text.StringBuilder();
                    for (int m = 0; m < marks.Length; m++)
                    {
                        if (m > 0) sb.Append(' ');
                        sb.Append(marks[m].ToString("0")).Append("s=").Append((at[m] / 1024.0 / 1024).ToString("0.0"));
                    }
                    sb.Append("  [first 10s avg ").Append(Harness.Mbps(at[2], 10)).Append(']');
                    return sb.ToString();
                }).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Reproduces the live failure: successive open-ended requests, each abandoned
        /// mid-transfer, which collapsed 25.14 -> 2.58 -> 0.57 -> 0.15 -> 0.23 Mbps and stayed
        /// there. Loopback cannot reproduce it (the stale socket is an artefact of the proxy and
        /// the signed CDN target), so this has to run against the real origin.
        /// </summary>
        internal static async Task SeekStormAsync(int seekCount, int holdSeconds, CancellationToken ct)
        {
            Harness.Section("live origin: " + seekCount + " abandoned seeks, then measure");

            long[] offsets = { 5_242_880_000L, 1_073_741_824L, 7_516_192_768L, 5_242_880_000L,
                               2_684_354_560L, 8_589_934_592L, 4_294_967_296L, 6_442_450_944L };
            double[] rates = new double[seekCount];

            for (int i = 0; i < seekCount; i++)
            {
                long offset = offsets[i % offsets.Length];
                int index = i;
                await Harness.RunAsync("seek " + (i + 1) + "/" + seekCount + " @ " + offset, async () =>
                {
                    // null options => environment overrides apply, so the sweep can be driven
                    // from the shell without a rebuild.
                    Stopwatch sw = Stopwatch.StartNew();
                    long? t, cl;
                    Stream s = ParallelFetch.OpenWith(_url, offset, 0, null, out t, out cl, ct);
                    byte[] buf = new byte[81920];
                    long read = 0;
                    try
                    {
                        while (sw.Elapsed.TotalSeconds < holdSeconds)
                        {
                            int n = await s.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false);
                            if (n <= 0) break;
                            read += n;
                        }
                    }
                    finally
                    {
                        s.Dispose();   // abandon mid-transfer, exactly like a player seeking
                    }
                    sw.Stop();
                    double mbps = read * 8.0 / sw.Elapsed.TotalSeconds / 1e6;
                    rates[index] = mbps;
                    return Harness.MiB(read) + " in " + sw.Elapsed.TotalSeconds.ToString("0.0") + "s = " +
                           mbps.ToString("0.00") + " Mbps";
                }).ConfigureAwait(false);
            }

            await Harness.RunAsync("no monotonic collapse across the sequence", async () =>
            {
                double first = rates[0];
                double last = rates[rates.Length - 1];
                double worst = double.MaxValue;
                for (int i = 0; i < rates.Length; i++) if (rates[i] < worst) worst = rates[i];
                Harness.Assert(first > 1.0, "even the first seek was slow (" + first.ToString("0.00") + " Mbps) - origin problem, not ours");
                Harness.Assert(last > first * 0.4,
                    "collapsed from " + first.ToString("0.00") + " to " + last.ToString("0.00") + " Mbps");
                Harness.Assert(worst > 1.0, "one seek fell to " + worst.ToString("0.00") + " Mbps");
                await Task.CompletedTask;
                return "first " + first.ToString("0.0") + ", last " + last.ToString("0.0") +
                       ", worst " + worst.ToString("0.0") + " Mbps";
            }).ConfigureAwait(false);
        }

        /// <summary>Ad-hoc measurement point, driven from the command line so the tuning grid can be re-run without a rebuild.</summary>
        internal static Task MeasureOneAsync(int connections, int chunkMiB, long wantMiB, long offset, CancellationToken ct)
        {
            return MeasureAsync(connections + " conn x " + chunkMiB + " MiB chunk, " + wantMiB + " MiB",
                                connections, chunkMiB * 1024 * 1024, wantMiB * 1024 * 1024, offset, ct);
        }

        private static async Task MeasureAsync(string label, int connections, int chunkSize, long want, long offset,
                                               CancellationToken ct)
        {
            await Harness.RunAsync(label, async () =>
            {
                ParallelFetchOptions o = new ParallelFetchOptions
                {
                    Connections = connections,
                    ChunkSize = chunkSize,
                    BlockSize = 64 * 1024,
                    MaxBufferBytes = (long)(connections + 4) * chunkSize,
                    ResponseHeadersTimeout = TimeSpan.FromSeconds(60),
                    ReadIdleTimeout = TimeSpan.FromSeconds(60)
                };

                Stopwatch sw = Stopwatch.StartNew();
                long? t, cl;
                Stream s = ParallelFetch.OpenWith(_url, offset, want, o, out t, out cl, ct);
                double openSeconds = sw.Elapsed.TotalSeconds;
                long ceiling = (s as ParallelRangeStream) != null ? ((ParallelRangeStream)s).MemoryCeilingBytes : 0;

                byte[] buf = new byte[81920];
                long read = 0;
                double firstByte = -1;
                using (s)
                {
                    while (true)
                    {
                        int got = await s.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false);
                        if (got <= 0) break;
                        if (firstByte < 0) firstByte = sw.Elapsed.TotalSeconds;
                        read += got;
                    }
                }
                sw.Stop();

                Harness.AssertEqual(want, read, "bytes delivered");
                double totalSeconds = sw.Elapsed.TotalSeconds;

                return Harness.MiB(read) + " in " + totalSeconds.ToString("0.0") + "s = " +
                       Harness.Mbps(read, totalSeconds) +
                       "  [open " + openSeconds.ToString("0.0") + "s, ttfb " + firstByte.ToString("0.0") +
                       "s, post-ttfb " + Harness.Mbps(read, totalSeconds - firstByte) +
                       ", ceiling " + Harness.MiB(ceiling) + "]";
            }).ConfigureAwait(false);
        }

        internal static async Task SoakAsync(long bytes, CancellationToken ct)
        {
            Harness.Section("live origin: sustained transfer with memory sampling");

            await Harness.RunAsync(Harness.MiB(bytes) + " from the real origin, default options", async () =>
            {
                ParallelFetchOptions o = new ParallelFetchOptions();
                GC.Collect(); GC.WaitForPendingFinalizers(); GC.Collect();
                Process p = Process.GetCurrentProcess();
                p.Refresh();
                long rssBefore = p.WorkingSet64;

                long? t, cl;
                Stream s = ParallelFetch.OpenWith(_url, 2_000_000_000L, bytes, o, out t, out cl, ct);
                ParallelRangeStream prs = (ParallelRangeStream)s;
                long ceiling = prs.MemoryCeilingBytes;
                FetchMetrics.ResetPeak();

                long peakRss = rssBefore;
                using (CancellationTokenSource stop = new CancellationTokenSource())
                {
                    Task sampler = Task.Run(async () =>
                    {
                        while (!stop.IsCancellationRequested)
                        {
                            p.Refresh();
                            long ws = p.WorkingSet64;
                            if (ws > peakRss) peakRss = ws;
                            try { await Task.Delay(250, stop.Token).ConfigureAwait(false); } catch { return; }
                        }
                    });

                    Stopwatch sw = Stopwatch.StartNew();
                    long read;
                    using (s) read = await Harness.DrainAsync(s, 81920, 0, ct, null).ConfigureAwait(false);
                    sw.Stop();
                    stop.Cancel();
                    try { await sampler.ConfigureAwait(false); } catch { }

                    Harness.AssertEqual(bytes, read, "bytes delivered");
                    long peakBuffered = FetchMetrics.PeakBufferedBytes;
                    Harness.Assert(peakBuffered <= ceiling + (long)(o.Connections + 2) * o.BlockSize,
                        "peak buffered " + peakBuffered + " over ceiling " + ceiling);

                    return Harness.Mbps(read, sw.Elapsed.TotalSeconds) + " sustained, ceiling " + Harness.MiB(ceiling) +
                           ", peak buffered " + Harness.MiB(peakBuffered) +
                           ", RSS " + Harness.MiB(rssBefore) + " -> " + Harness.MiB(peakRss);
                }
            }).ConfigureAwait(false);
        }

        internal static async Task CancellationAsync(CancellationToken ct)
        {
            await Harness.RunAsync("live: cancellation mid-stream tears down promptly", async () =>
            {
                ParallelFetchOptions o = new ParallelFetchOptions
                {
                    Connections = 6,
                    ChunkSize = 4 * 1024 * 1024,
                    BlockSize = 64 * 1024,
                    MaxBufferBytes = 10L * 4 * 1024 * 1024,
                    ResponseHeadersTimeout = TimeSpan.FromSeconds(60),
                    ReadIdleTimeout = TimeSpan.FromSeconds(60)
                };
                using (CancellationTokenSource cts = new CancellationTokenSource())
                {
                    long? t, cl;
                    Stream s = ParallelFetch.OpenWith(_url, 4_000_000_000L, 400L * 1024 * 1024, o, out t, out cl, cts.Token);
                    ParallelRangeStream prs = (ParallelRangeStream)s;

                    byte[] buf = new byte[81920];
                    long read = 0;
                    while (read < 512 * 1024)
                    {
                        int n = await s.ReadAsync(buf.AsMemory(0, buf.Length), CancellationToken.None).ConfigureAwait(false);
                        if (n <= 0) break;
                        read += n;
                    }

                    Stopwatch sw = Stopwatch.StartNew();
                    cts.Cancel();
                    bool threw = false;
                    try
                    {
                        while (true)
                        {
                            int n = await s.ReadAsync(buf.AsMemory(0, buf.Length), CancellationToken.None).ConfigureAwait(false);
                            if (n <= 0) break;
                        }
                    }
                    catch (Exception ex) when (ex is OperationCanceledException || ex is IOException) { threw = true; }
                    double abort = sw.Elapsed.TotalSeconds;
                    s.Dispose();

                    Task done = prs.WorkersCompletion;
                    Stopwatch teardown = Stopwatch.StartNew();
                    Task finished = await Task.WhenAny(done, Task.Delay(10000)).ConfigureAwait(false);
                    teardown.Stop();

                    Harness.Assert(threw, "cancel did not surface on Read");
                    Harness.Assert(abort < 5, "Read needed " + abort.ToString("0.0") + "s to notice cancellation");
                    Harness.Assert(ReferenceEquals(finished, done), "workers still alive 10s after dispose");
                    return "aborted in " + abort.ToString("0.00") + "s, 6 live connections closed in " +
                           teardown.Elapsed.TotalSeconds.ToString("0.00") + "s";
                }
            }).ConfigureAwait(false);
        }
    }
}
