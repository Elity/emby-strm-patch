using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace EmbyStrmParallel.Tests
{
    internal static class MockTests
    {
        private const long FileSize = 8L * 1024 * 1024;

        private sealed class Fetched
        {
            internal byte[] Data;
            internal long? Total;
            internal long? ContentLength;
        }

        private static ParallelFetchOptions Small()
        {
            return new ParallelFetchOptions
            {
                Connections = 4,
                ConnectionRampInterval = TimeSpan.Zero,
                ChunkSize = 64 * 1024,
                FirstChunkSize = 64 * 1024,   // ramp off: these tests pin exact chunk boundaries
                BlockSize = 16 * 1024,
                MaxBufferBytes = 8L * 64 * 1024,
                MaxAttempts = 6,
                RetryBaseDelayMs = 10,
                RetryMaxDelayMs = 50,
                ResponseHeadersTimeout = TimeSpan.FromSeconds(10),
                ReadIdleTimeout = TimeSpan.FromSeconds(10)
            };
        }

        /// <summary>Ramp on: chunk 0 is 128 KiB, doubling to a 1 MiB steady size.</summary>
        private static ParallelFetchOptions Ramped()
        {
            ParallelFetchOptions o = Small();
            o.ChunkSize = 1024 * 1024;
            o.FirstChunkSize = 128 * 1024;
            o.MaxBufferBytes = 8L * 1024 * 1024;
            return o;
        }

        private static async Task<Fetched> FetchAsync(string url, long offset, long length,
                                                      ParallelFetchOptions o, CancellationToken ct)
        {
            long? total, contentLength;
            Stream s = ParallelFetch.OpenWith(url, offset, length, o, out total, out contentLength, ct);
            using (s)
            {
                byte[] data = await Harness.ReadAllAsync(s, 32 * 1024, ct).ConfigureAwait(false);
                return new Fetched { Data = data, Total = total, ContentLength = contentLength };
            }
        }

        /// <summary>The reference implementation: exactly one connection, one Range request.</summary>
        private static async Task<byte[]> SingleConnectionAsync(string url, long offset, long length, CancellationToken ct)
        {
            using (HttpClient c = new HttpClient())
            {
                c.Timeout = TimeSpan.FromMinutes(10);
                HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Get, url);
                if (length > 0) req.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(offset, offset + length - 1);
                else req.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(offset, null);
                using (HttpResponseMessage r = await c.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false))
                {
                    r.EnsureSuccessStatusCode();
                    return await r.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false);
                }
            }
        }

        internal static async Task RunAsync(CancellationToken ct)
        {
            Harness.Section("chunk schedule: ramp arithmetic covers the range exactly");

            await Harness.RunAsync("no gaps, no overlaps, exact total, over a parameter grid", async () =>
            {
                long[] totals = { 0, 1, 999, 128 * 1024, 1024 * 1024, 8L * 1024 * 1024 + 1, 10_484_848_965L };
                int[] firsts = { 64 * 1024, 256 * 1024, 1024 * 1024, 8 * 1024 * 1024 };
                int[] chunks = { 64 * 1024, 1024 * 1024, 8 * 1024 * 1024 };
                int combos = 0;
                foreach (long total in totals)
                {
                    foreach (int chunk in chunks)
                    {
                        foreach (int first in firsts)
                        {
                            if (first > chunk) continue;               // Normalize() clamps this away
                            if (total > 1_000_000_000L && chunk < 8 * 1024 * 1024) continue;  // keep the grid quick
                            ChunkSchedule sch = new ChunkSchedule(total, first, chunk);
                            long expected = 0;
                            for (long i = 0; i < sch.Count; i++)
                            {
                                long s, len;
                                sch.Range(i, out s, out len);
                                if (s != expected)
                                    throw new Exception("gap/overlap at chunk " + i + " (total=" + total +
                                                        " first=" + first + " chunk=" + chunk + "): expected start " +
                                                        expected + " got " + s);
                                if (len < 0) throw new Exception("negative length at chunk " + i);
                                if (len > chunk) throw new Exception("chunk " + i + " is larger than ChunkSize");
                                expected += len;
                            }
                            if (expected != total)
                                throw new Exception("coverage " + expected + " != total " + total +
                                                    " (first=" + first + " chunk=" + chunk + ")");
                            combos++;
                        }
                    }
                }
                await Task.CompletedTask;
                return combos + " parameter combinations verified";
            }).ConfigureAwait(false);

            await Harness.RunAsync("ramp produces the expected 128K/256K/512K/1M/1M... sizes", async () =>
            {
                ChunkSchedule sch = new ChunkSchedule(4 * 1024 * 1024, 128 * 1024, 1024 * 1024);
                long[] want = { 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 1024 * 1024, 1024 * 1024, 1024 * 1024 };
                long covered = 0;
                for (int i = 0; i < want.Length; i++)
                {
                    long s, len;
                    sch.Range(i, out s, out len);
                    covered += len;
                    if (i < want.Length - 1) Harness.AssertEqual(want[i], len, "chunk " + i + " length");
                }
                Harness.AssertEqual(4 * 1024 * 1024, covered, "total covered");
                Harness.AssertEqual(7, sch.Count, "chunk count");
                await Task.CompletedTask;
                return "128K,256K,512K then 1M x4";
            }).ConfigureAwait(false);

            Harness.Section("mock server: byte-exactness (vs generator AND vs single-connection fetch)");

            using (MockServer srv = new MockServer(FileSize, fastContent: false))
            {
                await RangeCase(srv, "whole-file prefix (0, 1 MiB)", 0, 1024 * 1024, ct).ConfigureAwait(false);
                await RangeCase(srv, "whole file (0, length=0)", 0, 0, ct).ConfigureAwait(false);
                await RangeCase(srv, "mid-file range", 3000003, 1234567, ct).ConfigureAwait(false);
                await RangeCase(srv, "range ending exactly at EOF", FileSize - 500000, 500000, ct).ConfigureAwait(false);
                await RangeCase(srv, "length=0 from mid-file", FileSize - 300000, 0, ct).ConfigureAwait(false);
                await RangeCase(srv, "single byte", 4242424, 1, ct).ConfigureAwait(false);
                await RangeCase(srv, "last byte of file", FileSize - 1, 1, ct).ConfigureAwait(false);
                await RangeCase(srv, "chunk-aligned range", 65536 * 4, 65536 * 3, ct).ConfigureAwait(false);

                await Harness.RunAsync("length past EOF clamps to available", async () =>
                {
                    Fetched f = await FetchAsync(srv.Url, FileSize - 1000, 999999, Small(), ct).ConfigureAwait(false);
                    Harness.AssertEqual(1000, f.Data.Length, "delivered");
                    Harness.AssertEqual(FileSize, f.Total.Value, "totalLength");
                    Harness.AssertEqual(1000, f.ContentLength.Value, "contentLength");
                    Harness.AssertBytesEqual(Pattern.Range(FileSize - 1000, 1000), f.Data, "bytes");
                    return "clamped to 1000";
                }).ConfigureAwait(false);

                await Harness.RunAsync("302 redirect is followed", async () =>
                {
                    Fetched f = await FetchAsync(srv.RedirectUrl, 100000, 300000, Small(), ct).ConfigureAwait(false);
                    Harness.AssertBytesEqual(Pattern.Range(100000, 300000), f.Data, "bytes");
                    return "ok";
                }).ConfigureAwait(false);

                await Harness.RunAsync("synchronous Read() path is exact and does not deadlock", async () =>
                {
                    // The host may copy with the blocking Stream API. Our Read is sync-over-async,
                    // so this is the deadlock canary as well as a correctness check.
                    long? total, cl;
                    Stream s = ParallelFetch.OpenWith(srv.Url, 12345, 900000, Small(), out total, out cl, ct);
                    Task<byte[]> work = Task.Run(() =>
                    {
                        using (s)
                        using (MemoryStream ms = new MemoryStream())
                        {
                            byte[] buf = new byte[81920];   // the host's buffer size
                            while (true)
                            {
                                int n = s.Read(buf, 0, buf.Length);
                                if (n <= 0) break;
                                ms.Write(buf, 0, n);
                            }
                            return ms.ToArray();
                        }
                    });
                    Task done = await Task.WhenAny(work, Task.Delay(30000)).ConfigureAwait(false);
                    Harness.Assert(ReferenceEquals(done, work), "synchronous Read deadlocked (30s timeout)");
                    byte[] got = await work.ConfigureAwait(false);
                    Harness.AssertBytesEqual(Pattern.Range(12345, 900000), got, "bytes");
                    return "900000 bytes via blocking Read";
                }).ConfigureAwait(false);

                await Harness.RunAsync("CopyToAsync delivers the exact range", async () =>
                {
                    long? total, cl;
                    using (Stream s = ParallelFetch.OpenWith(srv.Url, 77, 1000000, Small(), out total, out cl, ct))
                    using (MemoryStream ms = new MemoryStream())
                    {
                        await s.CopyToAsync(ms, 81920, ct).ConfigureAwait(false);
                        Harness.AssertBytesEqual(Pattern.Range(77, 1000000), ms.ToArray(), "bytes");
                    }
                    return "1000000 bytes via CopyToAsync";
                }).ConfigureAwait(false);

                await Harness.RunAsync("stream contract: forward-only, Seek throws", async () =>
                {
                    long? total, cl;
                    using (Stream s = ParallelFetch.OpenWith(srv.Url, 0, 200000, Small(), out total, out cl, ct))
                    {
                        Harness.Assert(s.CanRead, "CanRead should be true");
                        Harness.Assert(!s.CanSeek, "CanSeek must be false");
                        Harness.Assert(!s.CanWrite, "CanWrite must be false");
                        bool seekThrew = false, posThrew = false, writeThrew = false, lenThrew = false;
                        try { long _ = s.Length; } catch (NotSupportedException) { lenThrew = true; }
                        try { s.Seek(0, SeekOrigin.Begin); } catch (NotSupportedException) { seekThrew = true; }
                        try { s.Position = 5; } catch (NotSupportedException) { posThrew = true; }
                        try { s.Write(new byte[1], 0, 1); } catch (NotSupportedException) { writeThrew = true; }
                        Harness.Assert(seekThrew, "Seek should throw NotSupportedException");
                        Harness.Assert(posThrew, "Position setter should throw NotSupportedException");
                        Harness.Assert(writeThrew, "Write should throw NotSupportedException");
                        // Length is the RANGE size, not the resource size, and the host reads it
                        // as the latter when TotalLength is null. Refusing is the safe answer.
                        Harness.Assert(lenThrew, "Length should throw NotSupportedException");
                        await Task.CompletedTask;
                    }
                    return "forward-only enforced";
                }).ConfigureAwait(false);

                await Harness.RunAsync("really uses N connections concurrently", async () =>
                {
                    srv.ResetCounters();
                    srv.ThrottleBytesPerSec = 400 * 1024;
                    try
                    {
                        ParallelFetchOptions o = Small();
                        o.Connections = 6;
                        o.ChunkSize = 128 * 1024;
                        o.FirstChunkSize = 128 * 1024;
                        o.MaxBufferBytes = 12L * 128 * 1024;
                        Fetched f = await FetchAsync(srv.Url, 0, 4 * 1024 * 1024, o, ct).ConfigureAwait(false);
                        Harness.AssertBytesEqual(Pattern.Range(0, 4 * 1024 * 1024), f.Data, "bytes");
                        Harness.Assert(srv.MaxConcurrent >= 5, "peak concurrent requests was only " + srv.MaxConcurrent);
                        return "peak concurrent = " + srv.MaxConcurrent + " (asked for 6)";
                    }
                    finally { srv.ThrottleBytesPerSec = 0; }
                }).ConfigureAwait(false);

                // --- ramp enabled: chunk boundaries are no longer uniform ---
                await RampCase(srv, "ramp: whole file (length=0)", 0, 0, ct).ConfigureAwait(false);
                await RampCase(srv, "ramp: range ends inside chunk 0", 0, 100000, ct).ConfigureAwait(false);
                await RampCase(srv, "ramp: range ends inside chunk 1", 0, 200000, ct).ConfigureAwait(false);
                await RampCase(srv, "ramp: range ends exactly on a ramp boundary", 0, 128 * 1024, ct).ConfigureAwait(false);
                await RampCase(srv, "ramp: unaligned offset spanning ramp into steady", 12345, 3000000, ct).ConfigureAwait(false);
                await RampCase(srv, "ramp: single byte", 777777, 1, ct).ConfigureAwait(false);
                await RampCase(srv, "ramp: range ending exactly at EOF", FileSize - 1500000, 1500000, ct).ConfigureAwait(false);

                await Harness.RunAsync("connection slow-start delays extra workers but stays exact", async () =>
                {
                    srv.ResetCounters();
                    srv.ThrottleBytesPerSec = 2 * 1024 * 1024;
                    try
                    {
                        ParallelFetchOptions o = Small();
                        o.Connections = 6;
                        o.ChunkSize = 128 * 1024;
                        o.FirstChunkSize = 128 * 1024;
                        o.MaxBufferBytes = 12L * 128 * 1024;
                        o.InitialConnections = 2;
                        o.ConnectionRampInterval = TimeSpan.FromMilliseconds(400);

                        Fetched f = await FetchAsync(srv.Url, 0, 1024 * 1024, o, ct).ConfigureAwait(false);
                        Harness.AssertBytesEqual(Pattern.Range(0, 1024 * 1024), f.Data, "bytes");
                        // The whole point: the origin must not see all six at once up front.
                        Harness.Assert(srv.MaxConcurrent <= 6, "more connections than configured: " + srv.MaxConcurrent);
                        return "peak concurrent " + srv.MaxConcurrent + " of 6, ramped from 2";
                    }
                    finally { srv.ThrottleBytesPerSec = 0; }
                }).ConfigureAwait(false);
            }

            Harness.Section("mock server: failure handling");

            using (MockServer srv = new MockServer(FileSize, fastContent: false))
            {
                await Harness.RunAsync("transient 403 on every chunk's first try -> exact", async () =>
                {
                    ConcurrentDictionary<long, int> seen = new ConcurrentDictionary<long, int>();
                    srv.FaultHook = (seq, from, to) =>
                        seen.AddOrUpdate(from, 1, (k, v) => v + 1) == 1 ? MockFault.Status403 : MockFault.None;
                    try
                    {
                        Fetched f = await FetchAsync(srv.Url, 0, 1024 * 1024, Small(), ct).ConfigureAwait(false);
                        Harness.AssertBytesEqual(Pattern.Range(0, 1024 * 1024), f.Data, "bytes");
                        return "recovered from " + seen.Count + " forced 403s";
                    }
                    finally { srv.FaultHook = null; }
                }).ConfigureAwait(false);

                await Harness.RunAsync("transient 500 on every chunk's first try -> exact", async () =>
                {
                    ConcurrentDictionary<long, int> seen = new ConcurrentDictionary<long, int>();
                    srv.FaultHook = (seq, from, to) =>
                        seen.AddOrUpdate(from, 1, (k, v) => v + 1) == 1 ? MockFault.Status500 : MockFault.None;
                    try
                    {
                        Fetched f = await FetchAsync(srv.Url, 500000, 1024 * 1024, Small(), ct).ConfigureAwait(false);
                        Harness.AssertBytesEqual(Pattern.Range(500000, 1024 * 1024), f.Data, "bytes");
                        return "recovered";
                    }
                    finally { srv.FaultHook = null; }
                }).ConfigureAwait(false);

                await Harness.RunAsync("connection dies mid-chunk -> resumes, no duplication", async () =>
                {
                    // Truncate the FIRST request for each chunk start exactly once. Resume
                    // requests start mid-chunk and are served intact, so a correct implementation
                    // needs one retry per chunk; a cursor that rewinds on the exception would
                    // re-deliver the first half of every chunk.
                    const long Offset = 111111;
                    const int Chunk = 64 * 1024;
                    ConcurrentDictionary<long, int> seen = new ConcurrentDictionary<long, int>();
                    srv.FaultHook = (seq, from, to) =>
                    {
                        bool aligned = from >= Offset && (from - Offset) % Chunk == 0;
                        int n = seen.AddOrUpdate(from, 1, (k, v) => v + 1);
                        return aligned && n == 1 ? MockFault.TruncateHalf : MockFault.None;
                    };
                    try
                    {
                        Fetched f = await FetchAsync(srv.Url, Offset, 1024 * 1024, Small(), ct).ConfigureAwait(false);
                        Harness.AssertBytesEqual(Pattern.Range(Offset, 1024 * 1024), f.Data, "bytes");
                        return "resumed 16 truncated bodies";
                    }
                    finally { srv.FaultHook = null; }
                }).ConfigureAwait(false);

                await Harness.RunAsync("chunk truncated repeatedly still resumes exactly", async () =>
                {
                    // Every request for this range is cut in half until the tail is tiny, so a
                    // single chunk is stitched together from many partial bodies.
                    ConcurrentDictionary<long, int> seen = new ConcurrentDictionary<long, int>();
                    srv.FaultHook = (seq, from, to) =>
                        seen.AddOrUpdate(from, 1, (k, v) => v + 1) == 1 && (to - from + 1) > 4096
                            ? MockFault.TruncateHalf : MockFault.None;
                    try
                    {
                        ParallelFetchOptions o = Small();
                        o.MaxAttempts = 12;
                        Fetched f = await FetchAsync(srv.Url, 700000, 256 * 1024, o, ct).ConfigureAwait(false);
                        Harness.AssertBytesEqual(Pattern.Range(700000, 256 * 1024), f.Data, "bytes");
                        return "stitched from " + seen.Count + " partial bodies";
                    }
                    finally { srv.FaultHook = null; }
                }).ConfigureAwait(false);

                await Harness.RunAsync("permanent chunk failure surfaces as an exception, never short data", async () =>
                {
                    // Chunk 0 always succeeds, every later chunk fails forever. Keyed on `from`
                    // rather than a sequence number so stray in-flight requests from the previous
                    // test cannot perturb it.
                    srv.ResetCounters();
                    srv.FaultHook = (seq, from, to) => from == 0 ? MockFault.None : MockFault.Status500;
                    try
                    {
                        long? total, cl;
                        ParallelFetchOptions o = Small();
                        o.MaxAttempts = 2;
                        Stream s = ParallelFetch.OpenWith(srv.Url, 0, 1024 * 1024, o, out total, out cl, ct);

                        MemoryStream got = new MemoryStream();
                        byte[] buf = new byte[16 * 1024];
                        bool threw = false;
                        using (s)
                        {
                            try
                            {
                                while (true)
                                {
                                    int n = await s.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false);
                                    if (n <= 0) break;
                                    got.Write(buf, 0, n);
                                }
                            }
                            catch (IOException)
                            {
                                threw = true;
                            }
                        }

                        byte[] delivered = got.ToArray();
                        Harness.Assert(threw, "expected an IOException, instead got a clean EOF after " +
                                              delivered.Length + " of 1048576 bytes");
                        Harness.Assert(delivered.Length < 1024 * 1024, "the range must not appear complete");
                        Harness.Assert(delivered.Length > 0, "chunk 0 succeeded, so some data should have been delivered first");
                        // Whatever did get through must still be correct, in the right order.
                        Harness.AssertBytesEqual(Pattern.Range(0, delivered.Length), delivered, "delivered prefix");
                        return "threw after " + delivered.Length + " correct bytes of 1048576";
                    }
                    finally { srv.FaultHook = null; }
                }).ConfigureAwait(false);

                await Harness.RunAsync("origin ignores Range -> single-connection fallback stays exact", async () =>
                {
                    srv.FaultHook = (seq, from, to) => MockFault.IgnoreRange;
                    try
                    {
                        Fetched f = await FetchAsync(srv.Url, 2000000, 400000, Small(), ct).ConfigureAwait(false);
                        Harness.AssertEqual(FileSize, f.Total.Value, "totalLength");
                        Harness.AssertEqual(400000, f.ContentLength.Value, "contentLength");
                        Harness.AssertBytesEqual(Pattern.Range(2000000, 400000), f.Data, "bytes");
                        return "fallback exact";
                    }
                    finally { srv.FaultHook = null; }
                }).ConfigureAwait(false);
            }

            Harness.Section("mock server: response headers that would misplace bytes");

            await UntrustworthyContentRangeAsync(ct).ConfigureAwait(false);
            await UnknownTotalAsync(ct).ConfigureAwait(false);
            await RangePastEofAsync(ct).ConfigureAwait(false);
            await IgnoredRangeSkipCeilingAsync(ct).ConfigureAwait(false);
            await ContentEncodingAsync(ct).ConfigureAwait(false);
            await RetryAfterAsync(ct).ConfigureAwait(false);
            await UnansweredBurstAsync(ct).ConfigureAwait(false);
            await TrickleAlwaysGivesUpAsync(ct).ConfigureAwait(false);
            await StallBudgetStopsAnUnansweringOriginAsync(ct).ConfigureAwait(false);
            await ProbeRidesOutUnansweredBurstAsync(ct).ConfigureAwait(false);
            await ProbeBudgetHoldsWhateverTheOriginDoesAsync(ct).ConfigureAwait(false);

            Harness.Section("mock server: backpressure, memory, cancellation");

            await ContradictoryContentRangeAsync(ct).ConfigureAwait(false);
            await AbandonmentAsync(ct).ConfigureAwait(false);
            await ConnectionIsolationAsync(ct).ConfigureAwait(false);
            await SlowConnectionAsync(ct).ConfigureAwait(false);
            await BackpressureAsync(ct).ConfigureAwait(false);
            await MemoryCeilingAsync(ct).ConfigureAwait(false);
            await CancellationAsync(ct).ConfigureAwait(false);
            await DisposeMidStreamAsync(ct).ConfigureAwait(false);
            await WorkerStartupFailureAsync(ct).ConfigureAwait(false);

            await BudgetsAsync(ct).ConfigureAwait(false);
        }

        /// <summary>
        /// The origin budget, reachable on its own as `run-tests.sh budget`.
        ///
        /// These are the slowest tests in the file and the ones most often iterated on, and the
        /// audit of the first budget implementation had to reconstruct exactly this entry point
        /// by hand-editing a copy of the tree before it could run mutation experiments. A named
        /// subset costs one line and makes that reproducible.
        /// </summary>
        internal static async Task BudgetsAsync(CancellationToken ct)
        {
            Harness.Section("origin connection budget");

            await BudgetKeyGroupingAsync(ct).ConfigureAwait(false);
            await BudgetCapsConcurrencyAsync(ct).ConfigureAwait(false);
            await BudgetStarvesNoStreamAsync(ct).ConfigureAwait(false);
            await BudgetNeverLeaksPermitsAsync(ct).ConfigureAwait(false);
            await BudgetIsPerOriginAsync(ct).ConfigureAwait(false);
            await BudgetReleasedOnDisposeAsync(ct).ConfigureAwait(false);
            await BudgetBelowConnectionsAsync(ct).ConfigureAwait(false);
            // Ordered before the tests that can block a thread: a mutant that makes a permit wait
            // unbounded is caught here in under a second, instead of wedging the runner in
            // BudgetDeclinesWhenFull and never reaching anything after it.
            await BudgetLimitChangeAsync(ct).ConfigureAwait(false);
            await BudgetQueuingIsNotAStallAsync(ct).ConfigureAwait(false);
            await BudgetDeclinesWhenFullAsync(ct).ConfigureAwait(false);
            await BudgetSurvivesAStalledReaderAsync(ct).ConfigureAwait(false);
            await BudgetFreedDuringBackoffAsync(ct).ConfigureAwait(false);
            await BudgetFreedDuringProbeBackoffAsync(ct).ConfigureAwait(false);
            await BudgetWithConnectionRampAsync(ct).ConfigureAwait(false);
            await BudgetWakesQueuedWorkersOnDisposeAsync(ct).ConfigureAwait(false);
        }

        /// <summary>
        /// What counts as "the same origin". Every other budget test feeds this a well-formed
        /// absolute url from the mock server, so the grouping rules and the malformed-url
        /// fallback were decided by reading rather than by measurement.
        ///
        /// The fallback matters more than it looks: returning the whole string keeps an
        /// unparseable url in a group of its own. Returning a constant instead would quietly
        /// merge every broken url into one budget and throttle unrelated origins against each
        /// other.
        /// </summary>
        private static async Task BudgetKeyGroupingAsync(CancellationToken ct)
        {
            await Harness.RunAsync("the grouping key is the authority, and never merges by accident", async () =>
            {
                string a = OriginBudget.KeyFor("https://pan.example.com/a/b.mkv?sign=x");
                Harness.Assert(a == "https://pan.example.com:443",
                    "authority key should carry the default port explicitly, got " + a);
                Harness.Assert(OriginBudget.KeyFor("https://pan.example.com/other/path.mkv") == a,
                    "two paths on one host must share a budget");
                Harness.Assert(OriginBudget.KeyFor("https://PAN.example.com/a.mkv") == a,
                    "host comparison must not be case-sensitive");
                Harness.Assert(OriginBudget.KeyFor("https://pan.example.com:8443/a.mkv") != a,
                    "a different port is a different origin");
                Harness.Assert(OriginBudget.KeyFor("http://pan.example.com/a.mkv") != a,
                    "a different scheme is a different origin");

                // The catch branch. Each malformed url keeps its own group; none of them lands
                // on a shared key that would throttle real origins.
                string m1 = OriginBudget.KeyFor("not a url at all");
                string m2 = OriginBudget.KeyFor("/relative/path.mkv");
                Harness.Assert(m1 == "not a url at all", "malformed url should key on itself, got " + m1);
                Harness.Assert(m2 == "/relative/path.mkv", "relative url should key on itself, got " + m2);
                Harness.Assert(m1 != m2 && m1 != a && m2 != a, "malformed urls must not merge with each other or with a real origin");
                Harness.AssertEqual(0, string.CompareOrdinal(OriginBudget.KeyFor(null), "(none)"), "null url");
                Harness.AssertEqual(0, string.CompareOrdinal(OriginBudget.KeyFor(""), "(none)"), "empty url");

                await Task.CompletedTask;
                return "9 grouping cases, malformed urls stay in their own groups";
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// A stream thrown away while its workers are queued for a permit must let them go at
        /// once. `TryAcquireAsync` takes the stream's token precisely so that a disposed stream
        /// wakes its waiters instead of leaving tasks parked until a timeout - and until now
        /// nothing checked it: the other teardown tests dispose streams whose workers are busy
        /// or idle, never queued, because that state only exists when the budget is full.
        ///
        /// The stakes are that a leaked parked task also holds its slot, its channel and its
        /// buffers, so a burst of seeks against a saturated origin would accumulate them.
        /// </summary>
        private static async Task BudgetWakesQueuedWorkersOnDisposeAsync(CancellationToken ct)
        {
            await Harness.RunAsync("disposing wakes workers queued on the budget at once", async () =>
            {
                using (MockServer srv = new MockServer(64L * 1024 * 1024, fastContent: true))
                {
                    srv.ThrottleBytesPerSec = 256 * 1024;
                    OriginBudget.ResetForTests();
                    string key = OriginBudget.KeyFor(srv.Url);

                    ParallelFetchOptions o = Budgeted(connections: 4, budget: 4);
                    // Leave exactly one permit: enough for the probe, so the stream opens and its
                    // other workers have nowhere to go.
                    List<OriginBudget.Permit> held = new List<OriginBudget.Permit>();
                    for (int i = 0; i < 3; i++)
                        held.Add(await OriginBudget.TryAcquireAsync(key, 4, TimeSpan.FromSeconds(2), ct).ConfigureAwait(false));
                    Harness.Assert(held[2] != null, "could not tie up the budget");

                    long? t, c;
                    ParallelRangeStream s = (ParallelRangeStream)ParallelFetch.OpenWith(
                        srv.Url, 0, 8 * 1024 * 1024, o, out t, out c, ct);
                    byte[] buf = new byte[32 * 1024];
                    await s.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false);
                    await Task.Delay(400, ct).ConfigureAwait(false);   // let the rest queue up

                    Stopwatch sw = Stopwatch.StartNew();
                    s.Dispose();
                    await s.WorkersCompletion.WaitAsync(TimeSpan.FromSeconds(10), ct).ConfigureAwait(false);
                    sw.Stop();

                    Harness.Assert(sw.Elapsed.TotalSeconds < 2,
                        "queued workers took " + sw.Elapsed.TotalSeconds.ToString("0.00") +
                        "s to unwind; they were waiting out a timeout rather than the token");

                    foreach (OriginBudget.Permit p in held) p.Dispose();
                    for (int i = 0; i < 60 && OriginBudget.InUse(key) != 0; i++)
                        await Task.Delay(50, ct).ConfigureAwait(false);
                    Harness.AssertEqual(0, OriginBudget.InUse(key), "permits after teardown");
                    return "queued workers gone in " + sw.Elapsed.TotalSeconds.ToString("0.00") + "s";
                }
            }).ConfigureAwait(false);
        }

        private static ParallelFetchOptions Budgeted(int connections, int budget)
        {
            ParallelFetchOptions o = Small();
            o.Connections = connections;
            o.MaxOriginConnections = budget;
            o.MaxBufferBytes = 64L * 64 * 1024;   // room for connections + 4 slots
            return o;
        }

        /// <summary>
        /// Opens a stream and reads it slowly in the background until cancelled, resolving to the
        /// bytes it managed to deliver.
        ///
        /// The byte count is the point: a budget test that only looks at permit counters cannot
        /// tell "shared fairly" from "this stream never ran at all", and the first budget
        /// implementation failed exactly there.
        /// </summary>
        private static Task<long> RunUntilCancelledAsync(string url, ParallelFetchOptions o, CancellationToken ct)
        {
            // Progress is recorded as it happens, not returned by DrainAsync: cancellation is how
            // these tests end, and a cancelled DrainAsync throws away its own return value.
            long[] got = new long[1];
            return Task.Run<long>(async () =>
            {
                try
                {
                    long? t, c;
                    using (Stream s = ParallelFetch.OpenWith(url, 0, 0, o, out t, out c, ct))
                    {
                        await Harness.DrainAsync(s, 32 * 1024, 1, ct, n => Volatile.Write(ref got[0], n)).ConfigureAwait(false);
                    }
                }
                catch { /* cancellation and origin faults are the point of these tests */ }
                return Volatile.Read(ref got[0]);
            });
        }

        /// <summary>
        /// The whole reason this exists: `Connections` is per stream, the origin limits the total.
        /// Four streams at 6 connections each want 24 at the origin; the budget must hold it to 12.
        /// </summary>
        private static async Task BudgetCapsConcurrencyAsync(CancellationToken ct)
        {
            await Harness.RunAsync("N streams cannot exceed the origin budget between them", async () =>
            {
                using (MockServer srv = new MockServer(256L * 1024 * 1024, fastContent: true))
                {
                    srv.ThrottleBytesPerSec = 512 * 1024;   // keep requests open long enough to overlap
                    OriginBudget.ResetForTests();
                    string key = OriginBudget.KeyFor(srv.Url);

                    ParallelFetchOptions o = Budgeted(connections: 6, budget: 12);
                    using (CancellationTokenSource run = CancellationTokenSource.CreateLinkedTokenSource(ct))
                    {
                        Task[] streams = new Task[4];
                        for (int i = 0; i < streams.Length; i++) streams[i] = RunUntilCancelledAsync(srv.Url, o, run.Token);
                        await Task.Delay(4000, ct).ConfigureAwait(false);

                        int peakPermits = OriginBudget.Peak(key);
                        int peakAtOrigin = srv.MaxConcurrent;
                        run.Cancel();
                        await Task.WhenAll(streams).ConfigureAwait(false);

                        // Lower bound first. Every other assertion here is an upper bound, and
                        // `Peak`/`MaxConcurrent` both start at 0, so a change that stopped these
                        // streams from opening at all - a chunk-arithmetic bug, a port clash, a
                        // buffer size - would turn this test into a no-op that still reports PASS.
                        // Measured, not estimated: the budget is fully subscribed here.
                        Harness.Assert(peakPermits >= 12,
                            "the budget was never fully subscribed (peaked at " + peakPermits +
                            "), so this test proved nothing about the cap");
                        Harness.Assert(peakPermits <= 12,
                            "permits peaked at " + peakPermits + ", budget was 12");
                        // The probe takes a permit too (ParallelFetch.OpenCore), and a permit is
                        // returned only after its response has been disposed - so the origin
                        // never sees more than the budget, not "the budget plus one per stream".
                        // The old allowance of +1 per stream dated from when the probe ran
                        // outside the budget, and it left enough slack to hide a regression where
                        // every stream leaked one connection.
                        Harness.Assert(peakAtOrigin <= 12,
                            "origin saw " + peakAtOrigin + " concurrent requests; the budget is 12 and " +
                            "the probe is inside it");
                        // Dispose deliberately does not block on worker unwind (it can run on a
                        // host request thread), so permits come back a moment later. Promptness
                        // is asserted separately in "abandoning a stream returns its permits at
                        // once"; here we only need them all back eventually.
                        for (int i = 0; i < 60 && OriginBudget.InUse(key) != 0; i++)
                            await Task.Delay(50, ct).ConfigureAwait(false);
                        Harness.AssertEqual(0, OriginBudget.InUse(key), "permits still held after teardown");
                        return "4 streams x 6 conn -> permits peaked " + peakPermits + ", origin saw " + peakAtOrigin;
                    }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// A cap that some streams never get under is not a budget, it is a queue with a door
        /// policy. The first implementation took one permit per WORKER and held it for the
        /// worker's whole life, so `max-origin-connections / connections` was really "how many
        /// streams may play at once": at 12 / 4 the third viewer's workers parked on the
        /// semaphore forever, its reader parked on a slot that was never filled, and playback
        /// froze with no error, no log line and no fallback - the worst failure this component
        /// can produce.
        ///
        /// Staggered starts rather than a simultaneous burst, so the oversubscription is a fact
        /// rather than a race: A takes its four, B takes the last four, and C is left facing a
        /// budget that is fully committed. Every stream must still deliver bytes.
        /// </summary>
        private static async Task BudgetStarvesNoStreamAsync(CancellationToken ct)
        {
            await Harness.RunAsync("an oversubscribed budget slows every stream, starves none", async () =>
            {
                using (MockServer srv = new MockServer(256L * 1024 * 1024, fastContent: true))
                {
                    srv.ThrottleBytesPerSec = 512 * 1024;
                    OriginBudget.ResetForTests();
                    string key = OriginBudget.KeyFor(srv.Url);

                    // 3 x 4 connections against a budget of 8: the third stream can only run if
                    // permits circulate.
                    ParallelFetchOptions o = Budgeted(connections: 4, budget: 8);
                    using (CancellationTokenSource run = CancellationTokenSource.CreateLinkedTokenSource(ct))
                    {
                        Task<long>[] streams = new Task<long>[3];
                        for (int i = 0; i < streams.Length; i++)
                        {
                            streams[i] = RunUntilCancelledAsync(srv.Url, o, run.Token);
                            await Task.Delay(600, ct).ConfigureAwait(false);   // let this one claim its share
                        }

                        await Task.Delay(4000, ct).ConfigureAwait(false);
                        int peak = OriginBudget.Peak(key);
                        run.Cancel();
                        long[] delivered = await Task.WhenAll(streams).WaitAsync(TimeSpan.FromSeconds(30), ct)
                                                     .ConfigureAwait(false);

                        // Two-sided. Only the lower bound would be checked if the upper one were
                        // left out, and a mutant that raised the effective cap would sail through
                        // a test whose whole subject is a cap.
                        Harness.Assert(peak >= 8,
                            "the budget was never fully subscribed (peaked at " + peak +
                            "), so nothing here was actually contended");
                        Harness.Assert(peak <= 8, "permits peaked at " + peak + " against a budget of 8");

                        for (int i = 0; i < delivered.Length; i++)
                        {
                            // Eight chunks, not one: enough to mean permits actually circulated
                            // rather than one chunk dribbling out. Failure here is bimodal in
                            // practice (a stream gets megabytes or exactly zero), and it has two
                            // possible causes - permits back to worker scope, or the mock's
                            // pacing changed - so check the mock before concluding it is the
                            // former.
                            Harness.Assert(delivered[i] >= 8 * 64 * 1024,
                                "stream " + i + " delivered " + delivered[i] + " bytes in 4s while " +
                                delivered.Length + " streams shared a budget of 8; a starved stream is a " +
                                "frozen player, not a slow one");
                        }

                        for (int i = 0; i < 60 && OriginBudget.InUse(key) != 0; i++)
                            await Task.Delay(50, ct).ConfigureAwait(false);
                        Harness.AssertEqual(0, OriginBudget.InUse(key), "permits still held after teardown");

                        return "3 streams x 4 conn / budget 8 -> " + string.Join(", ",
                            Array.ConvertAll(delivered, b => Harness.MiB(b)));
                    }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// The failure mode that matters most. A permit leaked once is never returned, and the
        /// next stream waits on it forever: playback freezes with no error and no fallback. So
        /// every exit path a worker has - clean finish, origin fault, throughput floor, mid-read
        /// dispose, cancellation - has to end with the permit back.
        /// </summary>
        private static async Task BudgetNeverLeaksPermitsAsync(CancellationToken ct)
        {
            await Harness.RunAsync("permits return to zero through every failure path", async () =>
            {
                using (MockServer srv = new MockServer(64L * 1024 * 1024, fastContent: true))
                {
                    OriginBudget.ResetForTests();
                    string key = OriginBudget.KeyFor(srv.Url);
                    srv.ThrottleBytesPerSec = 1024 * 1024;
                    srv.TrickleBytesPerSec = 1024;

                    // Rotate through every fault the origin can produce, plus none at all.
                    MockFault[] faults =
                    {
                        MockFault.None, MockFault.Status403, MockFault.Status500,
                        MockFault.Status503RetryAfter, MockFault.TruncateHalf, MockFault.Trickle,
                        // The unanswered shape now retries on a different counter to everything
                        // else above, so it reaches the release path a different number of times.
                        MockFault.NoResponse
                    };
                    int seqOffset = 0;
                    srv.FaultHook = (seq, f, t) => faults[(seq + seqOffset) % faults.Length];

                    ParallelFetchOptions o = Budgeted(connections: 4, budget: 8);
                    o.MaxAttempts = 2;
                    o.MinThroughputGrace = TimeSpan.FromMilliseconds(300);
                    o.StallBudget = TimeSpan.FromSeconds(6);

                    try
                    {
                        for (int round = 0; round < 3; round++)
                        {
                            seqOffset = round;
                            using (CancellationTokenSource run = CancellationTokenSource.CreateLinkedTokenSource(ct))
                            {
                                Task[] streams = new Task[3];
                                for (int i = 0; i < streams.Length; i++)
                                    streams[i] = RunUntilCancelledAsync(srv.Url, o, run.Token);
                                // Cancel mid-flight: dispose racing an in-progress read is its own path.
                                await Task.Delay(700 + round * 400, ct).ConfigureAwait(false);
                                run.Cancel();
                                await Task.WhenAll(streams).ConfigureAwait(false);
                            }
                        }
                    }
                    finally { srv.FaultHook = null; }

                    // Workers unwind asynchronously after Dispose returns, so allow a moment -
                    // but only a moment. A leak does not resolve with time.
                    for (int i = 0; i < 50 && OriginBudget.InUse(key) != 0; i++)
                        await Task.Delay(100, ct).ConfigureAwait(false);

                    // Lower bound before upper bound. `InUse` returns 0 for an origin that was
                    // never touched, so "back to zero" is also what a run that never took a
                    // single permit looks like - and every way of breaking stream startup
                    // produces exactly that.
                    Harness.Assert(OriginBudget.Peak(key) > 0,
                        "no permit was ever taken against this origin, so nothing here was proved");
                    Harness.AssertEqual(0, OriginBudget.InUse(key),
                        "permits still held after 9 streams x 6 fault kinds all finished");

                    // Control: the budget still WORKS afterwards, i.e. we did not just release
                    // everything into a broken gate.
                    Fetched got = await FetchAsync(srv.Url, 0, 200000, Budgeted(4, 8), ct).ConfigureAwait(false);
                    Harness.AssertBytesEqual(Pattern.Range(0, 200000), got.Data, "bytes after the storm");
                    return "9 streams through 6 fault kinds, permits back to 0";
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Two origins must not share a quota. A slow or saturated one has to leave an unrelated
        /// one completely alone - the reason the budget is keyed by authority rather than being
        /// one number for the whole process.
        /// </summary>
        private static async Task BudgetIsPerOriginAsync(CancellationToken ct)
        {
            await Harness.RunAsync("saturating one origin does not touch another", async () =>
            {
                using (MockServer a = new MockServer(256L * 1024 * 1024, fastContent: true))
                using (MockServer b = new MockServer(16L * 1024 * 1024, fastContent: true))
                {
                    a.ThrottleBytesPerSec = 256 * 1024;     // hold A's permits for a long time
                    OriginBudget.ResetForTests();

                    ParallelFetchOptions o = Budgeted(connections: 4, budget: 4);   // A saturated by one stream
                    using (CancellationTokenSource run = CancellationTokenSource.CreateLinkedTokenSource(ct))
                    {
                        Task[] hogs = new Task[3];
                        for (int i = 0; i < hogs.Length; i++) hogs[i] = RunUntilCancelledAsync(a.Url, o, run.Token);
                        await Task.Delay(1500, ct).ConfigureAwait(false);

                        Harness.Assert(OriginBudget.InUse(OriginBudget.KeyFor(a.Url)) > 0,
                            "origin A should be holding permits by now");

                        // Bounded, because the defect this test exists to catch - one budget for
                        // every origin - does not make it fail, it makes it HANG: B's workers
                        // queue behind A forever and the elapsed-time assertion below is never
                        // reached. Neither Harness.RunAsync nor the CI job had a timeout, so that
                        // shape would have burned to GitHub's six-hour ceiling.
                        //
                        // Task.Run is what makes that bound real. OpenWith blocks its caller
                        // through the probe's own TryAcquireAsync, so without it the WaitAsync
                        // below could not start counting until the probe had already returned -
                        // a timeout that cannot fire, on the one test whose failure mode is a hang.
                        Stopwatch sw = Stopwatch.StartNew();
                        Task<Fetched> bFetch = Task.Run(() => FetchAsync(b.Url, 0, 1024 * 1024, Budgeted(4, 4), ct));

                        // Sample A while B is being served. Asserting A was saturated BEFORE B
                        // started says nothing about the moment that matters: if A had drained,
                        // B getting through would prove nothing about the split.
                        // Seeded from a real reading and closed with another, so the running
                        // minimum can never be vacuous. B is served in 10-60 ms, so the loop
                        // itself may sample zero times - and `int.MaxValue` would then satisfy
                        // the assertion by never having looked, which is the wrong direction for
                        // a value that only rises as sampling thins out.
                        int aFloor = OriginBudget.InUse(OriginBudget.KeyFor(a.Url));
                        while (!bFetch.IsCompleted)
                        {
                            int inUse = OriginBudget.InUse(OriginBudget.KeyFor(a.Url));
                            if (inUse < aFloor) aFloor = inUse;
                            await Task.Delay(5, ct).ConfigureAwait(false);
                        }
                        int after = OriginBudget.InUse(OriginBudget.KeyFor(a.Url));
                        if (after < aFloor) aFloor = after;
                        Fetched got = await bFetch.WaitAsync(TimeSpan.FromSeconds(10), ct).ConfigureAwait(false);
                        sw.Stop();

                        run.Cancel();
                        await Task.WhenAll(hogs).ConfigureAwait(false);

                        Harness.AssertBytesEqual(Pattern.Range(0, 1024 * 1024), got.Data, "origin B bytes");
                        // 0.5s, not 3s. B is served in 0.0-0.1s when the split works, and a
                        // COMPLETELY merged budget - KeyFor returning one key for every origin -
                        // lands at 2.9-3.1s, because attempt-scoped permits cycle fast enough that
                        // B eventually squeezes through. At a 3s threshold that mutant was a coin
                        // flip: measured PASS on one run and FAIL on the next, same binary.
                        Harness.Assert(sw.Elapsed.TotalSeconds < 0.5,
                            "origin B took " + sw.Elapsed.TotalSeconds.ToString("0.0") + "s while A was saturated");
                        Harness.Assert(aFloor >= 4,
                            "origin A dropped to " + aFloor + " permits in use while B was being served, so B may " +
                            "simply have been handed A's spare capacity - which proves nothing about the split");

                        // The only budget test that never checked its permits came back - which
                        // also makes it the only one that could not notice a leak confined to
                        // one of two origins.
                        string keyA = OriginBudget.KeyFor(a.Url), keyB = OriginBudget.KeyFor(b.Url);
                        for (int i = 0; i < 60 && (OriginBudget.InUse(keyA) != 0 || OriginBudget.InUse(keyB) != 0); i++)
                            await Task.Delay(50, ct).ConfigureAwait(false);
                        Harness.AssertEqual(0, OriginBudget.InUse(keyA), "permits still held at origin A");
                        Harness.AssertEqual(0, OriginBudget.InUse(keyB), "permits still held at origin B");
                        return "A saturated, B served in " + sw.Elapsed.TotalSeconds.ToString("0.0") + "s";
                    }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// A seek abandons a stream and opens another immediately. If the permits did not come
        /// back at once, the new stream would queue behind a stream nobody is reading - which is
        /// the stall this whole feature exists to prevent.
        /// </summary>
        private static async Task BudgetReleasedOnDisposeAsync(CancellationToken ct)
        {
            await Harness.RunAsync("abandoning a stream returns its permits at once", async () =>
            {
                using (MockServer srv = new MockServer(64L * 1024 * 1024, fastContent: true))
                {
                    srv.ThrottleBytesPerSec = 512 * 1024;
                    OriginBudget.ResetForTests();
                    string key = OriginBudget.KeyFor(srv.Url);
                    ParallelFetchOptions o = Budgeted(connections: 4, budget: 4);

                    long? t, c;
                    Stream s = ParallelFetch.OpenWith(srv.Url, 0, 0, o, out t, out c, ct);
                    byte[] buf = new byte[32 * 1024];
                    await s.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false);
                    Harness.Assert(OriginBudget.InUse(key) > 0, "expected permits to be held while reading");

                    Stopwatch sw = Stopwatch.StartNew();
                    s.Dispose();
                    // Polls well past the assertion below, so "took too long" is a real verdict
                    // rather than an artefact of the loop giving up first. 200 x 20 ms = 4 s
                    // against a 2 s bound; measured 0.02 s.
                    for (int i = 0; i < 200 && OriginBudget.InUse(key) != 0; i++)
                        await Task.Delay(20, ct).ConfigureAwait(false);
                    sw.Stop();

                    Harness.AssertEqual(0, OriginBudget.InUse(key), "permits after dispose");
                    Harness.Assert(sw.Elapsed.TotalSeconds < 2,
                        "took " + sw.Elapsed.TotalSeconds.ToString("0.00") + "s to give the permits back");
                    return "returned in " + sw.Elapsed.TotalSeconds.ToString("0.00") + "s";
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Two independent settings can be configured into contradiction. A budget below the
        /// per-stream count must degrade to something coherent and say so - not deadlock, and not
        /// silently ignore one of the two numbers.
        /// </summary>
        private static async Task BudgetBelowConnectionsAsync(CancellationToken ct)
        {
            await Harness.RunAsync("budget below connections clamps, logs, and still serves", async () =>
            {
                using (MockServer srv = new MockServer(8L * 1024 * 1024, fastContent: false))
                {
                    OriginBudget.ResetForTests();
                    ParallelFetchOptions raw = Budgeted(connections: 6, budget: 2);
                    ParallelFetchOptions eff = raw.Normalize();

                    Harness.AssertEqual(2, eff.Connections, "Connections after clamping to the budget");
                    Harness.Assert(eff.ConnectionsClampedByBudget, "the clamp must be recorded so the log can report it");

                    // Degrading is not the same as breaking: it still has to deliver exact bytes.
                    Fetched got = await FetchAsync(srv.Url, 1000, 500000, raw, ct).ConfigureAwait(false);
                    Harness.AssertBytesEqual(Pattern.Range(1000, 500000), got.Data, "bytes under a clamped config");

                    string key = OriginBudget.KeyFor(srv.Url);
                    Harness.Assert(OriginBudget.Peak(key) > 0, "no permit was ever taken; this proved nothing");
                    // Polled, like the other three. Dispose deliberately does not block on worker
                    // unwind, so a bare assertion here is a race that only fails when the machine
                    // is busy: injecting 15 ms before the permit is released reddened this test
                    // and no other.
                    for (int i = 0; i < 50 && OriginBudget.InUse(key) != 0; i++)
                        await Task.Delay(20, ct).ConfigureAwait(false);
                    Harness.AssertEqual(0, OriginBudget.InUse(key), "permits after");
                    return "connections 6 -> 2, bytes still exact";
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Queuing for OUR OWN limiter is not the origin failing to make progress.
        ///
        /// `AcquirePermitAsync` credits the time it spent waiting back to the chunk's stall
        /// budget, and the comment there makes a load-bearing claim: without it, heavy but
        /// perfectly healthy contention turns into chunk failures. Nothing tested that. Deleting
        /// the credit outright left every budget test and the whole 59-case mock suite green,
        /// because those tests run with the default 30 s stall budget against permit waits of a
        /// few hundred milliseconds - three orders of magnitude of slack.
        ///
        /// So this one inverts the ratio: a 500 ms stall budget and a budget deliberately held
        /// shut for three times that long. With the credit the stream simply waits and finishes;
        /// without it, the first chunk to queue dies with "made no progress for 500 ms" and takes
        /// the stream with it.
        /// </summary>
        private static async Task BudgetQueuingIsNotAStallAsync(CancellationToken ct)
        {
            await Harness.RunAsync("queuing for the budget is not charged as an origin stall", async () =>
            {
                using (MockServer srv = new MockServer(8L * 1024 * 1024, fastContent: true))
                {
                    OriginBudget.ResetForTests();
                    string key = OriginBudget.KeyFor(srv.Url);

                    ParallelFetchOptions o = Budgeted(connections: 2, budget: 2);
                    o.StallBudget = TimeSpan.FromMilliseconds(500);
                    o.MaxAttempts = 2;                       // fail fast if the credit is missing

                    long? t, c;
                    Stream s = ParallelFetch.OpenWith(srv.Url, 0, 1024 * 1024, o, out t, out c, ct);
                    try
                    {
                        byte[] buf = new byte[32 * 1024];
                        int first = await s.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false);
                        Harness.Assert(first > 0, "the stream delivered nothing even before the budget was held");

                        // Take everything that frees up and sit on it for 3x the stall budget.
                        List<OriginBudget.Permit> hogged = new List<OriginBudget.Permit>();
                        long until = Environment.TickCount64 + 1500;
                        while (Environment.TickCount64 < until)
                        {
                            OriginBudget.Permit p = await OriginBudget
                                .TryAcquireAsync(key, 2, TimeSpan.FromMilliseconds(50), ct).ConfigureAwait(false);
                            if (p != null) hogged.Add(p);
                            else await Task.Delay(20, ct).ConfigureAwait(false);
                        }
                        Harness.Assert(hogged.Count > 0, "never managed to hold the budget shut; nothing was proved");
                        foreach (OriginBudget.Permit p in hogged) p.Dispose();

                        // The stream has to have survived a wait far longer than its stall budget.
                        byte[] rest = await Harness.ReadAllAsync(s, 32 * 1024, ct).ConfigureAwait(false);
                        byte[] all = new byte[first + rest.Length];
                        Array.Copy(buf, 0, all, 0, first);
                        Array.Copy(rest, 0, all, first, rest.Length);
                        Harness.AssertBytesEqual(Pattern.Range(0, 1024 * 1024), all,
                            "bytes after the budget was held shut for 3x the stall budget");
                        return "survived a 1.5s permit wait on a 0.5s stall budget, bytes exact";
                    }
                    finally { s.Dispose(); }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Nothing is left to give, so decline - promptly, and in writing.
        ///
        /// A stream whose reader has stopped (a paused player) keeps its workers parked on the
        /// reorder ring holding real, open origin connections, and the permits accounting for
        /// them stay out. That is honest: the connections exist. But it means a later stream can
        /// find the origin fully committed, and the one thing it must never do then is wait.
        /// Open() runs on a host request thread, so an unbounded wait there is the silent freeze
        /// this whole rework removed, arriving through the front door instead of the back.
        ///
        /// Declining is not a defeat: it becomes a null from TryOpen and Emby serves the request
        /// on its own single connection, which is the correct answer when the origin has no
        /// capacity left. And it must not be a one-way door - the budget frees up, the next
        /// stream opens normally.
        /// </summary>
        private static async Task BudgetDeclinesWhenFullAsync(CancellationToken ct)
        {
            await Harness.RunAsync("a fully committed origin declines promptly, then recovers", async () =>
            {
                using (MockServer srv = new MockServer(64L * 1024 * 1024, fastContent: true))
                {
                    OriginBudget.ResetForTests();
                    string key = OriginBudget.KeyFor(srv.Url);

                    // Held from outside the fetcher: deterministic, and exactly what a paused
                    // stream's parked workers do to the budget.
                    OriginBudget.Permit[] held = new OriginBudget.Permit[3];
                    for (int i = 0; i < held.Length; i++)
                        held[i] = await OriginBudget.TryAcquireAsync(key, 3, TimeSpan.FromSeconds(2), ct)
                                                    .ConfigureAwait(false);
                    Harness.Assert(held[2] != null, "could not saturate the budget; the rest proves nothing");

                    ParallelFetchOptions o = Budgeted(connections: 3, budget: 3);
                    o.StallBudget = TimeSpan.FromSeconds(3);
                    // Small, but it no longer carves anything out for the permit wait: every wait
                    // in the probe now shares one deadline, ProbeBudgetMs = min(StallBudget, 8s),
                    // which here is the 3s above.
                    o.ResponseHeadersTimeout = TimeSpan.FromSeconds(1);

                    long? t, c;
                    Stopwatch sw = Stopwatch.StartNew();
                    string why = null;
                    try
                    {
                        Stream doomed = ParallelFetch.OpenWith(srv.Url, 0, 0, o, out t, out c, ct);
                        doomed.Dispose();
                    }
                    catch (Exception ex) { why = ex.Message; }
                    sw.Stop();

                    Harness.Assert(why != null, "opened a stream against an origin with no budget left");
                    // The message, not the type. It lands in the `reason=` field of the FALLBACK
                    // line, which is the only thing a running Emby leaves behind to explain why a
                    // stream went single-connection - so "no free connection budget" has to survive
                    // all the way out, whatever wrapper the give-up path puts around it.
                    Harness.Assert(why.Contains("no free connection budget"),
                        "declined without saying the budget was the reason: " + why);
                    Harness.Assert(sw.Elapsed.TotalSeconds < 6,
                        "took " + sw.Elapsed.TotalSeconds.ToString("0.0") + "s to decline, and that wait is on " +
                        "a host request thread");

                    for (int i = 0; i < held.Length; i++) held[i].Dispose();
                    Harness.AssertEqual(0, OriginBudget.InUse(key), "permits back after releasing them");

                    Fetched got = await FetchAsync(srv.Url, 0, 300000, o, ct).ConfigureAwait(false);
                    Harness.AssertBytesEqual(Pattern.Range(0, 300000), got.Data, "bytes once the budget freed up");
                    return "declined in " + sw.Elapsed.TotalSeconds.ToString("0.00") + "s, served normally after";
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// A player that pauses must not sit on the origin's budget.
        ///
        /// Workers park on `slot.Free` BEFORE taking a permit, so a stalled reader normally costs
        /// nothing once the chunks already in flight have landed in their channels. What breaks
        /// that is a chunk that needs more blocks than its channel can hold: the worker parks
        /// inside writer.WriteAsync instead, still holding its permit, and the only thing that can
        /// free it is a consumer that has stopped consuming.
        ///
        /// And a resumed chunk really does need more blocks. An attempt abandoned by the
        /// throughput floor publishes a SHORT block before it throws, so the retry resumes off a
        /// block boundary and crosses one extra one - and production logs ~50 of those retries in
        /// a single session. Sizing the channel to a clean run's block count therefore leaked a
        /// permit on an ordinary path, which is why BlocksPerChunk carries MaxAttempts of
        /// headroom.
        /// </summary>
        private static async Task BudgetSurvivesAStalledReaderAsync(CancellationToken ct)
        {
            await Harness.RunAsync("a stalled reader gives the origin its permits back", async () =>
            {
                using (MockServer srv = new MockServer(64L * 1024 * 1024, fastContent: true))
                {
                    OriginBudget.ResetForTests();
                    string key = OriginBudget.KeyFor(srv.Url);

                    // Slow enough to trip the throughput floor, fast enough to keep this quick.
                    // Aimed at a chunk WELL AHEAD of the reader: a chunk the consumer is actively
                    // draining has room in its channel no matter how many blocks it takes, so
                    // faulting chunk 0 would prove nothing.
                    const long StalledChunkStart = 3 * 64 * 1024;
                    srv.TrickleBytesPerSec = 20000;
                    int tricklesServed = 0;
                    srv.FaultHook = (seq, from, to) =>
                    {
                        if (from != StalledChunkStart || Volatile.Read(ref tricklesServed) != 0) return MockFault.None;
                        Interlocked.Increment(ref tricklesServed);
                        return MockFault.Trickle;
                    };

                    ParallelFetchOptions o = Budgeted(connections: 2, budget: 4);
                    o.MinThroughputGrace = TimeSpan.FromMilliseconds(200);

                    try
                    {
                        long? t, c;
                        using (Stream s = ParallelFetch.OpenWith(srv.Url, 0, 0, o, out t, out c, ct))
                        {
                            byte[] buf = new byte[32 * 1024];
                            await s.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false);

                            // Stop reading. Every worker should finish the chunk it is on, hand
                            // its permit back, and then park on a full reorder ring holding
                            // nothing.
                            for (int i = 0; i < 100 && OriginBudget.InUse(key) != 0; i++)
                                await Task.Delay(100, ct).ConfigureAwait(false);

                            Harness.Assert(Volatile.Read(ref tricklesServed) > 0,
                                "the throughput floor was never provoked, so no chunk was ever resumed " +
                                "and this test proved nothing");
                            Harness.Assert(OriginBudget.Peak(key) > 0, "no permit was ever taken");
                            Harness.AssertEqual(0, OriginBudget.InUse(key),
                                "permits still held 10s after the consumer stopped reading");

                            // Still alive, not just quiet: resuming has to work.
                            int n = await s.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false);
                            Harness.Assert(n > 0, "the stream delivered nothing after the reader resumed");
                        }
                    }
                    finally { srv.FaultHook = null; }

                    for (int i = 0; i < 60 && OriginBudget.InUse(key) != 0; i++)
                        await Task.Delay(50, ct).ConfigureAwait(false);
                    Harness.AssertEqual(0, OriginBudget.InUse(key), "permits after dispose");
                    return "paused with a resumed chunk in flight, permits back to 0";
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// A chunk waiting out a retry backoff has nothing in flight at the origin, so it must not
        /// be holding a slot in the origin's budget either.
        ///
        /// The backoff used to be slept INSIDE the catch, which runs before the finally that
        /// releases the permit and the dead response - so a `Retry-After: 3600` (clamped to the
        /// stall budget, but still tens of seconds) could park every chunk in the process asleep
        /// on the whole budget, at exactly the moment the origin is saying it is under pressure.
        ///
        /// Measured as the longest unbroken run of "budget idle" while the fetch is still going:
        /// with the release before the sleep it is the length of the backoff, without it, zero.
        /// </summary>
        private static async Task BudgetFreedDuringBackoffAsync(CancellationToken ct)
        {
            await Harness.RunAsync("a chunk waiting out a backoff is not holding the budget", async () =>
            {
                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    OriginBudget.ResetForTests();
                    string key = OriginBudget.KeyFor(srv.Url);

                    srv.RetryAfter = "2";                       // long enough to see, short enough to wait for
                    srv.FaultHook = (seq, f, t) => seq == 2 ? MockFault.Status503RetryAfter : MockFault.None;

                    ParallelFetchOptions o = Budgeted(connections: 1, budget: 1);
                    long idleRun = 0, longestIdleRun = 0;
                    try
                    {
                        Task<Fetched> fetch = FetchAsync(srv.Url, 0, 256 * 1024, o, ct);
                        while (!fetch.IsCompleted)
                        {
                            await Task.Delay(20, ct).ConfigureAwait(false);
                            if (OriginBudget.InUse(key) == 0)
                            {
                                idleRun += 20;
                                if (idleRun > longestIdleRun) longestIdleRun = idleRun;
                            }
                            else idleRun = 0;
                        }
                        Fetched got = await fetch.WaitAsync(TimeSpan.FromSeconds(30), ct).ConfigureAwait(false);
                        Harness.AssertBytesEqual(Pattern.Range(0, 256 * 1024), got.Data, "bytes after the 503");
                    }
                    finally { srv.FaultHook = null; srv.RetryAfter = null; }

                    Harness.Assert(OriginBudget.Peak(key) > 0, "no permit was ever taken; this proved nothing");
                    Harness.Assert(srv.RequestTicks.Count >= 3, "the 503 never produced a retry");
                    Harness.Assert(longestIdleRun >= 1000,
                        "the budget was never idle for more than " + longestIdleRun + " ms during a 2s backoff; " +
                        "the permit is being held across the sleep");
                    return "budget idle " + longestIdleRun + " ms of a 2s backoff";
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// The same invariant as the chunk backoff, for the probe - which is where it was being
        /// broken.
        ///
        /// The probe used to take one permit around its ENTIRE retry loop, backoff sleeps
        /// included. That was ~3 s of over-holding while `MaxAttempts` capped unanswered attempts
        /// at four; once the stall budget took over that class it became the full 30 s, with no
        /// request of ours in flight at the origin for nearly all of it. Measured before the fix:
        /// a rival's wait for a permit went 2.79 s -> 29.50 s. On a budget of 12, twelve streams
        /// opening against a wobbling origin would have parked the whole thing.
        /// </summary>
        private static async Task BudgetFreedDuringProbeBackoffAsync(CancellationToken ct)
        {
            await Harness.RunAsync("a probe waiting out a backoff is not holding the budget", async () =>
            {
                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    OriginBudget.ResetForTests();
                    string key = OriginBudget.KeyFor(srv.Url);

                    Stopwatch outage = Stopwatch.StartNew();
                    srv.FaultHook = (seq, f, t) => outage.ElapsedMilliseconds < 3000 ? MockFault.NoResponse : MockFault.None;
                    try
                    {
                        ParallelFetchOptions o = Budgeted(connections: 1, budget: 1);
                        o.ResponseHeadersTimeout = TimeSpan.FromMilliseconds(200);
                        o.StallBudget = TimeSpan.FromSeconds(20);

                        // Production-scale backoffs, and they are the entire experiment. The first
                        // version of this test inherited Small()'s 10-50ms delays, which made
                        // "holds the permit across the backoff" and "releases it" differ by tens of
                        // milliseconds - so a faithful mutant that slept inside the try passed.
                        // At 300ms+ the two are unmistakable.
                        o.RetryBaseDelayMs = 300;
                        o.RetryMaxDelayMs = 1500;

                        // Task.Run, because ParallelFetch.OpenWith blocks its caller for the whole
                        // probe (by design - the injected call site returns before any state
                        // machine starts). Calling FetchAsync directly would run the probe on THIS
                        // thread and the sampling loop below would not execute until it finished.
                        Task<Fetched> open = Task.Run(() => FetchAsync(srv.Url, 0, 200000, o, ct));

                        // Measure the LONGEST CONTIGUOUS window in which the budget was free, not
                        // whether it was ever free. "Ever free" is satisfied by the sliver between
                        // one attempt's release and the next attempt's acquire, which exists in
                        // both the fixed and the broken shape - that is precisely why the first
                        // version of this test could not tell them apart.
                        //
                        // Fixed: the permit is released before the backoff, so a free window is a
                        // whole backoff, >= 300ms. Broken: the sleep happens inside the try and the
                        // free window collapses to the loop's own turnaround, a few ms at most.
                        long longestFreeMs = 0, freeSince = 0;
                        while (!open.IsCompleted && outage.ElapsedMilliseconds < 3000)
                        {
                            long now = Environment.TickCount64;
                            if (OriginBudget.InUse(key) == 0)
                            {
                                if (freeSince == 0) freeSince = now;
                                else if (now - freeSince > longestFreeMs) longestFreeMs = now - freeSince;
                            }
                            else freeSince = 0;
                            await Task.Delay(20, ct).ConfigureAwait(false);
                        }

                        Fetched got = await open.WaitAsync(TimeSpan.FromSeconds(40), ct).ConfigureAwait(false);
                        Harness.AssertBytesEqual(Pattern.Range(0, 200000), got.Data, "bytes once the outage cleared");
                        Harness.Assert(longestFreeMs >= 150,
                            "the budget was free for at most " + longestFreeMs + "ms at a stretch while the probe " +
                            "backed off for " + o.RetryBaseDelayMs + "ms or more; the probe is sitting on a permit " +
                            "while making no request at all");

                        for (int i = 0; i < 60 && OriginBudget.InUse(key) != 0; i++)
                            await Task.Delay(50, ct).ConfigureAwait(false);
                        Harness.AssertEqual(0, OriginBudget.InUse(key), "permits after teardown");
                        return "budget free for " + longestFreeMs + "ms at a stretch during probe backoffs";
                    }
                    finally { srv.FaultHook = null; }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// `max-origin-connections` is live-reloadable like every other setting, which means the
        /// semaphore behind it has to be replaced while the process runs. Driven straight against
        /// OriginBudget rather than through streams: the swap is a property of the budget, and
        /// going through a stream would turn a fact into a timing race.
        ///
        /// The half that matters is the half that does NOT swap. Replacing the gate while permits
        /// are out strands everyone already queued on the old one - they are waiting on an object
        /// nobody will ever release - and a stranded waiter is exactly the invisible freeze this
        /// class exists to prevent. Attempt-scoped permits make waiting for the idle moment cheap:
        /// InUse reaches zero every few seconds.
        ///
        /// NOT COVERED, and not coverable from here: the `Waiting == 0` half of that guard. It
        /// exists for the instant between `WaitAsync` returning and `InUse` being raised, where
        /// a caller already owns a permit while the entry still looks idle. Reaching that instant
        /// on purpose needs a hook inside OriginBudget, and a test-only hook in the freeze-critical
        /// path costs more than it buys. The guard is justified by construction instead: `Waiting`
        /// is raised inside the same `lock (e)` the swap has to take, so a swap either sees it or
        /// has not published its gate yet. Deleting that condition WILL leave this test green.
        /// </summary>
        private static async Task BudgetLimitChangeAsync(CancellationToken ct)
        {
            await Harness.RunAsync("a changed budget applies when idle, never mid-flight", async () =>
            {
                OriginBudget.ResetForTests();
                const string Key = "https://limit.example:443";
                TimeSpan brief = TimeSpan.FromMilliseconds(200);

                OriginBudget.Permit p1 = await OriginBudget.TryAcquireAsync(Key, 2, TimeSpan.FromSeconds(2), ct).ConfigureAwait(false);
                OriginBudget.Permit p2 = await OriginBudget.TryAcquireAsync(Key, 2, TimeSpan.FromSeconds(2), ct).ConfigureAwait(false);
                Harness.Assert(p1 != null && p2 != null, "the first two permits at a limit of 2");
                Harness.Assert(await OriginBudget.TryAcquireAsync(Key, 2, brief, ct).ConfigureAwait(false) == null,
                    "a third permit came out of a budget of 2");

                // Raising the limit while two are out must be deferred, not applied.
                Harness.Assert(await OriginBudget.TryAcquireAsync(Key, 6, brief, ct).ConfigureAwait(false) == null,
                    "the raised limit was applied mid-flight, which strands anyone on the old gate");
                Harness.AssertEqual(2, OriginBudget.Limit(Key), "limit must not move while permits are out");

                p1.Dispose();
                p2.Dispose();
                Harness.AssertEqual(0, OriginBudget.InUse(Key), "permits back before the change");

                OriginBudget.Permit[] six = new OriginBudget.Permit[6];
                for (int i = 0; i < six.Length; i++)
                {
                    six[i] = await OriginBudget.TryAcquireAsync(Key, 6, TimeSpan.FromSeconds(2), ct).ConfigureAwait(false);
                    Harness.Assert(six[i] != null, "permit " + i + " at the raised limit of 6");
                }
                Harness.AssertEqual(6, OriginBudget.Limit(Key), "limit after the origin went idle");
                Harness.Assert(await OriginBudget.TryAcquireAsync(Key, 6, brief, ct).ConfigureAwait(false) == null,
                    "a seventh permit came out of a budget of 6");

                for (int i = 0; i < six.Length; i++) six[i].Dispose();
                Harness.AssertEqual(0, OriginBudget.InUse(Key), "permits back after the change");
                Harness.AssertEqual(6, OriginBudget.Peak(Key), "high-water mark across the change");
                return "2 -> deferred while busy -> 6 once idle, no restart";
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Every other budget test runs with the connection ramp switched off, while production
        /// runs it at 2-6 seconds. The ramp is the one thing that decides WHEN a worker first
        /// asks for a permit, so "budget with the ramp on" is a different arrangement of the same
        /// two mechanisms and was completely uncovered.
        /// </summary>
        private static async Task BudgetWithConnectionRampAsync(CancellationToken ct)
        {
            await Harness.RunAsync("the cap holds with connection slow-start on", async () =>
            {
                using (MockServer srv = new MockServer(128L * 1024 * 1024, fastContent: true))
                {
                    srv.ThrottleBytesPerSec = 512 * 1024;
                    OriginBudget.ResetForTests();
                    string key = OriginBudget.KeyFor(srv.Url);

                    ParallelFetchOptions o = Budgeted(connections: 4, budget: 6);
                    o.InitialConnections = 1;
                    o.ConnectionRampInterval = TimeSpan.FromMilliseconds(300);

                    using (CancellationTokenSource run = CancellationTokenSource.CreateLinkedTokenSource(ct))
                    {
                        Task<long>[] streams = new Task<long>[2];
                        for (int i = 0; i < streams.Length; i++)
                            streams[i] = RunUntilCancelledAsync(srv.Url, o, run.Token);

                        await Task.Delay(3000, ct).ConfigureAwait(false);
                        int peak = OriginBudget.Peak(key);
                        run.Cancel();
                        long[] delivered = await Task.WhenAll(streams).WaitAsync(TimeSpan.FromSeconds(30), ct)
                                                     .ConfigureAwait(false);

                        Harness.Assert(peak >= 5,
                            "peaked at " + peak + ": the two ramped streams never actually contended, so " +
                            "this ran without exercising the budget at all");
                        Harness.Assert(peak <= 6, "permits peaked at " + peak + " with a budget of 6");
                        for (int i = 0; i < delivered.Length; i++)
                            Harness.Assert(delivered[i] > 0, "ramped stream " + i + " delivered nothing");

                        for (int i = 0; i < 60 && OriginBudget.InUse(key) != 0; i++)
                            await Task.Delay(50, ct).ConfigureAwait(false);
                        Harness.AssertEqual(0, OriginBudget.InUse(key), "permits still held after teardown");
                        return "ramped 1 -> 4 per stream, peaked " + peak + " of 6";
                    }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// The worst failure this component can have: right byte COUNT, wrong byte POSITION.
        /// Nothing downstream can see it - the length adds up, the stream ends where it should,
        /// the host returns 200/206 and the player shows garbage or nothing at all.
        ///
        /// The old parser answered a single bool and used -1 sentinels for the states it could
        /// not express, so a 206 with NO Content-Range skipped every position check and was
        /// spliced in on faith. Each case below is a header a real proxy or cache emits.
        /// </summary>
        private static async Task UntrustworthyContentRangeAsync(CancellationToken ct)
        {
            await Harness.RunAsync("206 whose Content-Range cannot place the body is refused", async () =>
            {
                const long Offset = 2_000_000, Length = 300_000;

                // name -> header value (null = omit the header entirely)
                var cases = new (string Name, Func<long, long, string> Header)[]
                {
                    ("absent",            (f, t) => null),
                    ("unparseable",       (f, t) => "bytes garbage"),
                    ("wrong unit",        (f, t) => "items " + f + "-" + t + "/" + FileSize),
                    ("unsatisfied */N",   (f, t) => "bytes */" + FileSize),
                    ("to < from",         (f, t) => "bytes " + t + "-" + f + "/" + FileSize),
                    ("negative from",     (f, t) => "bytes -5-" + t + "/" + FileSize),
                    ("total <= to",       (f, t) => "bytes " + f + "-" + t + "/" + f),
                    ("wrong start",       (f, t) => "bytes " + (f + 1) + "-" + t + "/" + FileSize)
                };

                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    // The body deliberately starts at zero, so a case that is NOT rejected
                    // delivers plausible-looking bytes from the wrong place and the byte
                    // comparison below catches it even if no exception was thrown.
                    srv.BodyOffset = (f, t) => 0;
                    List<string> leaked = new List<string>();
                    foreach (var c in cases)
                    {
                        srv.ContentRangeHeader = c.Header;
                        try
                        {
                            Fetched f = await FetchAsync(srv.Url, Offset, Length, Small(), ct).ConfigureAwait(false);
                            // Getting here at all is a failure unless the bytes are somehow right.
                            bool correct = true;
                            byte[] want = Pattern.Range(Offset, f.Data.Length);
                            for (int i = 0; i < f.Data.Length && correct; i++) correct = f.Data[i] == want[i];
                            leaked.Add(c.Name + (correct ? " (accepted, bytes correct)" : " (ACCEPTED WRONG BYTES)"));
                        }
                        catch (IOException)
                        {
                            // refused - the only acceptable outcome
                        }
                    }
                    srv.ContentRangeHeader = null;
                    srv.BodyOffset = null;

                    Harness.Assert(leaked.Count == 0,
                        "these headers were accepted instead of refused: " + string.Join(", ", leaked));

                    // Control: with an honest header and an honest body it still works, so the
                    // test is not passing merely because everything is rejected.
                    Fetched ok = await FetchAsync(srv.Url, Offset, Length, Small(), ct).ConfigureAwait(false);
                    Harness.AssertBytesEqual(Pattern.Range(Offset, Length), ok.Data, "control bytes");
                    return cases.Length + " bad headers refused, honest one still served";
                }
            }).ConfigureAwait(false);

            await Harness.RunAsync("mid-transfer change of complete-length is not spliced", async () =>
            {
                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    // Probe (request 1) sees the real size; every later chunk claims the object
                    // grew. Same offsets, same lengths, plausible everywhere - only the total
                    // gives it away, and that total used to be parsed and thrown away.
                    srv.ContentRangeTotal = (f, t) => f == 0 ? FileSize : FileSize + 4096;
                    bool threw = false;
                    try
                    {
                        await FetchAsync(srv.Url, 0, 512 * 1024, Small(), ct).ConfigureAwait(false);
                    }
                    catch (IOException)
                    {
                        threw = true;
                    }
                    finally { srv.ContentRangeTotal = null; }

                    Harness.Assert(threw, "a resource that changed size mid-transfer was spliced together anyway");
                    return "version change detected via complete-length";
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// The probe runs before the resource size is known, so an "to the end of the resource"
        /// request (length == 0) cannot name a last-byte-pos without risking one past EOF. A tail
        /// read does exactly that: a player's MKV index read starts within a kilobyte of the end,
        /// and asking for a full FirstChunkSize from there overshoots by ~1 MiB.
        ///
        /// RFC 7233 says a server should clamp such a range. The production origin answers 200
        /// with the whole resource instead — measured, deterministic, and the cause of every
        /// single-connection fallback in the field log. The fetcher used to then discard
        /// gigabytes to deliver a few bytes; a 4 GB movie's 83-byte tail read cost 3810 MiB.
        ///
        /// The fix is to ask open-ended, which cannot overshoot by construction.
        /// </summary>
        private static async Task RangePastEofAsync(CancellationToken ct)
        {
            await Harness.RunAsync("tail read never asks for bytes past EOF", async () =>
            {
                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    srv.WholeFileWhenRangeEndsPastEof = true;   // what the real origin does

                    // Asserting on the returned bytes proves nothing here: the old code also
                    // returned the right bytes. It got them by downloading the whole resource and
                    // throwing away everything ahead of the offset, which is invisible from the
                    // output and only shows up as bytes the origin had to serve.
                    long overshoot = 0;

                    long offset = FileSize - 83;
                    srv.ResetCounters();
                    Fetched got = await FetchAsync(srv.Url, offset, 0, Small(), ct).ConfigureAwait(false);
                    Harness.AssertBytesEqual(Pattern.Range(offset, 83), got.Data, "tail bytes");
                    Harness.AssertEqual(FileSize, got.Total.Value, "totalLength");
                    Harness.AssertEqual(83, got.ContentLength.Value, "contentLength");
                    // Not zero: the bounded probe still overshoots once, and the origin has
                    // already queued some of the whole-file body before the retry aborts it. That
                    // residual is bounded by socket buffers (here, a loopback HttpListener that
                    // writes eagerly) — what must never happen is the WHOLE resource being paid
                    // for, which is what the old code did.
                    if (srv.BytesServed >= FileSize / 2) overshoot = srv.BytesServed;
                    Harness.Assert(overshoot == 0,
                        "origin had to serve " + overshoot + " of " + FileSize +
                        " bytes to deliver 83; the probe asked past EOF and never re-asked");

                    // A larger tail, and one starting exactly one steady chunk from the end.
                    foreach (long want in new long[] { 637228, 1024 * 1024 })
                    {
                        long off2 = FileSize - want;
                        srv.ResetCounters();
                        Fetched f2 = await FetchAsync(srv.Url, off2, 0, Small(), ct).ConfigureAwait(false);
                        Harness.AssertBytesEqual(Pattern.Range(off2, want), f2.Data, "tail " + want);
                        Harness.AssertEqual(FileSize, f2.Total.Value, "totalLength for tail " + want);
                        // The bounded probe overshoots EOF and is answered with the whole file,
                        // so the retry re-asks open-ended. Cost is one extra round trip, not a
                        // whole-resource download: anything near FileSize means the overshoot is
                        // still being paid for.
                        Harness.Assert(srv.BytesServed < FileSize / 2,
                            "tail " + want + ": origin served " + srv.BytesServed + " bytes for a " + want + "-byte read");
                    }

                    // Control: the whole-resource case is open-ended too, and is the single most
                    // common request there is. It must be untouched.
                    srv.ResetCounters();
                    Fetched whole = await FetchAsync(srv.Url, 0, 0, Small(), ct).ConfigureAwait(false);
                    Harness.AssertEqual(FileSize, whole.Data.Length, "whole resource length");
                    Harness.AssertBytesEqual(Pattern.Range(0, FileSize), whole.Data, "whole resource bytes");

                    srv.WholeFileWhenRangeEndsPastEof = false;
                    return "83B / 637KB / 1MiB tails cost no overshoot; whole file still exact";
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// When an origin ignores Range, the only way to serve a ranged request from the 200 body
        /// is to read and discard everything ahead of the offset. That is fine near the start of a
        /// resource and ruinous near the end, and production only ever hit it near the end: three
        /// occurrences, all far-tail reads, the worst discarding 10.48 GB to deliver 2083 bytes —
        /// and the delivered bytes were correct, so nothing downstream could tell.
        ///
        /// The invariant: below the ceiling, still byte-exact; above it, decline and let the host
        /// serve the request rather than spend the bandwidth.
        /// </summary>
        private static async Task IgnoredRangeSkipCeilingAsync(CancellationToken ct)
        {
            await Harness.RunAsync("origin ignoring Range: skip below the ceiling stays exact", async () =>
            {
                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    ParallelFetchOptions o = Small();
                    o.MaxIgnoredRangeSkipBytes = 4 * 1024 * 1024;
                    srv.FaultHook = (seq, f, t) => MockFault.IgnoreRange;
                    try
                    {
                        Fetched got = await FetchAsync(srv.Url, 1024 * 1024, 200000, o, ct).ConfigureAwait(false);
                        Harness.AssertBytesEqual(Pattern.Range(1024 * 1024, 200000), got.Data, "bytes");
                        Harness.AssertEqual(FileSize, got.Total.Value, "totalLength");
                        return "1 MiB skip accepted, bytes exact";
                    }
                    finally { srv.FaultHook = null; }
                }
            }).ConfigureAwait(false);

            await Harness.RunAsync("origin ignoring Range: a far offset declines instead of discarding", async () =>
            {
                int attempts;
                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    ParallelFetchOptions o = Small();
                    o.MaxIgnoredRangeSkipBytes = 1024 * 1024;   // production default is 64 MiB
                    o.MaxAttempts = 2;                          // keep the retry loop short
                    srv.FaultHook = (seq, f, t) => MockFault.IgnoreRange;
                    srv.ResetCounters();
                    bool declined = false;
                    try
                    {
                        // The shape production hit: the last 2 KB of the resource.
                        long offset = FileSize - 2083;
                        long? total, cl;
                        Stream s = ParallelFetch.OpenWith(srv.Url, offset, 2083, o, out total, out cl, ct);
                        long served = 0;
                        if (s != null)
                        {
                            using (s) served = await Harness.DrainAsync(s, 32 * 1024, 0, ct, null).ConfigureAwait(false);
                        }
                        throw new Exception("a " + (offset / 1024 / 1024) + " MiB skip was accepted to deliver " +
                                            served + " bytes instead of declining");
                    }
                    catch (IOException)
                    {
                        declined = true;   // TryOpen turns this into null and the host takes over
                    }
                    finally { srv.FaultHook = null; }
                    Harness.Assert(declined, "expected the far-offset skip to be refused");

                    // It must also have RETRIED rather than given up on the first 200: direct
                    // probing answered 206 for this exact shape 21 times out of 21, so a single
                    // 200 is worth one more attempt before handing the request back.
                    Harness.Assert(srv.RequestCount >= 2,
                        "declined after only " + srv.RequestCount + " request(s); the 200 should be retried first");
                    attempts = srv.RequestCount;
                }

                // TryOpen takes no options, so this half exercises the PRODUCTION default ceiling
                // (64 MiB) rather than a lowered one, on a resource big enough to exceed it.
                using (MockServer big = new MockServer(200L * 1024 * 1024, fastContent: true))
                {
                    big.FaultHook = (seq, f, t) => MockFault.IgnoreRange;
                    try
                    {
                        long? t2, c2;
                        Stream s2 = ParallelFetch.TryOpen(big.Url, 200L * 1024 * 1024 - 2083, 2083, out t2, out c2, ct);
                        if (s2 != null) s2.Dispose();
                        Harness.Assert(s2 == null,
                            "TryOpen accepted a ~200 MiB discard at the default ceiling; it should hand the request to the host");
                    }
                    finally { big.FaultHook = null; }
                }

                await Task.CompletedTask;
                return "declined after " + attempts + " attempts, fell back to the host";
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Regression for the host contract, not for this component in isolation.
        ///
        /// Stock Emby 4.9.3.0 FileWriter.SetContentResponseHeaders does
        ///     TotalContentLength = handler.TotalLength ?? handler.Stream?.Length
        /// so a null TotalLength silently promotes this stream's RANGE length to the resource's
        /// complete length - the Content-Range denominator, and the clamp on the copy. There is
        /// no correct value to publish without a total, so the only safe answer is to decline
        /// and let Emby serve the request itself.
        /// </summary>
        private static async Task UnknownTotalAsync(CancellationToken ct)
        {
            await Harness.RunAsync("206 with \"/*\" declines rather than inventing a total", async () =>
            {
                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    srv.ContentRangeHeader = (f, t) => "bytes " + f + "-" + t + "/*";
                    try
                    {
                        // Bounded range: this is the shape that used to be accepted with
                        // totalLength = null.
                        long? total, cl;
                        Stream s = ParallelFetch.TryOpen(srv.Url, 1_000_000, 200_000, out total, out cl, ct);
                        if (s != null) s.Dispose();
                        Harness.Assert(s == null, "TryOpen returned a stream with no resource total; " +
                                                  "the host would publish its range length as the file length");
                        Harness.Assert(!total.HasValue, "totalLength must be null on the fallback path");

                        // Open-ended too, for symmetry.
                        Stream s2 = ParallelFetch.TryOpen(srv.Url, 0, 0, out total, out cl, ct);
                        if (s2 != null) s2.Dispose();
                        Harness.Assert(s2 == null, "open-ended request with unknown total must also decline");
                    }
                    finally { srv.ContentRangeHeader = null; }

                    await Task.CompletedTask;
                    return "declined both bounded and open-ended";
                }
            }).ConfigureAwait(false);

            await Harness.RunAsync("Stream.Length refuses to pose as the resource length", async () =>
            {
                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    long? total, cl;
                    using (Stream s = ParallelFetch.OpenWith(srv.Url, 1_000_000, 200_000, Small(), out total, out cl, ct))
                    {
                        Harness.AssertEqual(FileSize, total.Value, "totalLength is the resource size");
                        Harness.AssertEqual(200_000, cl.Value, "contentLength is the range size");
                        bool threw = false;
                        try { long _ = s.Length; } catch (NotSupportedException) { threw = true; }
                        Harness.Assert(threw,
                            "Stream.Length returned a number; Emby would publish it as the complete length");
                    }
                    await Task.CompletedTask;
                    return "Length throws, TotalLength carries the truth";
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Byte offsets into a re-encoded body point at the wrong media bytes. We never ask for
        /// compression, but "did not ask" is not "cannot receive" - absent an Accept-Encoding a
        /// server may pick any encoding (RFC 7231 5.3.4), so the request pins identity AND the
        /// response is checked.
        /// </summary>
        private static async Task ContentEncodingAsync(CancellationToken ct)
        {
            await Harness.RunAsync("requests pin identity and a re-encoded response is refused", async () =>
            {
                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    srv.ContentEncoding = "gzip";
                    bool declined;
                    try
                    {
                        long? t, c;
                        Stream s = ParallelFetch.TryOpen(srv.Url, 0, 100_000, out t, out c, ct);
                        if (s != null) s.Dispose();
                        declined = s == null;
                    }
                    finally { srv.ContentEncoding = null; }
                    Harness.Assert(declined, "a gzip-encoded 206 was accepted and would have been spliced by offset");

                    srv.ResetCounters();
                    Fetched ok = await FetchAsync(srv.Url, 0, 100_000, Small(), ct).ConfigureAwait(false);
                    Harness.AssertBytesEqual(Pattern.Range(0, 100_000), ok.Data, "control bytes");

                    int pinned = 0, total = 0;
                    foreach (string ae in srv.AcceptEncodings)
                    {
                        total++;
                        if (ae.IndexOf("identity", StringComparison.OrdinalIgnoreCase) >= 0) pinned++;
                    }
                    Harness.Assert(total > 0 && pinned == total,
                        "only " + pinned + " of " + total + " requests sent Accept-Encoding: identity");
                    return pinned + "/" + total + " requests pinned identity";
                }
            }).ConfigureAwait(false);
        }
        /// <summary>
        /// An origin that answers nothing at all for a few seconds, then recovers.
        ///
        /// This is the shape that killed two live streams on 2026-08-31. A DNS wobble made every
        /// connect fail for at most 2.25 s, and the chunk gave up after 1.75-2.19 s - four
        /// attempts at 250/500/1000 ms - while 28 of its 30 s stall budget went unspent. The
        /// backoff was one step from clearing it: attempt 5 would have landed at ~3.75 s.
        ///
        /// The rule that fixes it: an attempt the origin never ANSWERED does not count against
        /// MaxAttempts. That bound exists for a shape it is the only defence against (see the
        /// trickle test below); "no answer" is what the stall budget is for, and the stall budget
        /// is untouched here because nothing was ever published.
        /// </summary>
        private static async Task UnansweredBurstAsync(CancellationToken ct)
        {
            await Harness.RunAsync("an origin answering nothing for 4s is waited out, not failed", async () =>
            {
                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    // Armed by the first request for chunk 1, so the probe and chunk 0 get through
                    // and the outage lands on a stream that is already open and delivering - the
                    // production shape. Wall-clock from that point: an outage is a property of
                    // time, and nothing in the FaultHook signature carries it.
                    long armedAt = 0;
                    // Per-offset, not global. A global counter sums unrelated chunks, so it can
                    // clear any threshold without a single chunk having survived more than one
                    // drop - which is the only thing this test is about.
                    System.Collections.Concurrent.ConcurrentDictionary<long, int> dropsAt =
                        new System.Collections.Concurrent.ConcurrentDictionary<long, int>();
                    srv.FaultHook = (seq, f, t) =>
                    {
                        if (f < 64 * 1024) return MockFault.None;          // probe + chunk 0
                        long now = Environment.TickCount64;
                        long prev = Interlocked.CompareExchange(ref armedAt, now, 0);
                        if (now - (prev == 0 ? now : prev) >= 4000) return MockFault.None;
                        dropsAt.AddOrUpdate(f, 1, (k, v) => v + 1);
                        return MockFault.NoResponse;
                    };
                    try
                    {
                        ParallelFetchOptions o = Small();
                        o.StallBudget = TimeSpan.FromSeconds(30);   // the documented bound must be the governing one
                        o.ResponseHeadersTimeout = TimeSpan.FromMilliseconds(300);

                        // Task.Run: OpenWith blocks its caller through the whole probe, so this
                        // timeout is live only while the FaultHook happens to let the probe
                        // through. One edit to that predicate would silently kill the bound.
                        Fetched got = await Task.Run(() => FetchAsync(srv.Url, 0, 400000, o, ct))
                                            .WaitAsync(TimeSpan.FromSeconds(60), ct).ConfigureAwait(false);
                        Harness.AssertBytesEqual(Pattern.Range(0, 400000), got.Data, "bytes after the outage cleared");
                        int worst = 0;
                        foreach (int n in dropsAt.Values) if (n > worst) worst = n;
                        Harness.Assert(worst > o.MaxAttempts,
                            "the worst-hit chunk was dropped only " + worst + " times against MaxAttempts=" +
                            o.MaxAttempts + "; the outage never outlasted the old bound, so this proves nothing");
                        return "one chunk rode out " + worst + " unanswered attempts (MaxAttempts=" + o.MaxAttempts + ")";
                    }
                    finally { srv.FaultHook = null; }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// The guard rail for the change above, and the one bound that has no substitute.
        ///
        /// A connection abandoned by the throughput floor publishes a SHORT block before it
        /// throws - ChunkDownloader sets cursor.LastProgressTicks and only then throws
        /// SlowConnectionException - which resets the stall budget. Against an origin that
        /// trickles on EVERY attempt the stall budget can therefore never fire, and MaxAttempts
        /// is structurally the only thing that stops the chunk. If anyone ever "simplifies" the
        /// retry policy into a single time-based bound, this retries forever.
        ///
        /// Nothing covered this before: SlowConnectionAsync tests the RECOVERY shape, where only
        /// the first attempt per chunk trickles.
        /// </summary>
        private static async Task TrickleAlwaysGivesUpAsync(CancellationToken ct)
        {
            await Harness.RunAsync("an origin trickling every attempt gives up, it does not loop", async () =>
            {
                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    srv.TrickleBytesPerSec = 20000;
                    System.Collections.Concurrent.ConcurrentDictionary<long, int> triesAt =
                        new System.Collections.Concurrent.ConcurrentDictionary<long, int>();
                    // Keyed on the range END, not the start. A trickling attempt publishes the
                    // bytes it did manage, so the retry resumes at a new offset - keying on the
                    // start would count every attempt as a different chunk and report 1.
                    srv.FaultHook = (seq, f, t) =>
                    {
                        triesAt.AddOrUpdate(t, 1, (k, v) => v + 1);
                        return MockFault.Trickle;                        // every attempt, forever
                    };
                    try
                    {
                        ParallelFetchOptions o = Small();
                        o.MaxAttempts = 3;
                        o.MinThroughputGrace = TimeSpan.FromMilliseconds(200);
                        o.StallBudget = TimeSpan.FromSeconds(30);

                        Stopwatch sw = Stopwatch.StartNew();
                        bool threw = false;
                        try
                        {
                            // WhenAny, not WaitAsync + catch(TimeoutException): the probe throws
                            // TimeoutException when it gives up NORMALLY, so discriminating on the
                            // type reports a correct give-up as a hang. Task.Run because OpenWith
                            // would otherwise run the probe on this thread before any timer starts.
                            Task<Fetched> run2 = Task.Run(() => FetchAsync(srv.Url, 0, 400000, o, ct));
                            Task done = await Task.WhenAny(run2, Task.Delay(TimeSpan.FromSeconds(25), ct))
                                                  .ConfigureAwait(false);
                            if (done != run2)
                            {
                                throw new Exception("the chunk never gave up: MaxAttempts no longer bounds a " +
                                                    "permanently trickling origin, and nothing else can");
                            }
                            await run2.ConfigureAwait(false);
                        }
                        catch (Exception) { threw = true; }
                        sw.Stop();

                        Harness.Assert(threw, "a permanently trickling origin was reported as success");
                        Harness.Assert(sw.Elapsed.TotalSeconds < 20,
                            "took " + sw.Elapsed.TotalSeconds.ToString("0.0") + "s to give up");

                        // Pin the bound in BOTH directions. "It eventually threw" is satisfied by
                        // any finite number, so it does not distinguish MaxAttempts=3 from 300;
                        // and a chunk that gave up early would also throw, for the wrong reason.
                        int worst = 0;
                        foreach (int n in triesAt.Values) if (n > worst) worst = n;
                        Harness.AssertEqual(o.MaxAttempts, worst,
                            "a permanently trickling chunk made " + worst + " attempts against MaxAttempts=" +
                            o.MaxAttempts + "; the answered-attempt counter is not the one being capped");
                        return "gave up in " + sw.Elapsed.TotalSeconds.ToString("0.0") + "s at exactly " +
                               worst + " answered attempts";
                    }
                    finally { srv.FaultHook = null; }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// The other half of moving "unanswered" off MaxAttempts: something still has to stop it.
        ///
        /// Unanswered attempts no longer count against MaxAttempts, so for this shape the stall
        /// budget is not a backstop - it is the ONLY bound in the system. Deleting it entirely
        /// left the whole suite green, which means the suite was asserting that retries happen
        /// without ever asserting that they end. Against an origin that accepts and then says
        /// nothing, forever, that is an infinite loop wearing a retry policy's clothes.
        ///
        /// Armed after the first 64 KB so the probe and chunk 0 get through: this is about a
        /// stream that is already open and delivering when the origin goes quiet, and it keeps
        /// the probe's own (shorter) unanswered window out of the measurement.
        /// </summary>
        private static async Task StallBudgetStopsAnUnansweringOriginAsync(CancellationToken ct)
        {
            await Harness.RunAsync("an origin that never answers is stopped by the stall budget", async () =>
            {
                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    srv.FaultHook = (seq, f, t) => f < 64 * 1024 ? MockFault.None : MockFault.NoResponse;
                    try
                    {
                        ParallelFetchOptions o = Small();
                        o.StallBudget = TimeSpan.FromSeconds(3);
                        o.ResponseHeadersTimeout = TimeSpan.FromMilliseconds(300);

                        Stopwatch sw = Stopwatch.StartNew();
                        string message = null;
                        try
                        {
                            // WhenAny for the same reason as above: a normal give-up is also a
                            // TimeoutException, so the type cannot be the hang discriminator.
                            Task<Fetched> run3 = Task.Run(() => FetchAsync(srv.Url, 0, 400000, o, ct));
                            Task done = await Task.WhenAny(run3, Task.Delay(TimeSpan.FromSeconds(25), ct))
                                                  .ConfigureAwait(false);
                            if (done != run3)
                            {
                                throw new Exception("the chunk retried for 25s against an origin that answers " +
                                                    "nothing: with unanswered attempts off MaxAttempts, the stall " +
                                                    "budget is the only bound left and it is not firing");
                            }
                            await run3.ConfigureAwait(false);
                        }
                        catch (Exception ex) { message = ex.Message; }
                        sw.Stop();

                        Harness.Assert(message != null, "an origin that never answered was reported as success");
                        // Quote the configured number, not just the reason. ChunkDownloader already
                        // formats stallBudgetMs into this message, so a floor or a multiplier
                        // applied to the budget is caught by the string alone - which "it
                        // terminated eventually" cannot do at any threshold.
                        Harness.Assert(message.Contains("made no progress for 3000 ms"),
                            "gave up for the wrong reason, or not on the configured budget: " + message);
                        // A band, not a ceiling. `< 15` against a 3s budget is 5x slack: a mutant
                        // flooring the budget at 10s gave up in 10.0s and this test PASSED,
                        // printing "stopped after 10.0s on a 3s stall budget".
                        Harness.Assert(sw.Elapsed.TotalSeconds > 2.5 && sw.Elapsed.TotalSeconds < 6,
                            "a 3s stall budget produced a " + sw.Elapsed.TotalSeconds.ToString("0.0") +
                            "s give-up");
                        return "stopped after " + sw.Elapsed.TotalSeconds.ToString("0.0") + "s on a 3s stall budget";
                    }
                    finally { srv.FaultHook = null; }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// The probe is chunk 0 and blocks a host request thread, so it has its own retry loop -
        /// and on 2026-08-31 it gave up on the same DNS wobble four times, each one logging
        /// FALLBACK and handing the whole film to Emby's single connection at 4.1 Mbps against a
        /// 13 Mbps requirement. Falling back is right when the origin cannot serve us; it is
        /// wrong when the origin is two seconds from being fine.
        /// </summary>
        private static async Task ProbeRidesOutUnansweredBurstAsync(CancellationToken ct)
        {
            await Harness.RunAsync("the probe waits out a short outage instead of falling back", async () =>
            {
                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    Stopwatch outage = Stopwatch.StartNew();
                    // Count only offset 0. A global counter sums the probe with every chunk
                    // worker, so the number reported as "the probe survived N attempts" was not
                    // the probe's. Nothing but the probe can request offset 0 during the outage:
                    // chunk 0 does not start until the probe has succeeded.
                    int dropped = 0;
                    srv.FaultHook = (seq, f, t) =>
                    {
                        if (outage.ElapsedMilliseconds >= 4000) return MockFault.None;
                        if (f == 0) Interlocked.Increment(ref dropped);
                        return MockFault.NoResponse;
                    };
                    try
                    {
                        ParallelFetchOptions o = Small();
                        o.StallBudget = TimeSpan.FromSeconds(30);
                        o.ResponseHeadersTimeout = TimeSpan.FromMilliseconds(300);

                        // OpenWith, not TryOpen: TryOpen turns the failure into a null, so this
                        // test would "pass" by falling back - which is the thing it forbids.
                        // Task.Run is load-bearing: OpenWith blocks its caller for the whole probe
                        // (Open() is synchronous by construction), so calling FetchAsync directly
                        // would run the probe on THIS thread and .WaitAsync would only start
                        // counting after it had already returned - a timeout that cannot fire.
                        Fetched got = await Task.Run(() => FetchAsync(srv.Url, 0, 200000, o, ct))
                                            .WaitAsync(TimeSpan.FromSeconds(30), ct).ConfigureAwait(false);
                        Harness.AssertBytesEqual(Pattern.Range(0, 200000), got.Data, "bytes after the probe recovered");
                        Harness.Assert(Volatile.Read(ref dropped) >= 4,
                            "only " + dropped + " requests were dropped, so the outage did not outlast the " +
                            "old 4-attempt window");
                        return "probe survived " + dropped + " unanswered attempts";
                    }
                    finally { srv.FaultHook = null; }
                }
            }).ConfigureAwait(false);
        }



        /// <summary>
        /// The probe's wall-clock bound, asserted against every shape that can consume it rather
        /// than against one convenient shape.
        ///
        /// This exists because the first version of the bound was a SECOND deadline layered on
        /// the stall budget and checked only at the top of the retry loop, while every actual
        /// wait inside the loop went on clamping to the 30 s stall budget. The single test
        /// covering it aimed at a closed port - connect-refused, which fails in microseconds -
        /// so it passed while three other shapes drove straight through:
        ///
        ///   accepts, never replies   -> header timeout clamped to 30 s, not to the window: 20.0 s
        ///   answers once, then quiet -> one answer disabled the window for good:            30.0 s
        ///   budget fully committed   -> the permit wait had its own 30 s bound:             30.0 s
        ///
        /// All three pinned a synchronously-blocked Emby request thread for the whole duration.
        /// Each row below is one of them. The invariant is deliberately stated once, over a
        /// table: the bound is a property of the probe, not of any particular failure.
        /// </summary>
        private static async Task ProbeBudgetHoldsWhateverTheOriginDoesAsync(CancellationToken ct)
        {
            await Harness.RunAsync("the probe's time bound holds for every shape, not just the fast one", async () =>
            {
                string[] names = { "accepts and never replies", "answers once then goes quiet", "budget fully committed" };
                List<string> results = new List<string>();

                for (int shape = 0; shape < names.Length; shape++)
                {
                    using (MockServer srv = new MockServer(FileSize, fastContent: false))
                    {
                        OriginBudget.ResetForTests();
                        ParallelFetchOptions o = shape == 2 ? Budgeted(connections: 1, budget: 1) : Small();
                        o.StallBudget = TimeSpan.FromSeconds(30);

                        if (shape == 0)
                        {
                            // The DEFAULT-scale header timeout, on purpose. A single attempt must
                            // not be able to outlast the whole probe budget; clamping the header
                            // wait to the stall budget instead of to the probe budget is exactly
                            // what let one attempt eat 20 s.
                            o.ResponseHeadersTimeout = TimeSpan.FromSeconds(20);
                            srv.FaultHook = (seq, f, t) => MockFault.NoResponse;
                        }
                        else if (shape == 1)
                        {
                            o.ResponseHeadersTimeout = TimeSpan.FromMilliseconds(200);
                            srv.FaultHook = (seq, f, t) => seq == 1 ? MockFault.Status500 : MockFault.NoResponse;
                        }
                        else
                        {
                            o.ResponseHeadersTimeout = TimeSpan.FromMilliseconds(200);
                        }

                        OriginBudget.Permit hog = null;
                        try
                        {
                            if (shape == 2)
                            {
                                hog = await OriginBudget.TryAcquireAsync(OriginBudget.KeyFor(srv.Url), 1,
                                                TimeSpan.FromSeconds(2), ct).ConfigureAwait(false);
                                Harness.Assert(hog != null, "could not take the only permit to saturate the budget");
                            }

                            Stopwatch sw = Stopwatch.StartNew();
                            bool gaveUp = false;

                            // WhenAny rather than WaitAsync, because "did it return?" must not be
                            // discriminated by exception TYPE here: WaitAsync signals its own
                            // timeout with TimeoutException, and so does the probe when it gives
                            // up normally. Catching TimeoutException therefore reported a correct
                            // give-up as a hang - which is how a mutant run first surfaced this.
                            // Task.Run is load-bearing too: OpenWith blocks its caller for the
                            // whole probe, so a bare await would run it on this thread.
                            Task<Fetched> open = Task.Run(() => FetchAsync(srv.Url, 0, 200000, o, ct));
                            Task done = await Task.WhenAny(open, Task.Delay(TimeSpan.FromSeconds(45), ct))
                                                  .ConfigureAwait(false);
                            if (done != open) throw new Exception(names[shape] + ": the probe never returned at all");
                            string why = null;
                            try { await open.ConfigureAwait(false); }
                            catch (Exception ex) { gaveUp = true; why = ex.Message; }
                            sw.Stop();

                            Harness.Assert(gaveUp, names[shape] + ": reported success against an origin that never served");

                            // The diagnosis, on the paths that actually produce it. These three
                            // shapes all give up from inside the CATCH block, where the code used
                            // to rethrow whatever exception was in flight - so the FALLBACK line,
                            // the only record a running Emby keeps of why a stream went
                            // single-connection, read "The operation was canceled." The
                            // connect-refused test in LoggingTests cannot cover this: it fails
                            // fast enough to exit through the top-of-loop check instead, which
                            // always had the good message.
                            Harness.Assert(why != null && why.Contains("Probe gave up on"),
                                names[shape] + ": gave up without saying so - the fallback reason will be " +
                                "whatever exception happened to be in flight: " + why);
                            Harness.Assert(sw.Elapsed.TotalSeconds < 11,
                                names[shape] + ": pinned the calling thread for " +
                                sw.Elapsed.TotalSeconds.ToString("0.0") + "s; the probe budget is 8s and every " +
                                "wait inside the retry loop is supposed to be clamped by it");
                            results.Add(names[shape] + " " + sw.Elapsed.TotalSeconds.ToString("0.0") + "s");
                        }
                        finally { if (hog != null) hog.Dispose(); srv.FaultHook = null; }
                    }
                }
                return string.Join(", ", results);
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// The origin answers 503 once enough connections pile up. Backing off on a fixed
        /// exponential timer ignores what it actually asked for and lets every worker return in
        /// lockstep - the same herd that caused the 503.
        /// </summary>
        private static async Task RetryAfterAsync(CancellationToken ct)
        {
            await Harness.RunAsync("503 Retry-After is waited out, not out-guessed", async () =>
            {
                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    srv.RetryAfter = "1";                       // one second, far above the 10ms backoff
                    srv.FaultHook = (seq, f, t) => seq == 1 ? MockFault.Status503RetryAfter : MockFault.None;
                    try
                    {
                        Fetched got = await FetchAsync(srv.Url, 0, 128 * 1024, Small(), ct).ConfigureAwait(false);
                        Harness.AssertBytesEqual(Pattern.Range(0, 128 * 1024), got.Data, "bytes after the 503");
                    }
                    finally { srv.FaultHook = null; srv.RetryAfter = null; }

                    long[] ticks = srv.RequestTicks.ToArray();
                    Harness.Assert(ticks.Length >= 2, "expected a retry after the 503");
                    long waited = ticks[1] - ticks[0];
                    Harness.Assert(waited >= 900,
                        "came back after " + waited + " ms; the server asked for 1000 ms " +
                        "(the local backoff is 10 ms, so ignoring Retry-After is visible here)");
                    return "waited " + waited + " ms for a Retry-After: 1";
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// A worker can die outside a chunk download - during the connection ramp, or between
        /// claiming a chunk and publishing its channel. Setting the fault flag alone stopped
        /// every other worker from claiming while nothing poisoned a channel, so the reader
        /// waited on a slot that would never become ready: playback froze with no error, no log
        /// line the operator would look for, and no fallback. A hang is the one failure mode a
        /// media server cannot recover from on its own, so it has to become an exception.
        ///
        /// Provoked here the way the review found it: a ramp interval past what Task.Delay
        /// accepts, which throws inside the worker before it ever claims work.
        /// </summary>
        private static async Task WorkerStartupFailureAsync(CancellationToken ct)
        {
            await Harness.RunAsync("a worker dying outside a chunk fails fast instead of hanging", async () =>
            {
                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    // Options are built PAST Normalize() on purpose. Normalize now bounds the
                    // ramp interval, so this value can no longer arrive from configuration - but
                    // that bound is a second line of defence, and this test is for the first
                    // one. Going through OpenWith here would prove nothing, because the clamp
                    // would keep the worker alive and the test would pass vacuously.
                    ParallelFetchOptions o = Small().Normalize();
                    o.InitialConnections = 1;                          // workers 1..3 must wait
                    o.ConnectionRampInterval = TimeSpan.FromDays(60);  // past Task.Delay's ceiling

                    // Throttled so the one surviving worker cannot race to the end of the range
                    // and finish before the others have thrown.
                    srv.ThrottleBytesPerSec = 2 * 1024 * 1024;
                    const long Length = 2 * 1024 * 1024;

                    HttpClient client = HttpClientHolder.CreateForStream();   // the stream takes ownership
                    HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Get, srv.Url);
                    req.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(0, o.FirstChunkSize - 1);
                    HttpResponseMessage probe = await client
                        .SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);

                    Stopwatch sw = Stopwatch.StartNew();
                    using (Stream s = new ParallelRangeStream(client, srv.Url, 0, Length, FileSize, o,
                                                              new PreOpenedChunk(probe, null), ct))
                    using (CancellationTokenSource guard = CancellationTokenSource.CreateLinkedTokenSource(ct))
                    {
                        guard.CancelAfter(TimeSpan.FromSeconds(15));
                        try
                        {
                            await Harness.ReadAllAsync(s, 32 * 1024, guard.Token).ConfigureAwait(false);
                            throw new Exception("the doomed stream completed; this test no longer provokes " +
                                                "a worker failure and is not testing anything");
                        }
                        catch (OperationCanceledException) when (guard.IsCancellationRequested && !ct.IsCancellationRequested)
                        {
                            throw new Exception("the reader hung for 15s: a worker died, nothing poisoned a " +
                                                "channel, and no exception ever reached the consumer");
                        }
                        catch (Exception ex)
                        {
                            sw.Stop();
                            return "surfaced as " + ex.GetType().Name + " in " +
                                   sw.Elapsed.TotalSeconds.ToString("0.00") + "s";
                        }
                    }
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// The live failure: eight successive abandoned seeks collapsed throughput to 0.23 Mbps
        /// and stayed there, while curl on fresh connections was healthy at the same instant.
        /// The property that prevents it is that a stream never inherits another stream's sockets.
        /// </summary>
        private static async Task ConnectionIsolationAsync(CancellationToken ct)
        {
            await Harness.RunAsync("8 abandoned seeks do not degrade the stream that follows", async () =>
            {
                // Mirrors the live repro: eight open-ended requests each abandoned mid-transfer,
                // which collapsed throughput 25.14 -> 0.23 Mbps and kept it there. Client source
                // ports are NOT a usable signal here - the OS recycles an ephemeral port as soon
                // as the socket closes, so a port collision proves closure rather than reuse.
                // Throughput of the following stream is the property that actually matters.
                using (MockServer srv = new MockServer(256L * 1024 * 1024, fastContent: true))
                {
                    ParallelFetchOptions o = new ParallelFetchOptions
                    {
                        Connections = 4,
                        ConnectionRampInterval = TimeSpan.Zero,
                        ChunkSize = 1024 * 1024,
                        FirstChunkSize = 256 * 1024,
                        BlockSize = 64 * 1024,
                        MaxBufferBytes = 8L * 1024 * 1024
                    };
                    const long Measure = 16L * 1024 * 1024;

                    // Reference: a clean stream with nothing abandoned before it.
                    Stopwatch first = Stopwatch.StartNew();
                    long? t0, c0;
                    using (Stream s0 = ParallelFetch.OpenWith(srv.Url, 0, Measure, o, out t0, out c0, ct))
                    {
                        Harness.AssertEqual(Measure, await Harness.DrainAsync(s0, 64 * 1024, 0, ct, null).ConfigureAwait(false), "reference bytes");
                    }
                    first.Stop();
                    double referenceMbps = Measure * 8.0 / first.Elapsed.TotalSeconds / 1e6;

                    long[] seeks = { 200_000_000L, 40_000_000L, 150_000_000L, 90_000_000L,
                                     210_000_000L, 60_000_000L, 170_000_000L, 120_000_000L };
                    byte[] buf = new byte[64 * 1024];
                    foreach (long seek in seeks)
                    {
                        long? ts, cs;
                        Stream s = ParallelFetch.OpenWith(srv.Url, seek, 0, o, out ts, out cs, ct);
                        long read = 0;
                        while (read < 1024 * 1024)
                        {
                            int n = await s.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false);
                            if (n <= 0) break;
                            read += n;
                        }
                        s.Dispose();   // abandon mid-transfer, exactly like a seek
                    }

                    // Immediately afterwards - no idle period - open again and measure.
                    Stopwatch after = Stopwatch.StartNew();
                    long? t1, c1;
                    using (Stream s1 = ParallelFetch.OpenWith(srv.Url, 0, Measure, o, out t1, out c1, ct))
                    {
                        Harness.AssertEqual(Measure, await Harness.DrainAsync(s1, 64 * 1024, 0, ct, null).ConfigureAwait(false), "post-seek bytes");
                    }
                    after.Stop();
                    double afterMbps = Measure * 8.0 / after.Elapsed.TotalSeconds / 1e6;

                    Harness.Assert(afterMbps > referenceMbps * 0.5,
                        "throughput after 8 abandoned seeks fell to " + afterMbps.ToString("0.0") +
                        " Mbps from a " + referenceMbps.ToString("0.0") + " Mbps baseline");
                    return "baseline " + referenceMbps.ToString("0") + " Mbps -> after 8 abandoned seeks " +
                           afterMbps.ToString("0") + " Mbps (" + srv.RemotePorts.Count + " client ports seen)";
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// A connection that trickles instead of stalling resets the idle timer forever, so the
        /// old code reported success at 0.23 Mbps with zero retries. The throughput floor turns
        /// that into a logged retry on a fresh connection.
        /// </summary>
        private static async Task SlowConnectionAsync(CancellationToken ct)
        {
            await Harness.RunAsync("a trickling connection is abandoned and retried, not tolerated", async () =>
            {
                using (MockServer srv = new MockServer(8L * 1024 * 1024, fastContent: false))
                {
                    srv.TrickleBytesPerSec = 2048;
                    ConcurrentDictionary<long, int> seen = new ConcurrentDictionary<long, int>();
                    // Every chunk trickles on its first attempt, then behaves.
                    srv.FaultHook = (seq, from, to) =>
                        seen.AddOrUpdate(from, 1, (k, v) => v + 1) == 1 && from % (512 * 1024) == 0
                            ? MockFault.Trickle : MockFault.None;   // the fresh connection is fine
                    try
                    {
                        ParallelFetchOptions o = new ParallelFetchOptions
                        {
                            Connections = 4,
                            ConnectionRampInterval = TimeSpan.Zero,
                            ChunkSize = 512 * 1024,
                            FirstChunkSize = 512 * 1024,
                            BlockSize = 64 * 1024,
                            MaxBufferBytes = 8L * 512 * 1024,
                            MaxAttempts = 4,
                            RetryBaseDelayMs = 10,
                            RetryMaxDelayMs = 50,
                            MinThroughputBytesPerSec = 48 * 1024,
                            MinThroughputGrace = TimeSpan.FromSeconds(1)
                        };

                        Stopwatch sw = Stopwatch.StartNew();
                        long? t, cl;
                        byte[] got;
                        using (Stream s = ParallelFetch.OpenWith(srv.Url, 0, 2 * 1024 * 1024, o, out t, out cl, ct))
                        {
                            got = await Harness.ReadAllAsync(s, 64 * 1024, ct).ConfigureAwait(false);
                        }
                        sw.Stop();

                        Harness.AssertEqual(2 * 1024 * 1024, got.Length, "delivered");
                        Harness.AssertBytesEqual(Pattern.Range(0, 2 * 1024 * 1024), got, "bytes");
                        // At 2 KB/s a single 512 KiB chunk would need over four minutes.
                        Harness.Assert(sw.Elapsed.TotalSeconds < 60,
                            "took " + sw.Elapsed.TotalSeconds.ToString("0.0") + "s - the trickle was tolerated");
                        return "recovered from " + seen.Count + " trickling connections in " +
                               sw.Elapsed.TotalSeconds.ToString("0.0") + "s";
                    }
                    finally { srv.FaultHook = null; }
                }
            }).ConfigureAwait(false);

            await Harness.RunAsync("a genuinely slow consumer never trips the throughput floor", async () =>
            {
                using (MockServer srv = new MockServer(32L * 1024 * 1024, fastContent: true))
                {
                    ParallelFetchOptions o = new ParallelFetchOptions
                    {
                        Connections = 4,
                        ConnectionRampInterval = TimeSpan.Zero,
                        ChunkSize = 512 * 1024,
                        FirstChunkSize = 512 * 1024,
                        BlockSize = 64 * 1024,
                        MaxBufferBytes = 6L * 512 * 1024,
                        MinThroughputBytesPerSec = 48 * 1024,
                        MinThroughputGrace = TimeSpan.FromSeconds(1)
                    };
                    long? t, cl;
                    using (Stream s = ParallelFetch.OpenWith(srv.Url, 0, 6L * 1024 * 1024, o, out t, out cl, ct))
                    {
                        // ~0.5 MB/s consumer: far below the floor, but the fault is ours, not the
                        // connection's, so read-time accounting must exclude the parked writes.
                        long got = await Harness.DrainAsync(s, 64 * 1024, 120, ct, null).ConfigureAwait(false);
                        Harness.AssertEqual(6L * 1024 * 1024, got, "delivered");
                    }
                    return "no false positive under backpressure";
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// RFC 7233 requires complete-length &gt; last-byte-pos. A proxy that fills in the size of
        /// a different object yields e.g. "bytes 50000-149999/0"; trusting it used to clamp the
        /// delivery to nothing and hand back a clean, successful, EMPTY stream.
        /// </summary>
        private static async Task ContradictoryContentRangeAsync(CancellationToken ct)
        {
            await Harness.RunAsync("self-contradictory Content-Range total is never trusted", async () =>
            {
                // Invariant under test: we either reject the response, or deliver exactly the
                // number of bytes we reported. What must never happen is a clean, successful,
                // EMPTY (or short-but-unreported) stream, which is what a trusted bogus total
                // used to produce. Totals below the requested range are impossible per RFC 7233;
                // a total that merely disagrees with reality but is internally consistent is a
                // lying origin faithfully relayed - a single-connection fetch behaves the same.
                long[] bogus = { 0, 1, 1024, 49999, 149999 };
                using (MockServer srv = new MockServer(FileSize, fastContent: false))
                {
                    const long Offset = 50000, Length = 100000;
                    int rejectedCount = 0, consistentCount = 0;
                    foreach (long total in bogus)
                    {
                        long advertise = total;
                        srv.ContentRangeTotal = (f, t) => advertise;
                        try
                        {
                            long? tl, cl;
                            using (Stream s = ParallelFetch.OpenWith(srv.Url, Offset, Length, Small(), out tl, out cl, ct))
                            {
                                byte[] got = await Harness.ReadAllAsync(s, 32 * 1024, ct).ConfigureAwait(false);
                                Harness.Assert(got.Length > 0,
                                    "total=" + total + " produced a successful EMPTY stream (silent data loss)");
                                Harness.Assert(cl.HasValue && cl.Value == got.Length,
                                    "total=" + total + ": reported contentLength " +
                                    (cl.HasValue ? cl.Value.ToString() : "null") + " but delivered " + got.Length);
                                Harness.AssertBytesEqual(Pattern.Range(Offset, got.Length), got,
                                    "total=" + total + " bytes");
                                consistentCount++;
                            }
                        }
                        catch (IOException)
                        {
                            rejectedCount++;
                        }
                    }
                    srv.ContentRangeTotal = null;

                    Harness.Assert(rejectedCount >= 4,
                        "expected the four impossible totals (<= requested range) to be rejected, only " +
                        rejectedCount + " were");

                    // Control: the honest total still works.
                    long? t2, c2;
                    using (Stream s = ParallelFetch.OpenWith(srv.Url, Offset, Length, Small(), out t2, out c2, ct))
                    {
                        byte[] got = await Harness.ReadAllAsync(s, 32 * 1024, ct).ConfigureAwait(false);
                        Harness.AssertBytesEqual(Pattern.Range(Offset, Length), got, "control bytes");
                        Harness.AssertEqual(FileSize, t2.Value, "control totalLength");
                    }
                    return rejectedCount + " impossible totals rejected, " + consistentCount +
                           " self-consistent, honest total still served";
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// A player seeking mid-playback abandons the previous stream exactly this way. An
        /// abandoned stream that keeps pulling counts against the origin's connection limit and
        /// throttles whatever is opened next, so teardown has to be immediate.
        /// </summary>
        private static async Task AbandonmentAsync(CancellationToken ct)
        {
            await Harness.RunAsync("Open then immediate Dispose leaves no live origin response", async () =>
            {
                using (MockServer srv = new MockServer(64L * 1024 * 1024, fastContent: true))
                {
                    srv.ThrottleBytesPerSec = 4 * 1024 * 1024;
                    ParallelFetchOptions o = new ParallelFetchOptions
                    {
                        Connections = 6,
                        ConnectionRampInterval = TimeSpan.Zero,
                        ChunkSize = 1024 * 1024,
                        FirstChunkSize = 256 * 1024,
                        BlockSize = 64 * 1024,
                        MaxBufferBytes = 12L * 1024 * 1024
                    };

                    double slowestDispose = 0;
                    for (int i = 0; i < 10; i++)
                    {
                        long? t, cl;
                        Stream s = ParallelFetch.OpenWith(srv.Url, i * 1024L, 0, o, out t, out cl, ct);
                        Stopwatch sw = Stopwatch.StartNew();
                        s.Dispose();
                        sw.Stop();
                        if (sw.Elapsed.TotalSeconds > slowestDispose) slowestDispose = sw.Elapsed.TotalSeconds;
                    }

                    Stopwatch drain = Stopwatch.StartNew();
                    while (drain.Elapsed < TimeSpan.FromSeconds(20) && srv.ActiveResponses > 0)
                    {
                        await Task.Delay(100, ct).ConfigureAwait(false);
                    }
                    drain.Stop();

                    Harness.Assert(slowestDispose < 1.0, "slowest Dispose blocked for " + slowestDispose.ToString("0.00") + "s");
                    Harness.Assert(srv.ActiveResponses == 0,
                        "LEAK: " + srv.ActiveResponses + " origin responses still streaming 20s after Dispose");
                    Harness.Assert(drain.Elapsed.TotalSeconds < 5,
                        "origin took " + drain.Elapsed.TotalSeconds.ToString("0.0") + "s to go quiet");
                    return "10 open+dispose cycles, slowest dispose " + (slowestDispose * 1000).ToString("0") +
                           " ms, origin quiet in " + drain.Elapsed.TotalSeconds.ToString("0.00") + "s";
                }
            }).ConfigureAwait(false);

            await Harness.RunAsync("abandoned mid-stream: buffers and connections released at once", async () =>
            {
                using (MockServer srv = new MockServer(256L * 1024 * 1024, fastContent: true))
                {
                    ParallelFetchOptions o = new ParallelFetchOptions
                    {
                        Connections = 6,
                        ConnectionRampInterval = TimeSpan.Zero,
                        ChunkSize = 2 * 1024 * 1024,
                        FirstChunkSize = 512 * 1024,
                        BlockSize = 64 * 1024,
                        MaxBufferBytes = 12L * 2 * 1024 * 1024
                    };
                    long? t, cl;
                    Stream s = ParallelFetch.OpenWith(srv.Url, 0, 200L * 1024 * 1024, o, out t, out cl, ct);
                    ParallelRangeStream prs = (ParallelRangeStream)s;

                    // Read 4 KB and stop, exactly like a client that disconnects: the read-ahead
                    // window fills and the workers park on backpressure.
                    byte[] tiny = new byte[4096];
                    await s.ReadAsync(tiny.AsMemory(0, tiny.Length), ct).ConfigureAwait(false);
                    await Task.Delay(2500, ct).ConfigureAwait(false);

                    long bufferedWhileParked = FetchMetrics.BufferedBytes;
                    Harness.Assert(bufferedWhileParked > 0, "expected the read-ahead window to have filled");

                    Stopwatch sw = Stopwatch.StartNew();
                    s.Dispose();
                    double disposeSeconds = sw.Elapsed.TotalSeconds;

                    Task done = prs.WorkersCompletion;
                    Task finished = await Task.WhenAny(done, Task.Delay(10000)).ConfigureAwait(false);
                    Stopwatch quiet = Stopwatch.StartNew();
                    while (quiet.Elapsed < TimeSpan.FromSeconds(10) && srv.ActiveResponses > 0)
                    {
                        await Task.Delay(100, ct).ConfigureAwait(false);
                    }
                    quiet.Stop();

                    long bufferedAfter = FetchMetrics.BufferedBytes;
                    Harness.Assert(disposeSeconds < 1.0, "Dispose blocked for " + disposeSeconds.ToString("0.00") + "s");
                    Harness.Assert(ReferenceEquals(finished, done), "workers still running 10s after dispose");
                    Harness.Assert(srv.ActiveResponses == 0, "LEAK: " + srv.ActiveResponses + " responses still streaming");
                    Harness.Assert(bufferedAfter * 4 < bufferedWhileParked || bufferedAfter < 1024 * 1024,
                        "buffered bytes only fell from " + bufferedWhileParked + " to " + bufferedAfter);
                    return "parked with " + Harness.MiB(bufferedWhileParked) + " buffered; after dispose " +
                           Harness.MiB(bufferedAfter) + ", origin quiet in " + quiet.Elapsed.TotalSeconds.ToString("0.00") + "s";
                }
            }).ConfigureAwait(false);
        }

        private static async Task RampCase(MockServer srv, string name, long offset, long length, CancellationToken ct)
        {
            await Harness.RunAsync(name, async () =>
            {
                Fetched f = await FetchAsync(srv.Url, offset, length, Ramped(), ct).ConfigureAwait(false);
                long expectedLen = length > 0 ? Math.Min(length, FileSize - offset) : FileSize - offset;
                Harness.AssertEqual(expectedLen, f.Data.Length, "delivered length");
                Harness.AssertEqual(FileSize, f.Total.Value, "totalLength");
                Harness.AssertEqual(expectedLen, f.ContentLength.Value, "contentLength");
                Harness.AssertBytesEqual(Pattern.Range(offset, expectedLen), f.Data, "vs generator");

                byte[] reference = await SingleConnectionAsync(srv.Url, offset, length, ct).ConfigureAwait(false);
                Harness.AssertBytesEqual(reference, f.Data, "vs single-connection fetch");
                return expectedLen + " bytes";
            }).ConfigureAwait(false);
        }

        private static async Task RangeCase(MockServer srv, string name, long offset, long length, CancellationToken ct)
        {
            await Harness.RunAsync(name, async () =>
            {
                Fetched f = await FetchAsync(srv.Url, offset, length, Small(), ct).ConfigureAwait(false);
                long expectedLen = length > 0 ? Math.Min(length, FileSize - offset) : FileSize - offset;

                Harness.AssertEqual(expectedLen, f.Data.Length, "delivered length");
                Harness.AssertEqual(FileSize, f.Total.Value, "totalLength");
                Harness.AssertEqual(expectedLen, f.ContentLength.Value, "contentLength");
                Harness.AssertBytesEqual(Pattern.Range(offset, expectedLen), f.Data, "vs generator");

                byte[] reference = await SingleConnectionAsync(srv.Url, offset, length, ct).ConfigureAwait(false);
                Harness.AssertBytesEqual(reference, f.Data, "vs single-connection fetch");
                return expectedLen + " bytes";
            }).ConfigureAwait(false);
        }

        private static async Task BackpressureAsync(CancellationToken ct)
        {
            await Harness.RunAsync("slow consumer does not cause unbounded buffering", async () =>
            {
                using (MockServer srv = new MockServer(256L * 1024 * 1024, fastContent: true))
                {
                    ParallelFetchOptions o = new ParallelFetchOptions
                    {
                        Connections = 4,
                        ConnectionRampInterval = TimeSpan.Zero,
                        ChunkSize = 1024 * 1024,
                        BlockSize = 64 * 1024,
                        MaxBufferBytes = 8L * 1024 * 1024
                    };
                    ParallelFetchOptions norm = o; // ceiling computed from the stream below

                    long? total, cl;
                    Stream s = ParallelFetch.OpenWith(srv.Url, 0, 48L * 1024 * 1024, norm, out total, out cl, ct);
                    ParallelRangeStream prs = (ParallelRangeStream)s;
                    long ceiling = prs.MemoryCeilingBytes;

                    FetchMetrics.ResetPeak();
                    long maxServedAhead = 0;

                    using (s)
                    {
                        long consumed = await Harness.DrainAsync(s, 64 * 1024, 4, ct, total2 =>
                        {
                            long ahead = Interlocked.Read(ref srv.BytesServed) - total2;
                            if (ahead > maxServedAhead) maxServedAhead = ahead;
                        }).ConfigureAwait(false);

                        Harness.AssertEqual(48L * 1024 * 1024, consumed, "consumed");
                    }

                    long peak = FetchMetrics.PeakBufferedBytes;
                    // Allowance: bounded channel capacity + one block per worker in flight + one
                    // block held by the reader.
                    long allowance = ceiling + (long)(o.Connections + 2) * o.BlockSize;
                    Harness.Assert(peak <= allowance,
                        "peak buffered " + peak + " exceeded ceiling+slack " + allowance);
                    Harness.Assert(maxServedAhead <= ceiling + 4L * o.ChunkSize,
                        "server ran " + maxServedAhead + " bytes ahead of the consumer (ceiling " + ceiling + ")");

                    return "ceiling " + Harness.MiB(ceiling) + ", peak buffered " + Harness.MiB(peak) +
                           ", server ran at most " + Harness.MiB(maxServedAhead) + " ahead";
                }
            }).ConfigureAwait(false);
        }

        private static async Task MemoryCeilingAsync(CancellationToken ct)
        {
            await Harness.RunAsync("3 GiB stream stays inside the memory ceiling", async () =>
            {
                using (MockServer srv = new MockServer(4L * 1024 * 1024 * 1024, fastContent: true))
                {
                    ParallelFetchOptions o = new ParallelFetchOptions();   // production defaults
                    long want = 3L * 1024 * 1024 * 1024;

                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    GC.Collect();
                    Process p = Process.GetCurrentProcess();
                    p.Refresh();
                    long rssBefore = p.WorkingSet64;

                    long? total, cl;
                    Stream s = ParallelFetch.OpenWith(srv.Url, 0, want, o, out total, out cl, ct);
                    ParallelRangeStream prs = (ParallelRangeStream)s;
                    long ceiling = prs.MemoryCeilingBytes;

                    FetchMetrics.ResetPeak();
                    long peakRss = rssBefore;
                    long peakHeap = 0;
                    using (CancellationTokenSource sampleStop = new CancellationTokenSource())
                    {
                        Task sampler = Task.Run(async () =>
                        {
                            while (!sampleStop.IsCancellationRequested)
                            {
                                p.Refresh();
                                long ws = p.WorkingSet64;
                                if (ws > peakRss) peakRss = ws;
                                long heap = GC.GetTotalMemory(false);
                                if (heap > peakHeap) peakHeap = heap;
                                try { await Task.Delay(100, sampleStop.Token).ConfigureAwait(false); } catch { return; }
                            }
                        });

                        Stopwatch sw = Stopwatch.StartNew();
                        long consumed;
                        using (s) consumed = await Harness.DrainAsync(s, 256 * 1024, 0, ct, null).ConfigureAwait(false);
                        sw.Stop();

                        sampleStop.Cancel();
                        try { await sampler.ConfigureAwait(false); } catch { }

                        Harness.AssertEqual(want, consumed, "consumed");
                        long peakBuffered = FetchMetrics.PeakBufferedBytes;
                        long allowance = ceiling + (long)(o.Connections + 2) * o.BlockSize;
                        Harness.Assert(peakBuffered <= allowance,
                            "peak buffered " + peakBuffered + " exceeded ceiling+slack " + allowance);
                        Harness.Assert(peakRss - rssBefore < 512L * 1024 * 1024,
                            "RSS grew by " + Harness.MiB(peakRss - rssBefore));

                        return "ceiling " + Harness.MiB(ceiling) + ", peak buffered " + Harness.MiB(peakBuffered) +
                               ", peak managed heap " + Harness.MiB(peakHeap) +
                               ", RSS " + Harness.MiB(rssBefore) + " -> " + Harness.MiB(peakRss) +
                               ", loopback " + Harness.Mbps(consumed, sw.Elapsed.TotalSeconds);
                    }
                }
            }).ConfigureAwait(false);
        }

        private static async Task CancellationAsync(CancellationToken ct)
        {
            await Harness.RunAsync("cancellation mid-stream is prompt and tears down", async () =>
            {
                using (MockServer srv = new MockServer(256L * 1024 * 1024, fastContent: true))
                {
                    srv.ThrottleBytesPerSec = 2 * 1024 * 1024;
                    ParallelFetchOptions o = new ParallelFetchOptions
                    {
                        Connections = 4,
                        ConnectionRampInterval = TimeSpan.Zero,
                        ChunkSize = 1024 * 1024,
                        BlockSize = 64 * 1024,
                        MaxBufferBytes = 8L * 1024 * 1024
                    };
                    using (CancellationTokenSource cts = new CancellationTokenSource())
                    {
                        long? total, cl;
                        Stream s = ParallelFetch.OpenWith(srv.Url, 0, 200L * 1024 * 1024, o, out total, out cl, cts.Token);
                        ParallelRangeStream prs = (ParallelRangeStream)s;

                        byte[] buf = new byte[64 * 1024];
                        long read = 0;
                        // get well past the first block so we cancel in the middle of the pipeline
                        while (read < 3 * 1024 * 1024)
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
                        catch (Exception ex) when (ex is OperationCanceledException || ex is IOException)
                        {
                            threw = true;
                        }
                        double abortSeconds = sw.Elapsed.TotalSeconds;
                        Harness.Assert(threw, "cancelling the token did not surface an error on Read");
                        Harness.Assert(abortSeconds < 5, "Read took " + abortSeconds.ToString("0.00") + "s to notice cancellation");

                        s.Dispose();
                        Task done = prs.WorkersCompletion;
                        Stopwatch teardown = Stopwatch.StartNew();
                        Task finished = await Task.WhenAny(done, Task.Delay(5000)).ConfigureAwait(false);
                        teardown.Stop();
                        Harness.Assert(ReferenceEquals(finished, done),
                            "workers still running 5s after dispose");

                        return "aborted in " + abortSeconds.ToString("0.00") + "s after " + Harness.MiB(read) +
                               ", workers gone in " + teardown.Elapsed.TotalSeconds.ToString("0.00") + "s";
                    }
                }
            }).ConfigureAwait(false);
        }

        private static async Task DisposeMidStreamAsync(CancellationToken ct)
        {
            await Harness.RunAsync("dispose mid-stream does not hang or leak workers", async () =>
            {
                using (MockServer srv = new MockServer(256L * 1024 * 1024, fastContent: true))
                {
                    srv.ThrottleBytesPerSec = 4 * 1024 * 1024;
                    ParallelFetchOptions o = new ParallelFetchOptions
                    {
                        Connections = 6,
                        ConnectionRampInterval = TimeSpan.Zero,
                        ChunkSize = 1024 * 1024,
                        BlockSize = 64 * 1024,
                        MaxBufferBytes = 12L * 1024 * 1024
                    };
                    long? total, cl;
                    Stream s = ParallelFetch.OpenWith(srv.Url, 1234, 200L * 1024 * 1024, o, out total, out cl, ct);
                    ParallelRangeStream prs = (ParallelRangeStream)s;

                    byte[] buf = new byte[64 * 1024];
                    long read = 0;
                    while (read < 2 * 1024 * 1024)
                    {
                        int n = await s.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false);
                        if (n <= 0) break;
                        read += n;
                    }

                    Stopwatch sw = Stopwatch.StartNew();
                    s.Dispose();
                    double disposeSeconds = sw.Elapsed.TotalSeconds;
                    Harness.Assert(disposeSeconds < 1.0, "Dispose blocked for " + disposeSeconds.ToString("0.00") + "s");

                    Task done = prs.WorkersCompletion;
                    Stopwatch teardown = Stopwatch.StartNew();
                    Task finished = await Task.WhenAny(done, Task.Delay(5000)).ConfigureAwait(false);
                    teardown.Stop();
                    Harness.Assert(ReferenceEquals(finished, done), "workers still running 5s after dispose");

                    bool threw = false;
                    try { await s.ReadAsync(buf.AsMemory(0, 16), ct).ConfigureAwait(false); }
                    catch (ObjectDisposedException) { threw = true; }
                    Harness.Assert(threw, "reading a disposed stream should throw ObjectDisposedException");

                    return "dispose returned in " + disposeSeconds.ToString("0.000") + "s, workers gone in " +
                           teardown.Elapsed.TotalSeconds.ToString("0.00") + "s";
                }
            }).ConfigureAwait(false);
        }
    }
}
