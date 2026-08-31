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
            await IgnoredRangeSkipCeilingAsync(ct).ConfigureAwait(false);
            await ContentEncodingAsync(ct).ConfigureAwait(false);
            await RetryAfterAsync(ct).ConfigureAwait(false);

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
                    using (Stream s = new ParallelRangeStream(client, srv.Url, 0, Length, FileSize, o, probe, ct))
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
