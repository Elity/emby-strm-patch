using System;
using System.Buffers;
using System.Globalization;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace EmbyStrmParallel.Tests
{
    internal enum MockFault
    {
        None = 0,
        Status403,
        Status500,
        Status503RetryAfter,   // 503 carrying the server's own Retry-After instruction
        TruncateHalf,   // announce the full length, send half, reset the connection
        Trickle,        // technically succeeds, but far too slow to be usable
        IgnoreRange     // answer 200 with the whole resource
    }

    /// <summary>
    /// Deterministic content: byte i is a hash of i, so any range can be predicted without
    /// storing the file and any reordering/duplication/truncation shows up immediately.
    /// </summary>
    internal static class Pattern
    {
        internal static byte At(long i)
        {
            ulong x = (ulong)i * 0x9E3779B97F4A7C15UL;
            x ^= x >> 33;
            x *= 0xC2B2AE3D27D4EB4FUL;
            x ^= x >> 29;
            return (byte)x;
        }

        internal static void Fill(Span<byte> dst, long start)
        {
            for (int k = 0; k < dst.Length; k++) dst[k] = At(start + k);
        }

        internal static byte[] Range(long start, long length)
        {
            byte[] b = new byte[length];
            Fill(b, start);
            return b;
        }
    }

    /// <summary>
    /// Loopback HTTP origin with Range support and injectable faults. Used for every
    /// correctness/edge-case test so they are fast and deterministic; the real URL is only
    /// used for throughput and a byte-exactness spot check.
    /// </summary>
    internal sealed class MockServer : IDisposable
    {
        private readonly HttpListener _listener = new HttpListener();
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly bool _fastContent;
        private readonly byte[] _tile;      // fast-content mode: 1 MiB tile, memcpy instead of per-byte hashing
        private Task _loop;
        private int _concurrent;

        internal string Url { get; private set; }
        internal string RedirectUrl { get; private set; }
        internal long Size { get; private set; }

        internal int RequestCount;
        internal long BytesServed;
        internal int MaxConcurrent;

        /// <summary>(requestSeq, from, toInclusive) -> fault. Called once per request.</summary>
        internal Func<int, long, long, MockFault> FaultHook;

        /// <summary>Bytes per second per response, 0 = unlimited.</summary>
        internal int ThrottleBytesPerSec;

        /// <summary>(from, toInclusive) -> the total to advertise in Content-Range. Null = the truth.</summary>
        internal Func<long, long, long> ContentRangeTotal;

        /// <summary>
        /// (from, toInclusive) -> the literal Content-Range header value, or null to send a 206
        /// with NO Content-Range at all. Takes precedence over ContentRangeTotal.
        /// Lets a test reproduce the header shapes a broken proxy emits: absent, unparseable,
        /// "bytes */N", or an unknown complete-length.
        /// </summary>
        internal Func<long, long, string> ContentRangeHeader;

        /// <summary>
        /// (from, toInclusive) -> the offset the body is actually generated from. Default: the
        /// truth. A proxy that answers a mid-file request with the start of the file produces
        /// the right byte COUNT at the wrong POSITION, which no length check can catch.
        /// </summary>
        internal Func<long, long, long> BodyOffset;

        /// <summary>Non-null = advertise this Content-Encoding (body is still identity; only the claim matters).</summary>
        internal string ContentEncoding;

        /// <summary>
        /// Answer a range whose last-byte-pos is past EOF with 200 + the whole resource, instead
        /// of clamping it. RFC 7233 says to clamp; the production origin does this instead, and
        /// it is the reason a tail read used to strand the fetcher discarding gigabytes.
        /// </summary>
        internal bool WholeFileWhenRangeEndsPastEof;

        /// <summary>Non-null = send this Retry-After value alongside a 503, and record the wait between attempts.</summary>
        internal string RetryAfter;

        /// <summary>TickCount64 of every request, so a test can assert how long the client waited.</summary>
        internal readonly System.Collections.Concurrent.ConcurrentQueue<long> RequestTicks =
            new System.Collections.Concurrent.ConcurrentQueue<long>();

        /// <summary>Accept-Encoding of every request, so a test can prove the representation was pinned.</summary>
        internal readonly System.Collections.Concurrent.ConcurrentQueue<string> AcceptEncodings =
            new System.Collections.Concurrent.ConcurrentQueue<string>();

        /// <summary>Responses currently being written. Mirrors what an origin would see as open connections.</summary>
        internal int ActiveResponses { get { return Volatile.Read(ref _concurrent); } }

        /// <summary>Bytes per second used for MockFault.Trickle responses.</summary>
        internal int TrickleBytesPerSec = 2048;

        /// <summary>Client source ports seen, so tests can prove two streams shared no socket.</summary>
        internal readonly System.Collections.Concurrent.ConcurrentDictionary<int, byte> RemotePorts =
            new System.Collections.Concurrent.ConcurrentDictionary<int, byte>();

        internal MockServer(long size, bool fastContent)
        {
            Size = size;
            _fastContent = fastContent;
            if (fastContent)
            {
                _tile = new byte[1024 * 1024];
                Pattern.Fill(_tile, 0);
            }

            int port = FreePort();
            string prefix = "http://127.0.0.1:" + port.ToString(CultureInfo.InvariantCulture) + "/";
            _listener.Prefixes.Add(prefix);
            _listener.Start();
            Url = prefix + "file";
            RedirectUrl = prefix + "redirect";
            _loop = Task.Run(AcceptLoopAsync);
        }

        private static int FreePort()
        {
            System.Net.Sockets.TcpListener l = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
            l.Start();
            int p = ((IPEndPoint)l.LocalEndpoint).Port;
            l.Stop();
            return p;
        }

        private async Task AcceptLoopAsync()
        {
            while (!_cts.IsCancellationRequested)
            {
                HttpListenerContext ctx;
                try { ctx = await _listener.GetContextAsync().ConfigureAwait(false); }
                catch { return; }
                _ = Task.Run(() => HandleAsync(ctx));
            }
        }

        private async Task HandleAsync(HttpListenerContext ctx)
        {
            int now = Interlocked.Increment(ref _concurrent);
            int peak = Volatile.Read(ref MaxConcurrent);
            while (now > peak)
            {
                int prior = Interlocked.CompareExchange(ref MaxConcurrent, now, peak);
                if (prior == peak) break;
                peak = prior;
            }

            try
            {
                if (ctx.Request.Url.AbsolutePath == "/redirect")
                {
                    ctx.Response.StatusCode = 302;
                    ctx.Response.RedirectLocation = Url;
                    ctx.Response.Close();
                    return;
                }

                int seq = Interlocked.Increment(ref RequestCount);
                RequestTicks.Enqueue(Environment.TickCount64);
                AcceptEncodings.Enqueue(ctx.Request.Headers["Accept-Encoding"] ?? "");
                try
                {
                    IPEndPoint rep = ctx.Request.RemoteEndPoint;
                    if (rep != null) RemotePorts.TryAdd(rep.Port, 0);
                }
                catch { }

                long from = 0;
                long to = Size - 1;
                long rawTo = -1;
                bool hasRange = TryParseRange(ctx.Request.Headers["Range"], Size, ref from, ref to, ref rawTo);
                if (hasRange && WholeFileWhenRangeEndsPastEof && rawTo >= Size) hasRange = false;

                MockFault fault = MockFault.None;
                Func<int, long, long, MockFault> hook = FaultHook;
                if (hook != null) fault = hook(seq, from, to);

                if (fault == MockFault.Status403) { ctx.Response.StatusCode = 403; ctx.Response.Close(); return; }
                if (fault == MockFault.Status500) { ctx.Response.StatusCode = 500; ctx.Response.Close(); return; }
                if (fault == MockFault.Status503RetryAfter)
                {
                    ctx.Response.StatusCode = 503;
                    string ra = RetryAfter;
                    if (ra != null) ctx.Response.Headers["Retry-After"] = ra;
                    ctx.Response.Close();
                    return;
                }

                if (fault == MockFault.IgnoreRange || !hasRange)
                {
                    from = 0;
                    to = Size - 1;
                    ctx.Response.StatusCode = 200;
                }
                else
                {
                    ctx.Response.StatusCode = 206;
                    Func<long, long, string> crh = ContentRangeHeader;
                    if (crh != null)
                    {
                        // null from the hook => a 206 with no Content-Range header at all
                        string header = crh(from, to);
                        if (header != null) ctx.Response.Headers["Content-Range"] = header;
                    }
                    else
                    {
                        long advertised = Size;
                        Func<long, long, long> crt = ContentRangeTotal;
                        if (crt != null) advertised = crt(from, to);
                        ctx.Response.Headers["Content-Range"] = "bytes " + from + "-" + to + "/" + advertised;
                    }
                }

                string ce = ContentEncoding;
                if (ce != null) ctx.Response.Headers["Content-Encoding"] = ce;

                long len = to - from + 1;
                long bodyFrom = from;
                Func<long, long, long> bo = BodyOffset;
                if (bo != null) bodyFrom = bo(from, to);

                ctx.Response.ContentType = "application/octet-stream";
                ctx.Response.ContentLength64 = len;

                long send = fault == MockFault.TruncateHalf ? len / 2 : len;
                int rate = fault == MockFault.Trickle ? TrickleBytesPerSec : ThrottleBytesPerSec;
                await WriteRangeAsync(ctx.Response.OutputStream, bodyFrom, send, rate).ConfigureAwait(false);

                if (fault == MockFault.TruncateHalf) { ctx.Response.Abort(); return; }
                ctx.Response.Close();
            }
            catch
            {
                try { ctx.Response.Abort(); } catch { }
            }
            finally
            {
                Interlocked.Decrement(ref _concurrent);
            }
        }

        private async Task WriteRangeAsync(System.IO.Stream output, long from, long count, int bytesPerSec)
        {
            const int Block = 256 * 1024;
            byte[] buf = ArrayPool<byte>.Shared.Rent(Block);
            try
            {
                long done = 0;
                long startTicks = Environment.TickCount64;
                while (done < count)
                {
                    int n = (int)Math.Min(Block, count - done);
                    if (_fastContent)
                    {
                        long abs = from + done;
                        int filled = 0;
                        while (filled < n)
                        {
                            int tileOff = (int)((abs + filled) % _tile.Length);
                            int take = Math.Min(n - filled, _tile.Length - tileOff);
                            Buffer.BlockCopy(_tile, tileOff, buf, filled, take);
                            filled += take;
                        }
                    }
                    else
                    {
                        Pattern.Fill(new Span<byte>(buf, 0, n), from + done);
                    }

                    await output.WriteAsync(buf.AsMemory(0, n), _cts.Token).ConfigureAwait(false);
                    done += n;
                    Interlocked.Add(ref BytesServed, n);

                    if (bytesPerSec > 0)
                    {
                        long shouldTakeMs = done * 1000L / bytesPerSec;
                        long elapsed = Environment.TickCount64 - startTicks;
                        if (shouldTakeMs > elapsed) await Task.Delay((int)(shouldTakeMs - elapsed), _cts.Token).ConfigureAwait(false);
                    }
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buf);
            }
        }

        private static bool TryParseRange(string header, long size, ref long from, ref long to, ref long rawTo)
        {
            if (string.IsNullOrWhiteSpace(header)) return false;
            header = header.Trim();
            if (!header.StartsWith("bytes=", StringComparison.OrdinalIgnoreCase)) return false;
            string spec = header.Substring(6).Trim();
            int dash = spec.IndexOf('-');
            if (dash < 0) return false;
            string a = spec.Substring(0, dash).Trim();
            string b = spec.Substring(dash + 1).Trim();
            long f;
            if (!long.TryParse(a, NumberStyles.Integer, CultureInfo.InvariantCulture, out f)) return false;
            long t;
            if (b.Length == 0) t = size - 1;
            else if (!long.TryParse(b, NumberStyles.Integer, CultureInfo.InvariantCulture, out t)) return false;
            rawTo = b.Length == 0 ? -1 : t;   // what the client literally asked for, before clamping
            if (t > size - 1) t = size - 1;
            if (f > t) return false;
            from = f;
            to = t;
            return true;
        }

        internal void ResetCounters()
        {
            Interlocked.Exchange(ref RequestCount, 0);
            Interlocked.Exchange(ref BytesServed, 0);
            Interlocked.Exchange(ref MaxConcurrent, 0);
            RemotePorts.Clear();
            RequestTicks.Clear();
            AcceptEncodings.Clear();
        }

        public void Dispose()
        {
            try { _cts.Cancel(); } catch { }
            try { _listener.Stop(); } catch { }
            try { _listener.Close(); } catch { }
        }
    }
}
