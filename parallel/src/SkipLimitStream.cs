using System;
using System.Buffers;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace EmbyStrmParallel
{
    /// <summary>
    /// Degraded path: the origin answered a Range request with 200 OK, i.e. it does not do
    /// ranges. Parallelism is impossible, so deliver the correct bytes over the one connection
    /// we already have - skip `skip` bytes, hand out `limit`, then stop. Correctness over speed.
    /// </summary>
    internal sealed class SkipLimitStream : Stream
    {
        private readonly HttpResponseMessage _response;
        private readonly Stream _inner;
        private readonly HttpClient _client;
        private readonly TimeSpan _readIdleTimeout;
        private readonly long _skipTotal;
        private readonly long _limit;
        private readonly long _startTicks = Environment.TickCount64;
        private long _toSkip;
        private long _remaining;
        private long _delivered;
        private int _disposed;

        internal SkipLimitStream(HttpResponseMessage response, Stream inner, HttpClient client,
                                 long skip, long limit, TimeSpan readIdleTimeout)
        {
            _response = response;
            _inner = inner;
            _client = client;
            _readIdleTimeout = readIdleTimeout;
            _skipTotal = skip;
            _limit = limit;
            _toSkip = skip;
            _remaining = limit;
        }

        /// <summary>
        /// One network read with a deadline on the read itself.
        ///
        /// Without this the only thing that could ever end a dead body was the caller's token:
        /// an origin that sent headers and then nothing held a request thread, a socket and the
        /// client's player indefinitely. The timer covers the underlying read only, so a
        /// consumer that stops asking is never mistaken for a stalled origin - it simply is not
        /// inside this method.
        /// </summary>
        private async ValueTask<int> ReadInnerAsync(Memory<byte> dst, CancellationToken ct)
        {
            using (CancellationTokenSource idle = CancellationTokenSource.CreateLinkedTokenSource(ct))
            {
                if (_readIdleTimeout > TimeSpan.Zero) idle.CancelAfter(_readIdleTimeout);
                try
                {
                    return await _inner.ReadAsync(dst, idle.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                {
                    throw new IOException("Origin sent no data for " +
                        _readIdleTimeout.TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture) +
                        "s on the single-connection path.");
                }
            }
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
        {
            while (_toSkip > 0)
            {
                int want = (int)Math.Min(_toSkip, 64 * 1024);
                byte[] scratch = ArrayPool<byte>.Shared.Rent(want);
                try
                {
                    int n = await ReadInnerAsync(scratch.AsMemory(0, want), cancellationToken).ConfigureAwait(false);
                    if (n <= 0) throw new IOException("Stream ended while skipping to the requested offset.");
                    _toSkip -= n;
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(scratch);
                }
            }

            if (_remaining <= 0 || buffer.Length == 0) return 0;
            int take = (int)Math.Min((long)buffer.Length, _remaining);
            int read = await ReadInnerAsync(buffer.Slice(0, take), cancellationToken).ConfigureAwait(false);
            if (read <= 0) throw new IOException("Stream ended " + _remaining + " bytes short of the requested range.");
            _remaining -= read;
            _delivered += read;
            return read;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return ReadAsync(new Memory<byte>(buffer, offset, count), CancellationToken.None).AsTask().GetAwaiter().GetResult();
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return ReadAsync(new Memory<byte>(buffer, offset, count), cancellationToken).AsTask();
        }

        public override bool CanRead { get { return true; } }
        public override bool CanSeek { get { return false; } }
        public override bool CanWrite { get { return false; } }
        public override long Length { get { throw new NotSupportedException(); } }
        public override long Position
        {
            get { throw new NotSupportedException(); }
            set { throw new NotSupportedException(); }
        }
        public override long Seek(long offset, SeekOrigin origin) { throw new NotSupportedException(); }
        public override void SetLength(long value) { throw new NotSupportedException(); }
        public override void Write(byte[] buffer, int offset, int count) { throw new NotSupportedException(); }
        public override void Flush() { }

        protected override void Dispose(bool disposing)
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                // The parallel path reports one line per stream at close; this one used to report
                // nothing at all, which made the most expensive path in the component the only
                // invisible one. When production discarded 1.47 GB to deliver 637 KB there was no
                // way to tell from the log how long it took or whether it even finished.
                LogSummary();
                try { _inner.Dispose(); } catch { }
                try { _response.Dispose(); } catch { }
                if (_client != null) { try { _client.Dispose(); } catch { } }
            }
            base.Dispose(disposing);
        }

        private void LogSummary()
        {
            if (!FetchLog.IsEnabled) return;
            long ms = Math.Max(1, Environment.TickCount64 - _startTicks);
            FetchLog.Write("single-conn closed " + (_delivered >= _limit ? "complete" : "ABANDONED") +
                           " skipped=" + FetchLog.Size(_skipTotal - _toSkip) + "/" + FetchLog.Size(_skipTotal) +
                           " delivered=" + FetchLog.Size(_delivered) + "/" + FetchLog.Size(_limit) +
                           " elapsed=" + (ms / 1000.0).ToString("0.0", System.Globalization.CultureInfo.InvariantCulture) + "s");
        }
    }
}
