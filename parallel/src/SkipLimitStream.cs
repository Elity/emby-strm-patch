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
        private long _toSkip;
        private long _remaining;
        private int _disposed;

        internal SkipLimitStream(HttpResponseMessage response, Stream inner, HttpClient client, long skip, long limit)
        {
            _response = response;
            _inner = inner;
            _client = client;
            _toSkip = skip;
            _remaining = limit;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
        {
            while (_toSkip > 0)
            {
                int want = (int)Math.Min(_toSkip, 64 * 1024);
                byte[] scratch = ArrayPool<byte>.Shared.Rent(want);
                try
                {
                    int n = await _inner.ReadAsync(scratch.AsMemory(0, want), cancellationToken).ConfigureAwait(false);
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
            int read = await _inner.ReadAsync(buffer.Slice(0, take), cancellationToken).ConfigureAwait(false);
            if (read <= 0) throw new IOException("Stream ended " + _remaining + " bytes short of the requested range.");
            _remaining -= read;
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
                try { _inner.Dispose(); } catch { }
                try { _response.Dispose(); } catch { }
                if (_client != null) { try { _client.Dispose(); } catch { } }
            }
            base.Dispose(disposing);
        }
    }
}
