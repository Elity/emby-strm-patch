using System;
using System.Buffers;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace EmbyStrmParallel
{
    /// <summary>
    /// Forward-only stream that delivers an exact byte range, fetched over N concurrent HTTP
    /// Range requests but handed to the consumer strictly in order.
    ///
    /// Shape: a ring of `Slots` reorder slots. Chunk i always lands in slot (i % Slots) and each
    /// slot is a bounded channel sized to hold one whole chunk. Memory ceiling is therefore
    /// Slots * ChunkSize regardless of file size. A worker cannot start chunk i until the reader
    /// has finished chunk i-Slots, which is the backpressure.
    ///
    /// Chunks are claimed under a single assignment lock, and the slot is acquired *inside* that
    /// lock. Claiming the index and the slot non-atomically would let a worker holding index i+S
    /// win the race for slot (i % S) against the worker holding index i, and deadlock the reader.
    /// Waiting for slots in index order costs nothing: slot (i % S) always frees before
    /// slot ((i+1) % S), because the reader drains in order.
    /// </summary>
    internal sealed class ParallelRangeStream : Stream
    {
        private sealed class Slot
        {
            internal readonly SemaphoreSlim Free = new SemaphoreSlim(1, 1);
            internal readonly SemaphoreSlim Ready = new SemaphoreSlim(0);
            internal Channel<Block> Chan;
        }

        private readonly ParallelFetchOptions _o;
        private readonly ChunkDownloader _downloader;
        private readonly ChunkSchedule _schedule;
        private readonly Slot[] _slots;
        private readonly SemaphoreSlim _assign = new SemaphoreSlim(1, 1);
        private readonly CancellationTokenSource _cts;
        private readonly Task _workers;
        private readonly string _tag;
        private readonly StreamStats _stats = new StreamStats();
        private HttpClient _client;   // owned: this stream's private connection pool

        private readonly long _rangeStart;
        private readonly long _totalToRead;
        private readonly long _chunkCount;

        private static int _nextStreamId;

        private long _nextChunk;              // guarded by _assign
        private int _faulted;                 // 0/1, stops further chunk claims

        private HttpResponseMessage _preOpened;

        // reader-side state (single consumer)
        private long _readerChunk;
        private bool _slotOpen;
        private Slot _curSlot;
        private ChannelReader<Block> _curReader;
        private byte[] _cur;
        private int _curPos;
        private int _curLen;
        private long _delivered;
        private Exception _fault;
        private int _disposed;

        internal ParallelRangeStream(HttpClient client, string url, long rangeStart, long totalToRead,
                                     ParallelFetchOptions options, HttpResponseMessage preOpenedFirstChunk,
                                     CancellationToken cancellationToken)
        {
            _o = options;
            _rangeStart = rangeStart;
            _totalToRead = totalToRead;
            _preOpened = preOpenedFirstChunk;
            _client = client;
            _schedule = new ChunkSchedule(totalToRead, options.FirstChunkSize, options.ChunkSize);
            _chunkCount = _schedule.Count;
            StreamId = Interlocked.Increment(ref _nextStreamId);
            _tag = "#" + StreamId;

            _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _downloader = new ChunkDownloader(client, url, options, _tag, _stats);

            int slotCount = (int)Math.Min(options.Slots, _chunkCount);
            if (slotCount < 1) slotCount = 1;
            _slots = new Slot[slotCount];
            for (int i = 0; i < slotCount; i++) _slots[i] = new Slot();

            int workerCount = (int)Math.Min(options.Connections, _chunkCount);
            if (workerCount < 1) workerCount = 1;

            Task[] tasks = new Task[workerCount];
            for (int i = 0; i < workerCount; i++)
            {
                int index = i;
                tasks[i] = Task.Run(() => WorkerLoopAsync(index));
            }
            _workers = Task.WhenAll(tasks);
        }

        internal int StreamId { get; }
        internal long TotalToRead { get { return _totalToRead; } }
        internal long ChunkCount { get { return _chunkCount; } }
        internal int SlotCount { get { return _slots.Length; } }
        internal long MemoryCeilingBytes { get { return (long)_slots.Length * _o.ChunkSize; } }
        /// <summary>Completes when every worker has exited. Used by tests to prove teardown is prompt.</summary>
        internal Task WorkersCompletion { get { return _workers; } }

        // ------------------------------------------------------------------ producer

        private async Task WorkerLoopAsync(int workerIndex)
        {
            try
            {
                // Connection slow-start: workers past InitialConnections join gradually, so a
                // stream abandoned shortly after opening never costs the full connection count.
                if (workerIndex >= _o.InitialConnections && _o.ConnectionRampInterval > TimeSpan.Zero)
                {
                    int steps = workerIndex - _o.InitialConnections + 1;
                    await Task.Delay(TimeSpan.FromTicks(_o.ConnectionRampInterval.Ticks * steps), _cts.Token)
                              .ConfigureAwait(false);
                }

                while (true)
                {
                    long index;
                    Slot slot;

                    // Cheapest possible bail-out for the Dispose-races-worker-startup case:
                    // never issue an origin request for a stream that is already abandoned.
                    if (Volatile.Read(ref _disposed) != 0) return;

                    await _assign.WaitAsync(_cts.Token).ConfigureAwait(false);
                    try
                    {
                        if (Volatile.Read(ref _faulted) != 0) return;
                        if (Volatile.Read(ref _disposed) != 0) return;
                        if (_nextChunk >= _chunkCount) return;
                        index = _nextChunk;
                        slot = _slots[(int)(index % _slots.Length)];
                        await slot.Free.WaitAsync(_cts.Token).ConfigureAwait(false);
                        _nextChunk++;
                    }
                    finally
                    {
                        _assign.Release();
                    }

                    Channel<Block> chan = Channel.CreateBounded<Block>(new BoundedChannelOptions(_o.BlocksPerChunk)
                    {
                        SingleReader = true,
                        SingleWriter = true,
                        FullMode = BoundedChannelFullMode.Wait,
                        AllowSynchronousContinuations = false
                    });
                    slot.Chan = chan;
                    slot.Ready.Release();

                    long relStart, relLength;
                    _schedule.Range(index, out relStart, out relLength);
                    long start = _rangeStart + relStart;
                    long end = start + relLength - 1;
                    HttpResponseMessage pre = index == 0 ? Interlocked.Exchange(ref _preOpened, null) : null;

                    try
                    {
                        await _downloader.DownloadAsync(start, end, chan.Writer, pre, _cts.Token).ConfigureAwait(false);
                        chan.Writer.TryComplete();
                        _stats.ChunkDone();
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Exchange(ref _faulted, 1);
                        // Poison the channel: the reader re-throws this instead of seeing a short read.
                        chan.Writer.TryComplete(ex);
                        FetchLog.Write(_tag + " chunk " + index + " [" + start + "-" + end + "] FAILED: " + FetchLog.Describe(ex));
                        return;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // stream disposed / caller cancelled
            }
            catch (Exception ex)
            {
                Interlocked.Exchange(ref _faulted, 1);
                FetchLog.Write(_tag + " worker aborted: " + FetchLog.Describe(ex));
            }
        }

        // ------------------------------------------------------------------ consumer

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
        {
            if (_fault != null) throw Rethrow(_fault);
            if (Volatile.Read(ref _disposed) != 0) throw new ObjectDisposedException(nameof(ParallelRangeStream));
            if (buffer.Length == 0) return 0;

            while (true)
            {
                if (_curPos < _curLen)
                {
                    // The host copies with ~80 KiB buffers, so top the caller's buffer up from
                    // any block already sitting in this slot. Only ever synchronously - we never
                    // wait for more data once we have something to return.
                    int written = 0;
                    while (written < buffer.Length)
                    {
                        if (_curPos >= _curLen)
                        {
                            Block extra;
                            ChannelReader<Block> r = _curReader;
                            if (r == null || !r.TryRead(out extra)) break;
                            Adopt(extra);
                        }
                        int n = Math.Min(_curLen - _curPos, buffer.Length - written);
                        new ReadOnlySpan<byte>(_cur, _curPos, n).CopyTo(buffer.Span.Slice(written));
                        _curPos += n;
                        _delivered += n;
                        written += n;
                        if (_curPos >= _curLen) ReleaseCurrentBlock();
                    }
                    return written;
                }

                // Returning 0 must mean genuine end of the requested range and nothing else.
                if (_delivered >= _totalToRead) return 0;

                await AdvanceAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Pulls the next block, opening/closing reorder slots as needed. Returns false when a slot boundary was crossed without producing data.</summary>
        private async Task<bool> AdvanceAsync(CancellationToken cancellationToken)
        {
            CancellationTokenSource linked = null;
            CancellationToken token = _cts.Token;
            if (cancellationToken.CanBeCanceled)
            {
                linked = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, cancellationToken);
                token = linked.Token;
            }

            try
            {
                if (!_slotOpen)
                {
                    if (_readerChunk >= _chunkCount)
                    {
                        throw Fail(new IOException("Parallel fetch ran out of chunks after " + _delivered +
                                                   " of " + _totalToRead + " bytes."));
                    }
                    _curSlot = _slots[(int)(_readerChunk % _slots.Length)];
                    await _curSlot.Ready.WaitAsync(token).ConfigureAwait(false);
                    _curReader = _curSlot.Chan.Reader;
                    _slotOpen = true;
                }

                Block block;
                if (_curReader.TryRead(out block))
                {
                    Adopt(block);
                    return true;
                }

                bool more;
                try
                {
                    more = await _curReader.WaitToReadAsync(token).ConfigureAwait(false);
                }
                catch (ChannelClosedException cce)
                {
                    CloseSlot();
                    throw Fail(cce.InnerException ?? cce);
                }

                if (more)
                {
                    if (_curReader.TryRead(out block))
                    {
                        Adopt(block);
                        return true;
                    }
                    return false;
                }

                // chunk delivered cleanly
                CloseSlot();
                return false;
            }
            catch (OperationCanceledException)
            {
                if (cancellationToken.IsCancellationRequested) throw;
                if (_fault != null) throw Rethrow(_fault);
                throw Fail(new IOException("Parallel fetch was aborted."));
            }
            finally
            {
                if (linked != null) linked.Dispose();
            }
        }

        private void Adopt(Block block)
        {
            _cur = block.Buffer;
            _curPos = 0;
            _curLen = block.Length;
            FetchMetrics.RemoveBuffered(block.Length);
        }

        private void ReleaseCurrentBlock()
        {
            byte[] b = _cur;
            _cur = null;
            _curPos = 0;
            _curLen = 0;
            if (b != null) ArrayPool<byte>.Shared.Return(b);
        }

        private void CloseSlot()
        {
            if (!_slotOpen) return;
            _slotOpen = false;
            _curReader = null;
            Slot s = _curSlot;
            _curSlot = null;
            _readerChunk++;
            if (s != null)
            {
                s.Chan = null;
                try { s.Free.Release(); } catch (ObjectDisposedException) { }
            }
        }

        private Exception Fail(Exception ex)
        {
            if (_fault == null) _fault = ex;
            return _fault;
        }

        private static Exception Rethrow(Exception ex)
        {
            return new IOException("Parallel fetch failed: " + ex.Message, ex);
        }

        // ------------------------------------------------------------------ Stream plumbing

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
        public override bool CanTimeout { get { return false; } }

        /// <summary>Number of bytes this stream will deliver in total. Safe to read; does not imply seekability.</summary>
        public override long Length { get { return _totalToRead; } }

        public override long Position
        {
            get { return _delivered; }
            set { throw new NotSupportedException("ParallelRangeStream is forward-only."); }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException("ParallelRangeStream is forward-only.");
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        protected override void Dispose(bool disposing)
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                // Order matters. Set the fault flag first so a worker that is *between*
                // Task.Run and its first await cannot claim a chunk and issue a request we
                // would then have to chase down.
                Interlocked.Exchange(ref _faulted, 1);
                try { _cts.Cancel(); } catch { }

                // Kill this stream's private connection pool outright. Cancellation alone left
                // sockets that the next stream could inherit and stall on; disposing the handler
                // closes them now instead of at some idle timeout.
                HttpClient client = Interlocked.Exchange(ref _client, null);
                if (client != null) { try { client.Dispose(); } catch { } }

                ReleaseCurrentBlock();

                HttpResponseMessage pre = Interlocked.Exchange(ref _preOpened, null);
                if (pre != null) { try { pre.Dispose(); } catch { } }

                // Release the whole read-ahead window now, not at the next GC: complete every
                // slot channel (which throws a worker parked on a full channel straight out of
                // WriteAsync) and hand the pooled blocks back.
                DrainAllSlots();
                LogSummary();

                // Do not block here: Dispose may run on a request thread. Workers unwind on the
                // cancellation above; the CTS is disposed once they are all gone.
                Task w = _workers;
                if (w != null)
                {
                    w.ContinueWith(_ => { try { _cts.Dispose(); } catch { } }, TaskScheduler.Default);
                }
            }
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            Task w = _workers;
            if (w != null)
            {
                try { await w.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false); }
                catch { }
            }
        }

        /// <summary>
        /// One line per stream at close. A throughput collapse used to produce no log output at
        /// all - every request succeeded, they just trickled - so this reports the outcome rather
        /// than only the failures.
        /// </summary>
        private void LogSummary()
        {
            if (!FetchLog.IsEnabled) return;
            long ms = Math.Max(1, Environment.TickCount64 - _stats.StartTicks);
            double mbps = _delivered * 8.0 / (ms / 1000.0) / 1e6;
            bool complete = _delivered >= _totalToRead;
            FetchLog.Write(_tag + " closed " + (complete ? "complete" : "ABANDONED") +
                           " delivered=" + FetchLog.Size(_delivered) + "/" + FetchLog.Size(_totalToRead) +
                           " elapsed=" + (ms / 1000.0).ToString("0.0", CultureInfo.InvariantCulture) + "s" +
                           " rate=" + mbps.ToString("0.00", CultureInfo.InvariantCulture) + "Mbps" +
                           " chunks=" + _stats.ChunksCompleted + "/" + _chunkCount +
                           " retries=" + _stats.Retries + " (slow=" + _stats.SlowRetries + ")" +
                           (_fault != null ? " fault=" + _fault.GetType().Name : ""));
        }

        /// <summary>
        /// Frees the entire read-ahead window. Completing each writer is the important part:
        /// a worker parked on a full bounded channel is otherwise waiting on a reader that is
        /// never coming back, and only notices the cancellation once its write unblocks.
        /// </summary>
        private void DrainAllSlots()
        {
            _curReader = null;
            _slotOpen = false;
            Slot[] slots = _slots;
            if (slots == null) return;
            for (int i = 0; i < slots.Length; i++)
            {
                Slot s = slots[i];
                if (s == null) continue;
                Channel<Block> c = s.Chan;
                if (c == null) continue;
                try { c.Writer.TryComplete(); } catch { }
                Block b;
                while (c.Reader.TryRead(out b))
                {
                    FetchMetrics.RemoveBuffered(b.Length);
                    try { ArrayPool<byte>.Shared.Return(b.Buffer); } catch { }
                }
                s.Chan = null;
            }
        }
    }
}
