using System;
using System.Buffers;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace EmbyStrmParallel
{
    /// <summary>
    /// Fetches one byte span [start, endInclusive] over HTTP Range and streams it into a
    /// bounded channel. Retries are *resumed*, never restarted: a chunk that dies after
    /// 3 MB re-requests only the remaining bytes, so already-delivered data is never
    /// duplicated and the retry cannot be defeated by a partially written channel.
    ///
    /// Failure policy: after MaxAttempts the exception propagates. The caller poisons the
    /// channel with it so the reader throws instead of seeing a short read. Truncation is
    /// never reported as success.
    /// </summary>
    /// <summary>A connection that is technically working but far too slow to be worth keeping.</summary>
    internal sealed class SlowConnectionException : IOException
    {
        internal SlowConnectionException(string message) : base(message) { }
    }

    /// <summary>
    /// No origin permit arrived within the watchdog. Deliberately NOT retryable: another attempt
    /// would only re-join the same queue behind the same holder that is not letting go, and four
    /// of those in a row would take the time-to-error from bounded to eight minutes.
    /// </summary>
    internal sealed class OriginBudgetTimeoutException : IOException
    {
        internal OriginBudgetTimeoutException(string message) : base(message) { }
    }

    internal sealed class ChunkDownloader
    {
        /// <summary>Wait long enough to be worth a line in the log; short enough to see it coming.</summary>
        private const long LongPermitWaitMs = 5000;

        private readonly HttpClient _client;
        private readonly string _url;
        private readonly string _originKey;
        private readonly ParallelFetchOptions _o;
        private readonly string _tag;
        private readonly StreamStats _stats;
        private readonly long _resourceTotal;

        internal ChunkDownloader(HttpClient client, string url, ParallelFetchOptions options, string tag,
                                 StreamStats stats, long resourceTotal)
        {
            _client = client;
            _url = url;
            _originKey = OriginBudget.KeyFor(url);
            _o = options;
            _tag = tag;
            _stats = stats;
            _resourceTotal = resourceTotal;
        }

        /// <summary>
        /// Publish progress must survive an exception. If it lived in a local that is only
        /// assigned on PumpAsync's normal return, a body that dies mid-chunk would rewind the
        /// cursor to the chunk start while the already-published blocks stayed in the channel,
        /// and the retry would deliver those bytes a second time.
        /// </summary>
        private sealed class Cursor
        {
            internal long Pos;
            /// <summary>Environment.TickCount64 of the last byte published. Resets the stall budget.</summary>
            internal long LastProgressTicks;
        }

        internal async Task DownloadAsync(long start, long endInclusive, ChannelWriter<Block> writer,
                                          PreOpenedChunk preOpened, CancellationToken ct)
        {
            Cursor cursor = new Cursor { Pos = start, LastProgressTicks = Environment.TickCount64 };
            // Every attempt, so the backoff keeps growing even through a run of unanswered ones -
            // without this an outage would spin at the 250 ms base delay.
            int attempt = 0;
            // Only the attempts the origin answered. This, not `attempt`, is what MaxAttempts caps.
            int answered = 0;
            PreOpenedChunk pre = preOpened;
            long stallBudgetMs = (long)_o.StallBudget.TotalMilliseconds;

            // One deadline for the WHOLE chunk, permit queuing included.
            //
            // The stall budget alone cannot bound this: queuing for our own limiter is credited
            // back to it (see AcquirePermitAsync), so four attempts that each waited 119 s and
            // then succeeded would stretch time-to-error past eight minutes with the consumer -
            // which is reading in order, behind this chunk - seeing nothing at all. Bounding the
            // wait by what is left of this deadline caps the total at stall budget + one permit
            // timeout, however the two are divided up.
            long chunkDeadline = Environment.TickCount64 + stallBudgetMs +
                                 (long)_o.OriginPermitTimeout.TotalMilliseconds;

            // The pre-opened probe response is a LIVE 206 body holding an origin connection, and
            // an origin permit alongside it. Every exit path must dispose it, including the
            // cancellation check on the very first line of the loop - otherwise abandoning a
            // stream before its first read leaks a connection that keeps pulling from the origin,
            // plus the permit that was accounting for it, which is unrecoverable.
            try
            {
            while (cursor.Pos <= endInclusive)
            {
                ct.ThrowIfCancellationRequested();

                // Bound time-to-error. Attempts that each hang until the header timeout would
                // otherwise add up, and in-order delivery means the consumer sees nothing for
                // the whole total.
                long remainingMs = stallBudgetMs - (Environment.TickCount64 - cursor.LastProgressTicks);
                if (remainingMs <= 0)
                {
                    throw new IOException("Chunk [" + start + "-" + endInclusive + "] made no progress for " +
                                          stallBudgetMs + " ms (stopped at offset " + cursor.Pos + ").");
                }

                HttpResponseMessage response = null;
                CancellationTokenSource phase = null;
                OriginBudget.Permit permit = null;
                long retryAfterMs = -1;
                long backoffMs = -1;
                bool fromProbe = false;
                try
                {
                    phase = CancellationTokenSource.CreateLinkedTokenSource(ct);

                    if (pre != null)
                    {
                        // Adopt both halves: the probe already paid for this connection, so
                        // taking a second permit would double-count the same socket.
                        response = pre.Response;
                        permit = pre.Permit;
                        pre = null; // consumed; only valid for the very first attempt of chunk 0
                        fromProbe = true;
                    }
                    else
                    {
                        permit = await AcquirePermitAsync(cursor, chunkDeadline, ct).ConfigureAwait(false);
                        if (permit == null)
                        {
                            throw new OriginBudgetTimeoutException(
                                "No origin permit for chunk [" + start + "-" + endInclusive + "] within " +
                                _o.OriginPermitTimeout.TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture) +
                                "s (max-origin-connections=" + _o.MaxOriginConnections + ").");
                        }
                        // Recomputed: waiting for our own limiter was added back to the stall
                        // budget, so the header timeout still gets the share it was promised.
                        remainingMs = stallBudgetMs - (Environment.TickCount64 - cursor.LastProgressTicks);
                        if (remainingMs <= 0)
                        {
                            throw new IOException("Chunk [" + start + "-" + endInclusive + "] made no progress for " +
                                                  stallBudgetMs + " ms (stopped at offset " + cursor.Pos + ").");
                        }

                        double headerMs = Math.Min(_o.ResponseHeadersTimeout.TotalMilliseconds, remainingMs);
                        phase.CancelAfter(TimeSpan.FromMilliseconds(headerMs));
                        response = await SendAsync(cursor.Pos, endInclusive, phase.Token).ConfigureAwait(false);
                    }

                    retryAfterMs = HttpRangeHelper.RetryAfterMs(response);
                    ValidateResponse(response, cursor.Pos, endInclusive, fromProbe);

                    using (Stream body = await response.Content.ReadAsStreamAsync(phase.Token).ConfigureAwait(false))
                    {
                        await PumpAsync(body, cursor, endInclusive, writer, phase, ct).ConfigureAwait(false);
                    }

                    if (cursor.Pos > endInclusive) return;

                    // Body ended before the range did. Treat as transient and resume.
                    throw new IOException("Range body ended early at offset " + cursor.Pos.ToString(System.Globalization.CultureInfo.InvariantCulture) +
                                          ", expected through " + endInclusive.ToString(System.Globalization.CultureInfo.InvariantCulture) + ".");
                }
                catch (Exception ex)
                {
                    if (ct.IsCancellationRequested) throw;

                    // Only an attempt the origin ANSWERED counts against MaxAttempts.
                    //
                    // The two bounds here are not interchangeable and each has exactly one shape
                    // it can police. The stall budget cannot stop a connection that trickles: the
                    // throughput floor publishes a short block before it throws, which resets
                    // LastProgressTicks, so MaxAttempts is that shape's only brake. And
                    // MaxAttempts is far too tight for the opposite shape: four attempts at
                    // 250/500/1000 ms give up 1.75-2.19 s in, which on 2026-08-31 killed two live
                    // streams over DNS wobbles that never lasted past 2.25 s - with 28 of the
                    // 30 s stall budget unspent, because nothing had been published for it to
                    // measure against.
                    //
                    // So: answered -> count it, the origin is talking and repeating is on us.
                    // Unanswered -> the stall budget owns it, which is what it was built for.
                    if (response != null) answered++;

                    if (!HttpRangeHelper.IsRetryable(ex) || answered >= _o.MaxAttempts)
                    {
                        throw new IOException(
                            "Parallel chunk [" + start.ToString(System.Globalization.CultureInfo.InvariantCulture) + "-" +
                            endInclusive.ToString(System.Globalization.CultureInfo.InvariantCulture) + "] failed after " +
                            (attempt + 1).ToString(System.Globalization.CultureInfo.InvariantCulture) + " attempt(s) at offset " +
                            cursor.Pos.ToString(System.Globalization.CultureInfo.InvariantCulture) + ".", ex);
                    }

                    attempt++;
                    _stats.Retry();
                    // Bounded by whatever is left of the stall budget: an origin that answers
                    // "Retry-After: 3600" must not park a chunk for an hour, but neither should
                    // we come back before it asked. Sleeping out the remainder means the next
                    // pass through the loop reports the failure, which is the right outcome.
                    long budgetLeft = stallBudgetMs - (Environment.TickCount64 - cursor.LastProgressTicks);
                    long delay = HttpRangeHelper.RetryDelayMs(
                        HttpRangeHelper.BackoffMs(attempt, _o.RetryBaseDelayMs, _o.RetryMaxDelayMs), retryAfterMs);
                    if (delay > budgetLeft) delay = budgetLeft;
                    if (delay < 0) delay = 0;
                    FetchLog.Write(_tag + " chunk [" + start + "-" + endInclusive + "] retry " + attempt +
                                   " (answered " + answered + "/" + _o.MaxAttempts + ")" +
                                   " resuming at " + cursor.Pos + " after " + FetchLog.Describe(ex));
                    // Slept AFTER the finally below, never here: nothing of ours is in flight at
                    // the origin during a backoff, so holding the permit (and the dead response)
                    // through it hands a Retry-After the power to park the whole budget asleep -
                    // at exactly the moment the origin is already saying it is under pressure.
                    backoffMs = delay;
                }
                finally
                {
                    if (response != null) { try { response.Dispose(); } catch { } }
                    if (phase != null) phase.Dispose();
                    // Last, and after the response: the connection this permit accounts for has
                    // to be closed before the slot is offered to anyone else, or the origin
                    // briefly sees more concurrent requests than the budget allows - which is the
                    // exact overshoot the budget exists to prevent.
                    if (permit != null) permit.Dispose();
                }

                if (backoffMs > 0) await Task.Delay((int)backoffMs, ct).ConfigureAwait(false);
            }
            }
            finally
            {
                // Unconsumed probe response and its permit (cancelled before the first attempt
                // got to use them).
                if (pre != null) { try { pre.Dispose(); } catch { } }
            }
        }

        /// <summary>
        /// Takes an origin permit for ONE attempt.
        ///
        /// Attempt-scoped rather than worker-scoped, which is the difference between throttling
        /// and rationing: a worker only exits when its stream is finished, so a worker-scoped
        /// permit meant `max-origin-connections / connections` was really the number of streams
        /// allowed to play at once, and every stream past that froze on a semaphore with no
        /// error and no fallback.
        ///
        /// Time spent queuing is added back to the chunk's stall budget. Waiting for our OWN
        /// limiter is not the origin failing to make progress, and charging it there would turn
        /// heavy - but perfectly healthy - contention into chunk failures. The overall
        /// <paramref name="chunkDeadline"/> is what keeps that credit from being unbounded.
        /// </summary>
        private async Task<OriginBudget.Permit> AcquirePermitAsync(Cursor cursor, long chunkDeadline,
                                                                   CancellationToken ct)
        {
            long waitStart = Environment.TickCount64;
            long budgetMs = Math.Min((long)_o.OriginPermitTimeout.TotalMilliseconds, chunkDeadline - waitStart);
            if (budgetMs <= 0) return null;

            OriginBudget.Permit permit = null;
            _stats.PermitWaitBegin();
            try
            {
                permit = await OriginBudget.TryAcquireAsync(_originKey, _o.MaxOriginConnections,
                                                            TimeSpan.FromMilliseconds(budgetMs), ct)
                                           .ConfigureAwait(false);
                return permit;
            }
            finally
            {
                // In the finally so a cancelled or timed-out wait still counts. Accumulating
                // after the await instead was exactly backwards: the closing line reported
                // permitWait=0.0s precisely when the budget was the culprit. The waiter COUNT is
                // raised before the await for the same reason from the other side - a stream
                // disposed with workers still queued writes its summary before they unwind, so
                // the elapsed total alone can never see them.
                long waited = Environment.TickCount64 - waitStart;
                cursor.LastProgressTicks += waited;
                _stats.PermitWaitEnd(waited);
                if (waited >= LongPermitWaitMs)
                {
                    FetchLog.Write(_tag + " waited " +
                                   (waited / 1000.0).ToString("0.0", System.Globalization.CultureInfo.InvariantCulture) +
                                   "s for an origin permit (max-origin-connections=" + _o.MaxOriginConnections +
                                   (permit == null ? ") and gave up" : ")"));
                }
            }
        }

        private Task<HttpResponseMessage> SendAsync(long from, long toInclusive, CancellationToken ct)
        {
            // Always the ORIGINAL url: the 302 is re-followed and the time-limited target re-signed.
            HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Get, _url);
            req.Headers.Range = new RangeHeaderValue(from, toInclusive);
            req.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue("identity"));
            req.Version = HttpVersion.Version11;
            req.VersionPolicy = HttpVersionPolicy.RequestVersionOrLower;
            return _client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct);
        }

        /// <summary>
        /// Everything that must hold before a single byte of this response is spliced into the
        /// output. All of it is about POSITION, not size: a chunk that arrives with the right
        /// length from the wrong offset is invisible downstream, because the byte count still
        /// adds up and the stream still ends where it should.
        /// </summary>
        private void ValidateResponse(HttpResponseMessage response, long expectedFrom, long expectedTo, bool bodyMayRunLonger)
        {
            if (response.StatusCode != HttpStatusCode.PartialContent)
            {
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    // Server ignored Range mid-stream. Splicing this body in would corrupt output.
                    throw new NotSupportedException("Server ignored the Range header (200 OK) for a partial request; refusing to splice.");
                }
                throw new HttpRequestException("Unexpected status " + (int)response.StatusCode + " for range request.",
                                               null, response.StatusCode);
            }

            HttpRangeHelper.EnsureIdentityEncoding(response);

            long from, to, total;
            ContentRangeStatus status = HttpRangeHelper.ParseContentRange(response, out from, out to, out total);
            if (status != ContentRangeStatus.Valid)
            {
                // Used to be "if it parsed, check it" - so a response with no Content-Range at all
                // skipped every check below and was spliced in on faith.
                throw new IOException("Chunk response carries " + HttpRangeHelper.Describe(status) +
                                      "; cannot confirm it starts at " + expectedFrom + ".");
            }
            if (from != expectedFrom)
            {
                throw new IOException("Content-Range start " + from + " does not match requested " + expectedFrom + ".");
            }
            // The opening probe asks open-ended (see ParallelFetch: a bounded span would run
            // past EOF near the tail, and this origin answers such a range with the whole file),
            // so its body legitimately covers more than chunk 0. Reading stops at endInclusive
            // either way, so the extra bytes are never consumed.
            if (!bodyMayRunLonger && to > expectedTo)
            {
                throw new IOException("Content-Range end " + to + " exceeds requested " + expectedTo + ".");
            }

            // Every chunk re-follows the original url and is re-signed, so nothing guarantees all
            // of them see the same object. A total that has moved means the resource changed
            // underneath us, and the pieces already delivered belong to a different version.
            // This catches any change of length; a same-length replacement still needs a strong
            // validator (If-Range), which this origin does not supply.
            if (_resourceTotal >= 0 && total >= 0 && total != _resourceTotal)
            {
                throw new IOException("Origin changed complete-length mid-transfer (probe saw " + _resourceTotal +
                                      ", this response says " + total + "); refusing to splice two versions.");
            }
        }

        /// <summary>
        /// Reads the body into pool-rented blocks and publishes them, advancing cursor.Pos as
        /// each block is accepted by the channel. Blocks are filled completely before publishing
        /// so a dribbling server cannot inflate the buffer with tiny blocks. Returns when the
        /// range is complete or the body ends short; throws if the body fails, with cursor.Pos
        /// left at the exact resume point.
        /// </summary>
        private async Task PumpAsync(Stream body, Cursor cursor, long endInclusive, ChannelWriter<Block> writer,
                                     CancellationTokenSource phase, CancellationToken ct)
        {
            // Throughput is measured over time spent READING only. Time parked on a full channel
            // is the consumer being slow, not the connection, and must not count against it.
            long readMs = 0;
            long attemptBytes = 0;
            long graceMs = (long)_o.MinThroughputGrace.TotalMilliseconds;
            int floor = _o.MinThroughputBytesPerSec;

            while (cursor.Pos <= endInclusive)
            {
                long remaining = endInclusive - cursor.Pos + 1;
                int want = (int)Math.Min((long)_o.BlockSize, remaining);

                byte[] buf = ArrayPool<byte>.Shared.Rent(_o.BlockSize);
                int filled = 0;
                bool published = false;
                bool tooSlow = false;
                long observedRate = 0;
                try
                {
                    long readStart = Environment.TickCount64;
                    while (filled < want)
                    {
                        phase.CancelAfter(_o.ReadIdleTimeout);
                        int n = await body.ReadAsync(buf.AsMemory(filled, want - filled), phase.Token).ConfigureAwait(false);
                        if (n <= 0) break;
                        filled += n;

                        // Checked per read, not per block: at the rate actually observed on the
                        // live host a 64 KiB block takes ~18s to fill, so a per-block check would
                        // barely beat doing nothing.
                        long soFarMs = readMs + (Environment.TickCount64 - readStart);
                        if (floor > 0 && soFarMs > graceMs && (attemptBytes + filled) * 1000L < floor * soFarMs)
                        {
                            observedRate = (attemptBytes + filled) * 1000L / Math.Max(1, soFarMs);
                            tooSlow = true;
                            break;
                        }
                    }
                    readMs += Environment.TickCount64 - readStart;
                    attemptBytes += filled;

                    if (filled > 0)
                    {
                        // Disarm the idle timer: blocking on a slow consumer is legitimate
                        // backpressure, not a stalled socket. Leaving it armed would cancel
                        // `phase` while we wait and poison the next read.
                        phase.CancelAfter(Timeout.InfiniteTimeSpan);

                        FetchMetrics.AddBuffered(filled);
                        try
                        {
                            // Bounded channel: this is where a slow consumer stalls the worker.
                            await writer.WriteAsync(new Block(buf, filled), ct).ConfigureAwait(false);
                            published = true;
                        }
                        catch
                        {
                            FetchMetrics.RemoveBuffered(filled);
                            throw;
                        }
                        cursor.Pos += filled;
                        cursor.LastProgressTicks = Environment.TickCount64;
                    }
                }
                finally
                {
                    if (!published) ArrayPool<byte>.Shared.Return(buf);
                }

                // Whatever this connection did manage to deliver is published and the cursor has
                // moved, so the retry resumes further along. Discarding it instead would let a
                // permanently slow source make zero net progress and burn the whole stall budget
                // without ever advancing.
                if (tooSlow)
                {
                    _stats.SlowRetry();
                    throw new SlowConnectionException(
                        "connection delivered " + observedRate + " B/s over " + readMs +
                        " ms (floor " + floor + " B/s); abandoning it at offset " + cursor.Pos);
                }

                if (filled < want) return; // body ended (or stalled short); caller decides
            }
        }


    }
}
