using System;
using System.Net.Http;
using System.Threading;

namespace EmbyStrmParallel
{
    /// <summary>
    /// Per-stream counters, so closing a stream can say in one line what it actually did.
    /// A total throughput collapse previously produced no log output at all, because every
    /// request succeeded - it just trickled.
    /// </summary>
    internal sealed class StreamStats
    {
        internal readonly long StartTicks = Environment.TickCount64;
        private int _chunksCompleted;
        private int _retries;
        private int _slowRetries;
        private long _permitWaitMs;
        private int _permitWaiters;

        internal int ChunksCompleted { get { return Volatile.Read(ref _chunksCompleted); } }
        internal int Retries { get { return Volatile.Read(ref _retries); } }
        internal int SlowRetries { get { return Volatile.Read(ref _slowRetries); } }

        /// <summary>
        /// Total time this stream's attempts spent queued for an origin permit. Distinguishes
        /// "the origin is slow" from "we are throttling ourselves", which look identical from
        /// the delivered-bytes rate alone.
        /// </summary>
        internal long PermitWaitMs { get { return Interlocked.Read(ref _permitWaitMs); } }

        /// <summary>
        /// Workers queued for a permit RIGHT NOW.
        ///
        /// PermitWaitMs cannot answer the question that actually came up in production. A stream
        /// is disposed with its starved workers still parked on the semaphore, and the closing
        /// line is written before those workers unwind - so time that has not finished being
        /// spent is not in the total yet, and the summary reads zero for the stream that was
        /// starved hardest. This counter is raised BEFORE the wait, so it is true at the moment
        /// the line is written: "four of my workers are stuck on the budget" is exactly the
        /// sentence the log needed to be able to say.
        /// </summary>
        internal int PermitWaiters { get { return Volatile.Read(ref _permitWaiters); } }

        internal void ChunkDone() { Interlocked.Increment(ref _chunksCompleted); }
        internal void Retry() { Interlocked.Increment(ref _retries); }
        internal void SlowRetry() { Interlocked.Increment(ref _slowRetries); }
        internal void PermitWaitBegin() { Interlocked.Increment(ref _permitWaiters); }
        internal void PermitWaitEnd(long ms)
        {
            Interlocked.Decrement(ref _permitWaiters);
            Interlocked.Add(ref _permitWaitMs, ms);
        }
    }

    /// <summary>
    /// The probe response, together with the origin permit its connection is occupying.
    ///
    /// The probe IS chunk 0, so its live 206 body is handed over to whichever worker claims
    /// chunk 0. The permit has to travel with it. Releasing it at hand-over would let another
    /// stream take the slot while this connection is still open, so the origin would see one
    /// more than the budget allows - the cliff the budget exists to stay under. Taking a second
    /// permit on the other side would double-count the same socket and deadlock a budget of one.
    ///
    /// Keeping both halves in one object is what makes that safe: the response already had
    /// exactly three disposal sites (consumed by an attempt, handed over but never used, never
    /// handed over at all) and each of them now releases the permit too. The alternative -
    /// tracking the permit separately - is a fourth ownership path through the same three races,
    /// and a permit leaked there freezes playback forever with no error. Both halves are
    /// idempotent to dispose, so a double release is a no-op rather than a corruption.
    /// </summary>
    internal sealed class PreOpenedChunk : IDisposable
    {
        internal readonly HttpResponseMessage Response;
        internal readonly OriginBudget.Permit Permit;

        internal PreOpenedChunk(HttpResponseMessage response, OriginBudget.Permit permit)
        {
            Response = response;
            Permit = permit;
        }

        public void Dispose()
        {
            if (Response != null) { try { Response.Dispose(); } catch { } }
            if (Permit != null) Permit.Dispose();
        }
    }

    /// <summary>A unit of hand-off between a chunk worker and the reader. Buffer is pool-rented.</summary>
    internal readonly struct Block
    {
        internal readonly byte[] Buffer;
        internal readonly int Length;

        internal Block(byte[] buffer, int length)
        {
            Buffer = buffer;
            Length = length;
        }
    }

    /// <summary>
    /// Process-wide counters. Exists so tests can prove the backpressure claim directly
    /// instead of inferring it from RSS, which is noisy.
    /// </summary>
    internal static class FetchMetrics
    {
        private static long _buffered;
        private static long _peakBuffered;

        internal static long BufferedBytes
        {
            get { return Interlocked.Read(ref _buffered); }
        }

        internal static long PeakBufferedBytes
        {
            get { return Interlocked.Read(ref _peakBuffered); }
        }

        internal static void AddBuffered(int n)
        {
            long now = Interlocked.Add(ref _buffered, n);
            long peak = Interlocked.Read(ref _peakBuffered);
            while (now > peak)
            {
                long prior = Interlocked.CompareExchange(ref _peakBuffered, now, peak);
                if (prior == peak) break;
                peak = prior;
            }
        }

        internal static void RemoveBuffered(int n)
        {
            Interlocked.Add(ref _buffered, -n);
        }

        internal static void ResetPeak()
        {
            Interlocked.Exchange(ref _peakBuffered, Interlocked.Read(ref _buffered));
        }
    }
}
