using System;
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

        internal int ChunksCompleted { get { return Volatile.Read(ref _chunksCompleted); } }
        internal int Retries { get { return Volatile.Read(ref _retries); } }
        internal int SlowRetries { get { return Volatile.Read(ref _slowRetries); } }

        internal void ChunkDone() { Interlocked.Increment(ref _chunksCompleted); }
        internal void Retry() { Interlocked.Increment(ref _retries); }
        internal void SlowRetry() { Interlocked.Increment(ref _slowRetries); }
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
