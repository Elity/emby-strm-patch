using System;
using System.Collections.Generic;

namespace EmbyStrmParallel
{
    /// <summary>
    /// Maps chunk index to byte span.
    ///
    /// Chunk 0 reaches the consumer at single-connection speed, because in-order delivery means
    /// nothing behind it can be handed over until it completes. With uniform 8 MiB chunks that
    /// is ~16 s of playback running at ~4 Mbps before parallelism becomes visible - the exact
    /// startup stutter this component exists to remove.
    ///
    /// So early chunks ramp: FirstChunkSize, then doubling, until ChunkSize is reached, after
    /// which every chunk is ChunkSize. Later chunks are still large, so the steady-state duty
    /// cycle (transfer time vs per-request latency) is unaffected. Set FirstChunkSize equal to
    /// ChunkSize to switch the ramp off.
    ///
    /// Memory is unaffected: ramp chunks are smaller than ChunkSize, so Slots * ChunkSize
    /// remains the ceiling.
    /// </summary>
    internal sealed class ChunkSchedule
    {
        private readonly long[] _rampStarts;   // relative starts of the ramp chunks, plus a terminator
        private readonly int _rampCount;
        private readonly long _steadyStart;    // relative offset where fixed-size chunks begin
        private readonly int _chunkSize;
        private readonly long _total;

        internal ChunkSchedule(long totalToRead, int firstChunkSize, int chunkSize)
        {
            _total = totalToRead < 0 ? 0 : totalToRead;
            _chunkSize = chunkSize;

            List<long> starts = new List<long>();
            long pos = 0;
            long size = firstChunkSize;
            while (pos < _total && size < chunkSize)
            {
                starts.Add(pos);
                pos += size;
                size *= 2;
            }
            _rampCount = starts.Count;
            _steadyStart = pos;
            starts.Add(pos);                   // terminator: end of the last ramp chunk
            _rampStarts = starts.ToArray();

            long steady = _total > _steadyStart ? (_total - _steadyStart + chunkSize - 1) / chunkSize : 0;
            long count = _rampCount + steady;
            Count = count < 1 ? 1 : count;
        }

        /// <summary>Number of chunks covering the range. Always at least 1 (a zero-length range yields one empty chunk).</summary>
        internal long Count { get; }

        /// <summary>Byte span of a chunk, relative to the start of the requested range.</summary>
        internal void Range(long index, out long relativeStart, out long length)
        {
            long start;
            long end;
            if (index < _rampCount)
            {
                start = _rampStarts[(int)index];
                end = _rampStarts[(int)index + 1];
            }
            else
            {
                start = _steadyStart + (index - _rampCount) * _chunkSize;
                end = start + _chunkSize;
            }

            if (start > _total) start = _total;
            if (end > _total) end = _total;
            relativeStart = start;
            length = end - start;
            if (length < 0) length = 0;
        }

        /// <summary>Length of chunk 0 - the span the opening probe request must ask for.</summary>
        internal static long FirstChunkLength(long totalToRead, int firstChunkSize, int chunkSize)
        {
            long first = firstChunkSize < chunkSize ? firstChunkSize : chunkSize;
            return totalToRead > 0 && totalToRead < first ? totalToRead : first;
        }
    }
}
