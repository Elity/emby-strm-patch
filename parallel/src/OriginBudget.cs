using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace EmbyStrmParallel
{
    /// <summary>
    /// A cap on how many range requests may be in flight against ONE origin at a time,
    /// shared by every stream in the process.
    ///
    /// Why this exists: `Connections` is a per-STREAM number, but an origin limits the
    /// connections IT sees, across everything. Seeking makes the two disagree — abandoning a
    /// stream does not release its connections at the origin instantly, so the next stream's
    /// are briefly added on top. Measured on the live host: Connections = 8 meant ~16 at the
    /// origin during a seek, every connection collapsed to ~33 KB/s, and playback errored out.
    /// Lowering Connections to 6 fixed it only because "at most two streams overlap" happened
    /// to be true; three viewers, or a burst of seeks, would cross the cliff again.
    ///
    /// So the limit belongs where the constraint actually lives: on the origin.
    ///
    /// **This shares a COUNT, never a connection pool.** Connection pools stay strictly
    /// per-stream (HttpClientHolder explains why: a shared pool let an abandoned stream poison
    /// the next one, 25.14 -> 0.15 Mbps, recovering only after ~90 s idle). A permit is a right
    /// to have one request in flight; it carries no state and cannot be poisoned.
    ///
    /// Grouping is per authority rather than per configured prefix, so two prefixes on the same
    /// host correctly contend, and two different hosts correctly do not — one origin being slow
    /// must not throttle an unrelated one.
    /// </summary>
    internal static class OriginBudget
    {
        internal sealed class Entry
        {
            internal SemaphoreSlim Gate;
            internal int Limit;
            internal int InUse;
            internal int Peak;
        }

        private static readonly ConcurrentDictionary<string, Entry> Origins =
            new ConcurrentDictionary<string, Entry>(StringComparer.Ordinal);

        /// <summary>
        /// One permit, released exactly once. Holds the semaphore it came from rather than
        /// looking it up again: a configuration change can swap the entry's gate, and a permit
        /// must always be returned to the object it was taken from.
        /// </summary>
        internal sealed class Permit : IDisposable
        {
            private SemaphoreSlim _gate;
            private Entry _entry;

            internal Permit(SemaphoreSlim gate, Entry entry)
            {
                _gate = gate;
                _entry = entry;
            }

            public void Dispose()
            {
                SemaphoreSlim g = Interlocked.Exchange(ref _gate, null);
                if (g == null) return;               // already released
                Entry e = Interlocked.Exchange(ref _entry, null);
                if (e != null) Interlocked.Decrement(ref e.InUse);
                try { g.Release(); } catch (ObjectDisposedException) { }
            }
        }

        /// <summary>
        /// The grouping key: scheme + host + port of the ORIGINAL url.
        ///
        /// Deliberately not the 302 target. The signed CDN host changes with every signature, so
        /// keying on it would shatter the budget into thousands of one-request groups; the
        /// original authority is stable, is what the user configured a prefix for, and is where
        /// the throttling evidence points (the 502s come from the front layer). If a deployment
        /// ever shows the real ceiling is at the CDN instead, this decision has to be revisited.
        ///
        /// Falls back to the whole string when the url will not parse, which keeps a malformed
        /// url in its own group rather than merging it with everything else.
        /// </summary>
        internal static string KeyFor(string url)
        {
            if (string.IsNullOrEmpty(url)) return "(none)";
            try
            {
                Uri u = new Uri(url, UriKind.Absolute);
                return u.Scheme + "://" + u.Host + ":" + u.Port;
            }
            catch
            {
                return url;
            }
        }

        /// <summary>
        /// Waits for a permit for this origin. Cancellation is the caller's stream token, so a
        /// stream that is disposed while queued wakes immediately instead of holding a task.
        ///
        /// A `limit` different from the one this origin was created with replaces the gate. That
        /// is what keeps `max-origin-connections` live-reloadable like every other setting;
        /// permits already held are returned to the old gate, so during the moment of a manual
        /// config change the two can briefly coexist. Bounded, rare, and far better than
        /// requiring a restart for this one knob.
        /// </summary>
        internal static async Task<Permit> AcquireAsync(string key, int limit, CancellationToken ct)
        {
            if (limit < 1) limit = 1;

            Entry e = Origins.GetOrAdd(key, _ => new Entry { Gate = new SemaphoreSlim(limit, limit), Limit = limit });

            SemaphoreSlim gate;
            lock (e)
            {
                if (e.Limit != limit)
                {
                    e.Gate = new SemaphoreSlim(limit, limit);
                    e.Limit = limit;
                }
                gate = e.Gate;
            }

            await gate.WaitAsync(ct).ConfigureAwait(false);

            int now = Interlocked.Increment(ref e.InUse);
            int peak = Volatile.Read(ref e.Peak);
            while (now > peak)
            {
                int prior = Interlocked.CompareExchange(ref e.Peak, now, peak);
                if (prior == peak) break;
                peak = prior;
            }
            return new Permit(gate, e);
        }

        /// <summary>Permits currently held for this origin. Must return to 0 once every stream is done.</summary>
        internal static int InUse(string key)
        {
            Entry e;
            return Origins.TryGetValue(key, out e) ? Volatile.Read(ref e.InUse) : 0;
        }

        /// <summary>High-water mark since the last reset. This is what proves the cap actually caps.</summary>
        internal static int Peak(string key)
        {
            Entry e;
            return Origins.TryGetValue(key, out e) ? Volatile.Read(ref e.Peak) : 0;
        }

        internal static void ResetPeak(string key)
        {
            Entry e;
            if (Origins.TryGetValue(key, out e)) Volatile.Write(ref e.Peak, Volatile.Read(ref e.InUse));
        }

        /// <summary>Test hook: forget every origin. Never called from the fetch path.</summary>
        internal static void ResetForTests()
        {
            Origins.Clear();
        }
    }
}
