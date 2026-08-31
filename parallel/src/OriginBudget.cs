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
    /// **A permit covers ONE request, not one worker and not one stream.** That distinction is
    /// the whole difference between a budget and an admission policy. The first version of this
    /// class handed a permit to each worker for the worker's whole life, which made
    /// `max-origin-connections / connections` the maximum number of streams that could play at
    /// once: at 12 / 6 the third viewer's workers queued behind two streams that would not let
    /// go for two hours, and playback froze silently. Request-scoped permits are returned every
    /// few seconds, so an oversubscribed origin makes every stream slower - which is what
    /// throttling is supposed to feel like - instead of starving some of them completely.
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
            /// <summary>
            /// Callers parked on Gate, or about to be. Counted inside the entry's lock BEFORE the
            /// wait starts, because InUse alone cannot make the gate swap safe: InUse is raised
            /// only after WaitAsync returns, so there is a window where a caller already owns a
            /// permit from the old gate while the entry still looks completely idle.
            /// </summary>
            internal int Waiting;
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
        /// Falls back to the whole string when the url has no origin to speak of, which keeps it
        /// in a group of its own rather than merging it with everything else. Parsing alone is
        /// not enough of a test: on Unix a bare path like "/a/b.mkv" parses happily as
        /// file:///a/b.mkv, so the authority check is what stops every path-shaped url in the
        /// process from collapsing onto one key and throttling unrelated origins against each
        /// other. Not reachable from the configured prefixes today - they are all http(s) - but
        /// this is the fallback, and a fallback that quietly merges is worse than none.
        /// </summary>
        internal static string KeyFor(string url)
        {
            if (string.IsNullOrEmpty(url)) return "(none)";
            try
            {
                Uri u = new Uri(url, UriKind.Absolute);
                if (string.IsNullOrEmpty(u.Host)) return url;
                return u.Scheme + "://" + u.Host + ":" + u.Port;
            }
            catch
            {
                return url;
            }
        }

        /// <summary>
        /// Waits up to <paramref name="timeout"/> for a permit for this origin, or returns null.
        /// Cancellation is the caller's stream token, so a stream that is disposed while queued
        /// wakes immediately instead of holding a task.
        ///
        /// **There is no unbounded overload, deliberately.** Every waiter here is either a host
        /// request thread (which must never block forever) or a chunk attempt whose reader is
        /// waiting in order behind it. An unbounded wait turns "the budget is busy" into a
        /// playback freeze with no error and no log, which is strictly worse than any error this
        /// component can report. The timeout is a watchdog against a permit that is never given
        /// back, not a contention limit: a chunk attempt holds its permit for seconds, so
        /// ordinary queuing resolves long before it. The one legitimate long holder is the
        /// degraded single-connection path (SkipLimitStream), which keeps one permit for the
        /// life of that stream - so on an origin serving several of those, reaching the timeout
        /// means "the budget is genuinely full", not "something leaked".
        ///
        /// A `limit` different from the one this origin was created with replaces the gate, but
        /// only while nothing is in flight and nobody is queued. Restricting it to that moment is
        /// what stops the swap from stranding whoever is already waiting on the old gate, and a
        /// stranded waiter is the invisible freeze this class exists to prevent.
        ///
        /// The cost is that the change is DEFERRED, not dropped, and two things can defer it for
        /// a while: a continuously busy origin, and the degraded single-connection path
        /// (SkipLimitStream), which holds one permit for a whole stream rather than for one
        /// attempt. So a `max-origin-connections` edit lands "at the next quiet moment for this
        /// origin", not "within a chunk". `embypatch check` still reports the configured value
        /// and where it came from, so nothing about it is silent.
        /// </summary>
        internal static async Task<Permit> TryAcquireAsync(string key, int limit, TimeSpan timeout,
                                                           CancellationToken ct)
        {
            if (limit < 1) limit = 1;

            Entry e = Origins.GetOrAdd(key, _ => new Entry { Gate = new SemaphoreSlim(limit, limit), Limit = limit });

            SemaphoreSlim gate;
            lock (e)
            {
                if (e.Limit != limit && Volatile.Read(ref e.InUse) == 0 && e.Waiting == 0)
                {
                    e.Gate = new SemaphoreSlim(limit, limit);
                    e.Limit = limit;
                }
                gate = e.Gate;
                e.Waiting++;
            }

            bool got;
            try
            {
                got = await gate.WaitAsync(timeout, ct).ConfigureAwait(false);
            }
            catch
            {
                lock (e) { e.Waiting--; }
                throw;
            }

            if (!got)
            {
                lock (e) { e.Waiting--; }
                return null;
            }

            // Raise InUse before dropping Waiting, so the pair is never both zero while this
            // permit exists - which is exactly the instant a concurrent limit change would
            // otherwise decide the origin was idle and swap the gate out from under it.
            int now = Interlocked.Increment(ref e.InUse);
            lock (e) { e.Waiting--; }

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

        /// <summary>The limit currently in force for this origin, or 0 if it has never been used.</summary>
        internal static int Limit(string key)
        {
            Entry e;
            if (!Origins.TryGetValue(key, out e)) return 0;
            lock (e) { return e.Limit; }
        }

        /// <summary>Test hook: forget every origin. Never called from the fetch path.</summary>
        internal static void ResetForTests()
        {
            Origins.Clear();
        }
    }
}
