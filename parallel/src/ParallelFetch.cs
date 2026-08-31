using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace EmbyStrmParallel
{
    /// <summary>
    /// Entry point. Fetches an HTTP byte range over several concurrent connections and
    /// delivers it as one in-order, forward-only stream.
    ///
    /// Designed to be called from hand-written CIL injected into a host: everything is static,
    /// there are no optional arguments, no tuples, no host types, and TryOpen signals failure
    /// with null instead of an exception so the injected call site needs no handler table.
    ///
    /// Range semantics mirror the host's:
    ///   length == 0                -> to end of resource
    ///   offset == 0 &amp;&amp; length == 0 -> whole resource
    ///   totalLength                -> full resource size (from Content-Range "/&lt;total&gt;")
    ///   contentLength              -> size of this particular response body
    /// </summary>
    public static class ParallelFetch
    {
        /// <summary>Optional diagnostics sink. Null (the default) disables logging entirely.</summary>
        public static Action<string> Logger;

        /// <summary>
        /// True when the url is routed to parallel mode by the shared routing configuration —
        /// that is, a matching prefix carrying the `parallel` token. A prefix left at the
        /// default 302 mode deliberately does NOT match: patch B has already redirected it and
        /// the server is no longer in the transfer path.
        /// Never throws; a missing or broken configuration means "no match".
        /// </summary>
        public static bool IsMatch(string url)
        {
            return StrmDirect.IsParallel(url);
        }

        /// <summary>
        /// Opens the range. Throws on failure. Blocks the calling thread for one HTTP round trip
        /// (the probe that establishes the resource size), because totalLength/contentLength must
        /// be known before returning.
        /// </summary>
        public static Stream Open(string url, long offset, long length,
                                  out long? totalLength, out long? contentLength,
                                  CancellationToken cancellationToken)
        {
            return OpenWith(url, offset, length, null, out totalLength, out contentLength, cancellationToken);
        }

        /// <summary>
        /// Same as Open but returns null instead of throwing, so an injected call site can fall
        /// back to the host's original code path with a single brtrue.
        ///
        /// Every fallback is logged unconditionally. This is the one outcome that is otherwise
        /// invisible from inside a running host: playback silently reverts to single-connection
        /// speed with nothing anywhere saying why.
        /// </summary>
        public static Stream TryOpen(string url, long offset, long length,
                                     out long? totalLength, out long? contentLength,
                                     CancellationToken cancellationToken)
        {
            totalLength = null;
            contentLength = null;
            try
            {
                return OpenWith(url, offset, length, null, out totalLength, out contentLength, cancellationToken);
            }
            catch (Exception ex)
            {
                // Two things must NOT become a fallback. A cancelled request is the host tearing
                // down, so retrying it on the original path would start a second fetch for a
                // client that has already gone; and a process-level failure is not the origin's
                // fault, so hiding it makes a sick host look merely slow.
                if (cancellationToken.IsCancellationRequested) throw;
                if (HttpRangeHelper.IsFatalToProcess(ex)) throw;

                totalLength = null;
                contentLength = null;
                FetchLog.Write("FALLBACK to host path: url=" + FetchLog.Tail(url) +
                               " offset=" + offset + " length=" + length +
                               " reason=" + FetchLog.Describe(ex));
                return null;
            }
        }

        /// <summary>Open with explicit tuning. Pass null for options to use the routing configuration.</summary>
        public static Stream OpenWith(string url, long offset, long length, ParallelFetchOptions options,
                                      out long? totalLength, out long? contentLength,
                                      CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(url)) throw new ArgumentException("url is required", "url");
            if (offset < 0) throw new ArgumentOutOfRangeException("offset");
            if (length < 0) throw new ArgumentOutOfRangeException("length");

            // Re-read per stream, not once per process: this is what lets an edit to
            // strm-routing.txt take effect inside the parser's 30 s window.
            ParallelFetchOptions o = (options ?? ParallelFetchOptions.FromConfiguration()).Normalize();

            // One connection pool per stream. See HttpClientHolder for why sharing one poisoned
            // every stream that followed an abandoned seek.
            HttpClient client = HttpClientHolder.CreateForStream();
            bool clientHandedOff = false;
            try
            {
                return OpenCore(client, url, offset, length, o, out totalLength, out contentLength,
                                cancellationToken, ref clientHandedOff);
            }
            finally
            {
                if (!clientHandedOff) { try { client.Dispose(); } catch { } }
            }
        }

        private static Stream OpenCore(HttpClient client, string url, long offset, long length, ParallelFetchOptions o,
                                       out long? totalLength, out long? contentLength,
                                       CancellationToken cancellationToken, ref bool clientHandedOff)
        {

            // The probe IS chunk 0. The origin costs ~1.7s to first byte (302 -> signed CDN url),
            // so a separate HEAD/size probe would double time-to-first-byte for no benefit.
            //
            // The probe runs BEFORE the resource size is known, so its span is a guess, and near
            // the end of the resource that guess runs past EOF - exactly what a player's MKV
            // index read at the tail does. RFC 7233 says a server should clamp such a range; this
            // origin answers 200 with the WHOLE resource instead, which used to strand the
            // fetcher discarding gigabytes to deliver a few bytes. Measured and deterministic:
            // end past EOF -> 200, end clamped to EOF -> 206, open-ended -> 206.
            //
            // SendProbe therefore retries once open-ended on a 200, which cannot overshoot by
            // construction. Bounded stays the first choice because an open-ended probe makes the
            // origin stream everything to EOF while we consume only chunk 0 from it - fine for a
            // tail read, wasteful for the whole-file request that is by far the most common one.
            // Every later chunk is bounded by ChunkSchedule against the total learned here and
            // can never exceed EOF.
            long probeEnd = offset + ChunkSchedule.ProbeSpan(length, o.FirstChunkSize) - 1;

            // A 200 here means the origin ignored Range, and the only way to honour the request
            // from a whole-resource body is to read and discard everything ahead of `offset`.
            // Worth doing for a small offset; ruinous for a large one. See
            // ParallelFetchOptions.MaxIgnoredRangeSkipBytes.
            bool refuseWholeBody = offset > o.MaxIgnoredRangeSkipBytes;

            // Open() blocks a host request thread for everything below, so the whole of it -
            // queuing for a permit AND every probe attempt - shares one deadline. Two independent
            // budgets here would let the worst case be their sum.
            //
            // Blocking a host thread on an internal resource rather than only on network I/O is
            // new, and under thread-pool starvation it can feed itself: the permit is released by
            // a continuation that needs a pool thread. The deadline plus the host fallback is the
            // whole recovery. Making TryOpen genuinely async is not a small change - the injected
            // call site returns before any state machine starts - and is tracked in
            // mode-routing.md 11.1 alongside the same limitation for the probe itself.
            long openDeadline = Environment.TickCount64 + ProbeBudgetMs(o);

            // The probe IS chunk 0, so it needs a permit like any other request. Taking it here
            // rather than inside the stream is what keeps a queued stream from sitting on a live
            // 206 body that the budget knows nothing about: at saturation that "few millisecond"
            // window is the entire wait, and ten clients starting at once put ten uncounted
            // connections on top of the budget - straight over the cliff it exists to avoid.
            //
            // Bounded, and failure is a clean degrade rather than an error: the throw becomes a
            // null from TryOpen, and Emby serves the request on its own single connection. That
            // path already exists, is correct, and is the right answer when the origin has no
            // capacity left to give.
            //
            string originKey = OriginBudget.KeyFor(url);

            // The permit is taken per ATTEMPT inside SendProbe and the successful one comes back
            // with the response, so the budget is never held across a backoff.
            ProbeResult probed = SendProbe(client, url, offset, probeEnd, o, refuseWholeBody,
                                           openDeadline, originKey, cancellationToken);
            HttpResponseMessage response = probed.Response;
            bool probeWentOpenEnded = probed.OpenEnded;
            OriginBudget.Permit probePermit = probed.Permit;
            long probePermitWaitMs = probed.PermitWaitMs;

            bool handedOff = false;
            try
            {
                if (response.StatusCode == HttpStatusCode.RequestedRangeNotSatisfiable)
                {
                    throw new IOException("Origin rejected range " + offset + "-" +
                                          probeEnd.ToString(CultureInfo.InvariantCulture) + " (416).");
                }

                HttpRangeHelper.EnsureIdentityEncoding(response);

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    // No range support. Degrade to one connection, still byte-exact.
                    long? cl = response.Content.Headers.ContentLength;
                    if (!cl.HasValue) throw new IOException("Origin ignored Range and did not report Content-Length.");
                    long full = cl.Value;
                    if (offset > full) throw new IOException("Offset " + offset + " is past end of resource (" + full + ").");
                    long eff0 = length > 0 ? Math.Min(length, full - offset) : full - offset;
                    totalLength = full;
                    contentLength = eff0;
                    Stream body0 = response.Content.ReadAsStreamAsync(cancellationToken).GetAwaiter().GetResult();
                    // The degraded path holds one origin connection for its whole life, so the
                    // permit goes with it rather than being released here - one uncounted
                    // connection per fallback stream is exactly the drift the budget prevents.
                    //
                    // Constructed BEFORE the hand-off flags are set. Setting them first and then
                    // throwing between the two disarms every guard in the finally at once, and
                    // leaks the response, the permit and the HttpClient together.
                    SkipLimitStream degraded = new SkipLimitStream(response, body0, client, probePermit,
                                                                   offset, eff0, o.ReadIdleTimeout);
                    handedOff = true;
                    clientHandedOff = true;
                    probePermit = null;
                    FetchLog.Write("open path=single-conn-fallback (origin ignored Range) url=" + FetchLog.Tail(url) +
                                   " offset=" + offset + " length=" + eff0 + " total=" + full +
                                   " skip=" + FetchLog.Size(offset));
                    return degraded;
                }

                if (response.StatusCode != HttpStatusCode.PartialContent)
                {
                    throw new HttpRequestException("Probe returned " + (int)response.StatusCode + ".", null, response.StatusCode);
                }

                // Everything below depends on knowing where these bytes belong. A 206 whose
                // Content-Range is missing, unparseable or "bytes */N" tells us nothing, and the
                // old code carried on regardless: a proxy answering a 1 MiB-offset request with a
                // header-less 206 whose body started at zero produced the right byte COUNT at the
                // wrong POSITION, with no error anywhere. Refusing hands the request back to
                // Emby's own single-connection path, which is slower and correct.
                long from, to, total;
                ContentRangeStatus status = HttpRangeHelper.ParseContentRange(response, out from, out to, out total);
                if (status != ContentRangeStatus.Valid)
                {
                    throw new IOException("Origin answered 206 with " + HttpRangeHelper.Describe(status) +
                                          "; cannot establish where the body belongs.");
                }
                if (from != offset)
                {
                    throw new IOException("Content-Range start " + from + " does not match requested " + offset + ".");
                }
                if (!probeWentOpenEnded && to > probeEnd)
                {
                    throw new IOException("Content-Range end " + to + " exceeds requested " + probeEnd + ".");
                }

                // No numeric complete-length means no fallback either: the host writes this
                // straight into the outgoing Content-Range denominator, and when TotalLength is
                // null it substitutes Stream.Length - which is this range's size, not the file's.
                // Confirmed against stock 4.9.3.0 FileWriter.SetContentResponseHeaders:
                //     TotalContentLength = handler.TotalLength ?? handler.Stream?.Length
                // Returning a range length there corrupts the response header and can clamp the
                // copy. There is nothing to salvage; decline and let Emby serve it.
                if (total < 0)
                {
                    throw new IOException("Origin answered 206 with an unknown complete-length (\"/*\"); " +
                                          "the host requires a total to build Content-Range.");
                }

                long available = total - offset;
                if (available <= 0)
                {
                    // Unreachable: the parser already rejects total <= last-byte-pos, and
                    // last-byte-pos >= offset. Refuse rather than return empty.
                    throw new IOException("Origin reported total " + total + " at or below the requested offset " +
                                          offset + " while answering 206.");
                }
                long effective = length > 0 ? Math.Min(length, available) : available;
                totalLength = total;
                contentLength = effective;

                if (effective == 0)
                {
                    FetchLog.Write("open path=empty url=" + FetchLog.Tail(url) + " offset=" + offset);
                    return new MemoryStream(Array.Empty<byte>(), false);
                }

                ParallelRangeStream stream = new ParallelRangeStream(
                    client, url, offset, effective, total, o,
                    new PreOpenedChunk(response, probePermit), cancellationToken);
                handedOff = true;
                clientHandedOff = true;
                probePermit = null;

                if (FetchLog.IsEnabled)
                {
                    FetchLog.Write("open path=parallel #" + stream.StreamId +
                        " url=" + FetchLog.Tail(url) +
                        " offset=" + offset + " length=" + effective +
                        " total=" + total.ToString(CultureInfo.InvariantCulture) +
                        " conn=" + o.Connections +
                        (o.ConnectionsClampedByBudget
                            ? "(clamped by max-origin-connections=" + o.MaxOriginConnections + ")"
                            : "") +
                        " originBudget=" + o.MaxOriginConnections +
                        // How long the probe queued for its slot. The closing line's permitWait
                        // only covers the workers, so without this a stream that took seconds to
                        // open because the origin was saturated looks identical to one that did
                        // not - and "why did playback take so long to start" is the question this
                        // whole budget makes it possible to answer.
                        (probePermitWaitMs >= 100
                            ? " openWait=" + (probePermitWaitMs / 1000.0).ToString("0.0", CultureInfo.InvariantCulture) + "s"
                            : "") +
                        " chunk=" + FetchLog.Size(o.ChunkSize) +
                        " firstChunk=" + FetchLog.Size(o.FirstChunkSize) +
                        " chunks=" + stream.ChunkCount +
                        " slots=" + stream.SlotCount +
                        " memCeiling=" + FetchLog.Size(stream.MemoryCeilingBytes));
                }

                return stream;
            }
            finally
            {
                if (!handedOff) { try { response.Dispose(); } catch { } }
                // Nulled at each hand-off site, so this covers exactly the paths that kept
                // neither the probe body nor a stream built from it.
                if (probePermit != null) probePermit.Dispose();
            }
        }

        private sealed class ProbeResult
        {
            internal HttpResponseMessage Response;
            internal bool OpenEnded;
            /// <summary>The permit of the attempt that succeeded. The caller owns it from here.</summary>
            internal OriginBudget.Permit Permit;
            internal long PermitWaitMs;
        }

        /// <summary>
        /// The smallest remainder worth starting an attempt with.
        ///
        /// An earlier design gave the permit wait the deadline minus one full header timeout, so
        /// "a permit won at the last moment is still a permit the probe has time to use". That
        /// formula had to go - 20 s subtracted from an 8 s budget clamps to nothing and the probe
        /// would never queue at all - but what it was protecting is real: a permit won with 100 ms
        /// left buys a TCP connection to the origin that is aborted 100 ms later, and abandoned
        /// connections lingering at this origin are exactly what the ramp measurements show
        /// collapsing it to 0.23 Mbps.
        ///
        /// RetryBaseDelayMs is admittedly the wrong DIMENSION for this - the question is "will a
        /// connection have time to be useful", which is a handshake quantity, not a backoff one.
        /// It is borrowed because at its 250 ms default it is about one handshake and inventing a
        /// knob for a bound nobody tunes costs more than it buys. The Math.Min is not decoration:
        /// the clamp on RetryBaseDelayMs admits 10000, above the whole 8000 ms budget, and a floor
        /// larger than the budget makes the probe give up having made ZERO requests - every
        /// TryOpen returning null instantly and every stream running at 4.1 Mbps forever. Not
        /// reachable from strm-routing.txt today, but the clamp advertises it.
        /// </summary>
        internal static int UsableRemainderMs(ParallelFetchOptions o)
        {
            return (int)Math.Min(o.RetryBaseDelayMs, Math.Max(1, ProbeBudgetMs(o) / 4));
        }

        /// <summary>
        /// The probe's backoff ceiling, derived from its own budget rather than inherited from the
        /// chunk loop's RetryMaxDelayMs (4000 ms).
        ///
        /// A single 4 s wait cannot fit in the tail of an 8 s budget, so the probe gave up at
        /// ~4.1 s and abandoned the other half of it, leaving its last attempt at ~3.75 s. A
        /// quarter of the budget keeps the tail productive: at 8 s that is 2000 ms, putting
        /// attempts at 0 / 0.25 / 0.75 / 1.75 / 3.75 / 5.75 s nominal and the give-up between 6.2 s
        /// (idle) and 7.9 s (under 8x CPU load) measured over 11 runs. (Nominal: RetryDelayMs adds up to 25% jitter, which is why the seventh
        /// attempt at 7.75 s is in the arithmetic but not in the measurements.)
        ///
        /// A separate method rather than a local, because this is the arithmetic a test should
        /// pin. Asserting it through a stopwatch does not work: the correct and the inherited
        /// shape differ by exactly one attempt, and under CPU contention an oversleeping
        /// Task.Delay costs the correct shape that attempt too - measured flaky in ~19% of loaded
        /// runs while still failing to separate the two. ConfigTests pins it directly instead.
        /// </summary>
        internal static int ProbeMaxDelayMs(ParallelFetchOptions o)
        {
            return (int)Math.Min(o.RetryMaxDelayMs, Math.Max(1, ProbeBudgetMs(o) / 4));
        }

        /// <summary>
        /// The one give-up exception, with the diagnosis attached.
        ///
        /// Its message has exactly one destination: the `reason=` field of the FALLBACK line,
        /// which is the only clue a running Emby leaves about why a stream went single-connection.
        /// Two of the three give-up paths used to rethrow the inner exception instead, so an
        /// origin that accepted and never replied - the commonest shape there is - reported
        /// "The operation was canceled." with no mention of the probe, the budget, or how many
        /// attempts it made. It was also two verbatim copies of one string, one of them
        /// mis-indented, neither reachable by the shapes that actually occur.
        /// </summary>
        private static TimeoutException ProbeGaveUp(ParallelFetchOptions o, string url, int attempt, int answered, Exception last)
        {
            return new TimeoutException("Probe gave up on " + FetchLog.Tail(url) + " after " +
                (ProbeBudgetMs(o) / 1000.0).ToString("0.#", CultureInfo.InvariantCulture) + "s (" +
                attempt + " attempts, " + answered + " answered); handing the request back to the host." +
                (last == null ? "" : " Last: " + FetchLog.Describe(last)));
        }

        /// <summary>
        /// How long the probe may take in total, across every attempt, whatever the origin does.
        ///
        /// Deliberately much tighter than the chunk-level stall budget, and the reason is where
        /// this code runs: Open() is synchronous by construction - the injected call site returns
        /// before any state machine starts - so every second here is an Emby request thread
        /// pinned, and the outcome of giving up is a graceful degrade (TryOpen -> null -> Emby
        /// serves it on one connection) rather than an error. A chunk failing is fatal to the
        /// stream, so a chunk is worth waiting 30 s for. An open is not.
        ///
        /// It is ONE deadline, applied to the whole probe, and that is the load-bearing part.
        /// The first version of this made it a second deadline layered on top of the stall
        /// budget and checked only at the top of the retry loop, which left three ways to blow
        /// straight through it, all measured: a header timeout still clamped to the 30 s budget
        /// held a thread 20 s on an origin that accepted and never replied; one answered attempt
        /// disabled the window permanently, so "answer once then go quiet" took 30 s and could
        /// never reach MaxAttempts; and the permit wait had its own 30 s bound, so a saturated
        /// budget took 30 s too. Every one of those sites was already clamping to `deadline`.
        /// Tightening `deadline` fixes all of them and deletes code instead of adding it.
        ///
        /// 8 s is measured, not guessed: the DNS wobbles that motivated the retry rework were at
        /// most 2.25 s, and 250/500/1000/2000 ms backoffs put attempt 6 at ~7.75 s - so this
        /// covers the observed outage more than three times over while failing fast against an
        /// origin that is simply gone. The cost is that an origin needing more than 8 s just to
        /// send response headers now falls back; at that point playback was not going to work.
        /// </summary>
        internal static long ProbeBudgetMs(ParallelFetchOptions o)
        {
            return (long)Math.Min(o.StallBudget.TotalMilliseconds, 8000);
        }

        private static ProbeResult SendProbe(HttpClient client, string url, long from, long toInclusive,
                                             ParallelFetchOptions o, bool refuseWholeBody, long deadline,
                                             string originKey, CancellationToken cancellationToken)
        {
            return SendProbeAsync(client, url, from, toInclusive, o, refuseWholeBody, deadline, originKey,
                                  cancellationToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// The probe IS chunk 0, so it gets the same bounded retry as any other chunk. Without
        /// this a single 403 from an expired redirect target - the exact failure this origin
        /// produces - would fail the whole request before a single byte was fetched.
        ///
        /// refuseWholeBody folds "the origin ignored Range on a far offset" into that same
        /// machinery. Direct probing answered 206 for that shape 21 times out of 21, including
        /// under concurrent load, so a 200 there is worth retrying; if every attempt gives one,
        /// the throw becomes a null from TryOpen and Emby serves the request itself.
        /// </summary>
        private static async Task<ProbeResult> SendProbeAsync(HttpClient client, string url, long from, long toInclusive,
                                                              ParallelFetchOptions o, bool refuseWholeBody, long deadline,
                                                              string originKey, CancellationToken cancellationToken)
        {
            // Same split as ChunkDownloader: `attempt` drives the backoff, `answered` is what
            // MaxAttempts caps. The probe has no progress cursor of any kind, so its deadline is
            // an absolute wall-clock cap that nothing can reset - which makes it exactly the
            // right owner for attempts the origin never answered. On 2026-08-31 the probe gave up
            // on four separate ~2 s DNS wobbles and logged FALLBACK each time, handing whole
            // films to Emby's single connection at 4.1 Mbps against a 13 Mbps requirement.
            int attempt = 0;
            int answered = 0;
            long permitWaitMs = 0;

            // -1 once we have fallen back to "bytes=<from>-".
            long rangeEnd = toInclusive;
            // The deadline is the CALLER's: Open() blocks the caller for the permit wait and this
            // whole retry loop together, so the total is capped, not just each attempt. Four
            // attempts x one header timeout each would otherwise pin a host request thread for
            // over a minute.
            while (true)
            {
                bool gotAnswer = false;
                long retryAfterMs = -1;
                long backoffMs = -1;
                long remainingMs = deadline - Environment.TickCount64;
                if (remainingMs <= 0) throw ProbeGaveUp(o, url, attempt, answered, null);
                // One permit per ATTEMPT, taken here rather than around the whole loop.
                //
                // Holding it across the retries meant holding it across the backoff sleeps too,
                // and once unanswered attempts stopped being capped at four that stretched from
                // ~3 s to the full 30 s - with nothing of ours in flight at the origin for almost
                // all of it. Measured: a rival stream's wait for a permit went 2.79 s -> 29.50 s.
                // ChunkDownloader already works this way and has a test named for it; the probe
                // was the one place still doing it the old way.
                OriginBudget.Permit permit = null;
                long permitStart = Environment.TickCount64;
                try
                {
                    permit = await OriginBudget.TryAcquireAsync(originKey, o.MaxOriginConnections,
                                    TimeSpan.FromMilliseconds(Math.Max(1, deadline - Environment.TickCount64)),
                                    cancellationToken).ConfigureAwait(false);
                    permitWaitMs += Environment.TickCount64 - permitStart;
                    if (permit == null)
                    {
                        throw new IOException("Origin " + originKey + " has no free connection budget (" +
                                              o.MaxOriginConnections + " in use).");
                    }
                    // A USABLE remainder, not merely a positive one. The earlier design gave the
                    // permit wait the deadline minus one full header timeout, so "a permit won at
                    // the last moment is still a permit the probe has time to use"; that formula
                    // had to go (20s subtracted from an 8s budget clamps to 1ms and the probe
                    // would never queue at all), but the consequence it named is real and came
                    // back with it. A permit won with 100ms left buys a TCP connection to the
                    // origin that is aborted 100ms later - and abandoned connections lingering at
                    // this origin are precisely what the ramp measurements show collapsing it.
                    // One existing knob as the floor, no second deadline.
                    remainingMs = deadline - Environment.TickCount64;
                    if (remainingMs < UsableRemainderMs(o)) throw ProbeGaveUp(o, url, attempt, answered, null);

                    HttpResponseMessage response;
                    CancellationTokenSource probe = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    try
                    {
                        double headerMs = Math.Min(o.ResponseHeadersTimeout.TotalMilliseconds, remainingMs);
                        probe.CancelAfter(TimeSpan.FromMilliseconds(headerMs));
                        HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Get, url);
                        // A negative end emits "bytes=<from>-", the only form that cannot ask
                        // for bytes past EOF when the size is still unknown.
                        req.Headers.Range = rangeEnd < 0
                            ? new RangeHeaderValue(from, null)
                            : new RangeHeaderValue(from, rangeEnd);
                        // Pin the representation. Absent this a server is free to answer with an
                        // encoding we never asked for, and byte offsets into a re-encoded body
                        // point at the wrong media bytes.
                        req.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue("identity"));
                        req.Version = HttpVersion.Version11;
                        req.VersionPolicy = HttpVersionPolicy.RequestVersionOrLower;
                        response = await client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, probe.Token)
                                               .ConfigureAwait(false);
                        gotAnswer = true;   // everything below this point is a reply we chose to reject
                    }
                    finally
                    {
                        // Disposing (not cancelling) stops the timeout timer and unlinks from the
                        // caller's token. The response body stays alive and is aborted, if needed,
                        // through the stream's own token instead.
                        probe.Dispose();
                    }

                    if (response.StatusCode == HttpStatusCode.OK && rangeEnd >= 0)
                    {
                        // Overwhelmingly this means the bounded range ran past EOF and the origin
                        // answered with the whole resource rather than clamping. Ask again the one
                        // way that cannot overshoot. If THAT still comes back 200 the origin
                        // genuinely does not do ranges, and the checks below decide what to do.
                        try { response.Dispose(); } catch { }
                        Log("probe re-asking open-ended url=" + FetchLog.Tail(url) + " from=" + from +
                            " (bounded range " + from + "-" + rangeEnd + " was answered with the whole resource)");
                        rangeEnd = -1;
                        continue;   // deliberately not an attempt: this is a different question
                    }

                    if (refuseWholeBody && response.StatusCode == HttpStatusCode.OK)
                    {
                        try { response.Dispose(); } catch { }
                        throw new IOException("Origin ignored Range and offered the whole resource; serving offset " +
                                              from + " from it would mean discarding " + FetchLog.Size(from) + ".");
                    }

                    if (!HttpRangeHelper.IsTransientStatus(response.StatusCode))
                    {
                        ProbeResult ok = new ProbeResult
                        {
                            Response = response,
                            OpenEnded = rangeEnd < 0,
                            Permit = permit,
                            PermitWaitMs = permitWaitMs
                        };
                        permit = null;   // ownership moves to the caller with the body
                        return ok;
                    }

                    HttpStatusCode transient = response.StatusCode;
                    retryAfterMs = HttpRangeHelper.RetryAfterMs(response);
                    try { response.Dispose(); } catch { }
                    throw new HttpRequestException("Probe returned " + (int)transient + ".", null, transient);
                }
                catch (TimeoutException)
                {
                    // Our own give-up, thrown from inside this try after the permit is taken. It
                    // escapes today only because IsRetryable happens to reject TimeoutException;
                    // adding that type to IsTransientException would silently turn the probe's
                    // terminal state into an infinite retry. Say it out loud instead.
                    throw;
                }
                catch (Exception ex)
                {
                    if (cancellationToken.IsCancellationRequested) throw;
                    if (gotAnswer) answered++;

                    // Retryability gate, shared with the chunk loop. Without it the only bound on
                    // a structurally impossible error - a malformed URL, an unsupported scheme, an
                    // OOM - is the wall clock, because those never set `gotAnswer` and so never
                    // touch `answered`. A bad prefix in strm-routing.txt would burn the full probe
                    // budget on a blocked host thread on every single request. It also restores
                    // the TryOpen fast path for OutOfMemoryException, which retrying swallowed.
                    if (!HttpRangeHelper.IsRetryable(ex)) throw;

                    // No `timedOut` special case here any more: a timeout never sets gotAnswer, so
                    // it never increments `answered`, so this branch could not be reached by one.
                    // The probe budget owns that shape and says so in its message.
                    if (answered >= o.MaxAttempts) throw;
                    attempt++;
                    Log("probe retry " + attempt + " (answered " + answered + "/" + o.MaxAttempts + ") url=" + FetchLog.Tail(url) +
                        " range=" + from + "-" + (rangeEnd < 0 ? "(end)" : rangeEnd.ToString(CultureInfo.InvariantCulture)) +
                        " after " + FetchLog.Describe(ex));
                    long delay = HttpRangeHelper.RetryDelayMs(
                        HttpRangeHelper.BackoffMs(attempt, o.RetryBaseDelayMs, ProbeMaxDelayMs(o)), retryAfterMs);
                    // Give up NOW rather than sleep out a backoff we already know is too long.
                    // Sleeping the remainder and failing on the next pass is what ChunkDownloader
                    // does, and it is right there: a chunk failing is fatal to the stream, so the
                    // last fraction of a second is worth spending. Here it is not - the probe's
                    // failure is a free degrade, and what it spends is a synchronously-blocked
                    // Emby request thread. A 503 with `Retry-After: 3600` would otherwise sleep
                    // the entire remaining budget doing nothing and then fail anyway.
                    long left = deadline - Environment.TickCount64;
                    if (left <= 0 || delay + UsableRemainderMs(o) > left) throw ProbeGaveUp(o, url, attempt, answered, ex);

                    // The permit is already back (the finally below runs first): sleeping on the
                    // origin's budget while making no request to the origin is the exact thing
                    // this loop was doing wrong.
                    backoffMs = delay;
                }
                finally
                {
                    if (permit != null) permit.Dispose();
                }

                if (backoffMs > 0) await Task.Delay((int)backoffMs, cancellationToken).ConfigureAwait(false);
            }
        }

        internal static void Log(string message)
        {
            FetchLog.Write(message);
        }
    }
}
