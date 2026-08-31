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

            bool probeWentOpenEnded;
            HttpResponseMessage response = SendProbe(client, url, offset, probeEnd, o, refuseWholeBody,
                                                     out probeWentOpenEnded, cancellationToken);
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
                    handedOff = true;
                    clientHandedOff = true;
                    FetchLog.Write("open path=single-conn-fallback (origin ignored Range) url=" + FetchLog.Tail(url) +
                                   " offset=" + offset + " length=" + eff0 + " total=" + full +
                                   " skip=" + FetchLog.Size(offset));
                    return new SkipLimitStream(response, body0, client, offset, eff0, o.ReadIdleTimeout);
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
                    client, url, offset, effective, total, o, response, cancellationToken);
                handedOff = true;
                clientHandedOff = true;

                if (FetchLog.IsEnabled)
                {
                    FetchLog.Write("open path=parallel #" + stream.StreamId +
                        " url=" + FetchLog.Tail(url) +
                        " offset=" + offset + " length=" + effective +
                        " total=" + total.ToString(CultureInfo.InvariantCulture) +
                        " conn=" + o.Connections +
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
            }
        }

        private sealed class ProbeResult
        {
            internal HttpResponseMessage Response;
            internal bool OpenEnded;
        }

        private static HttpResponseMessage SendProbe(HttpClient client, string url, long from, long toInclusive,
                                                     ParallelFetchOptions o, bool refuseWholeBody,
                                                     out bool wentOpenEnded, CancellationToken cancellationToken)
        {
            ProbeResult r = SendProbeAsync(client, url, from, toInclusive, o, refuseWholeBody, cancellationToken)
                            .GetAwaiter().GetResult();
            wentOpenEnded = r.OpenEnded;
            return r.Response;
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
                                                              ParallelFetchOptions o, bool refuseWholeBody,
                                                              CancellationToken cancellationToken)
        {
            int attempt = 0;
            // -1 once we have fallen back to "bytes=<from>-".
            long rangeEnd = toInclusive;
            // Open() blocks the caller for this whole thing, so the total is capped, not just
            // each attempt. Four attempts x one header timeout each would otherwise pin a host
            // request thread for over a minute.
            long deadline = Environment.TickCount64 + (long)o.StallBudget.TotalMilliseconds;
            while (true)
            {
                bool timedOut = false;
                long retryAfterMs = -1;
                long remainingMs = deadline - Environment.TickCount64;
                if (remainingMs <= 0)
                {
                    throw new TimeoutException("Probe gave up after " +
                        o.StallBudget.TotalSeconds.ToString(CultureInfo.InvariantCulture) + "s without a usable response.");
                }
                try
                {
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
                    }
                    catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                    {
                        timedOut = true;
                        throw;
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
                        return new ProbeResult { Response = response, OpenEnded = rangeEnd < 0 };

                    HttpStatusCode transient = response.StatusCode;
                    retryAfterMs = HttpRangeHelper.RetryAfterMs(response);
                    try { response.Dispose(); } catch { }
                    throw new HttpRequestException("Probe returned " + (int)transient + ".", null, transient);
                }
                catch (Exception ex)
                {
                    if (cancellationToken.IsCancellationRequested) throw;
                    if (attempt + 1 >= o.MaxAttempts)
                    {
                        if (timedOut)
                        {
                            throw new TimeoutException("Probe request timed out after " +
                                o.ResponseHeadersTimeout.TotalSeconds.ToString(CultureInfo.InvariantCulture) +
                                "s (" + o.MaxAttempts + " attempts).");
                        }
                        throw;
                    }
                    attempt++;
                    Log("probe retry " + attempt + "/" + (o.MaxAttempts - 1) + " url=" + FetchLog.Tail(url) +
                        " range=" + from + "-" + (rangeEnd < 0 ? "(end)" : rangeEnd.ToString(CultureInfo.InvariantCulture)) +
                        " after " + FetchLog.Describe(ex));
                    long backoff = Math.Min((long)o.RetryBaseDelayMs << Math.Min(attempt - 1, 20), o.RetryMaxDelayMs);
                    long delay = HttpRangeHelper.RetryDelayMs((int)backoff, retryAfterMs);
                    long left = deadline - Environment.TickCount64;
                    if (left <= 0) throw;
                    if (delay > left) delay = left;
                    await Task.Delay((int)delay, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        internal static void Log(string message)
        {
            FetchLog.Write(message);
        }
    }
}
