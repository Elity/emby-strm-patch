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
            long probeSpan = length > 0 ? Math.Min(length, o.FirstChunkSize) : o.FirstChunkSize;
            long probeEnd = offset + probeSpan - 1;

            HttpResponseMessage response = SendProbe(client, url, offset, probeEnd, o, cancellationToken);
            bool handedOff = false;
            try
            {
                if (response.StatusCode == HttpStatusCode.RequestedRangeNotSatisfiable)
                {
                    throw new IOException("Origin rejected range " + offset + "-" + probeEnd + " (416).");
                }

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
                                   " offset=" + offset + " length=" + eff0 + " total=" + full);
                    return new SkipLimitStream(response, body0, client, offset, eff0);
                }

                if (response.StatusCode != HttpStatusCode.PartialContent)
                {
                    throw new HttpRequestException("Probe returned " + (int)response.StatusCode + ".", null, response.StatusCode);
                }

                long from, to, total;
                bool haveContentRange = HttpRangeHelper.TryGetContentRange(response, out from, out to, out total);
                if (haveContentRange && from >= 0 && from != offset)
                {
                    throw new IOException("Content-Range start " + from + " does not match requested " + offset + ".");
                }

                // RFC 7233: complete-length must exceed last-byte-pos. A proxy or cache that
                // fills in the size of a *different* object produces e.g. "bytes 50000-149999/0".
                // Trusting that total would clamp the delivery to nothing and hand the host a
                // clean, successful, EMPTY 206 - silent data loss with no error anywhere. Treat
                // it as an unusable total instead.
                if (haveContentRange && total >= 0)
                {
                    long lastByte = to >= 0 ? to : offset;
                    if (total <= lastByte)
                    {
                        throw new IOException("Origin returned a self-contradictory Content-Range: total " + total +
                                              " does not exceed last-byte-pos " + lastByte +
                                              " (RFC 7233 requires complete-length > last-byte-pos).");
                    }
                }

                long effective;
                if (!haveContentRange || total < 0)
                {
                    // No usable "/total". Never invent one: the host puts this straight into the
                    // Content-Range response header. A bounded range can still be served exactly;
                    // an open-ended one cannot, because we do not know where the resource ends.
                    if (length <= 0)
                    {
                        throw new IOException("Origin returned 206 without a Content-Range total; cannot locate end of resource.");
                    }
                    effective = length;
                    totalLength = null;
                    contentLength = effective;
                }
                else
                {
                    long available = total - offset;
                    if (available <= 0)
                    {
                        // Unreachable given the RFC check above; refuse rather than return empty.
                        throw new IOException("Origin reported total " + total + " at or below the requested offset " +
                                              offset + " while answering 206.");
                    }
                    effective = length > 0 ? Math.Min(length, available) : available;
                    totalLength = total;
                    contentLength = effective;
                }

                if (effective == 0)
                {
                    FetchLog.Write("open path=empty url=" + FetchLog.Tail(url) + " offset=" + offset);
                    return new MemoryStream(Array.Empty<byte>(), false);
                }

                ParallelRangeStream stream = new ParallelRangeStream(
                    client, url, offset, effective, o, response, cancellationToken);
                handedOff = true;
                clientHandedOff = true;

                if (FetchLog.IsEnabled)
                {
                    FetchLog.Write("open path=parallel #" + stream.StreamId +
                        " url=" + FetchLog.Tail(url) +
                        " offset=" + offset + " length=" + effective +
                        " total=" + (totalLength.HasValue ? totalLength.Value.ToString(CultureInfo.InvariantCulture) : "unknown") +
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

        private static HttpResponseMessage SendProbe(HttpClient client, string url, long from, long toInclusive,
                                                     ParallelFetchOptions o, CancellationToken cancellationToken)
        {
            return SendProbeAsync(client, url, from, toInclusive, o, cancellationToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// The probe IS chunk 0, so it gets the same bounded retry as any other chunk. Without
        /// this a single 403 from an expired redirect target - the exact failure this origin
        /// produces - would fail the whole request before a single byte was fetched.
        /// </summary>
        private static async Task<HttpResponseMessage> SendProbeAsync(HttpClient client, string url, long from, long toInclusive,
                                                                      ParallelFetchOptions o, CancellationToken cancellationToken)
        {
            int attempt = 0;
            // Open() blocks the caller for this whole thing, so the total is capped, not just
            // each attempt. Four attempts x one header timeout each would otherwise pin a host
            // request thread for over a minute.
            long deadline = Environment.TickCount64 + (long)o.StallBudget.TotalMilliseconds;
            while (true)
            {
                bool timedOut = false;
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
                        req.Headers.Range = new RangeHeaderValue(from, toInclusive);
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

                    if (!HttpRangeHelper.IsTransientStatus(response.StatusCode)) return response;

                    HttpStatusCode transient = response.StatusCode;
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
                        " range=" + from + "-" + toInclusive + " after " + FetchLog.Describe(ex));
                    long delay = Math.Min((long)o.RetryBaseDelayMs << Math.Min(attempt - 1, 20), o.RetryMaxDelayMs);
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
