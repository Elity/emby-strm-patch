using System;
using System.Globalization;
using System.Net;
using System.Net.Http;

namespace EmbyStrmParallel
{
    /// <summary>
    /// How a 206's Content-Range header parsed. The distinction matters: a header that is
    /// absent is not the same as one that is present and unusable, and neither may be treated
    /// as "fine, carry on".
    ///
    /// This used to be a bool with -1 sentinels in the out params, and every call site had to
    /// reconstruct the missing states from those sentinels (`from >= 0`, `total >= 0`). They
    /// reconstructed them inconsistently, and a *missing* header skipped position validation
    /// entirely - fail-open on the one input that decides where the bytes belong.
    /// </summary>
    internal enum ContentRangeStatus
    {
        /// <summary>No Content-Range header at all. Illegal on a 206 (RFC 7233 4.1).</summary>
        Missing = 0,

        /// <summary>Present but unparseable, wrong unit, negative, or self-contradictory.</summary>
        Malformed = 1,

        /// <summary>"bytes */N" - an unsatisfied-range form, legal only on a 416.</summary>
        Unsatisfied = 2,

        /// <summary>first-byte-pos and last-byte-pos are present and coherent. total is -1 for "/*".</summary>
        Valid = 3
    }

    internal static class HttpRangeHelper
    {
        /// <summary>
        /// Parses "bytes 0-8388607/10484848965".
        ///
        /// Coherence is checked here rather than at the call sites so there is exactly one
        /// definition of "usable byte range": last-byte-pos must not precede first-byte-pos, and
        /// a numeric complete-length must exceed last-byte-pos (RFC 7233 4.2). A proxy or cache
        /// that fills in the size of a *different* object produces e.g. "bytes 50000-149999/0" -
        /// trusting that total would clamp delivery to nothing and hand the host a clean,
        /// successful, EMPTY 206.
        ///
        /// total is -1 when the server sent "/*"; the caller decides whether it can live
        /// without a complete-length.
        /// </summary>
        internal static ContentRangeStatus ParseContentRange(HttpResponseMessage response,
                                                             out long from, out long to, out long total)
        {
            from = -1;
            to = -1;
            total = -1;
            if (response == null || response.Content == null) return ContentRangeStatus.Missing;

            System.Collections.Generic.IEnumerable<string> values;
            if (!response.Content.Headers.TryGetValues("Content-Range", out values)) return ContentRangeStatus.Missing;

            string raw = null;
            foreach (string v in values) { raw = v; break; }
            if (string.IsNullOrWhiteSpace(raw)) return ContentRangeStatus.Missing;

            raw = raw.Trim();
            int sp = raw.IndexOf(' ');
            if (sp < 0) return ContentRangeStatus.Malformed;
            // unit must be bytes
            if (!raw.AsSpan(0, sp).Trim().Equals("bytes".AsSpan(), StringComparison.OrdinalIgnoreCase)) return ContentRangeStatus.Malformed;

            string rest = raw.Substring(sp + 1).Trim();
            int slash = rest.IndexOf('/');
            if (slash < 0) return ContentRangeStatus.Malformed;

            string span = rest.Substring(0, slash).Trim();
            string totalPart = rest.Substring(slash + 1).Trim();

            if (totalPart != "*")
            {
                if (!long.TryParse(totalPart, NumberStyles.Integer, CultureInfo.InvariantCulture, out total)) return ContentRangeStatus.Malformed;
                if (total < 0) return ContentRangeStatus.Malformed;
            }

            if (span == "*") return ContentRangeStatus.Unsatisfied;

            int dash = span.IndexOf('-');
            if (dash <= 0) return ContentRangeStatus.Malformed;
            if (!long.TryParse(span.Substring(0, dash).Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out from)) return ContentRangeStatus.Malformed;
            if (!long.TryParse(span.Substring(dash + 1).Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out to)) return ContentRangeStatus.Malformed;

            if (from < 0 || to < from) return ContentRangeStatus.Malformed;
            if (total >= 0 && total <= to) return ContentRangeStatus.Malformed;

            return ContentRangeStatus.Valid;
        }

        /// <summary>Wording for an error message; the caller supplies the consequence.</summary>
        internal static string Describe(ContentRangeStatus status)
        {
            if (status == ContentRangeStatus.Missing) return "no Content-Range header";
            if (status == ContentRangeStatus.Malformed) return "an unparseable or self-contradictory Content-Range";
            if (status == ContentRangeStatus.Unsatisfied) return "an unsatisfied-range Content-Range (\"bytes */N\")";
            return "a usable Content-Range";
        }

        /// <summary>
        /// Refuses a response whose body is not the raw representation.
        ///
        /// We never negotiate compression (the request pins `identity` and the handler has
        /// automatic decompression off), but "did not ask for it" is not the same as "cannot
        /// receive it": absent an Accept-Encoding a server may pick any encoding it likes
        /// (RFC 7231 5.3.4). Byte ranges of a re-encoded representation are not media bytes, and
        /// splicing them produces a file that is the right length and completely wrong.
        ///
        /// NotSupportedException on purpose: retrying cannot change a server's mind about how it
        /// encodes, so this must fail the chunk immediately rather than burn the attempt budget.
        /// </summary>
        internal static void EnsureIdentityEncoding(HttpResponseMessage response)
        {
            if (response == null || response.Content == null) return;
            foreach (string enc in response.Content.Headers.ContentEncoding)
            {
                if (string.IsNullOrWhiteSpace(enc)) continue;
                if (!string.Equals(enc.Trim(), "identity", StringComparison.OrdinalIgnoreCase))
                {
                    throw new NotSupportedException("Origin answered with Content-Encoding: " + enc.Trim() +
                                                    "; a re-encoded representation cannot be spliced by byte offset.");
                }
            }
        }

        /// <summary>
        /// The server's own instruction on when to come back, in milliseconds, or -1 when it
        /// did not say. Both RFC 7231 forms are accepted: delta-seconds and an HTTP-date.
        ///
        /// Worth honouring rather than guessing: this origin answers 503 once enough connections
        /// pile up, and several workers backing off on identical exponential timers reconverge
        /// into the same thundering herd that caused it.
        /// </summary>
        internal static long RetryAfterMs(HttpResponseMessage response)
        {
            if (response == null) return -1;
            try
            {
                System.Net.Http.Headers.RetryConditionHeaderValue ra = response.Headers.RetryAfter;
                if (ra == null) return -1;
                if (ra.Delta.HasValue)
                {
                    double ms = ra.Delta.Value.TotalMilliseconds;
                    return ms > 0 ? (long)ms : 0;
                }
                if (ra.Date.HasValue)
                {
                    double ms = (ra.Date.Value - DateTimeOffset.UtcNow).TotalMilliseconds;
                    return ms > 0 ? (long)ms : 0;
                }
            }
            catch
            {
                // A malformed Retry-After is not worth failing a request over.
            }
            return -1;
        }

        /// <summary>Transient == worth retrying against the ORIGINAL url (which re-signs the redirect).</summary>
        internal static bool IsTransientStatus(HttpStatusCode status)
        {
            int code = (int)status;
            if (code == 403) return true;   // time-limited redirect target expired
            if (code == 408) return true;
            if (code == 425) return true;
            if (code == 429) return true;
            if (code >= 500) return true;
            return false;
        }

        internal static bool IsTransientException(Exception ex)
        {
            if (ex is HttpRequestException) return true;
            if (ex is System.IO.IOException) return true;
            if (ex is System.Net.Sockets.SocketException) return true;
            if (ex is ObjectDisposedException) return false;
            return false;
        }

        /// <summary>
        /// Errors that mean the process is in trouble, not the origin. Converting these into a
        /// quiet fallback would hide a failing host behind "playback is just slow today".
        /// </summary>
        internal static bool IsFatalToProcess(Exception ex)
        {
            return ex is OutOfMemoryException;
        }

        /// <summary>
        /// Is retrying this exception capable of a different outcome?
        ///
        /// Shared by the chunk loop and the probe loop for the same reason BackoffMs is: they
        /// must not drift apart on retry policy. The probe had no gate at all, which was harmless
        /// only while MaxAttempts capped every shape at four - once unanswered attempts moved off
        /// that counter, a structurally impossible error (a malformed URL in strm-routing.txt
        /// throwing UriFormatException, an unsupported scheme throwing NotSupportedException) had
        /// nothing left to stop it but the wall clock, and burned the whole probe budget on a
        /// synchronously-blocked Emby request thread, per request, forever.
        /// </summary>
        internal static bool IsRetryable(Exception ex)
        {
            if (IsFatalToProcess(ex)) return false;                        // the process, not the origin
            if (ex is NotSupportedException) return false;                 // server ignored Range
            if (ex is OriginBudgetTimeoutException) return false;          // our own limiter, wedged
            if (ex is OperationCanceledException) return true;             // our own phase timeout
            HttpRequestException hre = ex as HttpRequestException;
            if (hre != null)
            {
                if (hre.StatusCode.HasValue) return IsTransientStatus(hre.StatusCode.Value);
                return true;                                               // connect/DNS/reset
            }
            if (IsTransientException(ex)) return true;
            return false;
        }

        /// <summary>
        /// Exponential backoff for attempt N (1-based), capped. One copy, because the chunk loop
        /// and the probe loop must not drift apart on retry policy - they were two identical
        /// hand-written expressions in two files.
        /// </summary>
        internal static int BackoffMs(int attempt, int baseMs, int maxMs)
        {
            // Math.Max guards the 1-based contract. A caller passing 0 shifts by -1, and a shift
            // count on a long is masked to & 63, so `base << -1` becomes `base << 63` - which
            // overflows to a NEGATIVE number that the clamp below then flattens to zero delay.
            // The result is not a slow retry, it is no retry delay at all: feeding the wrong
            // counter in here once produced 39,253 connect attempts in 30 seconds. Clamping the
            // shift instead turns a caller's bug into a merely-flat backoff rather than a storm.
            long d = (long)baseMs << Math.Min(Math.Max(attempt - 1, 0), 20);
            if (d > maxMs) d = maxMs;
            if (d < 0) d = 0;
            return (int)d;
        }

        /// <summary>
        /// Retry delay: never shorter than what the server asked for, plus up to 25% jitter so
        /// N workers that failed together do not come back together.
        ///
        /// backoffMs is our own guess and the caller has already capped it. Retry-After is not a
        /// guess and is deliberately NOT capped by that same ceiling - coming back early is
        /// exactly what the server just told us not to do. The caller bounds the result by the
        /// stall budget instead, so honouring a hostile Retry-After still ends in a prompt
        /// error rather than an unbounded wait.
        /// </summary>
        internal static int RetryDelayMs(int backoffMs, long retryAfterMs)
        {
            long d = backoffMs;
            if (retryAfterMs > d) d = retryAfterMs;
            if (d < 0) d = 0;
            long jitter = d / 4;
            if (jitter > 0) d += Random.Shared.NextInt64(0, jitter + 1);
            if (d > int.MaxValue) d = int.MaxValue;
            return (int)d;
        }
    }
}
