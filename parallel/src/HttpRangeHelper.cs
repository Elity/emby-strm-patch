using System;
using System.Globalization;
using System.Net;
using System.Net.Http;

namespace EmbyStrmParallel
{
    internal static class HttpRangeHelper
    {
        /// <summary>
        /// Parses "bytes 0-8388607/10484848965". Returns false if the header is absent or
        /// unparseable. total is -1 when the server sent "/*".
        /// </summary>
        internal static bool TryGetContentRange(HttpResponseMessage response, out long from, out long to, out long total)
        {
            from = -1;
            to = -1;
            total = -1;
            if (response == null || response.Content == null) return false;

            System.Collections.Generic.IEnumerable<string> values;
            if (!response.Content.Headers.TryGetValues("Content-Range", out values)) return false;

            string raw = null;
            foreach (string v in values) { raw = v; break; }
            if (string.IsNullOrWhiteSpace(raw)) return false;

            raw = raw.Trim();
            int sp = raw.IndexOf(' ');
            if (sp < 0) return false;
            // unit must be bytes
            if (!raw.AsSpan(0, sp).Trim().Equals("bytes".AsSpan(), StringComparison.OrdinalIgnoreCase)) return false;

            string rest = raw.Substring(sp + 1).Trim();
            int slash = rest.IndexOf('/');
            if (slash < 0) return false;

            string span = rest.Substring(0, slash).Trim();
            string totalPart = rest.Substring(slash + 1).Trim();

            if (totalPart != "*")
            {
                if (!long.TryParse(totalPart, NumberStyles.Integer, CultureInfo.InvariantCulture, out total)) return false;
            }

            if (span != "*")
            {
                int dash = span.IndexOf('-');
                if (dash <= 0) return false;
                if (!long.TryParse(span.Substring(0, dash).Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out from)) return false;
                if (!long.TryParse(span.Substring(dash + 1).Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out to)) return false;
            }

            return true;
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
    }
}
