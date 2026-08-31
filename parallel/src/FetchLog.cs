using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;

namespace EmbyStrmParallel
{
    /// <summary>
    /// Self-arming diagnostics. The call site is hand-written CIL that will never assign
    /// <see cref="ParallelFetch.Logger"/>, so without this the component would fail silently
    /// inside a running host - playback quietly dropping to single-connection speed with
    /// nothing anywhere explaining why.
    ///
    /// Sinks, both optional and independent:
    ///   * ParallelFetch.Logger, if a host ever does assign it
    ///   * a file named by the `log` setting in strm-routing.txt, or by the environment variable
    ///     EMBY_STRM_LOG which overrides it (append, timestamped, parent dir created)
    ///
    /// With neither configured this costs two volatile reads and returns. Logging failures are
    /// swallowed exactly like configuration failures, and the file sink disables itself after
    /// repeated failures so a bad path cannot make every request pay for an exception.
    /// </summary>
    internal static class FetchLog
    {
        /// <summary>The setting key; StrmDirect maps it to PathVariable for the env override.</summary>
        internal const string SettingKey = "log";
        internal const string PathVariable = "EMBY_STRM_LOG";
        private const long MaxBytes = 8L * 1024 * 1024;
        private const int MaxConsecutiveFailures = 5;
        private const int TailLength = 40;

        private static readonly object Gate = new object();
        private static int _failures;

        /// <summary>Cheap enough to guard string building at call sites.</summary>
        internal static bool IsEnabled
        {
            get { return ParallelFetch.Logger != null || FilePath() != null; }
        }

        internal static void Write(string message)
        {
            Action<string> sink = ParallelFetch.Logger;
            if (sink != null)
            {
                try { sink("[ParallelFetch] " + message); } catch { }
            }

            string path = FilePath();
            if (path == null) return;

            try
            {
                string line = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture) +
                              " [ParallelFetch] " + message + Environment.NewLine;
                lock (Gate)
                {
                    string dir = Path.GetDirectoryName(path);
                    if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);
                    RotateIfHuge(path);
                    File.AppendAllText(path, line);
                }
                Volatile.Write(ref _failures, 0);
            }
            catch
            {
                Interlocked.Increment(ref _failures);
            }
        }

        /// <summary>
        /// A short, non-sensitive identifier for a url.
        ///
        /// The query string is dropped outright and the last 40 characters of what remains are
        /// kept. On the real origin the signature lives entirely in the query (51 of 245 chars),
        /// while the path's trailing component is a 53-char file id - so this is both safer than
        /// a blind tail-40 (which would be pure signature) and more useful for telling two
        /// requests apart. Dropping the query first also means a *short* signed url cannot leak
        /// in full.
        /// </summary>
        internal static string Tail(string url)
        {
            if (string.IsNullOrEmpty(url)) return "(none)";
            int q = url.IndexOf('?');
            string withoutQuery = q >= 0 ? url.Substring(0, q) : url;
            string tail = withoutQuery.Length <= TailLength
                ? withoutQuery
                : "..." + withoutQuery.Substring(withoutQuery.Length - TailLength);
            return q >= 0 ? tail + "?<redacted>" : tail;
        }

        /// <summary>Byte counts in whichever unit does not round to zero.</summary>
        internal static string Size(long bytes)
        {
            if (bytes >= 1024 * 1024) return (bytes / 1024 / 1024) + "MiB";
            if (bytes >= 1024) return (bytes / 1024) + "KiB";
            return bytes + "B";
        }

        /// <summary>Renders a failure the way an operator wants to read it: HTTP status when there is one.</summary>
        internal static string Describe(Exception ex)
        {
            if (ex == null) return "(none)";
            HttpRequestException hre = ex as HttpRequestException;
            if (hre != null && hre.StatusCode.HasValue)
            {
                return "HTTP " + (int)hre.StatusCode.Value + " (" + hre.StatusCode.Value + ")";
            }
            string s = ex.GetType().Name + ": " + ex.Message;
            if (ex.InnerException != null)
            {
                HttpRequestException inner = ex.InnerException as HttpRequestException;
                if (inner != null && inner.StatusCode.HasValue)
                {
                    s += " <- HTTP " + (int)inner.StatusCode.Value;
                }
                else
                {
                    s += " <- " + ex.InnerException.GetType().Name + ": " + ex.InnerException.Message;
                }
            }
            return s;
        }

        /// <summary>
        /// Resolved on every call rather than latched once. The 30 s reload window lives in
        /// StrmDirect; caching the answer a second time here would mean the log path and the
        /// rest of strm-routing.txt went live at different moments, and a path latched at first
        /// use would ignore the file for the lifetime of the process. Both IsEnabled call sites
        /// are once per stream and Write only reaches this when logging is already on, so there
        /// is no hot loop paying for it.
        /// </summary>
        private static string FilePath()
        {
            if (Volatile.Read(ref _failures) >= MaxConsecutiveFailures) return null;
            try
            {
                string raw = StrmDirect.GetSetting(SettingKey);
                return string.IsNullOrWhiteSpace(raw) ? null : raw.Trim();
            }
            catch
            {
                return null;
            }
        }

        private static void RotateIfHuge(string path)
        {
            try
            {
                FileInfo fi = new FileInfo(path);
                if (!fi.Exists || fi.Length <= MaxBytes) return;
                string previous = path + ".1";
                if (File.Exists(previous)) File.Delete(previous);
                File.Move(path, previous);
            }
            catch
            {
                // A log that cannot be rotated is still better than a request that fails.
            }
        }

        /// <summary>
        /// Test hook: clear the failure counter and force the next read of the routing config,
        /// so a test that just rewrote the file or the environment sees it immediately instead
        /// of waiting out the 30 s window.
        /// </summary>
        internal static void ResetForTests()
        {
            lock (Gate)
            {
                Volatile.Write(ref _failures, 0);
                StrmDirect.InvalidateCache();
            }
        }
    }
}
