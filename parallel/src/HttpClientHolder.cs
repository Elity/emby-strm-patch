using System;
using System.Net;
using System.Net.Http;
using System.Threading;

namespace EmbyStrmParallel
{
    /// <summary>
    /// Connection pools are per-stream, deliberately.
    ///
    /// A shared pool made abandoning a stream poison the next one. Measured on the live host:
    /// eight successive open-ended requests, each abandoned after 25s, collapsed monotonically
    /// 25.14 -> 2.58 -> 0.57 -> 0.15 -> 0.23 Mbps and stayed there, while plain curl against the
    /// same origin at the same moment got 4.24 Mbps on one connection and 31.58 Mbps on eight.
    /// Nothing was logged, because the requests all *succeeded* - they merely trickled. Recovery
    /// was time-based (~90s idle), matching the pool's idle timeout rather than any request.
    ///
    /// Every chunk is 302'd to a freshly signed, short-lived CDN target, and the NAS egresses
    /// through a transparent proxy, so a pooled socket left over from an abandoned transfer can
    /// still look alive locally while its upstream leg is gone. Reusing it stalls.
    ///
    /// Giving each stream its own handler means an abandoned stream's sockets cannot be inherited
    /// by the next one, and disposing that handler closes them immediately instead of waiting for
    /// an idle timeout. Reuse *within* a stream is unaffected, which is where it actually pays:
    /// the workers still keep their connections hot across the ~1250 chunks of a 10 GB file.
    /// </summary>
    internal static class HttpClientHolder
    {
        internal const int MaxConnectionsPerServer = 64;

        internal static HttpClient CreateForStream()
        {
            SocketsHttpHandler handler = new SocketsHttpHandler
            {
                AllowAutoRedirect = true,
                MaxAutomaticRedirections = 5,
                AutomaticDecompression = DecompressionMethods.None,
                MaxConnectionsPerServer = MaxConnectionsPerServer,
                ConnectTimeout = TimeSpan.FromSeconds(20),
                // Short, because a pooled socket that has gone stale behind the proxy is the
                // failure mode this class exists to avoid. Workers request continuously, so an
                // in-use connection never reaches the idle timeout.
                PooledConnectionLifetime = TimeSpan.FromMinutes(2),
                PooledConnectionIdleTimeout = TimeSpan.FromSeconds(10),
                EnableMultipleHttp2Connections = false,
                UseCookies = false
            };

            HttpClient client = new HttpClient(handler, disposeHandler: true);
            // We manage every timeout ourselves; HttpClient.Timeout would also cap the (long)
            // body read of a chunk, which is not what we want.
            client.Timeout = Timeout.InfiniteTimeSpan;
            client.DefaultRequestVersion = HttpVersion.Version11;
            client.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrLower;
            return client;
        }
    }
}
