using System;
using System.Globalization;

namespace EmbyStrmParallel
{
    /// <summary>
    /// Tuning knobs for <see cref="ParallelFetch"/>.
    ///
    /// Defaults measured against the real origin (10.5 GB file, per-connection throttle):
    ///   1 conn  =  4.08 Mbps      8 conn x  2 MiB = 25.8 / 28.1 Mbps
    ///   4 conn  = 14.67 Mbps      8 conn x  8 MiB = 24.2 / 31.6 Mbps
    ///   8 conn  = 32.05 Mbps     12 conn x  8 MiB = 42.5 Mbps
    ///                            16 conn x  8 MiB = persistent HTTP 503 (origin refuses)
    /// 8 connections is deliberately below the 503 cliff, because a media server can have
    /// several streams open at once. Chunk sizes between 2 and 16 MiB were not separable
    /// above run-to-run variance, so 8 MiB was kept: it gives the best per-request duty cycle
    /// (~16 s of transfer against a 0.6-4.0 s time-to-first-byte) and the fewest requests,
    /// which matters because 403/503 responses are charged per request.
    ///
    /// On the reference deployment (a low-power NAS running Emby, egress through a transparent
    /// proxy), with ramp-seconds = 2: 96 MB ranges at 17.8 / 20.4 / 20.8 Mbps, 90-120 s
    /// continuous reads at 25.9-28.9 Mbps, against a 4.1 Mbps single-connection baseline on a
    /// file that needs 13. Zero fatal exceptions, zero host fallbacks, RSS bounded ~330 MB.
    ///
    /// Every default here is a measurement, not a guess, but they were measured against ONE
    /// origin behind ONE proxy. The two settings that are origin-specific rather than
    /// universal are Connections and ConnectionRampInterval - both have documented cliffs.
    /// Re-derive them with `run-tests.sh seeks 25` before assuming they transfer.
    ///
    /// Overridable without touching code, through the shared routing configuration
    /// (shared/RoutingConfig.cs). Keys in strm-routing.txt, with the environment variable that
    /// overrides each one:
    ///
    ///   connections           EMBY_STRM_CONNECTIONS
    ///   chunk-mb              EMBY_STRM_CHUNK_MB
    ///   buffer-mb             EMBY_STRM_BUFFER_MB
    ///   initial-connections   EMBY_STRM_INITIAL_CONNECTIONS
    ///   ramp-seconds          EMBY_STRM_RAMP_SECONDS
    ///
    /// Prefer the file. An Emby upgrade rewrites bin/emby-server, which is where the exported
    /// environment variables live, so tuning kept there vanishes on upgrade - and the symptom
    /// months later is "it started stuttering again", which nobody traces back to a missing
    /// knob. programdata/ survives.
    /// </summary>
    public sealed class ParallelFetchOptions
    {
        /// <summary>Number of concurrent HTTP Range requests.</summary>
        public int Connections { get; set; } = 8;

        /// <summary>
        /// Connections opened immediately; the rest join one per ConnectionRampInterval.
        /// Set equal to Connections, or set ConnectionRampInterval to zero, to disable.
        ///
        /// Abandoning a stream leaves its in-flight connections lingering at the origin, and the
        /// origin degrades sharply once enough accumulate. Eight successive abandoned seeks:
        ///   no ramp   : 25.61 -> 0.80 -> 0.11 and stuck at 0.11 for the rest of the sequence
        ///   with ramp : 5.05, 6.00, 5.97, 5.98, 5.17, 5.81, 5.84, 6.06 - flat, no collapse
        /// A stream abandoned seconds after opening simply should not have cost eight connections.
        /// </summary>
        public int InitialConnections { get; set; } = 2;

        /// <summary>
        /// Delay between adding each connection beyond InitialConnections.
        /// Set with `ramp-seconds` in strm-routing.txt (or EMBY_STRM_RAMP_SECONDS).
        /// THIS IS THE KNOB WORTH TUNING PER DEPLOYMENT.
        ///
        /// Measured sweep on the live host (8 open-ended requests, each abandoned after 25s,
        /// 100s cooldown before each run; per-seek Mbps, then a separate continuous read):
        ///
        ///   RAMP  per-seek Mbps                                          avg    sustained
        ///   ----  ---------------------------------------------------   ----   ---------
        ///    6    2.35  5.52 10.21  9.63 10.04  7.53 10.21  9.96          8.2     25.02
        ///    3   12.81 12.90  4.59 12.90  3.17 10.30 12.73  7.61          9.6     27.47
        ///    2    7.61 18.27 12.90 15.33 18.18  4.93 18.10 15.67         13.9     28.93
        ///    2    10.13 12.98 12.98 18.10 18.27 15.67 15.67 15.66        14.9     25.91
        ///    1   23.55 11.14  0.08  0.15  0.48  0.15  0.08  0.15      COLLAPSE     0.23
        ///
        /// Two findings that should shape how you tune this:
        ///
        /// 1. There is NO trade-off between 6 and 2. RAMP=2 is better on both axes at once -
        ///    seek recovery AND sustained throughput - reproduced across two cooled-down runs.
        ///    If you have measurements for your origin, 2 is the better setting.
        ///
        /// 2. Between 1 and 2 there is a CLIFF, not a slope, and it is worse than the bug this
        ///    ramp fixes. At RAMP=1 the collapse takes the *continuous* case down with it
        ///    (0.23 Mbps on a plain sequential read), and continuous playback is the dominant
        ///    workload. RAMP=2 sits exactly one notch from that edge.
        ///
        /// The default stays at 6 because of finding 2, not despite finding 1: the failure mode
        /// past the edge is catastrophic and origin-specific, so an untuned deployment should
        /// have margin rather than peak numbers. Tune down with evidence, never by guessing -
        /// `run-tests.sh seeks 25` reproduces the sweep above against your own origin.
        /// </summary>
        public TimeSpan ConnectionRampInterval { get; set; } = TimeSpan.FromSeconds(6);

        /// <summary>Bytes fetched per Range request, once the opening ramp has finished.</summary>
        public int ChunkSize { get; set; } = 8 * 1024 * 1024;

        /// <summary>
        /// Size of chunk 0. Early chunks double from here up to ChunkSize so that in-order
        /// delivery reaches full speed in a couple of seconds instead of ~16. Set equal to
        /// ChunkSize to disable the ramp.
        /// </summary>
        public int FirstChunkSize { get; set; } = 1024 * 1024;

        /// <summary>Hard ceiling on bytes held in the reorder buffer. Determines slot count.</summary>
        public long MaxBufferBytes { get; set; } = 128L * 1024 * 1024;

        /// <summary>Granularity of hand-off between workers and the reader. Kept under the LOH threshold.</summary>
        public int BlockSize { get; set; } = 64 * 1024;

        /// <summary>Total attempts per chunk (1 = no retry).</summary>
        public int MaxAttempts { get; set; } = 4;

        /// <summary>Base for exponential retry backoff.</summary>
        public int RetryBaseDelayMs { get; set; } = 250;

        /// <summary>Cap for exponential retry backoff.</summary>
        public int RetryMaxDelayMs { get; set; } = 4000;

        /// <summary>Budget for "request sent -> response headers received" on a single attempt.</summary>
        public TimeSpan ResponseHeadersTimeout { get; set; } = TimeSpan.FromSeconds(20);

        /// <summary>Budget for "no bytes arrived on an open body". Rescheduled on every successful read.</summary>
        public TimeSpan ReadIdleTimeout { get; set; } = TimeSpan.FromSeconds(20);

        /// <summary>
        /// Hard cap on how long a chunk may make NO progress before the error is surfaced,
        /// across all of its attempts and backoffs. Any byte delivered resets it, so a slow but
        /// advancing chunk is never killed.
        ///
        /// Without this, per-attempt timeouts multiply: four attempts that each hang until the
        /// header timeout took 92 s to report a dead chunk, and in-order delivery means the
        /// consumer sees nothing for that whole time. On a media server a stall is far more
        /// visible than a slow byte, so time-to-error has to be bounded, not just finite.
        /// </summary>
        public TimeSpan StallBudget { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Floor on a single connection's observed body throughput. An attempt that reads slower
        /// than this for MinThroughputGrace is abandoned and retried on a fresh connection.
        ///
        /// The idle timeout only ever caught a connection delivering *nothing*. A stale pooled
        /// socket behind the proxy delivers a trickle instead, which resets the idle timer
        /// forever: the live host collapsed to 0.23 Mbps across eight connections (~3.6 KB/s
        /// each) with zero retries and zero errors logged. 48 KB/s sits ~13x above that and
        /// ~10x below the 4.1 Mbps a healthy single connection sustains, so it separates the two
        /// cleanly. Time spent blocked on a slow consumer is excluded, so backpressure cannot
        /// trip it.
        ///
        /// Confirmed load-bearing in production, not merely defensive: ~50 slow-connection
        /// retries in a single live test session, every one of them recovering silently. Before
        /// this floor existed each of those would have been an invisible partial stall.
        /// </summary>
        public int MinThroughputBytesPerSec { get; set; } = 48 * 1024;

        /// <summary>Reading time an attempt gets before the throughput floor applies (TCP ramp, redirect).</summary>
        public TimeSpan MinThroughputGrace { get; set; } = TimeSpan.FromSeconds(6);

        /// <summary>
        /// How far the degraded "origin ignored Range" path may skip before it is refused.
        ///
        /// That path answers a ranged request from a 200 whole-resource body by reading and
        /// discarding everything ahead of the offset. For a small offset that is a reasonable
        /// way to stay byte-exact against an origin with no Range support. For a large one it is
        /// indefensible: production hit this three times, every one of them a far-tail read, and
        /// the worst asked for the last 2083 bytes of a 10.48 GB file — 10.48 GB downloaded and
        /// thrown away to deliver 2 KB, with nothing in the log saying so.
        ///
        /// Past this ceiling the fetcher retries the probe (the origin answered 206 for the same
        /// shape on 21 out of 21 direct attempts, so a 200 there looks transient) and then
        /// declines, handing the request back to Emby rather than spending the bandwidth.
        /// </summary>
        public long MaxIgnoredRangeSkipBytes { get; set; } = 64L * 1024 * 1024;

        // ---- derived, filled in by Normalize() ----

        internal int Slots { get; private set; }
        internal int BlocksPerChunk { get; private set; }

        /// <summary>
        /// Clamps everything into a self-consistent, safe range. Called by ParallelFetch before use.
        /// Never throws.
        /// </summary>
        internal ParallelFetchOptions Normalize()
        {
            ParallelFetchOptions o = (ParallelFetchOptions)MemberwiseClone();

            o.BlockSize = Clamp(o.BlockSize, 4 * 1024, 1024 * 1024);
            o.ChunkSize = Clamp(o.ChunkSize, o.BlockSize, 256 * 1024 * 1024);
            o.FirstChunkSize = Clamp(o.FirstChunkSize, o.BlockSize, o.ChunkSize);
            o.Connections = Clamp(o.Connections, 1, MaxConnections);
            o.MaxAttempts = Clamp(o.MaxAttempts, 1, 16);
            o.RetryBaseDelayMs = Clamp(o.RetryBaseDelayMs, 0, 10000);
            o.RetryMaxDelayMs = Clamp(o.RetryMaxDelayMs, o.RetryBaseDelayMs, 60000);
            if (o.MaxBufferBytes < o.ChunkSize) o.MaxBufferBytes = o.ChunkSize;
            // Also bound from above, for options built in code rather than read from the file:
            // Slots is capped at Connections + 4, so an unbounded buffer would still allow
            // 68 x 256 MiB = 17 GiB of read-ahead in a process that also has to serve video.
            if (o.MaxBufferBytes > MaxBufferMiB * 1024 * 1024) o.MaxBufferBytes = MaxBufferMiB * 1024 * 1024;

            // Slots = the reorder window. Memory ceiling == Slots * ChunkSize.
            long slots = o.MaxBufferBytes / o.ChunkSize;
            if (slots > o.Connections + 4) slots = o.Connections + 4;
            if (slots < 1) slots = 1;
            o.Slots = (int)slots;

            // A worker whose slot cannot hold a whole chunk would stall holding an open socket,
            // so never run more workers than slots.
            if (o.Connections > o.Slots) o.Connections = o.Slots;

            // 0 or >= Connections means "no ramp": every worker starts immediately.
            if (o.InitialConnections <= 0 || o.InitialConnections > o.Connections) o.InitialConnections = o.Connections;
            if (o.ConnectionRampInterval < TimeSpan.Zero) o.ConnectionRampInterval = TimeSpan.FromSeconds(4);
            // The ramp delay is multiplied by the worker's position, and Task.Delay rejects
            // anything past ~49 days. Bounding it here keeps that arithmetic in range.
            if (o.ConnectionRampInterval > TimeSpan.FromSeconds(MaxRampSeconds))
                o.ConnectionRampInterval = TimeSpan.FromSeconds(MaxRampSeconds);

            o.BlocksPerChunk = (o.ChunkSize + o.BlockSize - 1) / o.BlockSize;
            if (o.BlocksPerChunk < 1) o.BlocksPerChunk = 1;

            if (o.ResponseHeadersTimeout <= TimeSpan.Zero) o.ResponseHeadersTimeout = TimeSpan.FromSeconds(20);
            if (o.ReadIdleTimeout <= TimeSpan.Zero) o.ReadIdleTimeout = TimeSpan.FromSeconds(20);
            if (o.StallBudget <= TimeSpan.Zero) o.StallBudget = TimeSpan.FromSeconds(30);
            if (o.MinThroughputBytesPerSec < 0) o.MinThroughputBytesPerSec = 0;
            if (o.MinThroughputGrace <= TimeSpan.Zero) o.MinThroughputGrace = TimeSpan.FromSeconds(6);
            if (o.MaxIgnoredRangeSkipBytes < 0) o.MaxIgnoredRangeSkipBytes = 0;

            return o;
        }

        internal long MemoryCeilingBytes
        {
            get { return (long)Slots * ChunkSize; }
        }

        private static int Clamp(int v, int lo, int hi)
        {
            return v < lo ? lo : (v > hi ? hi : v);
        }

        /// <summary>
        /// Reads overrides from the shared routing configuration. Any parse failure leaves the
        /// default in place.
        ///
        /// There is deliberately NO environment-variable reading here: StrmDirect.GetSetting
        /// already resolves env &gt; file &gt; nothing for every key in one place. A second reader
        /// alongside it is exactly the two-implementations-drift problem the shared parser
        /// exists to kill (references/mode-routing.md 4.1).
        ///
        /// Called once per stream rather than cached, which is what makes an edit to
        /// strm-routing.txt go live inside the parser's 30 s window instead of at the next
        /// Emby restart. Sweeping ramp-seconds without restarting depends on that.
        /// </summary>
        internal static ParallelFetchOptions FromConfiguration()
        {
            ParallelFetchOptions o = new ParallelFetchOptions();
            try
            {
                long n;
                // Every value is bounded BEFORE it is scaled or narrowed. `chunk-mb = 2048` used
                // to overflow int during `n * 1024 * 1024`, land on int.MinValue, and then get
                // clamped by Normalize() into 64 KiB chunks - the opposite of what was asked
                // for, applied silently. A tuning knob that quietly means something else is
                // worse than one that is ignored, because the numbers still look plausible.
                if (TrySetting("connections", out n)) o.Connections = (int)Math.Min(n, MaxConnections);
                if (TrySetting("chunk-mb", out n)) o.ChunkSize = (int)(Math.Min(n, MaxChunkMiB) * 1024 * 1024);
                if (TrySetting("buffer-mb", out n)) o.MaxBufferBytes = Math.Min(n, MaxBufferMiB) * 1024 * 1024;
                if (TrySetting("initial-connections", out n)) o.InitialConnections = (int)Math.Min(n, MaxConnections);
                if (TrySetting("ramp-seconds", out n)) o.ConnectionRampInterval = TimeSpan.FromSeconds(Math.Min(n, MaxRampSeconds));
            }
            catch
            {
                // Hot path in a media server: configuration must never be able to throw.
            }
            return o;
        }

        private const int MaxConnections = 64;
        private const long MaxChunkMiB = 256;
        private const long MaxBufferMiB = 2048;
        private const long MaxRampSeconds = 600;

        private static bool TrySetting(string key, out long value)
        {
            value = 0;
            string raw = StrmDirect.GetSetting(key);
            if (string.IsNullOrWhiteSpace(raw)) return false;
            return long.TryParse(raw.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out value) && value > 0;
        }
    }
}
