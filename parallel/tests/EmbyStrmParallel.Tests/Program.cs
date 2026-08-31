using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace EmbyStrmParallel.Tests
{
    internal static class Program
    {
        private static async Task<int> Main(string[] args)
        {
            string mode = args.Length > 0 ? args[0].ToLowerInvariant() : "all";

            if (mode == "programdata-child") return ConfigTests.ProgramDataChild();

            Console.WriteLine("EmbyStrmParallel test runner  (mode: " + mode + ")");
            Console.WriteLine("runtime " + Environment.Version + "  cpus " + Environment.ProcessorCount);

            using (CancellationTokenSource cts = new CancellationTokenSource())
            {
                Console.CancelKeyPress += (s, e) => { e.Cancel = true; cts.Cancel(); };
                CancellationToken ct = cts.Token;

                bool wantMock = mode == "all" || mode == "mock";
                bool wantBudget = mode == "budget";
                bool wantConfig = mode == "all" || mode == "config";
                bool wantLogging = mode == "all" || mode == "config" || mode == "logging";
                bool wantLive = mode == "all" || mode == "live" || mode == "correctness-live";
                bool wantThroughput = mode == "all" || mode == "live" || mode == "throughput";
                bool wantSoak = mode == "soak";
                bool wantTune = mode == "tune";
                bool wantMeasure = mode == "measure";
                bool wantRamp = mode == "ramp";
                bool wantSeeks = mode == "seeks";

                if (wantConfig) await ConfigTests.RunAsync().ConfigureAwait(false);
                if (wantLogging) await LoggingTests.RunAsync(ct).ConfigureAwait(false);
                if (wantMock) await MockTests.RunAsync(ct).ConfigureAwait(false);
                if (wantBudget) await MockTests.BudgetsAsync(ct).ConfigureAwait(false);

                if (wantLive || wantThroughput || wantSoak || wantTune || wantMeasure || wantRamp || wantSeeks)
                {
                    // Live modes are sweeps and soaks against a real origin: minutes per test is
                    // the job, not a hang. The offline suites keep the ceiling.
                    Harness.PerTestTimeout = TimeSpan.Zero;

                    string reason;
                    if (!LiveTests.TryLoadUrl(out reason))
                    {
                        Console.WriteLine();
                        Console.WriteLine("!! live tests skipped: " + reason);

                        // "all" is a mixed run: skipping the live half there is normal and the
                        // offline half still means something. But a mode that asks for nothing
                        // BUT live tests and then runs none of them has not passed - it has not
                        // run. Exiting 0 there let a release gate read "TOTAL 0 PASS 0 FAIL 0"
                        // as a green light.
                        if (mode != "all")
                        {
                            Console.WriteLine("   " + mode + " is a live-only mode, so this is a failure, not a skip.");
                            Console.WriteLine("   Put the origin url in TEST_URL.txt or set EMBY_STRM_TEST_URL.");
                            return 2;
                        }
                    }
                    else
                    {
                        if (wantLive) await LiveTests.CorrectnessAsync(ct).ConfigureAwait(false);
                        if (wantLive) await LiveTests.CancellationAsync(ct).ConfigureAwait(false);
                        if (wantThroughput) await LiveTests.ThroughputAsync(ct).ConfigureAwait(false);
                        if (wantTune) await LiveTests.TuneAsync(ct).ConfigureAwait(false);
                        if (wantRamp) await LiveTests.RampAsync(ct).ConfigureAwait(false);
                        if (wantSeeks)
                        {
                            int hold = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 20;
                            await LiveTests.SeekStormAsync(8, hold, ct).ConfigureAwait(false);
                        }
                        if (wantMeasure)
                        {
                            // measure <connections> <chunkMiB> <wantMiB> <offset>
                            int conns = int.Parse(args[1], CultureInfo.InvariantCulture);
                            int chunkMiB = int.Parse(args[2], CultureInfo.InvariantCulture);
                            long wantMiB = long.Parse(args[3], CultureInfo.InvariantCulture);
                            long offset = args.Length > 4 ? long.Parse(args[4], CultureInfo.InvariantCulture) : 1_000_000_000L;
                            Harness.Section("live origin: ad-hoc measurement");
                            await LiveTests.MeasureOneAsync(conns, chunkMiB, wantMiB, offset, ct).ConfigureAwait(false);
                        }
                        if (wantSoak)
                        {
                            long mib = 512;
                            if (args.Length > 1) long.TryParse(args[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out mib);
                            await LiveTests.SoakAsync(mib * 1024 * 1024, ct).ConfigureAwait(false);
                        }
                    }
                }
            }

            return Harness.Summarize();
        }
    }
}
