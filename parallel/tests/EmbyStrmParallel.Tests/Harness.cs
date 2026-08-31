using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace EmbyStrmParallel.Tests
{
    internal static class Harness
    {
        private sealed class Result
        {
            internal string Name;
            internal bool Ok;
            internal string Detail;
            internal double Seconds;
        }

        private static readonly List<Result> Results = new List<Result>();

        internal static async Task RunAsync(string name, Func<Task<string>> body)
        {
            Console.Write("  " + name.PadRight(58));
            Stopwatch sw = Stopwatch.StartNew();
            try
            {
                string detail = await body().ConfigureAwait(false);
                sw.Stop();
                Results.Add(new Result { Name = name, Ok = true, Detail = detail, Seconds = sw.Elapsed.TotalSeconds });
                Console.WriteLine("PASS  " + sw.Elapsed.TotalSeconds.ToString("0.0") + "s  " + (detail ?? ""));
            }
            catch (Exception ex)
            {
                sw.Stop();
                string msg = ex.GetType().Name + ": " + ex.Message;
                Results.Add(new Result { Name = name, Ok = false, Detail = msg, Seconds = sw.Elapsed.TotalSeconds });
                Console.WriteLine("FAIL  " + sw.Elapsed.TotalSeconds.ToString("0.0") + "s");
                Console.WriteLine("        " + msg);
                if (ex.InnerException != null) Console.WriteLine("        inner: " + ex.InnerException.Message);
            }
        }

        internal static void Section(string title)
        {
            Console.WriteLine();
            Console.WriteLine("== " + title + " ==");
        }

        internal static int Summarize()
        {
            int pass = 0, fail = 0;
            foreach (Result r in Results) { if (r.Ok) pass++; else fail++; }
            Console.WriteLine();
            Console.WriteLine("-------------------------------------------------------------");
            Console.WriteLine("TOTAL " + (pass + fail) + "   PASS " + pass + "   FAIL " + fail);
            if (fail > 0)
            {
                Console.WriteLine("failed:");
                foreach (Result r in Results) if (!r.Ok) Console.WriteLine("  - " + r.Name + " :: " + r.Detail);
            }
            return fail == 0 ? 0 : 1;
        }

        internal static void Assert(bool condition, string message)
        {
            if (!condition) throw new Exception("assertion failed: " + message);
        }

        internal static void AssertEqual(long expected, long actual, string what)
        {
            if (expected != actual) throw new Exception(what + ": expected " + expected + " got " + actual);
        }

        internal static void AssertBytesEqual(byte[] expected, byte[] actual, string what)
        {
            if (expected.Length != actual.Length)
                throw new Exception(what + ": length expected " + expected.Length + " got " + actual.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                if (expected[i] != actual[i])
                    throw new Exception(what + ": first difference at index " + i +
                                        " expected 0x" + expected[i].ToString("x2") + " got 0x" + actual[i].ToString("x2"));
            }
        }

        internal static string Mbps(long bytes, double seconds)
        {
            if (seconds <= 0) return "n/a";
            return (bytes * 8.0 / seconds / 1e6).ToString("0.00") + " Mbps";
        }

        internal static string MiB(long bytes)
        {
            return (bytes / 1024.0 / 1024.0).ToString("0.0") + " MiB";
        }

        /// <summary>Reads to EOF, optionally throttling the consumer.</summary>
        internal static async Task<byte[]> ReadAllAsync(Stream s, int bufferSize, CancellationToken ct)
        {
            MemoryStream ms = new MemoryStream();
            byte[] buf = new byte[bufferSize];
            while (true)
            {
                int n = await s.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false);
                if (n <= 0) break;
                ms.Write(buf, 0, n);
            }
            return ms.ToArray();
        }

        /// <summary>Reads to EOF discarding data, returning the byte count.</summary>
        internal static async Task<long> DrainAsync(Stream s, int bufferSize, int delayMsPerRead, CancellationToken ct,
                                                    Action<long> onProgress)
        {
            byte[] buf = new byte[bufferSize];
            long total = 0;
            while (true)
            {
                int n = await s.ReadAsync(buf.AsMemory(0, buf.Length), ct).ConfigureAwait(false);
                if (n <= 0) break;
                total += n;
                if (onProgress != null) onProgress(total);
                if (delayMsPerRead > 0) await Task.Delay(delayMsPerRead, ct).ConfigureAwait(false);
            }
            return total;
        }
    }
}
