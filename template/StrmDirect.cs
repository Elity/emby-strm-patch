using System;
using System.IO;

// This type is cloned wholesale into Emby's target assembly by the patcher (see patcher/TypeCloner.cs).
//
// Constraints — the cloner is deliberately simple, so honour these when editing:
//   · static fields and static methods only, no instance members
//   · no generics (no List<T>, no LINQ), arrays only
//   · no static constructor (.cctor is not cloned); build constant arrays inline
//   · no lambdas / iterators / async — they emit compiler-generated closure types
//   · no array literals: `new char[] { 'a', 'b' }` gets optimised into
//     RuntimeHelpers.InitializeArray + an RVA field in <PrivateImplementationDetails>,
//     which cannot be cloned. Assign elements one by one instead.
//   · reference nothing from this project except the BCL
//
// Semantics: a path matching any configured prefix returns true. No configuration at all
// means it always returns false, which makes the patch inert and the server behave exactly
// like stock Emby. That safe default is what makes the patched binary shareable.
internal static class StrmDirect
{
    private static string[] _prefixes;
    private static long _nextReload;

    internal static bool IsMatch(string path)
    {
        if (path == null || path.Length == 0) return false;

        string[] p = _prefixes;
        if (p == null || DateTime.UtcNow.Ticks > _nextReload)
        {
            p = Load();
            _prefixes = p;
            _nextReload = DateTime.UtcNow.Ticks + 300000000L;   // 30s — edits take effect without a restart
        }

        for (int i = 0; i < p.Length; i++)
        {
            if (path.StartsWith(p[i], StringComparison.OrdinalIgnoreCase)) return true;
        }
        return false;
    }

    private static string[] Load()
    {
        try
        {
            string raw = Environment.GetEnvironmentVariable("EMBY_STRM_PREFIXES");
            if (raw == null || raw.Length == 0)
            {
                string file = FindFile();
                if (file != null) raw = File.ReadAllText(file);
            }
            if (raw == null || raw.Length == 0) return new string[0];

            // Element-wise assignment on purpose — see the constraints above.
            char[] seps = new char[3];
            seps[0] = ';'; seps[1] = '\n'; seps[2] = '\r';
            string[] parts = raw.Split(seps);

            int n = 0;
            for (int i = 0; i < parts.Length; i++)
            {
                string t = parts[i].Trim();
                if (t.Length > 0 && t[0] != '#') { parts[i] = t; n++; }
                else { parts[i] = null; }
            }
            if (n == 0) return new string[0];

            string[] result = new string[n];
            int k = 0;
            for (int i = 0; i < parts.Length; i++)
            {
                if (parts[i] != null) { result[k] = parts[i]; k++; }
            }
            return result;
        }
        catch
        {
            return new string[0];   // A broken config must never take the server down.
        }
    }

    // Layered lookup, first hit wins. The two environment variables work on every platform;
    // the remaining layers cover the usual Emby directory layouts.
    private static string FindFile()
    {
        string explicitPath = Environment.GetEnvironmentVariable("EMBY_STRM_CONFIG");
        if (explicitPath != null && explicitPath.Length > 0 && File.Exists(explicitPath)) return explicitPath;

        // -programdata <path>  ->  <path>/config/strm-direct.txt
        string[] args = Environment.GetCommandLineArgs();
        for (int i = 0; i + 1 < args.Length; i++)
        {
            if (string.Equals(args[i], "-programdata", StringComparison.OrdinalIgnoreCase))
            {
                string c = Path.Combine(Path.Combine(args[i + 1], "config"), "strm-direct.txt");
                if (File.Exists(c)) return c;
            }
        }

        string bd = AppContext.BaseDirectory;
        if (bd != null && bd.Length > 0)
        {
            // Next to the assemblies: <system>/strm-direct.txt
            string c1 = Path.Combine(bd, "strm-direct.txt");
            if (File.Exists(c1)) return c1;

            // <system>/../programdata/config/strm-direct.txt
            string parent = Path.GetDirectoryName(bd.TrimEnd(Path.DirectorySeparatorChar));
            if (parent != null && parent.Length > 0)
            {
                string c2 = Path.Combine(Path.Combine(Path.Combine(parent, "programdata"), "config"), "strm-direct.txt");
                if (File.Exists(c2)) return c2;
            }
        }
        return null;
    }
}
