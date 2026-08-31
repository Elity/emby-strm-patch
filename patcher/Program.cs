using System;
using System.IO;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;

// Three injection points, one shared routing table.
//
// A .strm file holds a single http(s) URL. Emby treats it as a remote MediaProtocol.Http
// source and puts that URL in MediaSourceInfo.Path. Two default behaviours then turn the
// server into a relay:
//
//   [A] Emby.Server.MediaEncoding.dll
//       MediaInfoService.SetDeviceSpecificData(..., MediaSourceInfo mediaSource, ...)
//       When the client's reported bitrate ceiling is below the source bitrate, Emby answers
//       PlaybackInfo with SupportsDirectPlay=false plus a TranscodingUrl, and starts ffmpeg.
//       Injected:  if (StrmDirect.IsMatch(mediaSource.Path)) mediaSource.SupportsTranscoding = false;
//       Emby's own code takes it from there:
//         · !SupportsTranscoding -> videoOptions.ForceDirectPlay   = true  (bypasses profile limits)
//         · !SupportsTranscoding -> videoOptions.ForceDirectStream = true
//         · `if (!mediaSource.SupportsTranscoding) return;`        -> TranscodingUrl is never built
//
//   [B] Emby.Server.Implementations.dll
//       HttpResultFactory.GetStaticFileResult(IRequest, StaticFileResultOptions)
//       Emby fetches the remote URL itself and forwards the bytes, so the client sees 206 and
//       every byte crosses the server twice.
//       Injected:  if (StrmDirect.IsRedirect(options.Path)) return Task.FromResult(GetRedirectResult(path));
//       so /Videos/{id}/stream and /Videos/{id}/original.* answer 302 and the client connects
//       to the origin directly.
//
//   [C] Emby.Server.Implementations.dll  (only with --parallel, see PatchParallel.cs)
//       HttpResultFactory.GetContent(...)  -> relay the source over several Range requests.
//
// B AND C GO INTO THE SAME ASSEMBLY, TOGETHER. They sit on different methods of the same class
// and carry independent marker fields, so one build serves both modes and the choice moves to
// runtime configuration: B fires only for prefixes whose mode is 302, C only for prefixes whose
// mode is parallel. Their exclusivity needs no enforcement here — when B answers a redirect it
// returns from GetStaticFileResult, no FileWriter is constructed, and GetContent is never
// reached. See references/mode-routing.md §2.
//
// Three patches ask the routing table three different questions (spec §4):
//   A -> IsMatch      any configured prefix, whatever its mode (transcoding defeats both modes)
//   B -> IsRedirect   prefix whose mode is 302
//   C -> ParallelFetch.IsMatch in the helper assembly, which forwards to IsParallel
//
// Every injection goes in front of the original first instruction, and every branch target is
// that original first instruction. When nothing matches, control flow is identical to stock.
// Prefixes are not baked into IL: the matcher is cloned in from template/ and reads its
// configuration at runtime.

internal static class Program
{
    // Where the cloned matcher lands inside the target assembly
    private const string HelperNs   = "Emby.Server.StrmDirect";
    private const string HelperName = "StrmDirect";

    // Patch B target
    internal const string RedirectType   = "Emby.Server.Implementations.HttpServer.HttpResultFactory";
    private  const string RedirectMethod = "GetStaticFileResult";
    internal const string RedirectMarker = "__strm302_patched";

    // Patch A target
    internal const string NoTranscodeType   = "Emby.Server.MediaEncoding.Api.MediaInfoService";
    private  const string NoTranscodeMethod = "SetDeviceSpecificData";
    internal const string NoTranscodeMarker = "__strm_notranscode_patched";

    private static int Main(string[] args)
    {
        // `check` needs neither an input nor an output assembly, so it is dispatched first.
        // It lives in this executable rather than in a script because it has to read marker
        // fields out of DLLs (Cecil) and parse the routing file with the very same parser the
        // runtime uses — see CheckConfig.cs.
        if (args.Length > 0 && (args[0] == "check" || args[0] == "check-config"))
            return CheckConfig.Run(args.Skip(1).ToArray());

        if (args.Length < 2)
        {
            Console.Error.WriteLine("usage: embypatch <input.dll> <output.dll> [referenceDir] [templateDll] [--parallel <helper.dll>]");
            Console.Error.WriteLine("       embypatch check <programdata-dir> <emby-system-dir>");
            Console.Error.WriteLine();
            Console.Error.WriteLine("  The patch to apply is chosen from the input assembly:");
            Console.Error.WriteLine("    Emby.Server.MediaEncoding.dll    -> A (never offer transcoding)");
            Console.Error.WriteLine("    Emby.Server.Implementations.dll  -> B (302), plus C when --parallel is given");
            Console.Error.WriteLine("  referenceDir defaults to the input's directory (point it at Emby's system/).");
            Console.Error.WriteLine("  templateDll  defaults to ../template/bin/Release/net8.0/StrmDirectTemplate.dll");
            return 2;
        }

        string input  = args[0];
        string output = args[1];

        // `--parallel <helper.dll>` ADDS patch C on top of patch B; it does not replace it.
        // One binary then serves both modes and strm-routing.txt decides per prefix. Leaving the
        // flag off is the escape hatch: a 302-only deployment needs no helper assembly and no
        // deps.json edit.
        string parallelAsm = null;
        var rest = new System.Collections.Generic.List<string>();
        for (int i = 2; i < args.Length; i++)
        {
            if (args[i] == "--parallel" && i + 1 < args.Length) parallelAsm = args[++i];
            else rest.Add(args[i]);
        }

        string refDir = rest.Count > 0 && rest[0].Length > 0 ? rest[0] : Path.GetDirectoryName(Path.GetFullPath(input));
        string tmpl   = rest.Count > 1 && rest[1].Length > 0 ? rest[1] : FindTemplate();

        // The template is required on every run now. Patch B is always applied, and B needs the
        // cloned matcher; --parallel only appends C, which brings its own matcher in the helper.
        if (tmpl == null || !File.Exists(tmpl))
        {
            Console.Error.WriteLine("x Template assembly not found. Run `dotnet build -c Release` in template/ first.");
            return 1;
        }
        if (parallelAsm != null && !File.Exists(parallelAsm))
        {
            Console.Error.WriteLine($"x Helper assembly not found: {parallelAsm}");
            return 1;
        }

        var resolver = new DefaultAssemblyResolver();
        resolver.AddSearchDirectory(refDir);

        using var asm = AssemblyDefinition.ReadAssembly(input,
            new ReaderParameters { AssemblyResolver = resolver, ReadWrite = false });
        var module = asm.MainModule;

        Console.WriteLine($"assembly : {module.Name}");

        bool isImplementations = module.GetType(RedirectType) != null;
        bool isMediaEncoding   = module.GetType(NoTranscodeType) != null;
        if (!isImplementations && !isMediaEncoding)
        {
            Console.Error.WriteLine($"x No known target type in this assembly ({RedirectType} / {NoTranscodeType})");
            return 1;
        }
        if (parallelAsm != null && !isImplementations)
        {
            Console.Error.WriteLine("x --parallel only applies to Emby.Server.Implementations.dll");
            return 1;
        }

        // Idempotency, checked before anything is cloned or injected so a refused run never
        // leaves a half-modified assembly behind. For Implementations.dll BOTH markers are
        // tested, not just the one this run would add: B and C share a class, and an assembly
        // that already carries one of them must be re-patched from stock rather than topped up.
        // Patching in place would stack a second copy of the injected prologue.
        if (isImplementations)
        {
            var host = module.GetType(RedirectType);
            bool hasB = AlreadyPatched(host, RedirectMarker);
            bool hasC = AlreadyPatched(host, PatchParallel.Marker);
            if (hasB || hasC)
            {
                Console.Error.WriteLine("  Re-patch from the stock DLL (system/_stock_*.bak) instead of patching the output again.");
                return 1;
            }
        }
        else if (AlreadyPatched(module.GetType(NoTranscodeType), NoTranscodeMarker))
        {
            return 1;
        }

        Console.WriteLine($"template : {Path.GetFileName(tmpl)}");

        MethodDefinition isMatch, isRedirect;
        try
        {
            using var tasm = AssemblyDefinition.ReadAssembly(tmpl);
            var srcType = tasm.MainModule.Types.FirstOrDefault(t => t.Name == "StrmDirect");
            if (srcType == null) { Console.Error.WriteLine("x StrmDirect not found in the template"); return 1; }

            var cloned = TypeCloner.Clone(srcType, module, HelperNs, HelperName);

            // A patches on "any mode", B only on "mode is 302" (spec §4). Resolve both up front
            // and name the missing one: a null slipping through would surface as a
            // NullReferenceException deep inside Cecil, miles from the actual cause.
            isMatch    = Matcher(cloned, "IsMatch");
            isRedirect = Matcher(cloned, "IsRedirect");
            if (isMatch == null || isRedirect == null)
            {
                Console.Error.WriteLine("x The cloned matcher is missing " +
                    (isMatch == null ? "IsMatch" : "IsRedirect") + ": expected `static bool <name>(string)`.");
                Console.Error.WriteLine("  Found on " + cloned.FullName + ": " +
                    string.Join(", ", cloned.Methods.Select(m => m.Name)));
                Console.Error.WriteLine("  The template is stale — rebuild template/ from shared/RoutingConfig.cs.");
                return 1;
            }
            Console.WriteLine($"matcher  : cloned {HelperNs}.{HelperName} " +
                              $"({cloned.Fields.Count} fields / {cloned.Methods.Count} methods)");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"x Cloning the matcher failed: {ex.Message}");
            return 1;
        }

        bool ok;
        if (isImplementations)
        {
            ok = PatchRedirect(module, isRedirect);
            if (ok && parallelAsm != null)
            {
                Console.WriteLine($"helper   : {Path.GetFileName(parallelAsm)}");
                ok = PatchParallel.Apply(module, parallelAsm);
            }
        }
        else
        {
            ok = PatchNoTranscode(module, isMatch);
        }
        if (!ok) return 1;

        // One write for however many patches were applied: a partially written assembly is the
        // one outcome worse than an unpatched one.
        asm.Write(output);
        Console.WriteLine($"OK       : wrote {output}");
        return 0;
    }

    /// Finds `static bool <name>(string)` on the cloned matcher, by shape rather than by name
    /// alone, so a same-named helper with the wrong signature is reported instead of emitted.
    private static MethodDefinition Matcher(TypeDefinition cloned, string name)
    {
        return cloned.Methods.FirstOrDefault(m =>
            m.Name == name && m.IsStatic && m.Parameters.Count == 1 &&
            m.Parameters[0].ParameterType.MetadataType == MetadataType.String &&
            m.ReturnType.MetadataType == MetadataType.Boolean);
    }

    private static string FindTemplate()
    {
        string[] candidates =
        {
            Path.Combine("..", "template", "bin", "Release", "net8.0", "StrmDirectTemplate.dll"),
            Path.Combine(AppContext.BaseDirectory, "StrmDirectTemplate.dll"),
            Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..",
                         "template", "bin", "Release", "net8.0", "StrmDirectTemplate.dll"),
        };
        return candidates.FirstOrDefault(File.Exists);
    }

    // ---------- Patch B: answer 302 from the streaming endpoint ----------
    // `isRedirect` is StrmDirect.IsRedirect, not IsMatch: only prefixes whose mode is 302 may
    // be redirected. A prefix marked `parallel` has to stay on the relay path so that patch C,
    // which may be sitting in this same assembly, still gets to see it.
    private static bool PatchRedirect(ModuleDefinition module, MethodDefinition isRedirect)
    {
        Console.WriteLine("patch    : B - 302 redirect from the streaming endpoint");
        var type = module.GetType(RedirectType);

        // The two-parameter overload (IRequest, StaticFileResultOptions)
        var method = type.Methods.FirstOrDefault(m =>
            m.Name == RedirectMethod && m.HasBody && m.Parameters.Count == 2 &&
            m.Parameters[1].ParameterType.Name == "StaticFileResultOptions");
        if (method == null) { Console.Error.WriteLine("x GetStaticFileResult(IRequest, StaticFileResultOptions) not found"); return false; }

        var getRedirect = type.Methods.FirstOrDefault(m => m.Name == "GetRedirectResult" && m.Parameters.Count == 1);
        if (getRedirect == null) { Console.Error.WriteLine("x GetRedirectResult(string) not found"); return false; }

        var optionsParam = method.Parameters[1];
        var getPath = FindAccessor(optionsParam.ParameterType.Resolve(), "Path", getter: true);
        if (getPath == null) { Console.Error.WriteLine("x StaticFileResultOptions.Path getter not found"); return false; }

        // Task.FromResult<object>
        var taskDef = ((GenericInstanceType)method.ReturnType).ElementType.Resolve();      // Task`1
        var taskNonGeneric = taskDef.Module.GetType("System.Threading.Tasks.Task");
        var fromResultDef = taskNonGeneric?.Methods.FirstOrDefault(m =>
            m.Name == "FromResult" && m.HasGenericParameters && m.Parameters.Count == 1);
        if (fromResultDef == null) { Console.Error.WriteLine("x Task.FromResult<T> not found"); return false; }
        var fromResult = new GenericInstanceMethod(module.ImportReference(fromResultDef));
        fromResult.GenericArguments.Add(module.TypeSystem.Object);

        Report(method);

        var body = method.Body;
        var il = body.GetILProcessor();
        var first = body.Instructions[0];
        var pathLocal = NewStringLocal(module, body);

        // if (StrmDirect.IsRedirect(options.Path))
        //     return Task.FromResult<object>(this.GetRedirectResult(path));
        var seq = new[]
        {
            il.Create(OpCodes.Ldarg, optionsParam),
            il.Create(OpCodes.Callvirt, module.ImportReference(getPath)),
            il.Create(OpCodes.Stloc, pathLocal),
            il.Create(OpCodes.Ldloc, pathLocal),
            il.Create(OpCodes.Call, isRedirect),
            il.Create(OpCodes.Brfalse, first),
            il.Create(OpCodes.Ldarg_0),
            il.Create(OpCodes.Ldloc, pathLocal),
            il.Create(OpCodes.Call, getRedirect),
            il.Create(OpCodes.Call, fromResult),
            il.Create(OpCodes.Ret),
        };
        Inject(il, first, seq, body, type, RedirectMarker, module);
        return true;
    }

    // ---------- Patch A: never offer transcoding for matched sources ----------
    // `isMatch` is StrmDirect.IsMatch — deliberately mode-blind. Both 302 and parallel are
    // pointless once ffmpeg re-encodes the source, so A covers every configured prefix and has
    // no per-prefix switch (spec §4).
    private static bool PatchNoTranscode(ModuleDefinition module, MethodDefinition isMatch)
    {
        Console.WriteLine("patch    : A - force direct play for matched sources");
        var type = module.GetType(NoTranscodeType);

        // Two overloads share the name: one takes PlaybackInfoResponse (batch), one takes a
        // single MediaSourceInfo. We want the latter.
        var method = type.Methods.FirstOrDefault(m =>
            m.Name == NoTranscodeMethod && m.HasBody &&
            m.Parameters.Any(p => p.ParameterType.Name == "MediaSourceInfo"));
        if (method == null) { Console.Error.WriteLine("x SetDeviceSpecificData overload taking MediaSourceInfo not found"); return false; }

        var msParam = method.Parameters.First(p => p.ParameterType.Name == "MediaSourceInfo");
        var msType = msParam.ParameterType.Resolve();

        var getPath = FindAccessor(msType, "Path", getter: true);
        if (getPath == null) { Console.Error.WriteLine("x MediaSourceInfo.Path getter not found"); return false; }

        var setSupportsTranscoding = FindAccessor(msType, "SupportsTranscoding", getter: false);
        if (setSupportsTranscoding == null) { Console.Error.WriteLine("x MediaSourceInfo.SupportsTranscoding setter not found"); return false; }

        Report(method);
        Console.WriteLine($"           mediaSource parameter index: #{msParam.Index} ({msParam.Name})");

        var body = method.Body;
        var il = body.GetILProcessor();
        var first = body.Instructions[0];

        // if (mediaSource != null && StrmDirect.IsMatch(mediaSource.Path))
        //     mediaSource.SupportsTranscoding = false;
        // then fall through to the original body
        var seq = new[]
        {
            il.Create(OpCodes.Ldarg, msParam),
            il.Create(OpCodes.Brfalse, first),
            il.Create(OpCodes.Ldarg, msParam),
            il.Create(OpCodes.Callvirt, module.ImportReference(getPath)),
            il.Create(OpCodes.Call, isMatch),
            il.Create(OpCodes.Brfalse, first),
            il.Create(OpCodes.Ldarg, msParam),
            il.Create(OpCodes.Ldc_I4_0),                                  // false
            il.Create(OpCodes.Callvirt, module.ImportReference(setSupportsTranscoding)),
        };
        Inject(il, first, seq, body, type, NoTranscodeMarker, module);
        return true;
    }

    // ---------- shared ----------

    private static void Inject(ILProcessor il, Instruction first, Instruction[] seq,
                               MethodBody body, TypeDefinition type, string marker, ModuleDefinition module)
    {
        foreach (var ins in seq) il.InsertBefore(first, ins);
        if (body.MaxStackSize < 8) body.MaxStackSize = 8;

        // Idempotency marker: running the patcher twice on the same assembly is rejected.
        type.Fields.Add(new FieldDefinition(marker,
            FieldAttributes.Private | FieldAttributes.Static | FieldAttributes.InitOnly,
            module.TypeSystem.Boolean));

        Console.WriteLine($"           IL after: {body.Instructions.Count}  (+{seq.Length})");
    }

    private static bool AlreadyPatched(TypeDefinition type, string marker)
    {
        if (type.Fields.Any(f => f.Name == marker))
        {
            Console.Error.WriteLine($"x Already patched (marker field {marker} present); refusing to patch again");
            return true;
        }
        return false;
    }

    private static void Report(MethodDefinition m)
    {
        Console.WriteLine($"           target: {m.FullName}");
        Console.WriteLine($"           IL before: {m.Body.Instructions.Count}");
    }

    private static VariableDefinition NewStringLocal(ModuleDefinition module, MethodBody body)
    {
        var v = new VariableDefinition(module.TypeSystem.String);
        body.Variables.Add(v);
        body.InitLocals = true;
        return v;
    }

    /// Walks the base chain looking for a property accessor.
    private static MethodDefinition FindAccessor(TypeDefinition t, string propName, bool getter)
    {
        for (var cur = t; cur != null; cur = cur.BaseType?.Resolve())
        {
            var p = cur.Properties.FirstOrDefault(x => x.Name == propName);
            var m = getter ? p?.GetMethod : p?.SetMethod;
            if (m != null) return m;
        }
        return null;
    }
}
