using System;
using System.IO;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;

// Two injection points, one shared prefix matcher.
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
//       Injected:  if (StrmDirect.IsMatch(options.Path)) return Task.FromResult(GetRedirectResult(path));
//       so /Videos/{id}/stream and /Videos/{id}/original.* answer 302 and the client connects
//       to the origin directly.
//
// Both injections go in front of the original first instruction, and every branch target is
// that original first instruction. When the prefix does not match, control flow is identical
// to stock. Prefixes are not baked into IL: the matcher is cloned in from template/ and reads
// its configuration at runtime.

internal static class Program
{
    // Where the cloned matcher lands inside the target assembly
    private const string HelperNs   = "Emby.Server.StrmDirect";
    private const string HelperName = "StrmDirect";

    // Patch B target
    private const string RedirectType   = "Emby.Server.Implementations.HttpServer.HttpResultFactory";
    private const string RedirectMethod = "GetStaticFileResult";
    private const string RedirectMarker = "__strm302_patched";

    // Patch A target
    private const string NoTranscodeType   = "Emby.Server.MediaEncoding.Api.MediaInfoService";
    private const string NoTranscodeMethod = "SetDeviceSpecificData";
    private const string NoTranscodeMarker = "__strm_notranscode_patched";

    private static int Main(string[] args)
    {
        if (args.Length < 2)
        {
            Console.Error.WriteLine("usage: embypatch <input.dll> <output.dll> [referenceDir] [templateDll]");
            Console.Error.WriteLine("  The patch to apply is chosen from the input assembly.");
            Console.Error.WriteLine("  referenceDir defaults to the input's directory (point it at Emby's system/).");
            Console.Error.WriteLine("  templateDll  defaults to ../template/bin/Release/net8.0/StrmDirectTemplate.dll");
            return 2;
        }

        string input  = args[0];
        string output = args[1];
        string refDir = args.Length > 2 && args[2].Length > 0 ? args[2] : Path.GetDirectoryName(Path.GetFullPath(input));
        string tmpl   = args.Length > 3 && args[3].Length > 0 ? args[3] : FindTemplate();

        if (tmpl == null || !File.Exists(tmpl))
        {
            Console.Error.WriteLine("x Template assembly not found. Run `dotnet build -c Release` in template/ first.");
            return 1;
        }

        var resolver = new DefaultAssemblyResolver();
        resolver.AddSearchDirectory(refDir);

        using var asm = AssemblyDefinition.ReadAssembly(input,
            new ReaderParameters { AssemblyResolver = resolver, ReadWrite = false });
        var module = asm.MainModule;

        Console.WriteLine($"assembly : {module.Name}");
        Console.WriteLine($"template : {Path.GetFileName(tmpl)}");

        bool isRedirect    = module.GetType(RedirectType) != null;
        bool isNoTranscode = module.GetType(NoTranscodeType) != null;
        if (!isRedirect && !isNoTranscode)
        {
            Console.Error.WriteLine($"x No known target type in this assembly ({RedirectType} / {NoTranscodeType})");
            return 1;
        }

        // Check idempotency before cloning, so a rejected run never leaves a half-modified assembly.
        var targetType = module.GetType(isRedirect ? RedirectType : NoTranscodeType);
        if (AlreadyPatched(targetType, isRedirect ? RedirectMarker : NoTranscodeMarker)) return 1;

        MethodDefinition isMatch;
        try
        {
            using var tasm = AssemblyDefinition.ReadAssembly(tmpl);
            var srcType = tasm.MainModule.Types.FirstOrDefault(t => t.Name == "StrmDirect");
            if (srcType == null) { Console.Error.WriteLine("x StrmDirect not found in the template"); return 1; }

            var cloned = TypeCloner.Clone(srcType, module, HelperNs, HelperName);
            isMatch = cloned.Methods.FirstOrDefault(m => m.Name == "IsMatch" && m.Parameters.Count == 1);
            if (isMatch == null) { Console.Error.WriteLine("x IsMatch missing after clone"); return 1; }
            Console.WriteLine($"matcher  : cloned {HelperNs}.{HelperName} " +
                              $"({cloned.Fields.Count} fields / {cloned.Methods.Count} methods)");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"x Cloning the matcher failed: {ex.Message}");
            return 1;
        }

        bool ok = isRedirect ? PatchRedirect(module, isMatch) : PatchNoTranscode(module, isMatch);
        if (!ok) return 1;

        asm.Write(output);
        Console.WriteLine($"OK       : wrote {output}");
        return 0;
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
    private static bool PatchRedirect(ModuleDefinition module, MethodDefinition isMatch)
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

        // if (StrmDirect.IsMatch(options.Path))
        //     return Task.FromResult<object>(this.GetRedirectResult(path));
        var seq = new[]
        {
            il.Create(OpCodes.Ldarg, optionsParam),
            il.Create(OpCodes.Callvirt, module.ImportReference(getPath)),
            il.Create(OpCodes.Stloc, pathLocal),
            il.Create(OpCodes.Ldloc, pathLocal),
            il.Create(OpCodes.Call, isMatch),
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
