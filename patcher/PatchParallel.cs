using System;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;

// Patch C -- parallel fetch.
//
// Mutually exclusive with the 302 redirect patch: that one hands the transfer to the
// client and takes the server out of the path, so the fetch code below would never run.
//
// Target:
//   Emby.Server.Implementations.dll
//   HttpResultFactory.GetContent(StaticFileResultOptions, long offset, long length, CancellationToken)
//     -> Task<StreamHandler>
//
// This is an async method, so its body is just the state-machine kick-off stub (29 IL on
// 4.9.3.0, 1 local, no exception handlers). Returning Task.FromResult(...) before the first
// instruction is safe: the state machine never starts.
//
// Injected, in effect:
//   if (ParallelFetch.IsMatch(options.Path)) {
//       var sh = new StreamHandler();
//       sh.Stream = ParallelFetch.Open(path, offset, length, out var total, out var clen, ct);
//       sh.TotalLength = total;
//       sh.Length = clen;
//       return Task.FromResult(sh);
//   }
//   ...original...
//
// ParallelFetch lives in a separate assembly, so it must also be registered in
// EmbyServer.deps.json -- see deps_patch.py. A DLL merely dropped into system/ will not load.

internal static class PatchParallel
{
    private const string HostType   = "Emby.Server.Implementations.HttpServer.HttpResultFactory";
    private const string HostMethod = "GetContent";
    public  const string Marker     = "__strm_parallel_patched";

    public static bool Apply(ModuleDefinition module, string parallelAsmPath)
    {
        Console.WriteLine("patch    : C - parallel chunked fetch");

        var type = module.GetType(HostType);
        if (type == null) { Console.Error.WriteLine($"x {HostType} not found"); return false; }
        if (type.Fields.Any(f => f.Name == Marker))
        { Console.Error.WriteLine($"x already patched (marker {Marker})"); return false; }

        // GetContent(options, offset, length, ct) -> Task<StreamHandler>
        var method = type.Methods.FirstOrDefault(m =>
            m.Name == HostMethod && m.HasBody && m.Parameters.Count == 4 &&
            m.Parameters[0].ParameterType.Name == "StaticFileResultOptions" &&
            m.ReturnType is GenericInstanceType);
        if (method == null) { Console.Error.WriteLine("x GetContent(StaticFileResultOptions, long, long, CancellationToken) not found"); return false; }

        var optionsParam = method.Parameters[0];
        var offsetParam  = method.Parameters[1];
        var lengthParam  = method.Parameters[2];
        var ctParam      = method.Parameters[3];

        var getPath = FindAccessor(optionsParam.ParameterType.Resolve(), "Path", true);
        if (getPath == null) { Console.Error.WriteLine("x StaticFileResultOptions.Path getter not found"); return false; }

        // StreamHandler comes straight off the return type: Task`1<StreamHandler>
        var taskRet  = (GenericInstanceType)method.ReturnType;
        var shRef    = taskRet.GenericArguments[0];
        var shDef    = shRef.Resolve();
        if (shDef == null) { Console.Error.WriteLine("x cannot resolve StreamHandler"); return false; }

        var shCtor = shDef.Methods.FirstOrDefault(m => m.IsConstructor && !m.IsStatic && m.Parameters.Count == 0);
        var setStream = FindAccessor(shDef, "Stream", false);
        var setTotal  = FindAccessor(shDef, "TotalLength", false);
        var setLen    = FindAccessor(shDef, "Length", false);
        if (shCtor == null || setStream == null || setTotal == null || setLen == null)
        { Console.Error.WriteLine("x StreamHandler shape unexpected (ctor/Stream/TotalLength/Length)"); return false; }

        // Task.FromResult<StreamHandler>
        var taskDef = taskRet.ElementType.Resolve();
        var taskNonGeneric = taskDef.Module.GetType("System.Threading.Tasks.Task");
        var fromResultDef = taskNonGeneric?.Methods.FirstOrDefault(m =>
            m.Name == "FromResult" && m.HasGenericParameters && m.Parameters.Count == 1);
        if (fromResultDef == null) { Console.Error.WriteLine("x Task.FromResult<T> not found"); return false; }
        var fromResult = new GenericInstanceMethod(module.ImportReference(fromResultDef));
        fromResult.GenericArguments.Add(module.ImportReference(shRef));

        // Bind against the external helper assembly.
        MethodReference isMatchRef, openRef;
        bool nullMeansFallback;
        using (var pasm = AssemblyDefinition.ReadAssembly(parallelAsmPath))
        {
            var pt = pasm.MainModule.Types.FirstOrDefault(t => t.Name == "ParallelFetch");
            if (pt == null) { Console.Error.WriteLine("x ParallelFetch type not found in helper assembly"); return false; }

            var isMatch = pt.Methods.FirstOrDefault(m =>
                m.Name == "IsMatch" && m.IsStatic && m.Parameters.Count == 1 &&
                m.Parameters[0].ParameterType.Name == "String" &&
                m.ReturnType.Name == "Boolean");

            // Prefer TryOpen: it returns null instead of throwing, which lets a failed fetch fall
            // back to Emby's own single-connection path instead of failing playback outright.
            var open = pt.Methods.FirstOrDefault(m =>
                m.Name == "TryOpen" && m.IsStatic && m.Parameters.Count == 6 &&
                m.Parameters[0].ParameterType.Name == "String" &&
                m.ReturnType.Name == "Stream");
            nullMeansFallback = open != null;
            if (open == null)
                open = pt.Methods.FirstOrDefault(m =>
                    m.Name == "Open" && m.IsStatic && m.Parameters.Count == 6 &&
                    m.Parameters[0].ParameterType.Name == "String" &&
                    m.ReturnType.Name == "Stream");

            if (isMatch == null || open == null)
            {
                Console.Error.WriteLine("x helper assembly must expose:");
                Console.Error.WriteLine("    static bool IsMatch(string)");
                Console.Error.WriteLine("    static Stream TryOpen|Open(string, long, long, out long?, out long?, CancellationToken)");
                Console.Error.WriteLine("  found: " + string.Join(", ", pt.Methods.Where(m => m.IsStatic).Select(m => m.Name)));
                return false;
            }
            // Sanity-check the two by-ref out params so we never emit ldloca against a by-value slot.
            if (!open.Parameters[3].ParameterType.IsByReference || !open.Parameters[4].ParameterType.IsByReference)
            { Console.Error.WriteLine("x params 4 and 5 must be `out`"); return false; }

            Console.WriteLine($"           entry point: {open.Name}" +
                              (nullMeansFallback ? "  (null -> fall back to original path)" : "  (throws on failure)"));
            isMatchRef = module.ImportReference(isMatch);
            openRef    = module.ImportReference(open);
        }

        Console.WriteLine($"           target: {method.FullName}");
        Console.WriteLine($"           IL before: {method.Body.Instructions.Count}");

        var body = method.Body;
        var il   = body.GetILProcessor();
        var first = body.Instructions[0];

        var nullableLong = module.ImportReference(setTotal.Parameters[0].ParameterType);
        var pathLocal   = new VariableDefinition(module.TypeSystem.String);
        var shLocal     = new VariableDefinition(module.ImportReference(shRef));
        var totalLocal  = new VariableDefinition(nullableLong);
        var clenLocal   = new VariableDefinition(nullableLong);
        var streamLocal = new VariableDefinition(module.ImportReference(setStream.Parameters[0].ParameterType));
        foreach (var v in new[] { pathLocal, shLocal, totalLocal, clenLocal, streamLocal }) body.Variables.Add(v);
        body.InitLocals = true;

        var seq = new System.Collections.Generic.List<Instruction>
        {
            il.Create(OpCodes.Ldarg, optionsParam),
            il.Create(OpCodes.Callvirt, module.ImportReference(getPath)),
            il.Create(OpCodes.Stloc, pathLocal),
            il.Create(OpCodes.Ldloc, pathLocal),
            il.Create(OpCodes.Call, isMatchRef),
            il.Create(OpCodes.Brfalse, first),

            il.Create(OpCodes.Ldloc, pathLocal),
            il.Create(OpCodes.Ldarg, offsetParam),
            il.Create(OpCodes.Ldarg, lengthParam),
            il.Create(OpCodes.Ldloca, totalLocal),
            il.Create(OpCodes.Ldloca, clenLocal),
            il.Create(OpCodes.Ldarg, ctParam),
            il.Create(OpCodes.Call, openRef),
            il.Create(OpCodes.Stloc, streamLocal),
        };

        // A null stream means the helper declined; fall through to Emby's own path rather than
        // failing the request. Only meaningful for the TryOpen entry point.
        if (nullMeansFallback)
        {
            seq.Add(il.Create(OpCodes.Ldloc, streamLocal));
            seq.Add(il.Create(OpCodes.Brfalse, first));
        }

        seq.AddRange(new[]
        {
            il.Create(OpCodes.Newobj, module.ImportReference(shCtor)),
            il.Create(OpCodes.Stloc, shLocal),

            il.Create(OpCodes.Ldloc, shLocal),
            il.Create(OpCodes.Ldloc, streamLocal),
            il.Create(OpCodes.Callvirt, module.ImportReference(setStream)),

            il.Create(OpCodes.Ldloc, shLocal),
            il.Create(OpCodes.Ldloc, totalLocal),
            il.Create(OpCodes.Callvirt, module.ImportReference(setTotal)),

            il.Create(OpCodes.Ldloc, shLocal),
            il.Create(OpCodes.Ldloc, clenLocal),
            il.Create(OpCodes.Callvirt, module.ImportReference(setLen)),

            il.Create(OpCodes.Ldloc, shLocal),
            il.Create(OpCodes.Call, fromResult),
            il.Create(OpCodes.Ret),
        });

        foreach (var ins in seq) il.InsertBefore(first, ins);
        if (body.MaxStackSize < 10) body.MaxStackSize = 10;

        type.Fields.Add(new FieldDefinition(Marker,
            FieldAttributes.Private | FieldAttributes.Static | FieldAttributes.InitOnly,
            module.TypeSystem.Boolean));

        Console.WriteLine($"           IL after: {body.Instructions.Count}  (+{seq.Count})");
        return true;
    }

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
