using System;
using System.Collections.Generic;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;

/// Deep-copies one static class from the template assembly into a target module.
///
/// Why this exists: reading configuration at runtime needs try/catch, file IO and string
/// handling. Hand-writing several hundred IL instructions for that is not realistic.
/// Writing the matcher as ordinary C# and cloning it in means future changes touch C# only.
///
/// Only supports the shape the template uses: static fields plus static methods, no generics,
/// no .cctor, no compiler-generated closures. template/StrmDirect.cs documents the constraints.
internal static class TypeCloner
{
    public static TypeDefinition Clone(TypeDefinition source, ModuleDefinition target, string ns, string name)
    {
        if (target.GetType(ns, name) != null)
            throw new InvalidOperationException($"Target module already contains type {ns}.{name}");

        var t = new TypeDefinition(ns, name, source.Attributes, target.ImportReference(source.BaseType));
        target.Types.Add(t);

        // Create shells first — fields and method signatures. Bodies reference each other,
        // so they can only be filled in once every shell exists.
        var fieldMap = new Dictionary<string, FieldDefinition>();
        foreach (var f in source.Fields)
        {
            var nf = new FieldDefinition(f.Name, f.Attributes, target.ImportReference(f.FieldType));
            if (f.HasConstant) { nf.HasConstant = true; nf.Constant = f.Constant; }
            if (f.InitialValue != null && f.InitialValue.Length > 0) nf.InitialValue = f.InitialValue;
            t.Fields.Add(nf);
            fieldMap[f.Name] = nf;
        }

        var methodMap = new Dictionary<string, MethodDefinition>();
        foreach (var m in source.Methods)
        {
            if (m.HasGenericParameters) throw new NotSupportedException($"Template method {m.Name} has generic parameters; unsupported");
            var nm = new MethodDefinition(m.Name, m.Attributes, target.ImportReference(m.ReturnType));
            nm.ImplAttributes = m.ImplAttributes;
            foreach (var p in m.Parameters)
                nm.Parameters.Add(new ParameterDefinition(p.Name, p.Attributes, target.ImportReference(p.ParameterType)));
            t.Methods.Add(nm);
            methodMap[m.Name] = nm;
        }

        foreach (var m in source.Methods)
            CloneBody(m, methodMap[m.Name], target, source, fieldMap, methodMap);

        return t;
    }

    private static void CloneBody(MethodDefinition src, MethodDefinition dst, ModuleDefinition target,
                                  TypeDefinition srcType,
                                  Dictionary<string, FieldDefinition> fieldMap,
                                  Dictionary<string, MethodDefinition> methodMap)
    {
        if (!src.HasBody) return;
        var sb = src.Body;
        var db = dst.Body;
        db.InitLocals = sb.InitLocals;
        db.MaxStackSize = sb.MaxStackSize;

        var varMap = new Dictionary<VariableDefinition, VariableDefinition>();
        foreach (var v in sb.Variables)
        {
            var nv = new VariableDefinition(target.ImportReference(v.VariableType));
            db.Variables.Add(nv);
            varMap[v] = nv;
        }

        // First pass builds the instructions; branch targets are patched up in the second pass
        // because they may point forward.
        var insMap = new Dictionary<Instruction, Instruction>();
        foreach (var i in sb.Instructions)
        {
            var ni = Instruction.Create(OpCodes.Nop);
            ni.OpCode = i.OpCode;
            ni.Operand = MapOperand(i.Operand, target, srcType, fieldMap, methodMap, varMap, dst, src);
            insMap[i] = ni;
            db.Instructions.Add(ni);
        }

        foreach (var i in sb.Instructions)
        {
            var ni = insMap[i];
            if (i.Operand is Instruction one) ni.Operand = insMap[one];
            else if (i.Operand is Instruction[] many) ni.Operand = many.Select(x => insMap[x]).ToArray();
        }

        foreach (var h in sb.ExceptionHandlers)
        {
            db.ExceptionHandlers.Add(new ExceptionHandler(h.HandlerType)
            {
                CatchType    = h.CatchType == null ? null : target.ImportReference(h.CatchType),
                TryStart     = Look(insMap, h.TryStart),
                TryEnd       = Look(insMap, h.TryEnd),
                HandlerStart = Look(insMap, h.HandlerStart),
                HandlerEnd   = Look(insMap, h.HandlerEnd),
                FilterStart  = Look(insMap, h.FilterStart),
            });
        }
    }

    private static Instruction Look(Dictionary<Instruction, Instruction> map, Instruction k)
        => k == null ? null : map[k];

    /// Any reference from the template to *another* type in its own assembly becomes a dangling
    /// reference once cloned. The usual culprit is the compiler-generated
    /// <PrivateImplementationDetails> type: array literals (new char[]{...}) and switch jump
    /// tables get lowered into RuntimeHelpers.InitializeArray plus an RVA field there.
    ///
    /// Such a dangling reference throws at runtime, and it often throws inside a try/catch that
    /// swallows it — which shows up as "configuration is never read" and is miserable to debug.
    /// Failing loudly here is much better.
    private static void GuardNotTemplateInternal(MemberReference r, TypeReference declaring, TypeDefinition srcType)
    {
        TypeDefinition d;
        try { d = declaring?.Resolve(); } catch { return; }
        if (d == null || d.Module != srcType.Module) return;
        if (d.FullName == srcType.FullName) return;

        throw new NotSupportedException(
            $"Template references another type in its own assembly: {d.FullName}::{r.Name}. " +
            "This is usually the compiler-generated <PrivateImplementationDetails> (array initialiser " +
            "or switch jump table). Rewrite the template to avoid it — assign array elements one by one.");
    }

    private static object MapOperand(object op, ModuleDefinition target, TypeDefinition srcType,
                                     Dictionary<string, FieldDefinition> fieldMap,
                                     Dictionary<string, MethodDefinition> methodMap,
                                     Dictionary<VariableDefinition, VariableDefinition> varMap,
                                     MethodDefinition dstMethod, MethodDefinition srcMethod)
    {
        switch (op)
        {
            case null:
                return null;

            case Instruction:
            case Instruction[]:
                return op;                                     // resolved in the second pass

            case VariableDefinition v:
                return varMap[v];

            case ParameterDefinition p:
                return dstMethod.Parameters[srcMethod.Parameters.IndexOf(p)];

            case FieldReference fr:
                if (fr.DeclaringType.FullName == srcType.FullName) return fieldMap[fr.Name];
                GuardNotTemplateInternal(fr, fr.DeclaringType, srcType);
                return target.ImportReference(fr);

            case MethodReference mr:
                if (mr.DeclaringType.FullName == srcType.FullName) return methodMap[mr.Name];
                GuardNotTemplateInternal(mr, mr.DeclaringType, srcType);
                return target.ImportReference(mr);

            case TypeReference tr:
                GuardNotTemplateInternal(tr, tr, srcType);
                return target.ImportReference(tr);

            case CallSite:
                throw new NotSupportedException("Template contains calli; unsupported");

            default:
                return op;                                     // string / numeric / etc.
        }
    }
}
