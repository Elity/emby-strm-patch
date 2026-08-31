#!/usr/bin/env python3
"""Add (or remove) an extra assembly to EmbyServer.deps.json.

Emby is a framework-dependent .NET app: the host builds its trusted-assembly list
from deps.json, so a DLL merely dropped into system/ will not load. This registers
one.  Emby's own entries carry an empty sha512, so there is no integrity check to
satisfy -- verified on 4.9.3.0.

usage:
  deps_patch.py add    <deps.json> <AssemblyName> [version] [-o out.json]
  deps_patch.py remove <deps.json> <AssemblyName>          [-o out.json]
  deps_patch.py check  <deps.json> <AssemblyName>
"""
import json, sys, argparse, copy

HOST = "Emby.Server.Implementations"   # the assembly whose code will call into ours


def rid_target(d):
    """The runtime-specific target is the one that actually carries entries."""
    targets = d.get("targets", {})
    best, best_n = None, -1
    for name, entries in targets.items():
        if len(entries) > best_n:
            best, best_n = name, len(entries)
    if best is None:
        raise SystemExit("x no targets section")
    return best


def host_key(t):
    for k in t:
        if k.split("/")[0] == HOST:
            return k
    raise SystemExit(f"x {HOST} not found in targets")


def add(d, name, version):
    t_name = rid_target(d)
    t = d["targets"][t_name]
    key = f"{name}/{version}"
    t[key] = {"runtime": {f"{name}.dll": {"assemblyVersion": f"{version}.0",
                                          "fileVersion": f"{version}.0"}}}
    d.setdefault("libraries", {})[key] = {"type": "project", "serviceable": False, "sha512": ""}
    # Make it reachable from the dependency closure, otherwise the host may prune it.
    hk = host_key(t)
    t[hk].setdefault("dependencies", {})[name] = version
    return t_name, key, hk


def remove(d, name):
    t_name = rid_target(d)
    t = d["targets"][t_name]
    gone = []
    for key in [k for k in t if k.split("/")[0] == name]:
        del t[key]; gone.append(key)
    for key in [k for k in d.get("libraries", {}) if k.split("/")[0] == name]:
        del d["libraries"][key]
    hk = host_key(t)
    d["targets"][t_name][hk].get("dependencies", {}).pop(name, None)
    return gone


def check(d, name):
    t_name = rid_target(d)
    t = d["targets"][t_name]
    in_t = [k for k in t if k.split("/")[0] == name]
    in_l = [k for k in d.get("libraries", {}) if k.split("/")[0] == name]
    hk = host_key(t)
    in_dep = name in t[hk].get("dependencies", {})
    return t_name, in_t, in_l, in_dep


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["add", "remove", "check"])
    ap.add_argument("deps")
    ap.add_argument("name")
    ap.add_argument("version", nargs="?", default="1.0.0")
    ap.add_argument("-o", "--out")
    a = ap.parse_args()

    with open(a.deps, encoding="utf-8") as f:
        raw = f.read()
    d = json.loads(raw)
    before = copy.deepcopy(d)

    if a.action == "check":
        t_name, in_t, in_l, in_dep = check(d, a.name)
        print(f"  target        : {t_name}")
        print(f"  targets entry : {in_t or 'MISSING'}")
        print(f"  libraries     : {in_l or 'MISSING'}")
        print(f"  in {HOST} deps: {in_dep}")
        return 0 if (in_t and in_l and in_dep) else 1

    if a.action == "add":
        t_name, key, hk = add(d, a.name, a.version)
        print(f"  target : {t_name}")
        print(f"  added  : targets[{key}], libraries[{key}]")
        print(f"  linked : {hk} -> dependencies[{a.name}] = {a.version}")
    else:
        gone = remove(d, a.name)
        print(f"  removed: {gone or '(nothing)'}")

    # Sanity: only the intended keys changed.
    t_name = rid_target(d)
    n_before = len(before["targets"][t_name])
    n_after = len(d["targets"][t_name])
    print(f"  targets entries {n_before} -> {n_after}   libraries {len(before.get('libraries',{}))} -> {len(d.get('libraries',{}))}")

    out = a.out or a.deps
    with open(out, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)
    # Re-parse what we wrote; a deps.json the host cannot parse bricks the server.
    with open(out, encoding="utf-8") as f:
        json.load(f)
    print(f"  wrote  : {out}  (re-parsed OK)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
