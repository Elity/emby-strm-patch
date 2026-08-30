# emby-strm-follow-302

[![CI](https://github.com/Elity/emby-strm-follow-302/actions/workflows/ci.yml/badge.svg)](https://github.com/Elity/emby-strm-follow-302/actions/workflows/ci.yml)

Make Emby answer **302** for `.strm` media instead of relaying it — and stop it from transcoding
remote sources it has no business transcoding.

This repository is a [Claude Code skill](https://docs.claude.com/en/docs/claude-code/skills).
Clone it into your skills directory and ask your agent to patch Emby; it will walk the process,
starting by confirming where your Emby install lives.

```bash
git clone https://github.com/Elity/emby-strm-follow-302 ~/.claude/skills/emby-strm-follow-302
```

Everything it does is also documented well enough to follow by hand — see `SKILL.md`.

## The problem

A `.strm` file holds a single http(s) URL. Emby treats it as a remote source and applies two
defaults that assume the server sits closer to the origin than the client does:

- **PlaybackInfo.** If the client reports a bitrate ceiling below the source bitrate, Emby
  answers with `SupportsDirectPlay: false` and a `TranscodingUrl`, then starts ffmpeg. The server
  downloads the full-bitrate source *and* re-encodes it.
- **Streaming endpoint.** `/Videos/{id}/stream` and `/Videos/{id}/original.*` make Emby fetch the
  remote URL itself and forward the bytes. The client gets `206`, and every byte crosses the
  server.

When the origin is something the client could reach directly, both are pure overhead: doubled
transfer, saturated upload, needless CPU, and stuttering playback.

## What this does

Two short IL sequences are injected into Emby's own assemblies:

| Patch | Where | Effect |
|---|---|---|
| **A** | `MediaInfoService.SetDeviceSpecificData` | Sets `SupportsTranscoding = false` for matched sources. Emby's own `ForceDirectPlay` path then keeps direct play available and never builds a `TranscodingUrl`. |
| **B** | `HttpResultFactory.GetStaticFileResult` | Matched sources return **302** to the origin URL instead of being relayed. |

Which sources match is decided by a URL prefix list you configure. The prefixes are **not**
compiled into the binary.

## When 302 is not enough

302 takes the server out of the transfer. It does not add bandwidth. If your origin caps **each
connection** rather than total throughput, the client inherits that same single-connection limit
and a high-bitrate file still stutters after the patch — the bottleneck simply moved.

Worth ruling out before you go looking for a bug here. Compare one connection against four:

```bash
URL='https://origin.example.com/large-file.mkv'   # signed, if your origin requires it

probe() {   # $1 parallel 32 MiB range fetches, aggregate throughput
  for i in $(seq "$1"); do
    off=$(( i * 100000000 ))
    curl -s -o /dev/null -r "$off-$(( off + 33554431 ))" -w '%{speed_download}\n' "$URL" &
  done
  wait
}
for n in 1 4; do
  printf '%d connection(s): ' "$n"
  probe "$n" | awk '{s+=$1} END {printf "%.1f Mbps\n", s*8/1e6}'
done
```

Roughly flat means you are limited by the pipe, and 302 is the right fix. Roughly 4× means the
cap is per connection, and no direct-play scheme reaches the source faster than one connection
allows — the file has to be pulled over several connections at once. That is a different patch,
not a setting in this one.

## Design properties

- **No configuration means stock behaviour.** With an empty or missing prefix list the matcher
  always returns false and both injections fall through. This also makes "empty the config file"
  a zero-downtime rollback.
- **Non-matching paths are unchanged.** Injected code is prepended to the method body with every
  branch target set to the original first instruction. No existing instruction is modified.
- **Config reloads in 30 seconds.** Adding a URL prefix needs no rebuild and no restart.
- **The two patches are independent.** Apply or roll back either alone.
- **Idempotent.** A marker field on the patched type makes a second run refuse rather than stack.
- **Not pinned to an Emby version.** Targets are located by type and method signature, not by
  file offset. `SKILL.md` states what each target is *semantically*, so when Emby moves things
  the target can be re-identified rather than guessed; `references/internals.md` walks through it.

## Requirements

- .NET SDK 8.0 or newer
- An Emby Server install you can write to and restart
- The patcher runs on the same machine as Emby

The patcher tells you immediately if a target no longer matches, so trying it costs a build and
nothing else — it never writes to your install.

## Tests

```bash
bash tests/run.sh
```

Patches two synthetic assemblies shaped like Emby's, then invokes the patched methods and checks
what they actually return. No Emby binaries required, which is also how CI runs it (Linux,
Windows and macOS).

## Layout

```
SKILL.md                    the procedure, start to finish
references/internals.md     injection points, IL, and how to re-derive them
patcher/                    target detection and both injections (Mono.Cecil)
patcher/TypeCloner.cs       deep-copies the matcher type into the target assembly
template/StrmDirect.cs      the prefix matcher, ordinary C#
rtcheck/                    loads a patched assembly and executes the matcher
tests/                      synthetic fixtures plus the behavioural test runner
strm-direct.txt.example     annotated configuration template
```

## Configuration

`<programdata>/config/strm-direct.txt`, one prefix per line:

```
# Matching is StartsWith, case-insensitive, against MediaSourceInfo.Path.
https://pan.example.com/
```

Alternatives, in priority order: `EMBY_STRM_PREFIXES` (semicolon-separated),
`EMBY_STRM_CONFIG` (absolute path), then the file lookup.

Keep prefixes narrow. Anything else Emby serves over http — Live TV channels in particular —
must not match, or it will lose its transcoding fallback.

## After an Emby upgrade

Upgrading replaces the application directory, so the patched assemblies revert to stock. Your
configuration lives in `programdata/` and survives. Re-run the patcher against the new version.

The symptom is `.strm` playback quietly going back to stuttering or transcoding with no
configuration change on your side. Compare the assembly hashes before assuming anything else.

## Scope and disclaimer

This modifies assemblies belonging to a proprietary product, on your own machine, so that a
server you run behaves differently for media you own.

- It does **not** touch licensing, Emby Premiere entitlement, DRM, or any protection mechanism.
- It does **not** redistribute Emby binaries. The repository ships a patcher; the patched files
  are produced locally from your own installation.
- It is not affiliated with, endorsed by, or supported by Emby. Do not take patched-install
  problems to Emby support — roll back first and reproduce on stock.
- Modifying application files may violate your agreement with the vendor. That is your call to
  make. Back up before you patch, and keep the backups.

Provided as-is, with no warranty. You are responsible for what you run.

## License

MIT — see `LICENSE`. Applies to the code in this repository only, not to Emby.
