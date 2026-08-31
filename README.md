# emby-strm-patch

[![CI](https://github.com/Elity/emby-strm-patch/actions/workflows/ci.yml/badge.svg)](https://github.com/Elity/emby-strm-patch/actions/workflows/ci.yml)

Stop Emby from relaying `.strm` media (remote http/https direct links) badly — and stop it
transcoding remote sources it has no business transcoding.

Depending on what your origin does, "badly" has two different fixes, and this repository ships
both. They live in the **same patched assembly** and are selected **per URL prefix** by a
configuration file, so switching a library from one to the other is a one-line edit that takes
effect in 30 seconds without restarting Emby.

This repository is a [Claude Code skill](https://docs.claude.com/en/docs/claude-code/skills).
Clone it into your skills directory and ask your agent to patch Emby; it will walk the process,
starting by confirming where your Emby install lives.

```bash
git clone https://github.com/Elity/emby-strm-patch ~/.claude/skills/emby-strm-patch
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

## Step 0: find out which mode your origin needs

**Do this before you patch anything.** It needs no Emby, no patch, and no build — just a URL to a
large file on the origin you care about. Get this wrong and you will install the mode that cannot
help you, then go looking for a bug that is not there.

```bash
bash parallel/probe-origin.sh 'https://origin.example.com/large-file.mkv'   # signed, if required
```

It measures one connection against several, and prints a verdict:

| Verdict | What it means | Use |
|---|---|---|
| `the cap is THE PIPE` | throughput is flat no matter how many connections | mode **`302`** |
| `the cap is PER CONNECTION` | throughput scales with the connection count | mode **`parallel`** |
| `no verdict` | the measurement cannot be trusted — see below | fix, then re-run |

Why this matters: **302 takes the server out of the transfer, it does not add bandwidth.** If your
origin caps *each connection* rather than total throughput, the client inherits exactly the same
single-connection limit and a high-bitrate file still stutters after the patch — the bottleneck
has simply moved. The only thing that helps there is pulling the file over several connections at
once, which is what mode `parallel` does.

Expect it to take **4–5 minutes**, and possibly much longer: its runtime is set by your origin.
32 MiB over a 4 Mbps connection is 65 seconds per request. It prints its plan before starting.

The script refuses to guess. It checks the HTTP status *and* the byte count of every request (an
earlier hand-rolled version of this test proudly reported "6 MB in 146 ms" — every request had
been a 403 and it was timing error pages); it re-measures the single-connection baseline at the
end and withholds the verdict if the origin drifted; and it refuses to judge any round faster
than a plausibility ceiling, because past roughly a gigabit it is timing request overhead rather
than an origin, and a CDN cache hit reads exactly like that.

If you skip this step, take the default. `302` is what an unconfigured mode token means, and it
is the right answer for most origins.

## What this does

Short IL sequences are injected into Emby's own assemblies:

| Patch | Where | Effect | Applies to |
|---|---|---|---|
| **A** | `MediaEncoding.dll`<br>`MediaInfoService.SetDeviceSpecificData` | Sets `SupportsTranscoding = false` for matched sources. Emby's own `ForceDirectPlay` path then keeps direct play available and never builds a `TranscodingUrl`. | **every** configured prefix, in either mode |
| **B** | `Implementations.dll`<br>`HttpResultFactory.GetStaticFileResult` | Matched sources return **302** to the origin URL instead of being relayed. | prefixes whose mode is `302` |
| **C** | `Implementations.dll`<br>`HttpResultFactory.GetContent` | Matched sources are fetched over several concurrent Range requests and relayed, instead of over one connection. | prefixes whose mode is `parallel` |

Patch A always applies to both modes, because transcoding defeats either one: once ffmpeg
re-encodes, the server has already downloaded the full-bitrate source and the point is gone.

### B and C are not alternatives — they ship together

**One patched `Emby.Server.Implementations.dll` contains both.** They are injected into two
*different methods* of the same class and carry independent marker fields, so one patcher run
installs both, and the choice between them moves entirely into configuration:

```
https://throttled.example.com/     parallel      # this library is fetched over N connections
https://fast-cdn.example.net/      302           # this one is redirected to the client
```

Same binary, same process, same request cycle — the only thing that decides is which line the
source URL matches. Nothing enforces mutual exclusion because nothing has to: when B answers a
redirect it returns from `GetStaticFileResult`, so `GetContent` is never reached. Exclusivity is
a consequence of control flow.

Patch C is **opt-in at build time too**. Leave off the patcher's `--parallel` flag and only B is
injected; if you only want 302 you never build the helper assembly and never touch `deps.json`.

| | `302` | `parallel` |
|---|---|---|
| Against a per-connection cap | inherits the cap (no better than stock) | scales with connections |
| Server bandwidth | none — the client goes direct | the whole file crosses the server |
| Server memory | none | ~96 MB ceiling per stream |
| Needs | the patched assembly | patched assembly **+** helper DLL **+** `deps.json` entry |
| Right when | the origin does **not** throttle per connection | the origin **does** |

## Configuration

`<programdata>/config/strm-routing.txt`. Two kinds of line, plus comments:

```
# comments run to end of line

https://pan.example.com/          parallel   # prefix line: URL prefix, then a mode
https://fast-cdn.example.net/     302
https://other.example/                       # mode omitted = 302

ramp-seconds = 6                             # setting line: key = value
```

**Prefix lines.** Matching is `StartsWith`, case-insensitive, against `MediaSourceInfo.Path` —
which is the URL stored inside your `.strm` file, **verbatim**. Read one of your `.strm` files to
get the prefix right. If the URL contains non-ASCII characters, Emby stores it percent-encoded
and does not decode it, so **the prefix must be written percent-encoded too**; a decoded prefix
matches nothing, silently. `embypatch check` warns about this.

**Mode tokens** are `302` and `parallel`, case-insensitive. **Omitting the token means `302`.**

**A misspelled mode voids the whole line** — it is reported, never quietly downgraded to the
default. A typo that silently means 302 is indistinguishable from "parallel is enabled and
mysteriously not helping", which is a miserable thing to debug. The same holds for a misspelled
setting key. Everything else is fail-soft: a bad line is skipped and recorded, the rest of the
file still applies, and nothing throws.

**Setting lines** are global. Keys are case-insensitive and `-` and `_` are equivalent.
Priority is **environment variable > file > built-in default**.

| key | default | environment override | |
|---|---|---|---|
| `ramp-seconds` | 6 | `EMBY_STRM_RAMP_SECONDS` | seconds between adding connections |
| `connections` | 8 | `EMBY_STRM_CONNECTIONS` | concurrent Range requests |
| `chunk-mb` | 8 | `EMBY_STRM_CHUNK_MB` | bytes per Range request |
| `buffer-mb` | 128 | `EMBY_STRM_BUFFER_MB` | reorder buffer ceiling |
| `initial-connections` | 2 | `EMBY_STRM_INITIAL_CONNECTIONS` | connections opened immediately |
| `log` | off | `EMBY_STRM_LOG` | diagnostics file path |

Settings only affect `parallel`. Keep tuning in **this file** rather than in exported environment
variables: an Emby upgrade rewrites the launcher script where those exports live, and tuning that
vanishes on upgrade resurfaces months later as "it started stuttering again".

`ramp-seconds` is the one worth tuning per origin, and it has a **cliff** — one notch too low and
sustained throughput collapses below the original problem. The shipped default of 6 is
deliberately conservative. Sweep it against your own origin with `parallel/run-tests.sh seeks 25`
before changing it; that script's header documents the method and a measured table.

The whole file can also be supplied inline as `EMBY_STRM_PREFIXES` (semicolon-separated, same
syntax), or pointed at with `EMBY_STRM_CONFIG` (absolute path). `strm-routing.txt.example` is an
annotated template.

Keep prefixes narrow. Anything else Emby serves over http — Live TV channels in particular —
must not match, or it will lose its transcoding fallback.

### Checking it

```bash
dotnet run --project patcher -- check <programdata-dir> <emby-system-dir>
```

Reports which patches are actually installed, every prefix and the mode it routes to, whether
that mode is **satisfiable** (a `parallel` prefix with patch C not installed is not), every
setting's effective value **and where it came from**, and every rejected line with its number.
Exit code 0 when clean.

There is deliberately **no runtime self-healing**: a `parallel` prefix on an install without
patch C does not quietly fall back to 302. Silently doing something other than what was asked is
the failure mode this whole project exists to avoid.

## Design properties

- **No configuration means stock behaviour.** With an empty or missing config file every
  predicate returns false and all three injections fall through. This also makes "empty the config
  file" a zero-downtime rollback.
- **Non-matching paths are unchanged.** Injected code is prepended to the method body with every
  branch target set to the original first instruction. No existing instruction is modified.
- **Config reloads in 30 seconds.** Adding a prefix, or switching one between `302` and
  `parallel`, needs no rebuild and no restart.
- **The patches are independent.** Apply or roll back any of them alone; A and B work with no
  helper assembly present at all.
- **One parser, not two.** The cloned-in matcher and the helper assembly compile the *same*
  source file, and both compiled copies are asserted against a shared table of test vectors. Two
  hand-written parsers for one syntax is how "the checker says it's fine, the runtime disagrees"
  bugs are born.
- **Idempotent.** A marker field per patch makes a second run refuse rather than stack.
- **Not pinned to an Emby version.** Targets are located by type and method signature, not by
  file offset. `SKILL.md` states what each target is *semantically*, so when Emby moves things
  the target can be re-identified rather than guessed; `references/internals.md` walks through it.

## Requirements

- .NET SDK 8.0 or newer
- An Emby Server install you can write to and restart
- The patcher runs on the same machine as Emby
- For mode `parallel` only: Python 3, to register the helper assembly in `EmbyServer.deps.json`

The patcher tells you immediately if a target no longer matches, so trying it costs a build and
nothing else — it never writes to your install.

## Tests

```bash
bash tests/run.sh                          # patcher + injections + check-config, no Emby needed
bash parallel/run-tests.sh mock            # the fetcher: correctness, faults, memory  (~45 s)
bash parallel/run-tests.sh config          # config parsing, settings wiring, diagnostics
bash parallel/tests/probe-origin.test.sh   # the probe's verdicts, against a simulated origin
```

None of them touch the network or need Emby binaries. `tests/run.sh` patches two synthetic
assemblies shaped like Emby's, then invokes the patched methods and checks what they actually
return — which is what catches a clone that decompiles beautifully and matches nothing at
runtime. `probe-origin.test.sh` drives the probe against `parallel/tests/origin-sim.py`, which
can impose either a per-connection cap or a shared one, and asserts each shape yields the right
recommendation.

CI runs the timing-independent suites on Linux, Windows and macOS, and the full set on Linux.
Two of the fetcher's mock tests spend 20 s and 11.7 s deliberately waiting — a trickling
connection has to be abandoned and retried, and a genuinely slow consumer must *not* trip the
throughput floor — and those assertions are the kind that go red on a loaded shared runner for
reasons that have nothing to do with the code.

## Layout

```
SKILL.md                     the procedure, start to finish
references/internals.md      injection points, the IL, and how to re-derive them
references/mode-routing.md   why the mode is runtime configuration; the config grammar
patcher/                     target detection and all three injections (Mono.Cecil)
patcher/TypeCloner.cs        deep-copies the matcher type into the target assembly
patcher/CheckConfig.cs       the `check` subcommand
shared/RoutingConfig.cs      the routing table parser — compiled into BOTH products
shared/RoutingVectors.cs     shared parse vectors both compiled copies are asserted against
template/                    builds the cloneable copy of the parser
parallel/src/                the multi-connection fetcher (helper assembly, patch C)
parallel/probe-origin.sh     does your origin throttle per connection?  START HERE
parallel/deps_patch.py       registers the helper in EmbyServer.deps.json
parallel/run-tests.sh        the fetcher's test suites
rtcheck/                     loads a patched assembly and executes the matcher
tests/                       synthetic fixtures plus the behavioural test runner
strm-routing.txt.example     annotated configuration template
```

## After an Emby upgrade

Upgrading replaces the application directory, so everything installed into it reverts to stock:
both patched assemblies, and — if you use `parallel` — the helper DLL and its `deps.json` entry.

Your configuration lives in `programdata/` and **survives**, tuning settings included. That is
the whole reason the knobs live in `strm-routing.txt` rather than in environment variables: after
an upgrade you only re-run the patcher, with nothing to reconstruct by hand.

The symptom is `.strm` playback quietly going back to stuttering or transcoding with no
configuration change on your side. Compare the assembly hashes before assuming anything else, or
just run `embypatch check` — it names the patch that went missing.

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
