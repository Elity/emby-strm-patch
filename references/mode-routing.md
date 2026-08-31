# Mode routing: why one binary serves both delivery modes

This is the design rationale the source comments cite by section number. `internals.md` covers
*how* the IL injections work; this file covers *why* the mode is a runtime decision rather than
a build-time one.

## 1. The problem

There are two ways to stop Emby relaying a remote `.strm` source badly, and which one is correct
is decided by the **origin**, not by the machine Emby runs on:

- The origin throttles **each connection** rather than total bandwidth → the server must fetch
  the file over several concurrent Range requests and relay it (**parallel**). A 302 only hands
  the client the same single-connection ceiling; the bottleneck moves, it does not go away.
- The origin does **not** throttle per connection → **302** is the right answer, and parallel
  fetching is pure overhead: the bytes cross the server for nothing.

One Emby install can easily have libraries pointing at both kinds of origin. So the choice has to
be **per prefix**, and it has to be **runtime configuration** — baking it into the build means
rebuilding a DLL and restarting a media server to answer a question about somebody's CDN.

## 2. The structural fact that makes this possible

| Patch | Injection target | Marker field |
|---|---|---|
| A — never transcode | `MediaEncoding.dll` → `MediaInfoService.SetDeviceSpecificData` | `__strm_notranscode_patched` |
| B — 302 | `Implementations.dll` → `HttpResultFactory.`**`GetStaticFileResult`** | `__strm302_patched` |
| C — parallel relay | `Implementations.dll` → `HttpResultFactory.`**`GetContent`** | `__strm_parallel_patched` |

B and C sit on **different methods of the same class** and carry **independent marker fields**,
so both can be injected into one assembly in one pass.

Their exclusivity needs no enforcement. At runtime B returns the redirect from
`GetStaticFileResult`, so no `FileWriter` is constructed and `GetContent` is never reached.
Mutual exclusion is a **consequence of control flow**, not a build-time choice.

## 3. The configuration file

### 3.1 Name and lookup

The file is `strm-routing.txt`. (`direct` would have described only the 302 half; `parallel` is
precisely the mode that does *not* go direct. `routing` is neutral between the two and survives a
third mode being added.)

Five layers, first hit wins:

1. env `EMBY_STRM_PREFIXES` — inline, `;`-separated, same syntax as file lines
2. env `EMBY_STRM_CONFIG` — absolute path
3. command line `-programdata <p>` → `<p>/config/strm-routing.txt`
4. `AppContext.BaseDirectory/strm-routing.txt`
5. `<BaseDirectory>/../programdata/config/strm-routing.txt`

There is **no compatibility fallback to an older filename**. Two files that could both exist
would need a precedence rule, and "which of my two config files is live" is a new failure state,
not a migration convenience.

### 3.2 Syntax

```
# comment, to end of line

ramp-seconds = 6                          # setting line

https://pan.example.com/      parallel    # per-connection throttled origin
https://fast-cdn.example.net/ 302         # confirmed unthrottled
https://other.example/                    # mode omitted = 302
```

Parsing rules, **in this order** — the order is load-bearing:

1. Strip from the first `#`, trim, skip if empty.
2. **Line contains `://` → prefix line.** Split on whitespace: token 0 is the URL prefix,
   token 1 (optional) is the mode. More than two tokens is an error line.
   The `://` test comes first so that a prefix carrying a signed query string — which contains
   `=` — is not mistaken for a setting line.
3. Otherwise, **contains `=` → setting line**, `key = value`. Keys are case-insensitive and
   `-` and `_` are equivalent.
4. Otherwise → error line.

**Mode tokens** are `302` and `parallel`, case-insensitive. Omitting the token means `302`.

An **unknown mode token (a typo such as `paralell`) voids the entire line** and is reported. It is
never quietly downgraded to the default: a typo that silently means 302 is indistinguishable from
"parallel is enabled and mysteriously not helping", which is the most expensive kind of bug to
chase. The same reasoning applies to an unknown setting key.

Error handling is **fail-soft**: a bad line is skipped and recorded, the rest of the file still
applies, and nothing throws. This code runs on a media server's hot path, and one mistyped line
must not make a library unplayable. `embypatch check` is what surfaces the skipped lines (§5).

### 3.3 Settings

Settings are **global**. There is deliberately no per-prefix override: scope nesting buys
parsing complexity and a whole category of "I set it, why didn't it apply" bugs, in exchange for
a case that barely occurs — an install normally has at most one origin that needs `parallel`.

| key | default | environment override |
|---|---|---|
| `ramp-seconds` | 6 | `EMBY_STRM_RAMP_SECONDS` |
| `connections` | 6 | `EMBY_STRM_CONNECTIONS` |
| `chunk-mb` | 8 | `EMBY_STRM_CHUNK_MB` |
| `buffer-mb` | 128 | `EMBY_STRM_BUFFER_MB` |
| `initial-connections` | 2 | `EMBY_STRM_INITIAL_CONNECTIONS` |
| `log` | off | `EMBY_STRM_LOG` |

#### How to tune these

**Turn `log` on first.** Without it you are tuning by feel, and feel cannot separate "the origin
is slow" from "my connections are being throttled" from "it never finished ramping up". One line
is written per stream at close, and it carries all three numbers you need:

```
#20 closed ABANDONED delivered=1365MiB/10749MiB elapsed=732.4s rate=15.64Mbps chunks=177/1346 retries=5 (slow=0)
                                                              ~~~~~~~~~~~~~~~              ~~~~~~~
```

- **`rate=`** — what actually reached the player. Compare it against the source's bitrate
  (`file bytes × 8 ÷ duration in seconds`). Below the bitrate it will stutter; you want headroom
  above it, not parity.
- **`slow=`** — connections abandoned for falling under the throughput floor. **Anything above
  zero means you have opened too many** and the origin is throttling you. This is the only
  reliable signal for tuning `connections`.
- **`retries=`** — a handful is normal (expired signed URLs, the occasional 502). Steady growth
  means the origin is unwell.

Tune in this order, one setting at a time:

1. **`connections`** — the one that matters. Lower it while `slow > 0`; raise it if `rate` is
   below what the source needs *and* `slow` is still 0.
   ⚠️ **It is a per-STREAM limit, while the origin limits the total it sees.** A seek makes two
   streams overlap — the abandoned one's connections are not released at the origin
   immediately — so the origin briefly sees roughly **twice** this number. Measured: 8 → ~16 at
   the origin → collapse (every connection down to ~33 KB/s, 15 slow-retries, playback errored);
   6 → ~12 → a 732 s continuous read at 15.64 Mbps with zero slow events.
2. **`ramp-seconds`** — only if startup is slow; see the sweep in §7. **1 is a cliff**, do not
   go looking.
3. **`chunk-mb` / `buffer-mb`** — rarely worth touching. The memory ceiling is
   `slots × chunk-mb`, and `slots ≤ connections + 4`.

**Do not guess — verify from the log.** A config edit is live within 30 s: play something, seek a
few times, then read `slow=` and `rate=`.

**Priority: environment > file > built-in default.**

Tuning belongs in the **file**, not in exported environment variables. On the usual Linux
packaging the exports would live in `bin/emby-server`, and an Emby upgrade rewrites that file
while leaving `programdata/` alone. Tuning that disappears on upgrade shows up months later as
"it started stuttering again", and nobody traces that back to a missing knob.

## 4. Three patches, three different questions

One configuration, three questions:

| Patch | Asks | Why |
|---|---|---|
| A | `IsMatch(path)` — **any** configured prefix, whatever its mode | both modes need transcoding off; once ffmpeg re-encodes, the point of either mode is gone |
| B | `IsRedirect(path)` — mode is `302` | |
| C | `IsParallel(path)` — mode is `parallel` | |

B's and C's predicates are mutually exclusive by construction, so no extra interlock is needed.
A is deliberately not per-prefix switchable: there is no real scenario for "serve this over 302
but please do still transcode it".

### 4.1 One parser, two binaries

The routing configuration is read from two places that never reference each other at runtime:

- `shared/RoutingConfig.cs` — deep-copied by `patcher/TypeCloner.cs` into `MediaEncoding.dll`
  (patch A) and `Implementations.dll` (patch B). Must have **zero external dependencies**.
- the same file, compiled into `EmbyStrmParallel.dll` — the helper patch C calls into, which also
  resolves the tuning settings.

Two hand-written parsers for one syntax is how "the checker says it's fine, the runtime
disagrees" bugs are born. So there is **exactly one source file**, and the two projects each
compile it with `<Compile Include="../shared/RoutingConfig.cs" />`. One source, two build
outputs, zero runtime coupling — which is what lets A and B keep working with no helper assembly
installed at all (§6).

`shared/RoutingVectors.cs` holds a shared table of input lines and expected results, and **both**
compiled copies are asserted against it. Without that table the single-source rule is a
convention; with it, drift is a test failure.

⚠️ Because this file gets cloned, it may not contain array literals or `switch`-on-string —
both are lowered into `<PrivateImplementationDetails>` RVA fields that the cloner cannot copy —
and it may not have a static constructor. The full constraint list is in the file's own header,
and `internals.md` explains why each one exists.

## 5. The `check` subcommand

```
embypatch check <programdata-dir> <emby-system-dir>
```

It is a subcommand of the patcher rather than a standalone script for two hard reasons: deciding
whether patch C is installed means reading a marker field out of a DLL, which is Cecil's job; and
parsing the configuration must reuse the *same* parser, which a standalone script could only do
by reimplementing the syntax — recreating exactly the problem §4.1 exists to prevent.

It reports:

- whether each of the three patches is installed (by marker field)
- whether `EmbyStrmParallel.dll` is present and registered in `deps.json`
- every prefix → its mode → whether that mode is **satisfiable** (`parallel` configured while
  patch C is not installed is not)
- every setting's effective value **and where it came from** (env / file / default)
- every rejected line, with line number and reason

Exit code 0 when clean, 1 when something is wrong.

⚠️ **The checker cannot read the server's environment.** Any `export EMBY_STRM_*` lives in the
launcher script and is set *before* the server process is exec'd, so a separate checking process
does not inherit it. A report based only on its own environment would confidently state
"ramp-seconds = 6, from file" while the server is really running 2. The checker therefore
**parses the launcher script's export lines** and shows them as the highest-priority layer, and
warns about each one, because those exports are also what an upgrade silently discards.

**There is no runtime self-healing.** Patch B could technically reflect over patch C's marker
field — they are on the same class — and "helpfully" fall back to 302 when C is absent. It
deliberately does not. Silently changing behaviour is the failure mode this whole project exists
to avoid. "Not in effect, and saying so loudly" beats "in effect, but not the one you asked for".

## 6. What this changes about deployment

One patcher invocation puts **both B and C** into `Implementations.dll`.

**Escape hatch:** omit `--parallel <helper.dll>` and only B is injected. Anyone who wants nothing
but 302 never installs the helper assembly and never touches `deps.json`.

```
embypatch <input.dll> <output.dll> [referenceDir] [templateDll] [--parallel <helper.dll>]
```

`--parallel` **adds** C; it does not replace B. The template is therefore always required, since
B is always injected.

## 7. Two probes, in order

They answer different questions and are deliberately not merged:

| Script | Answers | Needs | Takes |
|---|---|---|---|
| `parallel/probe-origin.sh` | does my origin throttle per connection? | just a URL — **no Emby, no patch** | 4–5 min |
| `parallel/run-tests.sh seeks <n>` | what should `ramp-seconds` be? | a built helper and a URL in `TEST_URL.txt` | ~100 s cooldown per value |

**Only run the ramp sweep if `probe-origin.sh` said `parallel`.** Merging the two would force
somebody who only wants to know "do I even need this?" to install the patch first.

Both are slower than they look. `probe-origin.sh`'s runtime is set by the origin: 32 MiB over a
4 Mbps connection is 65 s per request, so three rounds plus the re-check is several minutes.
That is why it prints its own plan before starting.

`ramp-seconds` ships at **6**, conservatively. Lower values measured better on the reference
origin, but there is a **cliff** — one notch too low and sustained throughput collapses to well
below the original problem. That is not a decision to make on somebody else's behalf, and the
sweep exists so it can be made on evidence.

## 8. What a build has to pass

1. The offline suite (`tests/run.sh`) is green, and the patcher can apply A, then B, then C to
   the same stock assembly with all three marker fields present afterwards.
2. `check` is correct for three shapes of configuration: a bare URL line, a line tagged
   `parallel`, and a line with a misspelled mode.
3. **302 and parallel are simultaneously live in one DLL** — one prefix tagged `302` answers
   302, another tagged `parallel` answers 206 at multi-connection throughput. This is the one
   that matters; it is the entire claim of the design. The others are regressions.
4. Regression: local-library items still answer 206, and startup logs zero fatal exceptions.
5. Somebody actually watches a video. Throughput numbers and byte counts do not measure
   "does it stutter", and the two have disagreed before.

Step 1 patches the fixtures **with `--parallel`**, and the behaviour test then invokes the
injected `GetContent` through reflection. Before that, patch C's IL had never been executed by
any test: the helper's own suite was entirely green while saying nothing about whether Emby ever
calls it, whether the null-means-fallback branch is wired the right way round, or whether
`TotalLength` and `Length` land on the properties the host reads. **A direct helper test and an
injection test do not cover each other.**

### 8.1 Failure modes that now have a test

Every row below is a defect the layers downstream cannot see — either the right byte *count* at
the wrong byte *position*, or a hang. Each has a case that genuinely fails without the fix:

| Failure mode | Consequence | Where |
|---|---|---|
| 206 with a missing, unparseable or `bytes */N` Content-Range | body spliced in at an offset it did not come from; the length still adds up | `run-tests.sh mock` |
| complete-length changes mid-transfer | two versions of the object stitched into one file | same |
| 206 that reports only `/*` | host publishes this stream's *range* length as the *file* length | same + behaviour §7 |
| response carries a non-identity `Content-Encoding` | byte offsets into a re-encoded representation | `run-tests.sh mock` |
| a worker throws outside a chunk download | reader waits forever: no error, no fallback, playback just freezes | same |
| 503 with `Retry-After` ignored | retrying before the server said to, reconverging into the herd that caused it | same |
| origin ignores Range at a far offset | the whole file downloaded and discarded to deliver a few KB | same |

There is only one way to know a new case is worth having: **remove the fix and check that it goes
red.** All six were verified that way. Before the fix, the Content-Range case accepted 5 of 8
malformed headers and delivered bytes from the wrong position for every one of them.

### 8.2 Root cause: the probe asked for bytes past EOF

Of 26 opens in one deployment's log, **3** took the `single-conn-fallback` path, and **every one
of them was a tail read**:

```
skip=10484846882  want=2083 bytes    (last 2 KB of a 10.48 GB file)  x2
skip=1472540135   want=637228 bytes  (last 637 KB of a 1.47 GB file)
```

That path serves a ranged request out of a 200 whole-resource body by reading and discarding
everything ahead of the offset — 10.48 GB downloaded to deliver 2083 bytes. **The delivered bytes
are correct**, so nothing downstream can tell.

The first round of probing **could not reproduce it** — 21 clean 206s across offsets, idle and
under load. **Because those probes clamped the range end to `SZ-1` and the production code does
not**, which is exactly what hid the bug. Reproducing the real request shape made it deterministic:

| request | result |
|---|---|
| `bytes=(EOF-83)-(EOF-83+1MiB-1)` — **end past EOF** | **200, whole file** |
| `bytes=(EOF-83)-(EOF-1)` — clamped | 206 |
| `bytes=(EOF-83)-` — open-ended | 206 |
| `bytes=0-` — open-ended | 206 |

**The bug is on our side.** The probe runs before the resource size is known, so with `length == 0`
it guesses a span of `FirstChunkSize`, and any request starting within 1 MiB of the end overshoots
— which is precisely the shape of a player's MKV index read. RFC 7233 says a server should clamp
such a range; this origin returns the whole resource instead.

Two layers of fix:

1. **The probe sends a bounded range first and re-asks open-ended on a 200.** Open-ended cannot
   overshoot by construction. Bounded stays first because an open-ended probe makes the origin
   stream everything to EOF while only chunk 0 is consumed — irrelevant for a tail read, wasteful
   for the whole-file request that dominates. The cost is one extra round trip on tail reads.
2. Past `MaxIgnoredRangeSkipBytes` (64 MiB) a persistent 200 is declined and handed to the host.
   Smaller offsets still take the skip, the legitimate "this origin has no Range support" case.

**This test was nearly a tautology.** The first version asserted on the returned bytes — and the
bytes were correct both before and after the fix. It only discriminates once it asserts how many
bytes the origin was made to transfer (8 MiB versus ~0.5 MiB).

`SkipLimitStream` also gained a closing log line. **The most expensive path in the component was
the only one that logged nothing**, so there was no way to tell afterwards how long that 1.47 GB
discard took, or whether it finished.

## 9. Prefixes are matched against the **percent-encoded** URL

A `.strm` file stores a URL, and Emby puts that string into `MediaSourceInfo.Path` **verbatim** —
it is not decoded. Matching is a literal, case-insensitive `StartsWith`.

So a prefix containing non-ASCII characters must be written **percent-encoded**, exactly as it
appears inside the `.strm` file. A prefix written in decoded form can never match, and the
failure is completely silent: no match simply means stock behaviour, with nothing logged.

`check` warns when a configured prefix contains non-ASCII characters, because this failure is
close to impossible to diagnose from symptoms alone.

## 10. Changing your mind is cheap

- **Switch a prefix's mode** — edit its line to `302` or `parallel`. Live within 30 seconds. No
  restart, no assembly swap.
- **Disable everything** — empty `strm-routing.txt`. All three patches go inert and the server
  behaves exactly like stock. Still no restart.
- **Uninstall** — restore the stock assembly backups, and if the helper was registered, remove it
  from `deps.json` with `parallel/deps_patch.py remove`.

## 11. Deliberately not done

- per-prefix overrides for the tuning settings (§3.3)
- auto-detecting whether an origin throttles — it would mean benchmarking on the hot path, and
  the measurement is easily poisoned by a cache hit (see `probe-origin.sh`'s plausibility ceiling)
- runtime self-healing when a prefix says `parallel` but patch C is absent (§5)
- a compatibility fallback to the previous configuration filename (§3.1)
- a per-prefix switch for patch A (§4)

### 11.1 Known gaps, deliberately left open

Unlike the list above, these are real. They are recorded here so the next person to find them
does not have to re-derive the reasoning:

- **Cross-stream connection and memory budgets.** One connection pool per stream is a measured,
  deliberate choice: a shared pool let an abandoned stream poison the next one
  (25.14 → 2.58 → 0.57 → 0.15 Mbps, while curl on fresh connections was healthy at the same
  instant). So any global budget **must not reintroduce a shared pool**. `FetchMetrics` is
  already a process-wide buffered-byte counter; what is missing is admission control, not
  observability. Two default streams add up to 16 connections, which is past the 503 cliff
  measured on the reference origin — though that specific collision has not been demonstrated
  with a two-stream experiment.
- **The probe blocks a host request thread** for up to the stall budget (30s). This follows from
  the injection point: patch C returns `Task.FromResult` before the async state machine starts,
  so making the probe asynchronous means injecting a continuation. Not a small change.
- **Emby's `StaticFileResultOptions.RequestHeaders` are not forwarded** to the origin. Signed
  URLs carry their own authorisation, and an origin that needs more answers 401/403 → retry →
  fallback, which is a *loud* failure rather than a silent one.
- **No strong validator (`If-Range` / `ETag`).** Comparing complete-length catches every version
  change that alters the size, but not a same-length replacement. The reference origin does not
  supply a strong validator, so there is nothing to send.
