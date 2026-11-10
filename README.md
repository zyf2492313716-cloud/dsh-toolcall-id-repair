# dsh-toolcall-id-repair

Repair and prevent a session-bricking bug in [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) (dsh) 0.2.x.

## The bug in one paragraph

When an OpenAI-compatible provider streams a tool call, `pi-ai` stores the call's
id as `toolCall.id || ""`. The harness's stream validator only checks
`typeof chunk.id === 'string'`, and `""` satisfies that — so an empty id passes
validation, reaches the persisted session log, and is then rejected by the
session-format invariants. The turn dies with:

```
format v4 tool/result at seq N requires toolCallId matching its tool source
```

Neither side is wrong on its own. Upstream deliberately preserves empty adapter
identities (see `packages/llm/llm/tests/assistant-stream.spec.ts`, the test named
*"preserving JSON-safe empty adapter identities"*), while the persistence layer
requires a non-empty id (`packages/session/session-format-v3-to-v4/src/relationships.ts`).
The two together brick the session, and the turn cannot recover: the assistant
message is already committed and the matching `tool/result` can never be appended.

Verified against `deepseek-harness` at `0.2.1-alpha.5`. As of that revision the
inconsistency is still present.

## Which providers trigger it

Any OpenAI-compatible backend that omits tool-call ids. Confirmed in the wild on
an aggregator that routes to free-tier models: the failure rate rose with prompt
size, from **0.4% under 8k tokens to ~36% past 64k**. Paid, larger-context
routes on the same gateway showed zero failures across ~2000 calls.

## What's in here

| File | Purpose |
|---|---|
| `tools/guard-plugin.js` | A cordis plugin that fills empty ids on the `llm/stream` waterfall. Install once, and the bug stops happening. |
| `tools/repair-session.py` | Rewrites the three id sites in sessions that are already bricked. |
| `tools/verify-session.mjs` | Validates session logs using the harness's **own** validator. Use it to confirm a repair. |

## Why three id sites

An empty id is persisted in three places, and each one is independently fatal.
All three carry the same id for a given call, so all three must be rewritten
together:

1. `assistant/message` → `message.content[].id`
   Rejected on read: `tool call id requires a nonempty string`
2. `assistant/message` → `data.stream[].chunk` (the embedded replay)
   Rejected on the next append: `content disagrees with its embedded stream`
3. `tool/call` → `data.callId`
   Makes the matching `tool/result` unappendable

**Fixing only (1) and (3) is the dangerous case**: the log reads back fine and
looks repaired, then fails on the next append. A previous repair attempt in the
wild made exactly this mistake across five sessions.

## Suggested fix for the harness itself

The inconsistency is visible inside a single file. In
`packages/llm/llm/src/assistant-stream.ts`, two records carry the same data
under different rules:

| Record | Line | Rule for `id` |
|---|---|---|
| `tool-call-delta` | 138 | `if (chunk.id.length === 0 \|\| chunk.name === '')` — kept as a chunk, empty id allowed |
| `tool-call-chunks` | 477 | `if (typeof record.id !== 'string' \|\| record.id.length === 0) throw` — non-empty required |

So the compact form forbids what the delta form permits, and only the delta form
is reachable from a live provider stream. Tightening line 138 to reject an empty
id — or synthesizing one there — would close the bug at its source. Until that
lands, this plugin does the same job from the `llm/stream` waterfall, which is
the supported interception point.

## Install the guard

Copy `tools/guard-plugin.js` somewhere permanent, then register it in your
profile's patch layer. For the Desktop profile that is
`$DSH_HOME/profiles/desktop/cordis.patch.yml`; add to the trailing `insert` list:

```yaml
    - id: toolcall-id-guard
      name: '/absolute/path/to/guard-plugin.js'
```

An absolute path is resolved to a `file://` URL and does not need to be installed
as a package. **Note that `$DSH_HOME/config.yaml` is a dead file in 0.2.x** —
nothing reads it. Registering there silently does nothing, which is an easy and
invisible mistake.

Restart the app to be sure the plugin is loaded. Confirm with the audit log:

```bash
cat "$DSH_HOME/toolcall-id-guard.log"
```

`mounted pid=<n>` means the plugin is live. Each id it fills appends a `filled`
line, so the file doubles as a record of how often your provider misbehaves.

## Repair an already-bricked session

```bash
# Dry run first: prints the plan and writes nothing.
python3 tools/repair-session.py "$DSH_HOME/sessions/<escaped-cwd>/<session-id>"

# Then apply. A backup lands in $DSH_HOME/session-repair-backups/ first.
python3 tools/repair-session.py --apply "$DSH_HOME/sessions/<escaped-cwd>/<session-id>"

# Or sweep everything:
python3 tools/repair-session.py --apply --all
```

Requires Python 3 with `zstandard`.

`message.content[].id` is treated as authoritative: where it is missing an id is
synthesized, and where the embedded stream disagrees the stream is rewritten to
match. A `tool/call` is paired to its block by (turn, step, name, arguments) —
all four, so parallel calls sharing a name still pair by their distinct argument
strings. Synthesized ids are deterministic (`call_` + 24 hex from session, turn,
step and block position), so re-running is a no-op.

The tool refuses to write unless it can reproduce every row byte-for-byte through
its own serializer, and it verifies that exactly the rows carrying a bad id
changed — nothing else.

## Verify

```bash
node tools/verify-session.mjs "$DSH_HOME/sessions/<...>/<session-id>"
node tools/verify-session.mjs --all
```

This calls the harness's real `sessionFormatCatalog.createRestore` rather than
reimplementing the rules, and additionally checks append safety (no empty
tool-call ids anywhere).

**Do not validate with `zlib.zstdDecompressSync`.** dsh logs are concatenated
zstd frames, one per flush, and that function decodes only the first frame. A
scanner built on it reported a 646-event session as having 0 events and printed
`ALL CLEAN`. `verify-session.mjs` shells out to `zstdcat` for this reason.

## Verifying the bug in your own setup

To confirm a provider is at fault before changing your config, ask it for a
streamed tool call and inspect the raw id:

```bash
curl -sN http://127.0.0.1:3425/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'Authorization: Bearer magpie' \
  -d '{"model":"<your-model>","stream":true,
       "messages":[{"role":"user","content":"Call the bash tool with: echo hi"}],
       "tools":[{"type":"function","function":{"name":"bash",
         "parameters":{"type":"object","properties":{"command":{"type":"string"}},
         "required":["command"]}}}],"tool_choice":"auto"}' \
  | grep -o '"tool_calls":\[[^]]*\]'
```

An `"id":""` in the output reproduces the bug. Note it is intermittent — one
clean response proves nothing. Prompt size matters: reproduce with a long
context, since that is where it concentrates.

## Transcript shape that bricks a session

For reference, the failure always looks the same in the log: the turn ends on a
`tool/call` whose `callId` is `""`, with no matching `tool/result`, and every
tool-call block in that assistant message shares the empty id. One message, all
calls blank — not a per-call flake.

## Plugin Installation (DSH Store / Cordis)

This repository is packaged as a standard DeepSeek Harness plugin (`@dsh-local/dsh-toolcall-id-repair`).

### DSH Store

Install directly from DSH Store or by searching `dsh-toolcall-id-repair`.

### Manual Cordis Installation

Add the bundle patch to `~/.dsh/cordis.patch.yml`:

```yaml
- insert:
    - id: toolcall-id-repair
      name: '@dsh-local/dsh-toolcall-id-repair'
```

---

## Permissions & Safety Boundary

| Permission | Level | Details |
|---|---|---|
| **Files** | None | Pure in-memory stream normalizer. Zero filesystem reads or writes. |
| **Network** | None | No outbound requests, fetch, or sockets. |
| **Commands** | None | No subprocesses spawned. |
| **Credentials** | None | Does not read environment variables, API keys, or tokens. |

- **Security Rating**: Low risk, static stream transform.
- **Profiles**: `web`, `desktop`, `cli`.
- **System Requirements**: Node `^22.19 || >=24`, DSH `>=0.1.2`.

---

## Compatibility Evidence

| DSH Release | Status | Evidence |
|---|---|---|
| `0.2.1-rc.5` | compatible | In daily production use on local DSH host. |
| `0.2.2-alpha.2` | compatible | Verified against published npm closure (`ctx.on('llm/stream')` waterfall contract unchanged). |
| `0.2.2-alpha.3` | compatible | Verified against published npm closure. |
| `0.2.2-rc.1` | compatible | Verified against published npm closure. |

---

## License

MIT. The harness is MIT-licensed; the file paths and invariants quoted above come
from that project.
