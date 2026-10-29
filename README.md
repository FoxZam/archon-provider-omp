# archon-provider-omp

Archon provider plugin that drives the [oh-my-pi](https://github.com/can1357/oh-my-pi)
coding agent (`omp`) as an Archon provider. Every Archon workflow turn — triage,
plan, implement, review, validate — runs through `omp acp` against the models you
already configured in `~/.omp/agent` (e.g. a local Lemonade server), with
auto-approval because factory laps are unattended by design.

## How it works

Archon launches one instance of the compiled binary per turn and speaks the Archon
provider-plugin wire (NDJSON JSON-RPC) on its stdio. The bridge spawns one
`omp acp --approval-mode yolo [--model <provider/model>]` child per turn and
translates:

| omp (ACP) | Archon plugin wire |
|---|---|
| `agent_message_chunk` / `agent_thought_chunk` deltas | coalesced whole-block `agent_message_chunk` / `agent_thought_chunk` |
| `tool_call` / terminal `tool_call_update` | `tool_call` / `tool_call_update` (output capped at 16 384 code points) |
| `session/request_permission` | auto-answered `allow_always` |
| prompt `stopReason` + `usage` | `result` chunk (`stopReason`, `tokens`) then `settled` |
| `session/load` | Archon `resumeSessionId` |

`outputFormat: json_schema` is prompt-augmented (best-effort, like the pi
provider): the schema is appended to the prompt with a JSON-only instruction.

Model strings pass through verbatim: set tiers to omp ids such as
`lemonade/gufo.Qwen3.8-Flash-Next-UD-Q4_K_XL` (optionally with an `:effort`
suffix, which omp's `--model` flag understands).

Inline `agents:` on a workflow node are supported: before each turn the
provider writes the node's agent definitions into `<cwd>/.omp/agents/*.md`
(omp's project agent directory, so the session's `task` tool can spawn them)
and restores the directory when the turn ends. `description` and `prompt` map
directly; `model` passes through to omp's fuzzy matcher and `tools` are
translated from Claude SDK names to omp names (unmapped names pass through
lowercased); `disallowedTools`, `skills` and `maxTurns` have no omp equivalent
and are ignored.

## Install

```bash
archon plugin install FoxZam/archon-provider-omp
```

Then in `~/.archon/config.yaml`:

```yaml
defaultAssistant: omp
tiers:
  small:  { provider: omp, model: lemonade/gufo.Qwen3.8-Flash-Next-UD-Q4_K_XL }
  medium: { provider: omp, model: lemonade/gufo.Qwen3.8-Flash-Next-UD-Q4_K_XL }
  large:  { provider: omp, model: lemonade/gufo.Qwen3.8-Flash-Next-UD-Q4_K_XL }
```

Requirements on the host: `omp` on `PATH`, configured (models + credentials in
`~/.omp/agent`), and the model process actually served by your provider (a
Lemonade model that is not loaded fails fast with `cloud (<model>) request failed`).

## Development

```bash
bun build --compile --target=bun-darwin-arm64 src/main.ts --outfile dist/archon-provider-omp
bun test-host.ts ./dist/archon-provider-omp "Reply with exactly: pong" "lemonade/<model>"
```

`test-host.ts` plays the Archon host: initialize → session/new → session/prompt,
prints every `_archon/chunk`, then exercises `check_credential`, `list_models`
and `diagnose` on a second connection.

Release assets are named `archon-provider-omp-<os>-<arch>[.exe]` plus
`checksums.txt` (`shasum -a 256` format), matching Archon's installer contract.
