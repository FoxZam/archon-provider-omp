#!/usr/bin/env bun
/**
 * archon-provider-omp — Archon provider plugin wrapping the oh-my-pi (`omp`) CLI.
 *
 * Archon launches exactly one instance of this executable per turn (zero args) and
 * speaks the Archon provider plugin wire on stdin/stdout: newline-delimited JSON-RPC
 * 2.0 with methods `initialize`, `session/new`, `session/prompt`, `session/cancel`
 * and `_archon/*` calls, replying with `_archon/chunk` notifications.
 *
 * Inside the process we spawn ONE `omp acp` child — standard ACP, same framing —
 * and translate its session/update stream into Archon chunks. omp runs headless
 * with auto-approval because a factory lap is unattended by design.
 *
 * stdout carries RPC frames only; everything else goes to stderr.
 */

import { mkdir, readFile, rm, stat, writeFile } from "node:fs/promises";

type Json = string | number | boolean | null | Json[] | { [key: string]: Json };
type JsonObject = { [key: string]: Json };

interface RpcMessage {
  jsonrpc: "2.0";
  id?: number | string;
  method?: string;
  params?: Json;
  result?: Json;
  error?: { code: number; message: string; data?: Json };
}

/** The child-stdin-like sink this peer writes frames to. */
interface FrameWriter {
  write(chunk: string): unknown;
}

const MAX_LINE_BYTES = 16 * 1024 * 1024;

/** Minimal bidirectional NDJSON JSON-RPC peer. */
class Rpc {
  private nextId = 0;
  private readonly requests = new Map<string, (params: Json) => Promise<Json> | Json>();
  private readonly notifications = new Map<string, (params: Json) => void>();
  private readonly pending = new Map<number, { resolve(v: Json): void; reject(e: Error): void }>();
  private writes: Promise<void> = Promise.resolve();
  private ended = false;

  constructor(private readonly out: FrameWriter) {}

  onRequest(method: string, handler: (params: Json) => Promise<Json> | Json): void {
    this.requests.set(method, handler);
  }
  onNotification(method: string, handler: (params: Json) => void): void {
    this.notifications.set(method, handler);
  }

  request(method: string, params: JsonObject): Promise<Json> {
    const id = ++this.nextId;
    void this.write(JSON.stringify({ jsonrpc: "2.0", id, method, params }));
    const { promise, resolve, reject } = Promise.withResolvers<Json>();
    this.pending.set(id, { resolve, reject });
    return promise;
  }

  notify(method: string, params: JsonObject): Promise<void> {
    return this.write(JSON.stringify({ jsonrpc: "2.0", method, params }));
  }

  private write(line: string): Promise<void> {
    this.writes = this.writes.then(() => {
      if (this.ended) return;
      try {
        this.out.write(line + "\n");
      } catch {
        this.ended = true;
      }
    });
    return this.writes;
  }

  private reply(id: number | string, result: Json): void {
    void this.write(JSON.stringify({ jsonrpc: "2.0", id, result }));
  }
  private replyError(id: number | string, message: string): void {
    void this.write(JSON.stringify({ jsonrpc: "2.0", id, error: { code: -32603, message } }));
  }

  /** Consume a byte stream; dispatch complete lines. Resolves at stream end. */
  async consume(stream: ReadableStream<Uint8Array>): Promise<void> {
    const reader = stream.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let lineCount = 0;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      for (;;) {
        const nl = buffer.indexOf("\n");
        if (nl < 0) break;
        const line = buffer.slice(0, nl);
        buffer = buffer.slice(nl + 1);
        lineCount += 1;
        if (Buffer.byteLength(line) > MAX_LINE_BYTES)
          throw new Error(`rpc line ${String(lineCount)} exceeds transport bound`);
        if (!line.trim()) continue;
        let message: RpcMessage;
        try {
          message = JSON.parse(line) as RpcMessage;
        } catch {
          throw new Error(`rpc line ${String(lineCount)} is not valid JSON`);
        }
        this.dispatch(message);
      }
    }
  }

  private dispatch(message: RpcMessage): void {
    if (message.id !== undefined && message.method === undefined) {
      if (typeof message.id === "number") {
        const waiter = this.pending.get(message.id);
        if (waiter) {
          this.pending.delete(message.id);
          if (message.error) waiter.reject(new Error(message.error.message));
          else waiter.resolve((message.result ?? null) as Json);
        }
      }
      return;
    }
    if (message.method === undefined) return;
    const params = message.params ?? null;
    if (message.id === undefined) {
      this.notifications.get(message.method)?.(params);
      return;
    }
    const handler = this.requests.get(message.method);
    if (!handler) {
      this.replyError(message.id, `method not found: ${message.method}`);
      return;
    }
    const id = message.id;
    Promise.resolve()
      .then(() => handler(params))
      .then(result => this.reply(id, result))
      .catch(error => this.replyError(id, String(error)));
  }

  rejectAllPending(error: Error): void {
    for (const waiter of this.pending.values()) waiter.reject(error);
    this.pending.clear();
  }
}

// ---------------------------------------------------------------------------
// Archon plugin wire constants (mirrors @archon/provider-contract, pinned to
// Archon 0.11.1 / integration revision 1cfb8fc541580fe5edf15e9a504b572bc9bb45bb).
// ---------------------------------------------------------------------------

const DESCRIPTOR = {
  protocol: 1,
  id: "omp",
  displayName: "oh my pi",
  version: "0.2.0",
  capabilities: {
    backgroundWork: "unobserved",
    sessionResume: true,
    mcp: false,
    hooks: false,
    skills: false,
    plugins: false,
    agents: true,
    toolRestrictions: false,
    structuredOutput: "best-effort",
    requiresAllPropertiesRequired: false,
    envInjection: true,
    costControl: false,
    costReporting: false,
    tokenReporting: true,
    stopReasonReporting: true,
    effortControl: false,
    fallbackModel: false,
    sandbox: false,
    settingSources: false,
    nativeTools: false,
    containerExec: false,
  },
  credentials: { kind: "static", specs: [] },
  configSchema: { type: "object" },
} as const satisfies JsonObject;

/** One Archon inline sub-agent definition (`nodeConfig.agents`, Claude SDK shape). */
interface AgentDefinition {
  description: string;
  prompt: string;
  model?: string;
  tools?: string[];
}

interface SessionRequest {
  prompt: string;
  cwd: string;
  resumeSessionId?: string;
  model?: string;
  systemPrompt?: string | { append?: string };
  outputFormat?: { type: "json_schema"; schema: JsonObject };
  nativeTools?: { name: string }[];
  maxBudgetUsd?: number;
  forkSession?: boolean;
  env?: Record<string, string>;
  nodeConfig?: { agents?: Record<string, AgentDefinition> };
}

// ---------------------------------------------------------------------------
// omp acp child
// ---------------------------------------------------------------------------

interface OmpChild {
  rpc: Rpc;
  kill(): void;
}

function spawnOmp(
  model: string | undefined,
  cwd: string,
  extraEnv: Record<string, string>
): OmpChild {
  const args = ["acp", "--approval-mode", "yolo"];
  if (model !== undefined && model.length > 0) args.push("--model", model);
  const child = Bun.spawn(["omp", ...args], {
    cwd,
    stdin: "pipe",
    stdout: "pipe",
    stderr: "inherit",
    env: { ...process.env, ...extraEnv },
  });
  const rpc = new Rpc(child.stdin as unknown as FrameWriter);
  void rpc
    .consume(child.stdout as unknown as ReadableStream<Uint8Array>)
    .catch(error => process.stderr.write(`omp acp transport closed: ${String(error)}\n`));
  return {
    rpc,
    kill: () => {
      try {
        child.kill(9);
      } catch {
        /* already gone */
      }
    },
  };
}

// ---------------------------------------------------------------------------
// Inline node agents (Archon `agents:` → omp project agent files)
// ---------------------------------------------------------------------------

/** Claude SDK tool names → omp tool names; unmapped names pass through lowercased. */
const OMP_TOOL_NAMES: Record<string, string> = {
  read: "read",
  write: "write",
  edit: "edit",
  multiedit: "edit",
  bash: "bash",
  grep: "grep",
  glob: "glob",
  websearch: "web_search",
  webfetch: "web_fetch",
  task: "task",
  agent: "task",
};

function agentMarkdown(id: string, agent: AgentDefinition): string {
  const front: string[] = [
    "---",
    `name: ${JSON.stringify(id)}`,
    `description: ${JSON.stringify(agent.description)}`,
  ];
  if (agent.tools !== undefined && agent.tools.length > 0)
    front.push(
      `tools: [${agent.tools
        .map(t => OMP_TOOL_NAMES[t.toLowerCase()] ?? t.toLowerCase())
        .map(t => JSON.stringify(t))
        .join(", ")}]`
    );
  if (agent.model !== undefined && agent.model.length > 0)
    front.push(`model: ${JSON.stringify(agent.model)}`);
  front.push("---", "", agent.prompt.trim(), "");
  return front.join("\n");
}

/**
 * Write the node's inline agents into `<cwd>/.omp/agents` (omp's project agent
 * directory, discovered by the task tool) for the duration of one turn, and
 * return a cleanup restoring the directory: overwritten files are put back,
 * created files and directories are removed.
 */
async function materializeNodeAgents(
  cwd: string,
  agents: Record<string, AgentDefinition> | undefined
): Promise<() => Promise<void>> {
  if (agents === undefined || Object.keys(agents).length === 0) return async () => {};
  const ompDir = `${cwd}/.omp`;
  const agentsDir = `${ompDir}/agents`;
  const dirExisted = await stat(agentsDir).then(
    () => true,
    () => false
  );
  const ompExisted = await stat(ompDir).then(
    () => true,
    () => false
  );
  const written: { path: string; previous: string | null }[] = [];
  const undo = async (): Promise<void> => {
    for (const { path, previous } of written) {
      if (previous !== null) await writeFile(path, previous);
      else await rm(path, { force: true });
    }
    if (!dirExisted) await rm(agentsDir, { recursive: true }).catch(() => {});
    if (!ompExisted) await rm(ompDir, { recursive: true }).catch(() => {});
  };
  try {
    if (!dirExisted) await mkdir(agentsDir, { recursive: true });
    for (const [id, agent] of Object.entries(agents)) {
      const path = `${agentsDir}/${id.replace(/[^a-zA-Z0-9._-]/g, "-")}.md`;
      const previous = await readFile(path, "utf8").catch(() => null);
      await writeFile(path, agentMarkdown(id, agent));
      written.push({ path, previous });
    }
  } catch (error) {
    await undo();
    throw new Error(`cannot materialize node agents: ${String(error)}`);
  }
  return undo;
}

type ChunkSink = (chunk: JsonObject) => Promise<void>;

interface TurnResult {
  stopReason: string;
  tokens?: { input: number; output: number; total?: number };
  structuredOutput?: Json;
}

async function newOmpSession(child: OmpChild, cwd: string): Promise<string> {
  const created = (await child.rpc.request("session/new", { cwd, mcpServers: [] })) as JsonObject;
  if (typeof created.sessionId !== "string") throw new Error("omp session/new returned no sessionId");
  return created.sessionId;
}

function extractToolOutput(update: JsonObject): string | undefined {
  const parts: string[] = [];
  const content = update.content;
  if (Array.isArray(content)) {
    for (const item of content) {
      if (item !== null && typeof item === "object") {
        const entry = item as JsonObject;
        const nested = entry.content;
        if (nested !== null && typeof nested === "object" && typeof (nested as JsonObject).text === "string")
          parts.push((nested as JsonObject).text as string);
        else if (typeof entry.text === "string") parts.push(entry.text);
      }
    }
  }
  if (typeof update.rawOutput === "string") parts.push(update.rawOutput);
  else if (update.rawOutput !== undefined && update.rawOutput !== null) {
    try {
      parts.push(JSON.stringify(update.rawOutput));
    } catch {
      /* non-serializable rawOutput */
    }
  }
  return parts.length > 0 ? parts.join("\n") : undefined;
}

// Best-effort structured output repair, mirroring the engine's canonical tiers
// (trim, strip code fences, whole-text parse, forward scan from the first `{`).
// A model told to answer with one schema object sometimes appends a second JSON
// blob or wraps it in prose; the engine prefers a provider structuredOutput
// payload over parsing the text. Objects only: a schema node's payload is an
// object, and extracting an array or stray value would burn a reask.
function extractFirstJson(text: string): Json | undefined {
  const asObject = (candidate: string): Json | undefined => {
    try {
      const parsed = JSON.parse(candidate) as Json;
      return parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)
        ? parsed
        : undefined;
    } catch {
      return undefined;
    }
  };
  let body = text.trim();
  const fenced = /```(?:json)?\s*([\s\S]*?)```/.exec(body);
  if (fenced !== null) body = fenced[1]!.trim();
  const whole = asObject(body);
  if (whole !== undefined) return whole;
  const start = body.indexOf("{");
  if (start < 0) return undefined;
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = start; i < body.length; i += 1) {
    const ch = body[i];
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === "\\") escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') inString = true;
    else if (ch === "{") depth += 1;
    else if (ch === "}") {
      depth -= 1;
      if (depth === 0) {
        const found = asObject(body.slice(start, i + 1));
        if (found !== undefined) return found;
        const next = body.slice(i + 1).indexOf("{");
        if (next < 0) return undefined;
        return extractFirstJson(body.slice(i + 1 + next));
      }
    }
  }
  return undefined;
}

/**
 * Drive one omp turn: create (or resume) the session, translate updates to Archon
 * chunks (whole blocks, coalesced per messageId, flushed at tool boundaries),
 * send the prompt, return the ACP stop reason and usage.
 */
async function runOmpTurn(
  child: OmpChild,
  request: SessionRequest,
  sink: ChunkSink,
  onOmpSessionId: (id: string) => void
): Promise<TurnResult> {
  await child.rpc.request("initialize", { protocolVersion: 1, clientCapabilities: {} });

  let ompSessionId: string;
  if (request.resumeSessionId !== undefined && request.resumeSessionId.length > 0) {
    try {
      const loaded = (await child.rpc.request("session/load", {
        sessionId: request.resumeSessionId,
        cwd: request.cwd,
        mcpServers: [],
      })) as JsonObject;
      ompSessionId =
        typeof loaded.sessionId === "string" ? loaded.sessionId : request.resumeSessionId;
    } catch {
      await sink({
        type: "warning",
        code: "omp.resume_failed",
        message: "omp could not resume the session; started fresh",
      });
      ompSessionId = await newOmpSession(child, request.cwd);
    }
  } else {
    ompSessionId = await newOmpSession(child, request.cwd);
  }
  onOmpSessionId(ompSessionId);

  // Auto-approve permission cards: a factory lap is unattended. Even under yolo,
  // critical bash commands still raise cards the bridge must answer.
  child.rpc.onRequest("session/request_permission", params => {
    const record = (params ?? {}) as JsonObject;
    const options = Array.isArray(record.options) ? record.options : [];
    let chosen = "";
    for (const option of options) {
      if (option === null || typeof option !== "object") continue;
      const entry = option as JsonObject;
      const optionId = typeof entry.optionId === "string" ? entry.optionId : "";
      const kind = typeof entry.kind === "string" ? entry.kind : "";
      if (optionId.length > 0 && kind === "allow_always") {
        chosen = optionId;
        break;
      }
      if (optionId.length > 0 && chosen.length === 0 && kind.startsWith("allow")) chosen = optionId;
    }
    return { outcome: { outcome: "selected", optionId: chosen } };
  });
  const unsupported = (): never => {
    throw new Error("unsupported by the headless omp bridge");
  };
  child.rpc.onRequest("fs/read_text_file", unsupported);
  child.rpc.onRequest("fs/write_text_file", unsupported);
  child.rpc.onRequest("terminal/create", unsupported);
  child.rpc.onRequest("elicitation/create", unsupported);

  // Block coalescing: Archon wants whole text blocks; omp streams deltas tagged
  // per messageId. Flush on id change, tool boundary, or turn end.
  let messageText = "";
  let turnText = "";
  let messageId = "";
  let thoughtText = "";
  let thoughtId = "";
  const flushMessage = async (): Promise<void> => {
    if (messageText.length > 0) {
      await sink({ type: "agent_message_chunk", text: messageText });
      messageText = "";
    }
  };
  const flushThought = async (): Promise<void> => {
    if (thoughtText.length > 0) {
      await sink({ type: "agent_thought_chunk", text: thoughtText });
      thoughtText = "";
    }
  };

  child.rpc.onNotification("session/update", params => {
    const record = (params ?? {}) as JsonObject;
    const update = (record.update ?? {}) as JsonObject;
    const kind = update.sessionUpdate;
    const currentMessageId = typeof update.messageId === "string" ? update.messageId : "";
    void (async () => {
      if (kind === "agent_message_chunk") {
        if (currentMessageId !== messageId) await flushMessage();
        messageId = currentMessageId;
        const content = (update.content ?? {}) as JsonObject;
        if (typeof content.text === "string") {
          messageText += content.text;
          turnText += content.text;
        }
        return;
      }
      if (kind === "agent_thought_chunk") {
        if (currentMessageId !== thoughtId) await flushThought();
        thoughtId = currentMessageId;
        const content = (update.content ?? {}) as JsonObject;
        if (typeof content.text === "string") thoughtText += content.text;
        return;
      }
      await flushMessage();
      await flushThought();
      messageId = "";
      thoughtId = "";
      if (kind === "tool_call") {
        const name =
          typeof update.kind === "string" && update.kind.length > 0
            ? update.kind
            : typeof update.title === "string"
              ? update.title
              : "omp_tool";
        await sink({
          type: "tool_call",
          toolCallId: typeof update.toolCallId === "string" ? update.toolCallId : crypto.randomUUID(),
          name,
          ...(typeof update.title === "string" ? { title: update.title } : {}),
          ...(update.rawInput !== undefined ? { rawInput: update.rawInput } : {}),
        });
        return;
      }
      if (kind === "tool_call_update") {
        const status = update.status;
        if (status !== "completed" && status !== "failed" && status !== "cancelled") return;
        const chunk: JsonObject = {
          type: "tool_call_update",
          toolCallId: typeof update.toolCallId === "string" ? update.toolCallId : "",
          status,
        };
        const output = extractToolOutput(update);
        if (output !== undefined) {
          const cap = 16384;
          if (output.length > cap) {
            chunk.output = output.slice(0, cap);
            chunk.outputTruncated = true;
          } else {
            chunk.output = output;
          }
        }
        await sink(chunk);
      }
    })().catch(error => process.stderr.write(`update translation failed: ${String(error)}\n`));
  });

  let prompt = request.prompt;
  const system = request.systemPrompt;
  if (typeof system === "string" && system.length > 0) prompt = `${system}\n\n${prompt}`;
  else if (system !== null && typeof system === "object") {
    const append = (system as { append?: unknown }).append;
    if (typeof append === "string" && append.length > 0) prompt = `${prompt}\n\n${append}`;
  }
  if (request.outputFormat !== undefined) {
    prompt = `${prompt}\n\nRespond with ONLY a JSON value valid against this JSON Schema, no prose, no fences:\n${JSON.stringify(request.outputFormat.schema)}`;
  }

  const response = (await child.rpc.request("session/prompt", {
    sessionId: ompSessionId,
    prompt: [{ type: "text", text: prompt }],
  })) as JsonObject;

  await flushMessage();
  await flushThought();
  const stopReason = typeof response.stopReason === "string" ? response.stopReason : "end_turn";
  const usage = response.usage;
  let tokens: TurnResult["tokens"];
  if (usage !== null && typeof usage === "object") {
    const u = usage as JsonObject;
    if (typeof u.inputTokens === "number" && typeof u.outputTokens === "number") {
      tokens = { input: u.inputTokens, output: u.outputTokens };
      if (typeof u.totalTokens === "number") tokens.total = u.totalTokens;
    }
  }
  const structuredOutput =
    request.outputFormat !== undefined ? extractFirstJson(turnText) : undefined;
  return { stopReason, tokens, ...(structuredOutput !== undefined ? { structuredOutput } : {}) };
}

// ---------------------------------------------------------------------------
// Archon-side server
// ---------------------------------------------------------------------------

function mapStopReason(acp: string): string {
  if (acp === "max_tokens") return "max_tokens";
  if (acp === "refusal") return "refusal";
  if (acp === "cancelled") return "cancelled";
  return "end_turn";
}

async function main(): Promise<void> {
  const host = new Rpc(Bun.stdout as unknown as FrameWriter);
  let initialized = false;
  let cancelled = false;

  host.onRequest("initialize", params => {
    const record = (params ?? {}) as JsonObject;
    const caps = (record.clientCapabilities ?? {}) as JsonObject;
    const meta = (caps._meta ?? {}) as JsonObject;
    const archon = meta.archon;
    if (
      record.protocolVersion !== 1 ||
      archon === null ||
      typeof archon !== "object" ||
      (archon as JsonObject).protocol !== 1
    )
      throw new Error("not an Archon provider-plugin initialize");
    initialized = true;
    return {
      protocolVersion: 1,
      agentCapabilities: { _meta: { archon: DESCRIPTOR } },
      authMethods: [],
    };
  });

  interface Session {
    request: SessionRequest;
    child: OmpChild | undefined;
    ompSessionId: string;
  }
  const sessions = new Map<string, Session>();
  let nextSession = 0;

  host.onRequest("session/new", params => {
    if (!initialized) throw new Error("initialize must complete first");
    const record = (params ?? {}) as JsonObject;
    const meta = ((record._meta ?? {}) as JsonObject).archon;
    if (meta === null || typeof meta !== "object") throw new Error("session/new missing _meta.archon");
    const request = ((meta as JsonObject).request ?? {}) as unknown as SessionRequest;
    if (request.cwd !== record.cwd) throw new Error("session/new cwd disagrees with request");
    const sessionId = String(nextSession++);
    sessions.set(sessionId, { request, child: undefined, ompSessionId: "" });
    return { sessionId };
  });

  host.onRequest("session/prompt", async params => {
    if (!initialized) throw new Error("initialize must complete first");
    const record = (params ?? {}) as JsonObject;
    const sessionId = typeof record.sessionId === "string" ? record.sessionId : "";
    const session = sessions.get(sessionId);
    if (!session) throw new Error("unknown session");
    const blocks = record.prompt;
    const first = Array.isArray(blocks) ? blocks[0] : undefined;
    const text = first !== null && typeof first === "object" ? (first as JsonObject).text : undefined;
    if (text !== session.request.prompt) throw new Error("session/prompt disagrees with request");

    const cleanupAgents = await materializeNodeAgents(
      session.request.cwd,
      session.request.nodeConfig?.agents
    );
    const child = spawnOmp(session.request.model, session.request.cwd, session.request.env ?? {});
    session.child = child;
    const sink: ChunkSink = async chunk => {
      if (!cancelled) await host.notify("_archon/chunk", { sessionId, chunk });
    };

    try {
      const turn = await runOmpTurn(child, session.request, sink, ompSessionId => {
        session.ompSessionId = ompSessionId;
      });
      const stopReason = cancelled ? "cancelled" : mapStopReason(turn.stopReason);
      if (!cancelled) {
        await host.notify("_archon/chunk", {
          sessionId,
          chunk: {
            type: "result",
            sessionId: session.ompSessionId,
            stopReason,
            ...(turn.tokens !== undefined ? { tokens: turn.tokens } : {}),
            ...(turn.structuredOutput !== undefined
              ? { structuredOutput: turn.structuredOutput }
              : {}),
          },
        });
        await host.notify("_archon/chunk", { sessionId, chunk: { type: "settled" } });
      }
      return { stopReason };
    } finally {
      child.kill();
      await cleanupAgents();
      sessions.delete(sessionId);
    }
  });

  host.onNotification("session/cancel", params => {
    cancelled = true;
    const record = (params ?? {}) as JsonObject;
    const session = sessions.get(typeof record.sessionId === "string" ? record.sessionId : "");
    if (session !== undefined && session.child !== undefined)
      void session.child.rpc.notify("session/cancel", { sessionId: session.ompSessionId });
  });

  host.onRequest("_archon/check_credential", async (): Promise<Json> => {
    try {
      const probe = Bun.spawn(["omp", "--version"], { stdout: "pipe", stderr: "pipe" });
      const code = await probe.exited;
      if (code !== 0)
        return { state: "check_failed", source: "native", evidence: `omp --version exited ${String(code)}` };
      return { state: "usable", source: "native" };
    } catch (error) {
      return { state: "check_failed", source: "native", evidence: String(error) };
    }
  });

  host.onRequest("_archon/resolve_credential_model", () => ({}));

  host.onRequest("_archon/list_models", () => {
    const modelsPath = `${process.env["HOME"] ?? ""}/.omp/agent/models.yml`;
    return Bun.file(modelsPath)
      .text()
      .then(text => {
        const models: { id: string }[] = [];
        let provider = "";
        for (const raw of text.split("\n")) {
          const providerMatch = /^  ([a-zA-Z0-9_-]+):\s*$/.exec(raw);
          if (providerMatch !== null) {
            provider = providerMatch[1] ?? "";
            continue;
          }
          const idMatch = /^\s*-\s+id:\s*(\S+)/.exec(raw);
          if (idMatch !== null && provider.length > 0) models.push({ id: `${provider}/${idMatch[1]}` });
        }
        return { models };
      })
      .catch(() => ({ models: [] }));
  });

  host.onRequest("_archon/diagnose", async () => {
    const checks: JsonObject[] = [];
    try {
      const probe = Bun.spawn(["omp", "--version"], { stdout: "pipe", stderr: "pipe" });
      const text = await new Response(probe.stdout).text();
      const code = await probe.exited;
      checks.push({
        id: "omp-binary",
        label: "omp CLI",
        status: code === 0 ? "ok" : "fail",
        message: text.trim(),
      });
    } catch (error) {
      checks.push({
        id: "omp-binary",
        label: "omp CLI",
        status: "fail",
        message: String(error),
        remedy: "install omp (oh-my-pi) and ensure it is on PATH",
      });
    }
    const modelsPath = `${process.env["HOME"] ?? ""}/.omp/agent/models.yml`;
    const present = await Bun.file(modelsPath).exists();
    checks.push({
      id: "omp-models",
      label: "omp model configuration",
      status: present ? "ok" : "warn",
      message: present ? modelsPath : `${modelsPath} not found; omp defaults apply`,
    });
    return { checks };
  });

  await host.consume(Bun.stdin.stream() as unknown as ReadableStream<Uint8Array>);
  // Archon closes stdin for graceful shutdown; kill any live omp child and exit.
  for (const session of sessions.values()) session.child?.kill();
  host.rejectAllPending(new Error("host stdin closed"));
  process.exit(0);
}

void main().catch(error => {
  process.stderr.write(`archon-provider-omp fatal: ${String(error)}\n`);
  process.exit(1);
});
