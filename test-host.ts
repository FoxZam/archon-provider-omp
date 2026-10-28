// Throwaway: play Archon against the compiled plugin binary end to end.
import { readFileSync } from "node:fs";

const binary = process.argv[2] ?? "./dist/archon-provider-omp";
const prompt = process.argv[3] ?? "Reply with exactly: pong";
const model = process.argv[4] ?? "lemonade/halogen.halogen-qwen3.8-flash-next";

interface Frame { id?: number; method?: string; params?: unknown; result?: unknown; error?: { message: string } }

const child = Bun.spawn([binary], { stdin: "pipe", stdout: "pipe", stderr: "inherit" });
let nextId = 0;
const pending = new Map<number, (f: Frame) => void>();
const write = (line: string): void => {
  child.stdin.write(line + "\n");
};
const request = (method: string, params: unknown): Promise<Frame> => {
  const id = ++nextId;
  return new Promise(resolve => {
    pending.set(id, resolve);
    write(JSON.stringify({ jsonrpc: "2.0", id, method, params }));
  });
};

let chunks = 0;
let settled = false;
let buf = "";
void (async () => {
  const reader = child.stdout.getReader();
  const dec = new TextDecoder();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    for (;;) {
      const nl = buf.indexOf("\n");
      if (nl < 0) break;
      const line = buf.slice(0, nl);
      buf = buf.slice(nl + 1);
      if (!line.trim()) continue;
      const frame = JSON.parse(line) as Frame;
      if (frame.id !== undefined && pending.has(frame.id)) {
        pending.get(frame.id)!(frame);
        pending.delete(frame.id);
        continue;
      }
      if (frame.method === "_archon/chunk") {
        chunks += 1;
        const params = frame.params as { chunk: { type?: string; text?: string } };
        const c = params.chunk;
        if (c.type === "settled") settled = true;
        console.log(`CHUNK ${c.type} ${String(c.text ?? "").slice(0, 120)}`);
      } else if (frame.method === "_archon/log") {
        console.log("LOG " + line.slice(0, 200));
      }
    }
  }
})();

const init = await request("initialize", {
  protocolVersion: 1,
  clientCapabilities: { fs: { readTextFile: false, writeTextFile: false }, terminal: false, _meta: { archon: { protocol: 1 } } },
});
console.log("INIT", JSON.stringify(init).slice(0, 160));

const archonRequest = { prompt, cwd: process.env["PWD"] ?? "/tmp", model };
const ns = await request("session/new", { cwd: archonRequest.cwd, mcpServers: [], _meta: { archon: { request: archonRequest } } });
console.log("NEW_SESSION", JSON.stringify(ns));
const sessionId = (ns.result as { sessionId: string }).sessionId;

const timeout = setTimeout(() => { console.log("TIMEOUT"); child.kill(9); process.exit(1); }, 240000);
const pr = await request("session/prompt", { sessionId, prompt: [{ type: "text", text: prompt }] });
clearTimeout(timeout);
console.log("PROMPT_RESPONSE", JSON.stringify(pr));
console.log(`SUMMARY chunks=${String(chunks)} settled=${String(settled)}`);

// check_credential + list_models + diagnose in this same turn (host opens new processes; same binary)
child.stdin.end();
await child.exited;

const child2 = Bun.spawn([binary], { stdin: "pipe", stdout: "pipe", stderr: "inherit" });
// quick second-connection check
const w2 = (method: string, params: unknown): number => {
  const id = ++nextId;
  child2.stdin.write(JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n");
  return id;
};
const p2 = new Map<number, (f: Frame) => void>();
let buf2 = "";
void (async () => {
  const reader = child2.stdout.getReader();
  const dec = new TextDecoder();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buf2 += dec.decode(value, { stream: true });
    for (;;) {
      const nl = buf2.indexOf("\n");
      if (nl < 0) break;
      const line = buf2.slice(0, nl);
      buf2 = buf2.slice(nl + 1);
      if (!line.trim()) continue;
      const f = JSON.parse(line) as Frame;
      if (f.id !== undefined && p2.has(f.id)) { p2.get(f.id)!(f); p2.delete(f.id); }
    }
  }
})();
const req2 = (method: string, params: unknown): Promise<Frame> =>
  new Promise(resolve => { p2.set(w2(method, params), resolve); });
await req2("initialize", { protocolVersion: 1, clientCapabilities: { fs: { readTextFile: false, writeTextFile: false }, terminal: false, _meta: { archon: { protocol: 1 } } } });
console.log("CHECK_CREDENTIAL", JSON.stringify(await req2("_archon/check_credential", {})));
console.log("LIST_MODELS", JSON.stringify(await req2("_archon/list_models", {})).slice(0, 400));
console.log("DIAGNOSE", JSON.stringify(await req2("_archon/diagnose", {})).slice(0, 400));
child2.stdin.end();
await child2.exited;
readFileSync; // keep import used
