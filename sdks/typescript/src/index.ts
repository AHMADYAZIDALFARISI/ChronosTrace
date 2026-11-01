import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, renameSync, writeFileSync } from "node:fs";
import net from "node:net";
import path from "node:path";
import readline from "node:readline";
import { resolveChronosTraceBinary } from "./bin-path.js";

export { resolveChronosTraceBinary } from "./bin-path.js";

export type InteractionKind = "LlmCompletion" | "McpToolCall" | "HttpToolCall";

export type DiffKind =
  | "Identical"
  | "PromptDriftReconciled"
  | "ToolSchemaMutated"
  | "ToolArgumentMutated"
  | "ResponseDiverged"
  | "TopologyDiverged";

export interface FieldDelta {
  path: string;
  baseline_value: unknown;
  candidate_value: unknown;
}

export interface StepDiff {
  step_index: number;
  baseline_node_id: string | null;
  candidate_node_id: string | null;
  interaction_kind: InteractionKind;
  target_name: string;
  kind: DiffKind;
  prompt_similarity: number;
  baseline_latency_ms: number;
  candidate_latency_ms: number;
  argument_deltas: FieldDelta[];
  response_deltas: FieldDelta[];
}

export interface TrajectoryDiffReport {
  baseline_id: string;
  candidate_id: string;
  total_baseline_steps: number;
  total_candidate_steps: number;
  identical_count: number;
  reconciled_drift_count: number;
  mutated_args_count: number;
  response_diverged_count: number;
  topology_diverged_count: number;
  baseline_total_latency_ms: number;
  candidate_total_latency_ms: number;
  steps: StepDiff[];
}

export interface ChronosTraceOptions {
  cassette: string;
  chaosRateLimitProb?: number;
  /** Default false agar tidak memutasi process.env global pada pengujian paralel (CT-012) */
  injectEnv?: boolean;
}

export interface ChronosTraceContext {
  baseUrl: string;
  openAiBaseUrl: string;
  mcpHttpUrl: string;
  cassettePath: string;
}

async function getEphemeralPort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (addr && typeof addr === "object") {
        const port = addr.port;
        server.close(() => resolve(port));
      } else {
        server.close(() => reject(new Error("Failed to allocate ephemeral port")));
      }
    });
  });
}

async function waitForPortReady(port: number, child: ChildProcess, timeoutMs = 4000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(`ChronosTrace daemon exited prematurely with code ${child.exitCode}`);
    }
    try {
      await new Promise<void>((resolve, reject) => {
        const socket = net.connect(port, "127.0.0.1");
        socket.once("connect", () => {
          socket.destroy();
          resolve();
        });
        socket.once("error", (err) => {
          socket.destroy();
          reject(err);
        });
      });
      return;
    } catch {
      await new Promise((r) => setTimeout(r, 20));
    }
  }
  throw new Error(`Timed out waiting for ChronosTrace daemon on 127.0.0.1:${port}`);
}

export async function withChronosTrace<T>(
  options: ChronosTraceOptions,
  fn: (ctx: ChronosTraceContext) => Promise<T>
): Promise<T> {
  const bin = resolveChronosTraceBinary();
  const port = await getEphemeralPort();
  const bindAddr = `127.0.0.1:${port}`;
  const cassettePath = path.resolve(options.cassette);

  const args = ["replay", "--cassette", cassettePath, "--bind", bindAddr];
  if (typeof options.chaosRateLimitProb === "number") {
    args.push("--chaos-rate-limit-prob", String(options.chaosRateLimitProb));
  }

  const child = spawn(bin, args, {
    stdio: ["ignore", "pipe", "pipe"],
  });

  let stderrLog = "";
  child.stderr?.on("data", (chunk: Buffer) => {
    stderrLog += chunk.toString();
  });

  const prevOpenAi = process.env.OPENAI_BASE_URL;
  const prevAnthropic = process.env.ANTHROPIC_BASE_URL;
  const prevMcp = process.env.CHRONOSTRACE_MCP_PROXY;

  const baseUrl = `http://${bindAddr}`;
  const ctx: ChronosTraceContext = {
    baseUrl,
    openAiBaseUrl: `${baseUrl}/v1`,
    mcpHttpUrl: `${baseUrl}/mcp/call`,
    cassettePath,
  };

  if (options.injectEnv === true) {
    process.env.OPENAI_BASE_URL = ctx.openAiBaseUrl;
    process.env.ANTHROPIC_BASE_URL = ctx.baseUrl;
    process.env.CHRONOSTRACE_MCP_PROXY = ctx.mcpHttpUrl;
  }

  try {
    await waitForPortReady(port, child);
    return await fn(ctx);
  } catch (err) {
    if (stderrLog.trim()) {
      throw new Error(
        `${err instanceof Error ? err.message : String(err)}\n[ChronosTrace Daemon Logs]:\n${stderrLog}`
      );
    }
    throw err;
  } finally {
    if (options.injectEnv === true) {
      if (prevOpenAi === undefined) delete process.env.OPENAI_BASE_URL;
      else process.env.OPENAI_BASE_URL = prevOpenAi;

      if (prevAnthropic === undefined) delete process.env.ANTHROPIC_BASE_URL;
      else process.env.ANTHROPIC_BASE_URL = prevAnthropic;

      if (prevMcp === undefined) delete process.env.CHRONOSTRACE_MCP_PROXY;
      else process.env.CHRONOSTRACE_MCP_PROXY = prevMcp;
    }

    child.kill("SIGTERM");
    await new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        child.kill("SIGKILL");
        resolve();
      }, 1000);
      child.once("exit", () => {
        clearTimeout(timer);
        resolve();
      });
    });
  }
}

export interface McpStdioOptions {
  cassette: string;
  mode?: "replay" | "record" | "passthrough";
  similarityThreshold?: number;
  simulateLatency?: boolean;
  chaosServiceOutageProb?: number;
  requestTimeoutMs?: number;
  childCommand?: string[];
}

export interface JsonRpcResponse<T = unknown> {
  jsonrpc: "2.0";
  id: number | string;
  result?: T;
  error?: {
    code: number;
    message: string;
    data?: unknown;
  };
}

export class McpStdioTestClient {
  private child: ChildProcess | null = null;
  private nextId = 1;
  private pending = new Map<
    number | string,
    {
      resolve: (res: JsonRpcResponse) => void;
      reject: (err: Error) => void;
      timer: NodeJS.Timeout;
    }
  >();

  constructor(private readonly options: McpStdioOptions) {}

  async start(): Promise<void> {
    const bin = resolveChronosTraceBinary();
    const args = [
      "mcp-wrap",
      "--cassette",
      path.resolve(this.options.cassette),
      "--mode",
      this.options.mode ?? "replay",
      "--similarity-threshold",
      String(this.options.similarityThreshold ?? 0.85),
    ];

    if (this.options.simulateLatency) {
      args.push("--simulate-latency");
    }
    if (typeof this.options.chaosServiceOutageProb === "number") {
      args.push("--chaos-service-outage-prob", String(this.options.chaosServiceOutageProb));
    }
    if (this.options.childCommand && this.options.childCommand.length > 0) {
      args.push("--", ...this.options.childCommand);
    }

    this.child = spawn(bin, args, {
      stdio: ["pipe", "pipe", "pipe"],
    });

    const rl = readline.createInterface({
      input: this.child.stdout!,
      crlfDelay: Infinity,
    });

    rl.on("line", (line) => {
      const trimmed = line.trim();
      if (!trimmed) return;
      try {
        const msg = JSON.parse(trimmed) as JsonRpcResponse;
        if (msg.id !== undefined && this.pending.has(msg.id)) {
          const handler = this.pending.get(msg.id)!;
          clearTimeout(handler.timer);
          this.pending.delete(msg.id);
          handler.resolve(msg);
        }
      } catch {
        // Ignore non-JSON lines
      }
    });

    this.child.on("exit", (code) => {
      for (const [, handler] of this.pending) {
        clearTimeout(handler.timer);
        handler.reject(new Error(`chronostrace mcp-wrap exited prematurely with code ${code}`));
      }
      this.pending.clear();
    });
  }

  async callTool<T = unknown>(
    toolName: string,
    toolArgs: Record<string, unknown>
  ): Promise<JsonRpcResponse<T>> {
    return this.request<T>("tools/call", {
      name: toolName,
      arguments: toolArgs,
    });
  }

  async request<T = unknown>(
    method: string,
    params?: Record<string, unknown>
  ): Promise<JsonRpcResponse<T>> {
    if (!this.child || !this.child.stdin) {
      throw new Error("McpStdioTestClient has not been started. Call await client.start() first.");
    }

    const id = this.nextId++;
    const timeoutMs = this.options.requestTimeoutMs ?? 5000;
    const payload = JSON.stringify({
      jsonrpc: "2.0",
      id,
      method,
      ...(params !== undefined ? { params } : {}),
    });

    return new Promise<JsonRpcResponse<T>>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`MCP request id=${id} (${method}) timed out after ${timeoutMs}ms`));
      }, timeoutMs);

      this.pending.set(id, {
        resolve: resolve as (res: JsonRpcResponse) => void,
        reject,
        timer,
      });
      this.child!.stdin!.write(`${payload}\n`, (err) => {
        if (err) {
          clearTimeout(timer);
          this.pending.delete(id);
          reject(err);
        }
      });
    });
  }

  async close(): Promise<void> {
    if (!this.child) return;
    const proc = this.child;
    this.child = null;
    proc.stdin?.end();
    proc.kill("SIGTERM");
    await new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        proc.kill("SIGKILL");
        resolve();
      }, 1000);
      proc.once("exit", () => {
        clearTimeout(timer);
        resolve();
      });
    });
  }
}

export function diffCassettes(
  baselinePath: string,
  candidatePath: string
): { compatible: boolean; report: TrajectoryDiffReport } {
  const bin = resolveChronosTraceBinary();
  const result = spawnSync(
    bin,
    [
      "diff",
      path.resolve(baselinePath),
      path.resolve(candidatePath),
      "--format",
      "json",
    ],
    { encoding: "utf-8" }
  );

  if (!result.stdout) {
    throw new Error(`chronostrace diff failed: ${result.stderr || result.error?.message}`);
  }

  const report = JSON.parse(result.stdout) as TrajectoryDiffReport;
  return {
    compatible: result.status === 0,
    report,
  };
}

export function canonicalizeJson(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalizeJson);
  }
  if (value !== null && typeof value === "object") {
    const sortedKeys = Object.keys(value as Record<string, unknown>).sort();
    const result: Record<string, unknown> = {};
    for (const k of sortedKeys) {
      result[k] = canonicalizeJson((value as Record<string, unknown>)[k]);
    }
    return result;
  }
  return value;
}

export function sha256CanonicalJson(value: unknown): string {
  const canonical = canonicalizeJson(value);
  const serialized = JSON.stringify(canonical ?? null);
  return createHash("sha256").update(serialized, "utf8").digest("hex");
}

const FNV_OFFSET_BASIS = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;
const MASK_64 = 0xffffffffffffffffn;

function fnv1a64(input: string): bigint {
  const bytes = Buffer.from(input, "utf8");
  let hash = FNV_OFFSET_BASIS;
  for (const byte of bytes) {
    hash ^= BigInt(byte);
    hash = (hash * FNV_PRIME) & MASK_64;
  }
  return hash;
}

/**
 * Selaras dengan `chronostrace_core::simhash::compute_simhash` (Unicode-aware alphanumeric + lowercase).
 */
export function computeSimHashHex(text: string): string {
  const normalized = text
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .toLowerCase()
    .trim();

  if (!normalized) return "0000000000000000";

  const tokens = normalized.split(/\s+/).filter(Boolean);
  const bitWeights = new Int32Array(64);

  const accumulate = (h: bigint) => {
    for (let i = 0n; i < 64n; i++) {
      if (((h >> i) & 1n) === 1n) {
        bitWeights[Number(i)] += 1;
      } else {
        bitWeights[Number(i)] -= 1;
      }
    }
  };

  if (tokens.length >= 3) {
    for (let i = 0; i <= tokens.length - 3; i++) {
      const shingle = `${tokens[i]} ${tokens[i + 1]} ${tokens[i + 2]}`;
      accumulate(fnv1a64(shingle));
    }
  } else {
    for (const token of tokens) {
      accumulate(fnv1a64(token));
    }
  }

  let fingerprint = 0n;
  for (let i = 0; i < 64; i++) {
    if (bitWeights[i] > 0) {
      fingerprint |= 1n << BigInt(i);
    }
  }

  return fingerprint.toString(16).padStart(16, "0");
}

export interface RecordedTurnInput {
  kind: InteractionKind;
  requestPayload: Record<string, unknown>;
  responsePayload: Record<string, unknown>;
  recordedLatencyMs?: number;
}

export class CassetteBuilder {
  private nodes: Array<Record<string, unknown>> = [];
  private lastNodeId: string | null = null;

  constructor(
    private readonly cassetteId: string,
    private readonly similarityThreshold = 0.85
  ) {}

  addTurn(input: RecordedTurnInput): this {
    const req = { ...input.requestPayload };
    if (input.kind === "McpToolCall") {
      delete req.id;
    }

    let promptText = JSON.stringify(canonicalizeJson(req));
    let structuralTools: unknown = null;

    if (input.kind === "LlmCompletion") {
      const messages = Array.isArray(req.messages) ? req.messages : [];
      promptText = messages
        .map((m: Record<string, unknown>) => (typeof m?.content === "string" ? m.content : ""))
        .filter(Boolean)
        .join("\n");
      structuralTools = req.tools ?? req.params ?? null;
    } else if (input.kind === "McpToolCall") {
      const method = typeof req.method === "string" ? req.method : "";
      const params = req.params ?? null;
      promptText = `${method}:${JSON.stringify(canonicalizeJson(params))}`;
      structuralTools = { method, params };
    }

    const exactRequestHash = sha256CanonicalJson(req);
    const structuralToolHash = sha256CanonicalJson(structuralTools);
    const promptSimhash = computeSimHashHex(promptText);
    const nodeId = `node_${String(this.nodes.length + 1).padStart(4, "0")}_${exactRequestHash.slice(0, 8)}`;

    this.nodes.push({
      node_id: nodeId,
      parent_id: this.lastNodeId,
      kind: input.kind,
      exact_request_hash: exactRequestHash,
      prompt_simhash: promptSimhash,
      structural_tool_hash: structuralToolHash,
      request_payload: req,
      response_payload: input.responsePayload,
      recorded_latency_ms: input.recordedLatencyMs ?? 15,
    });

    this.lastNodeId = nodeId;
    return this;
  }

  toJSON(): Record<string, unknown> {
    return {
      version: "1.0",
      cassette_id: this.cassetteId,
      metadata: {
        created_at: new Date().toISOString(),
        similarity_threshold: this.similarityThreshold,
        scrubbed: true,
        scrubber_policy_version: "ts-builder-1.0",
        redaction_count: 0,
      },
      nodes: this.nodes,
    };
  }

  saveToFile(filePath: string): string {
    const resolved = path.resolve(filePath);
    mkdirSync(path.dirname(resolved), { recursive: true });
    const tmpPath = `${resolved}.${process.pid}.tmp`;
    writeFileSync(tmpPath, JSON.stringify(this.toJSON(), null, 2), "utf8");
    renameSync(tmpPath, resolved);
    return resolved;
  }
}
