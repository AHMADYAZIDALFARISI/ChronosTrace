import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  CassetteBuilder,
  diffCassettes,
  McpStdioTestClient,
  withChronosTrace,
} from "../src/index.js";
import { runRefundAgentWorkflow } from "../../../examples/langgraph-refund-agent/src/agent.js";

describe("ChronosTrace Multi-Turn Agent Replay & Chaos Fault Injection Suite", () => {
  let tempDir: string;
  let llmCassettePath: string;
  let mcpCassettePath: string;

  const SYSTEM_PROMPT_V1 =
    "You are a helpful financial refund agent for AcmeShop. Always verify the customer order_id in the database before issuing a refund to the customer.";
  const USER_PROMPT = "Please refund my damaged order ORD-900.";

  const AGENT_TOOLS = [
    {
      type: "function",
      function: { name: "verify_order", params: { order_id: "string" } },
    },
    {
      type: "function",
      function: { name: "issue_refund", params: { order_id: "string", amount: "number" } },
    },
  ];

  beforeAll(() => {
    tempDir = mkdtempSync(path.join(tmpdir(), "chronostrace-vitest-"));
    llmCassettePath = path.join(tempDir, "refund_llm.ctrace");
    mcpCassettePath = path.join(tempDir, "refund_mcp.ctrace");

    new CassetteBuilder("refund_llm_trajectory", 0.80)
      .addTurn({
        kind: "LlmCompletion",
        requestPayload: {
          model: "gpt-4o",
          messages: [
            { role: "system", content: SYSTEM_PROMPT_V1 },
            { role: "user", content: USER_PROMPT },
          ],
          tools: AGENT_TOOLS,
        },
        responsePayload: {
          choices: [
            {
              message: {
                tool_calls: [
                  {
                    function: {
                      name: "verify_order",
                      arguments: { order_id: "ORD-900" },
                    },
                  },
                ],
              },
            },
          ],
        },
        recordedLatencyMs: 380,
      })
      .addTurn({
        kind: "LlmCompletion",
        requestPayload: {
          model: "gpt-4o",
          messages: [
            { role: "system", content: SYSTEM_PROMPT_V1 },
            { role: "user", content: USER_PROMPT },
            {
              role: "assistant",
              content: "Verified order ORD-900 and processed refund REF-2026-881 for $50.",
            },
          ],
          tools: AGENT_TOOLS,
        },
        responsePayload: {
          choices: [
            {
              message: {
                content:
                  "Your refund of $50.00 for order ORD-900 has been processed (Confirmation: REF-2026-881).",
              },
            },
          ],
        },
        recordedLatencyMs: 310,
      })
      .saveToFile(llmCassettePath);

    new CassetteBuilder("refund_mcp_trajectory", 0.85)
      .addTurn({
        kind: "McpToolCall",
        requestPayload: {
          jsonrpc: "2.0",
          method: "tools/call",
          params: {
            name: "verify_order",
            arguments: { order_id: "ORD-900" },
          },
        },
        responsePayload: {
          result: {
            content: [
              {
                type: "text",
                text: JSON.stringify({ valid: true, refundable_amount: 50.0 }),
              },
            ],
          },
        },
        recordedLatencyMs: 12,
      })
      .addTurn({
        kind: "McpToolCall",
        requestPayload: {
          jsonrpc: "2.0",
          method: "tools/call",
          params: {
            name: "issue_refund",
            arguments: { order_id: "ORD-900", amount: 50.0 },
          },
        },
        responsePayload: {
          result: {
            content: [
              {
                type: "text",
                text: JSON.stringify({
                  refund_id: "REF-2026-881",
                  status: "processed",
                  amount: 50.0,
                }),
              },
            ],
          },
        },
        recordedLatencyMs: 19,
      })
      .saveToFile(mcpCassettePath);
  });

  afterAll(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("1. Replays the 4-step LLM + MCP stdio refund trajectory deterministically offline", async () => {
    await withChronosTrace({ cassette: llmCassettePath }, async (ctx) => {
      const mcpClient = new McpStdioTestClient({ cassette: mcpCassettePath });
      await mcpClient.start();

      try {
        const result = await runRefundAgentWorkflow("ORD-900", USER_PROMPT, {
          llmBaseUrl: ctx.openAiBaseUrl,
          mcpClient,
        });

        expect(result.status).toBe("REFUND_COMPLETED");
        expect(result.refundId).toBe("REF-2026-881");
        expect(result.llmAttempts).toBe(2);
        expect(result.trajectory).toHaveLength(4);
      } finally {
        await mcpClient.close();
      }
    });
  });

  it("2. Tolerates minor system prompt phrasing edits via Tier-2 SimHash matching", async () => {
    const editedSystemPrompt =
      "You are a helpful financial refund assistant for AcmeShop! Always verify the customer order_id in the database prior to issuing a refund to the customer.";

    await withChronosTrace({ cassette: llmCassettePath }, async (ctx) => {
      const mcpClient = new McpStdioTestClient({ cassette: mcpCassettePath });
      await mcpClient.start();

      try {
        const result = await runRefundAgentWorkflow("ORD-900", USER_PROMPT, {
          llmBaseUrl: ctx.openAiBaseUrl,
          mcpClient,
          systemPrompt: editedSystemPrompt,
        });

        expect(result.status).toBe("REFUND_COMPLETED");
        expect(result.refundId).toBe("REF-2026-881");
      } finally {
        await mcpClient.close();
      }
    });
  });

  it("3. Recovers from Chaos HTTP 429 rate-limit faults using retry backoff", async () => {
    await withChronosTrace(
      { cassette: llmCassettePath, chaosRateLimitProb: 0.65 },
      async (ctx) => {
        const mcpClient = new McpStdioTestClient({ cassette: mcpCassettePath });
        await mcpClient.start();

        try {
          const result = await runRefundAgentWorkflow("ORD-900", USER_PROMPT, {
            llmBaseUrl: ctx.openAiBaseUrl,
            mcpClient,
            maxLlmRetries: 5,
            retryDelayMs: 5,
          });

          expect(result.status).toBe("REFUND_COMPLETED");
          expect(result.llmAttempts).toBeGreaterThan(2);
          expect(result.refundId).toBe("REF-2026-881");
        } finally {
          await mcpClient.close();
        }
      }
    );
  });

  it("4. Trips the MCP circuit breaker cleanly when Chaos injects an MCP 503 tool outage", async () => {
    await withChronosTrace({ cassette: llmCassettePath }, async (ctx) => {
      const faultyMcpClient = new McpStdioTestClient({
        cassette: mcpCassettePath,
        chaosServiceOutageProb: 1.0,
      });
      await faultyMcpClient.start();

      try {
        const result = await runRefundAgentWorkflow("ORD-900", USER_PROMPT, {
          llmBaseUrl: ctx.openAiBaseUrl,
          mcpClient: faultyMcpClient,
        });

        expect(result.status).toBe("ESCALATED_TO_HUMAN_TICKET");
        expect(result.refundId).toBeNull();
        expect(result.trajectory.at(-1)?.step).toBe("mcp_verify_order_fault");
      } finally {
        await faultyMcpClient.close();
      }
    });
  });

  it("5. Detects breaking tool argument mutations ($50 -> $500) via diffCassettes", () => {
    const mutatedCassettePath = path.join(tempDir, "refund_mcp_mutated.ctrace");

    new CassetteBuilder("refund_mcp_mutated", 0.85)
      .addTurn({
        kind: "McpToolCall",
        requestPayload: {
          jsonrpc: "2.0",
          method: "tools/call",
          params: {
            name: "verify_order",
            arguments: { order_id: "ORD-900" },
          },
        },
        responsePayload: {
          result: {
            content: [{ type: "text", text: JSON.stringify({ valid: true, refundable_amount: 50.0 }) }],
          },
        },
      })
      .addTurn({
        kind: "McpToolCall",
        requestPayload: {
          jsonrpc: "2.0",
          method: "tools/call",
          params: {
            name: "issue_refund",
            arguments: { order_id: "ORD-900", amount: 500.0 },
          },
        },
        responsePayload: {
          result: {
            content: [
              {
                type: "text",
                text: JSON.stringify({
                  refund_id: "REF-2026-881",
                  status: "processed",
                  amount: 500.0,
                }),
              },
            ],
          },
        },
      })
      .saveToFile(mutatedCassettePath);

    const { compatible, report } = diffCassettes(mcpCassettePath, mutatedCassettePath);

    expect(compatible).toBe(false);
    expect(report.mutated_args_count).toBe(1);
    expect(report.steps[1].kind).toBe("ToolArgumentMutated");
    expect(report.steps[1].argument_deltas[0].path).toBe("request.params.arguments.amount");
    expect(report.steps[1].argument_deltas[0].baseline_value).toBe(50.0);
    expect(report.steps[1].argument_deltas[0].candidate_value).toBe(500.0);
  });
});
