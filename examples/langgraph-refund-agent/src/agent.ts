import { McpStdioTestClient } from "../../../sdks/typescript/src/index.js";

export interface RefundAgentConfig {
  llmBaseUrl: string;
  mcpClient: McpStdioTestClient;
  systemPrompt?: string;
  maxLlmRetries?: number;
  retryDelayMs?: number;
}

export interface AgentTurnArtifact {
  step: string;
  detail: Record<string, unknown>;
}

export interface RefundWorkflowResult {
  status: "REFUND_COMPLETED" | "ESCALATED_TO_HUMAN_TICKET";
  orderId: string;
  refundId: string | null;
  finalMessage: string;
  llmAttempts: number;





    trajectory: AgentTurnArtifact[];
}

const DEFAULT_SYSTEM_PROMPT =
  "You are a helpful financial refund agent for AcmeShop. Always verify the customer order_id in the database before

const AGENT_TOOLS = [
  {
     type: "function",
     function: {
        name: "verify_order",
        params: { order_id: "string" },
     },
  },
  {
     type: "function",
     function: {
        name: "issue_refund",
        params: { order_id: "string", amount: "number" },
     },
  },
];

/**
 * Executes the multi-turn Refund Agent state machine against an OpenAI-compatible
 * endpoint and an MCP stdio tool server.
 */
export async function runRefundAgentWorkflow(
  orderId: string,
  userMessage: string,
  config: RefundAgentConfig
): Promise<RefundWorkflowResult> {
  const systemPrompt = config.systemPrompt ?? DEFAULT_SYSTEM_PROMPT;
  const maxRetries = config.maxLlmRetries ?? 3;
  const retryDelayMs = config.retryDelayMs ?? 15;
  const trajectory: AgentTurnArtifact[] = [];
  let llmAttempts = 0;

    // Helper: Calls LLM completion endpoint with retry backoff on HTTP 429 rate-limit faults
    async function callLlmWithBackoff(messages: Array<{ role: string; content: string }>) {
      for (let attempt = 1; attempt <= maxRetries; attempt++) {
        llmAttempts++;
        const response = await fetch(`${config.llmBaseUrl}/chat/completions`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            model: "gpt-4o",
            messages,
            tools: AGENT_TOOLS,
          }),
        });

          if (response.status === 429) {
            if (attempt === maxRetries) {
              throw new Error(`LLM Rate Limit Exhausted after ${maxRetries} attempts`);
            }
            await new Promise((r) => setTimeout(r, retryDelayMs * attempt));
            continue;
          }

          if (!response.ok) {
            const errBody = await response.text();
            throw new Error(`LLM HTTP ${response.status}: ${errBody}`);
          }

          return (await response.json()) as {
             choices: Array<{
               message: {
                  content?: string | null;
                  tool_calls?: Array<{
                    function: { name: string; arguments: Record<string, unknown> };
                  }>;
               };
             }>;
          };
        }
        throw new Error("Unreachable retry state");
    }






   // -------------------------------------------------------------------------
   // Turn 1 (LLM Node): Plan initial verification tool call
   // -------------------------------------------------------------------------
   const turn1 = await callLlmWithBackoff([
     { role: "system", content: systemPrompt },
     { role: "user", content: userMessage },
   ]);

   const plannedTool = turn1.choices[0]?.message?.tool_calls?.[0]?.function;
   if (!plannedTool || plannedTool.name !== "verify_order") {
     throw new Error(`Expected agent to call verify_order first, got: ${JSON.stringify(plannedTool)}`);
   }
   trajectory.push({ step: "llm_plan_verify", detail: plannedTool });

   // -------------------------------------------------------------------------
   // Turn 2 (MCP Stdio Node): Execute `verify_order` over MCP JSON-RPC 2.0
   // -------------------------------------------------------------------------
   const verifyRpc = await config.mcpClient.callTool<{
     isError?: boolean;
     content: Array<{ type: string; text: string }>;
   }>("verify_order", { order_id: orderId });

   if (verifyRpc.error || verifyRpc.result?.isError) {
     const faultText =
        verifyRpc.error?.message ?? verifyRpc.result?.content?.[0]?.text ?? "Unknown MCP error";
     trajectory.push({ step: "mcp_verify_order_fault", detail: { faultText } });
     return {
        status: "ESCALATED_TO_HUMAN_TICKET",
        orderId,
        refundId: null,
        finalMessage: `Unable to verify order ${orderId} due to upstream tool outage. Escalated to support queue.`,
        llmAttempts,
        trajectory,
     };
   }

   const orderData = JSON.parse(verifyRpc.result!.content[0].text) as {
      valid: boolean;
      refundable_amount: number;
   };
   trajectory.push({ step: "mcp_verify_order_ok", detail: orderData });

   // -------------------------------------------------------------------------
   // Turn 3 (MCP Stdio Node): Execute `issue_refund` over MCP JSON-RPC 2.0
   // -------------------------------------------------------------------------
   const refundRpc = await config.mcpClient.callTool<{
     isError?: boolean;
     content: Array<{ type: string; text: string }>;
   }>("issue_refund", {
     order_id: orderId,
     amount: orderData.refundable_amount,
   });

   if (refundRpc.error || refundRpc.result?.isError) {
     const faultText =
        refundRpc.error?.message ?? refundRpc.result?.content?.[0]?.text ?? "Unknown MCP error";
     trajectory.push({ step: "mcp_issue_refund_fault", detail: { faultText } });
     return {
        status: "ESCALATED_TO_HUMAN_TICKET",
        orderId,
        refundId: null,
        finalMessage: `Order ${orderId} verified ($${orderData.refundable_amount}), but payment tool failed. Created p
        llmAttempts,
        trajectory,
     };
   }

   const refundConfirmation = JSON.parse(refundRpc.result!.content[0].text) as {
      refund_id: string;
      status: string;
      amount: number;
   };
   trajectory.push({ step: "mcp_issue_refund_ok", detail: refundConfirmation });

   // -------------------------------------------------------------------------
   // Turn 4 (LLM Node): Synthesize customer confirmation message
   // -------------------------------------------------------------------------
   const turn4 = await callLlmWithBackoff([
     { role: "system", content: systemPrompt },





      { role: "user", content: userMessage },
      {
         role: "assistant",
         content: `Verified order ${orderId} and processed refund ${refundConfirmation.refund_id} for $${refundConfirma
      },
    ]);

    const finalMessage = turn4.choices[0]?.message?.content ?? "Refund processed.";
    trajectory.push({ step: "llm_final_synthesis", detail: { finalMessage } });

    return {
       status: "REFUND_COMPLETED",
       orderId,
       refundId: refundConfirmation.refund_id,
       finalMessage,
       llmAttempts,
       trajectory,
    };
}
