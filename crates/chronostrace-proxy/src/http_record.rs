//! HTTP record mode: forward requests to a live upstream, store the
//! scrubbed request/response as a cassette node, and return the upstream
//! response to the client verbatim.
//!
//! Two upstream response shapes are supported:
//!
//! * JSON (`application/json`): the body is stored as the node's response
//!   payload, exactly like a replayed node.
//! * SSE (`text/event-stream`): the event `data:` payloads are stored in
//!   a marker object under the `_chronostrace_sse` key of the response
//!   payload, and replay streams them back as `text/event-stream`.
//!
//! Anything else (including non-2xx statuses) is passed through to the
//! client without recording a node.

use serde_json::{json, Value};

/// Marker key under which SSE chunks are stored in a node's response
/// payload. A payload is an SSE recording if and only if it is an object
/// containing this key with a `chunks` array of strings.
pub const SSE_MARKER_KEY: &str = "_chronostrace_sse";

/// Build the response payload that represents a recorded SSE stream.
pub fn sse_marker_payload(chunks: Vec<String>) -> Value {
    json!({
        SSE_MARKER_KEY: {
            "version": 1,
            "chunks": chunks,
        }
    })
}

/// Extract SSE chunks from a recorded response payload, or `None` when
/// the payload is a plain JSON response.
pub fn sse_chunks_from_payload(payload: &Value) -> Option<Vec<String>> {
    let chunks = payload.get(SSE_MARKER_KEY)?.get("chunks")?.as_array()?;
    let mut out = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        out.push(chunk.as_str()?.to_string());
    }
    Some(out)
}

/// Parse a raw SSE body into its event `data:` payloads, in order.
/// Handles CRLF line endings, ignores comments (`:` lines) and
/// non-data fields (`event:`, `id:`, `retry:`), and joins multi-line
/// `data:` fields with `\n`. The `[DONE]` terminator is kept as a
/// regular data payload.
pub fn parse_sse_body(body: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(body).replace("\r\n", "\n");
    let mut chunks = Vec::new();
    for block in text.split("\n\n") {
        let mut data_lines: Vec<&str> = Vec::new();
        for line in block.lines() {
            if let Some(rest) = line.strip_prefix("data:") {
                data_lines.push(rest.strip_prefix(' ').unwrap_or(rest));
            }
        }
        if !data_lines.is_empty() {
            chunks.push(data_lines.join("\n"));
        }
    }
    chunks
}

/// Upstream forwarding result: HTTP status, content type, raw body.
pub struct UpstreamResponse {
    pub status: u16,
    pub content_type: String,
    pub body: Vec<u8>,
}

/// POST a JSON body to `{base_url}{path}` and return the raw upstream
/// response. Only `Authorization` and `Content-Type` are forwarded;
/// nothing else from the client request (cookies, tracing headers)
/// reaches the upstream, and no header is ever stored in a cassette.
pub async fn forward_to_upstream(
    client: &reqwest::Client,
    base_url: &str,
    path: &str,
    body: &Value,
    auth: Option<&str>,
) -> Result<UpstreamResponse, String> {
    let url = format!("{base_url}{path}");
    let mut req = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(body);
    if let Some(token) = auth {
        req = req.header("Authorization", token);
    }
    let resp = req.send().await.map_err(|e| e.to_string())?;
    let status = resp.status().as_u16();
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let body = resp.bytes().await.map_err(|e| e.to_string())?.to_vec();
    Ok(UpstreamResponse {
        status,
        content_type,
        body,
    })
}

/// Scrub one SSE data payload for storage: JSON payloads go through the
/// JSON scrubber (so a secret inside a streamed tool call is redacted
/// before it is written to disk); non-JSON payloads such as `[DONE]`
/// pass through unchanged.
pub fn scrub_sse_chunk(scrubber: &chronostrace_scrub::SecretScrubber, chunk: &str) -> String {
    match serde_json::from_str::<Value>(chunk) {
        Ok(value) => {
            let scrubbed = scrubber.scrub_json(&value);
            serde_json::to_string(&scrubbed).unwrap_or_else(|_| chunk.to_string())
        }
        Err(_) => chunk.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_openai_style_sse_body() {
        let body = b"data: {\"delta\":\"hi\"}\n\ndata: {\"delta\":\"there\"}\n\ndata: [DONE]\n\n";
        assert_eq!(
            parse_sse_body(body),
            vec!["{\"delta\":\"hi\"}", "{\"delta\":\"there\"}", "[DONE]"]
        );
    }

    #[test]
    fn parses_crlf_comments_and_multiline_data() {
        let body = b": keep-alive comment\r\n\r\ndata: line one\r\ndata: line two\r\n\r\nevent: ignored\r\ndata: after\r\n\r\n";
        assert_eq!(parse_sse_body(body), vec!["line one\nline two", "after"]);
    }

    #[test]
    fn parses_empty_and_non_event_bodies_to_no_chunks() {
        assert!(parse_sse_body(b"").is_empty());
        assert!(parse_sse_body(b": only a comment\n\n").is_empty());
    }

    #[test]
    fn sse_marker_round_trips_and_plain_json_is_not_sse() {
        let chunks = vec!["a".to_string(), "[DONE]".to_string()];
        let payload = sse_marker_payload(chunks.clone());
        assert_eq!(sse_chunks_from_payload(&payload), Some(chunks));
        assert_eq!(sse_chunks_from_payload(&json!({"ok": true})), None);
        assert_eq!(
            sse_chunks_from_payload(&json!({ SSE_MARKER_KEY: { "chunks": "not-an-array" } })),
            None
        );
    }

    #[test]
    fn scrub_sse_chunk_redacts_json_secrets_and_keeps_done() {
        let scrubber = chronostrace_scrub::SecretScrubber::new();
        let scrubbed = scrub_sse_chunk(
            &scrubber,
            "{\"token\": \"sk-proj-abcdefghijklmnopqrstuvwx\"}",
        );
        assert!(!scrubbed.contains("sk-proj-"));
        assert_eq!(scrub_sse_chunk(&scrubber, "[DONE]"), "[DONE]");
    }
}
