//! HTTP record mode: forward requests to a live upstream, store the
//! scrubbed request/response as a cassette node, and return the upstream
//! response to the client verbatim.
//!
//! Two upstream response shapes are supported:
//!
//! * JSON (`application/json`): the body is stored as the node's response
//!   payload, exactly like a replayed node.
//! * SSE (`text/event-stream`): events are stored structurally (see
//!   [`SseEvent`]) under the `_chronostrace_sse` marker, and replay
//!   re-serializes them with their fields. Version-1 recordings (bare
//!   `chunks` arrays) keep replaying as data-only events.

use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// Marker key under which SSE recordings are stored in a node's response
/// payload.
pub const SSE_MARKER_KEY: &str = "_chronostrace_sse";

/// Headers that may be forwarded to an upstream provider, matched
/// case-insensitively against incoming header names. Anything else —
/// cookies, tracing headers, custom secrets — is dropped, and no header
/// is ever stored in a cassette. `Content-Type` is deliberately absent:
/// the proxy parses the body as JSON and always re-serializes it, so
/// the outgoing content type is truthfully `application/json`.
pub const FORWARDABLE_HEADERS: &[&str] = &[
    "authorization",
    "x-api-key",
    "anthropic-version",
    "anthropic-beta",
];

/// Response headers that are safe to store in a cassette and replay.
/// Everything else — `set-cookie`, `www-authenticate`, `authorization`,
/// rate-limit internals — is dropped, because a cassette must never
/// become a credential store or a fingerprinting oracle.
pub const SAFE_RESPONSE_HEADERS: &[&str] = &["content-type", "retry-after", "x-request-id"];

/// Collect the safe subset of upstream response headers, keyed by
/// lowercase name. Values that are not valid UTF-8 are skipped.
pub fn collect_safe_response_headers(headers: &HeaderMap) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for name in SAFE_RESPONSE_HEADERS {
        if let Some(value) = headers.get(*name).and_then(|v| v.to_str().ok()) {
            out.insert(name.to_string(), value.to_string());
        }
    }
    out
}
/// Collect the forwardable subset of incoming headers, preserving all
/// values for multi-value headers (e.g. repeated `anthropic-beta`).
/// Values that are not valid UTF-8 are skipped, never forwarded raw.
pub fn collect_forwardable_headers(headers: &HeaderMap) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for name in FORWARDABLE_HEADERS {
        for value in headers.get_all(*name).iter() {
            if let Ok(v) = value.to_str() {
                out.push((name.to_string(), v.to_string()));
            }
        }
    }
    out
}

/// One SSE event, with the protocol fields that give it meaning. `data`
/// holds the event's data lines joined with `\n` (the wire form is one
/// `data:` line per line).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SseEvent {
    pub data: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<u64>,
}

/// Incremental SSE parser. Bytes arrive in arbitrary chunks (TCP
/// boundaries do not respect event boundaries), so complete events are
/// emitted only when their terminating blank line has been seen.
/// Line endings: LF and CRLF are normalized; a trailing CR is held back
/// because it may be the first half of a CRLF split across chunks.
/// Per the SSE specification: `:` lines are comments, unknown fields
/// are ignored, a line without a colon is a field name with an empty
/// value, `id` values containing NUL are ignored, and non-numeric
/// `retry` values are ignored. An event is emitted only when it has at
/// least one `data:` line.
#[derive(Default)]
pub struct SseParser {
    buf: String,
    cur_data: Vec<String>,
    cur_event: Option<String>,
    cur_id: Option<String>,
    cur_retry: Option<u64>,
}

impl SseParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed bytes; returns the events completed by this chunk.
    pub fn feed(&mut self, bytes: &[u8]) -> Vec<SseEvent> {
        self.buf.push_str(&String::from_utf8_lossy(bytes));
        // Normalize CRLF pairs, but leave a trailing lone CR alone: it
        // may pair with a LF at the start of the next chunk.
        self.buf = self.buf.replace("\r\n", "\n");
        let mut events = Vec::new();
        while let Some(pos) = self.buf.find("\n\n") {
            let block: String = self.buf.drain(..pos + 2).collect();
            if let Some(event) = self.parse_block(&block) {
                events.push(event);
            }
        }
        events
    }

    /// Flush a pending unterminated event at clean stream end, if it
    /// has data. This deliberately deviates from the browser rule
    /// (which drops unterminated events): a recorder that drops the
    /// tail of a cleanly finished stream would lose data, and replay
    /// must reproduce what the upstream sent.
    pub fn finish(mut self) -> Option<SseEvent> {
        let mut tail = std::mem::take(&mut self.buf);
        tail = tail.replace("\r\n", "\n").replace('\r', "\n");
        if tail.trim().is_empty() {
            return None;
        }
        self.parse_block(&tail)
    }

    fn parse_block(&mut self, block: &str) -> Option<SseEvent> {
        for line in block.split('\n') {
            let line = line.trim_end_matches('\r');
            if line.is_empty() {
                continue;
            }
            if line.starts_with(':') {
                continue;
            }
            let (field, value) = match line.split_once(':') {
                Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
                None => (line, ""),
            };
            match field {
                "data" => self.cur_data.push(value.to_string()),
                "event" => self.cur_event = Some(value.to_string()),
                "id" => {
                    if !value.contains('\0') {
                        self.cur_id = Some(value.to_string());
                    }
                }
                "retry" if !value.is_empty() && value.chars().all(|c| c.is_ascii_digit()) => {
                    if let Ok(n) = value.parse::<u64>() {
                        self.cur_retry = Some(n);
                    }
                }
                _ => {}
            }
        }
        if self.cur_data.is_empty() {
            // No data lines: per spec this block dispatches nothing and
            // the event type buffer resets, while the last event id and
            // the retry value persist to later events. (Stored events
            // keep only the id/retry sent with their own block; replay
            // re-emits exactly what the upstream sent per event.)
            self.cur_event = None;
            return None;
        }
        let event = SseEvent {
            data: self.cur_data.join("\n"),
            event: self.cur_event.take(),
            id: self.cur_id.take(),
            retry: self.cur_retry.take(),
        };
        self.cur_data.clear();
        Some(event)
    }
}

/// Serialize stored events back to SSE wire text, in canonical field
/// order (`event:`, `id:`, `retry:`, then one `data:` line per data
/// line), terminated by a blank line per event.
pub fn serialize_sse_events(events: &[SseEvent]) -> String {
    let mut out = String::new();
    for event in events {
        if let Some(name) = &event.event {
            out.push_str(&format!("event: {name}\n"));
        }
        if let Some(id) = &event.id {
            out.push_str(&format!("id: {id}\n"));
        }
        if let Some(retry) = &event.retry {
            out.push_str(&format!("retry: {retry}\n"));
        }
        for line in event.data.split('\n') {
            out.push_str(&format!("data: {line}\n"));
        }
        out.push('\n');
    }
    out
}

/// Build the version-2 response payload for a recorded SSE stream.
pub fn sse_marker_payload_v2(events: Vec<SseEvent>) -> Value {
    json!({
        SSE_MARKER_KEY: {
            "version": 2,
            "events": events,
        }
    })
}

/// Extract version-2 SSE events from a recorded response payload.
pub fn sse_events_from_payload(payload: &Value) -> Option<Vec<SseEvent>> {
    let marker = payload.get(SSE_MARKER_KEY)?;
    if marker.get("version")?.as_u64()? != 2 {
        return None;
    }
    let events = marker.get("events")?.as_array()?;
    let mut out = Vec::with_capacity(events.len());
    for event in events {
        out.push(serde_json::from_value::<SseEvent>(event.clone()).ok()?);
    }
    Some(out)
}

/// Build the legacy version-1 response payload (bare data chunks).
/// Kept so old recordings stay readable; new recordings use version 2.
pub fn sse_marker_payload(chunks: Vec<String>) -> Value {
    json!({
        SSE_MARKER_KEY: {
            "version": 1,
            "chunks": chunks,
        }
    })
}

/// Extract version-1 SSE chunks from a recorded response payload.
pub fn sse_chunks_from_payload(payload: &Value) -> Option<Vec<String>> {
    let marker = payload.get(SSE_MARKER_KEY)?;
    if marker.get("version")?.as_u64()? != 1 {
        return None;
    }
    let chunks = marker.get("chunks")?.as_array()?;
    let mut out = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        out.push(chunk.as_str()?.to_string());
    }
    Some(out)
}

/// Upstream forwarding result: HTTP status, content type, raw body.
pub struct UpstreamResponse {
    pub status: u16,
    pub content_type: String,
    pub body: Vec<u8>,
}

/// POST a JSON body to `{base_url}{path}` and return the live upstream
/// response (headers + streaming body) without consuming it. Only the
/// headers in `FORWARDABLE_HEADERS` are sent; the body is forwarded
/// exactly as given (callers decide whether it is the original or a
/// scrubbed copy). The outgoing content type is always
/// `application/json` because the body is re-serialized JSON.
pub async fn send_upstream_request(
    client: &reqwest::Client,
    base_url: &str,
    path: &str,
    body: &Value,
    headers: &[(String, String)],
) -> Result<reqwest::Response, String> {
    let url = format!("{base_url}{path}");
    let mut req = client
        .post(&url)
        .body(serde_json::to_vec(body).map_err(|e| e.to_string())?);
    for (name, value) in headers {
        req = req.header(name.as_str(), value.as_str());
    }
    let resp = req
        .header("Content-Type", "application/json")
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(resp)
}

/// Scrub one SSE data payload for storage: JSON payloads go through the
/// JSON scrubber (so a secret inside a streamed tool call is redacted
/// before it is written to disk); non-JSON payloads such as `[DONE]`
/// pass through unchanged. Event names, ids and retry values are
/// protocol metadata, not content, and are stored as sent.
pub fn scrub_sse_data(scrubber: &chronostrace_scrub::SecretScrubber, data: &str) -> String {
    match serde_json::from_str::<Value>(data) {
        Ok(value) => {
            let scrubbed = scrubber.scrub_json(&value);
            serde_json::to_string(&scrubbed).unwrap_or_else(|_| data.to_string())
        }
        Err(_) => data.to_string(),
    }
}

/// Scrub a recorded event's data payload for storage (see
/// [`scrub_sse_data`]).
pub fn scrub_sse_event(
    scrubber: &chronostrace_scrub::SecretScrubber,
    event: &SseEvent,
) -> SseEvent {
    SseEvent {
        data: scrub_sse_data(scrubber, &event.data),
        event: event.event.clone(),
        id: event.id.clone(),
        retry: event.retry,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_handles_openai_style_stream() {
        let mut parser = SseParser::new();
        let events = parser
            .feed(b"data: {\"delta\":\"hi\"}\n\ndata: {\"delta\":\"there\"}\n\ndata: [DONE]\n\n");
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].data, "{\"delta\":\"hi\"}");
        assert_eq!(events[0].event, None);
        assert_eq!(events[2].data, "[DONE]");
        assert!(parser.finish().is_none());
    }

    #[test]
    fn parser_handles_anthropic_style_events_ids_and_retry() {
        let mut parser = SseParser::new();
        let events = parser.feed(
            b"event: message_start\ndata: {\"type\":\"message_start\"}\n\nid: evt_1\nretry: 3000\nevent: content_block_delta\ndata: {\"type\":\"delta\"}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        );
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].event.as_deref(), Some("message_start"));
        assert_eq!(events[1].event.as_deref(), Some("content_block_delta"));
        assert_eq!(events[1].id.as_deref(), Some("evt_1"));
        assert_eq!(events[1].retry, Some(3000));
        assert_eq!(events[2].event.as_deref(), Some("message_stop"));
    }

    #[test]
    fn parser_handles_crlf_comments_unknown_fields_and_multiline_data() {
        let mut parser = SseParser::new();
        let events = parser.feed(
            b": keep-alive\r\n\r\ndata: line one\r\ndata: line two\r\n\r\nevent: ignored-event\r\n: comment\r\nweird-field: nope\r\ndata: after\r\n\r\n",
        );
        // The first event has no event name; the second block's
        // event/unknown fields precede its data line.
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data, "line one\nline two");
        assert_eq!(events[0].event, None);
        assert_eq!(events[1].event.as_deref(), Some("ignored-event"));
        assert_eq!(events[1].data, "after");
    }

    #[test]
    fn parser_handles_split_chunks_and_malformed_lines() {
        let mut parser = SseParser::new();
        // Split mid-line, mid-boundary, and mid CRLF pair. The CRLF
        // pair completes when the LF of the next chunk arrives, so the
        // first event dispatches on the fourth feed, not the fifth.
        let mut events = Vec::new();
        events.extend(parser.feed(b"data: {\"del"));
        events.extend(parser.feed(b"ta\":\"hi\"}\n"));
        events.extend(parser.feed(b"\r"));
        assert!(events.is_empty());
        events.extend(parser.feed(b"\ndata: second"));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "{\"delta\":\"hi\"}");
        events.extend(parser.feed(b"\n\n"));
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].data, "second");

        // Malformed lines: no colon, bad retry, NUL id — all ignored,
        // the valid data still dispatches.
        let mut parser = SseParser::new();
        let events = parser.feed(b"garbage line\nretry: not-a-number\nid: bad\0id\ndata: ok\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "ok");
        assert_eq!(events[0].retry, None);
        assert_eq!(events[0].id, None);
    }

    #[test]
    fn parser_resets_event_name_on_dataless_blocks_but_keeps_id() {
        let mut parser = SseParser::new();
        let events = parser.feed(b"event: stray\n\nid: evt_keep\ndata: real\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, None);
        assert_eq!(events[0].id.as_deref(), Some("evt_keep"));
        assert_eq!(events[0].data, "real");
    }

    #[test]
    fn parser_finish_flushes_unterminated_event_and_ignores_empty_tail() {
        let mut parser = SseParser::new();
        assert!(parser.feed(b"data: partial").is_empty());
        let flushed = parser
            .finish()
            .expect("unterminated event with data flushes");
        assert_eq!(flushed.data, "partial");

        let mut parser = SseParser::new();
        assert!(parser.feed(b"data: done\n\n").len() == 1);
        assert!(parser.finish().is_none());
        let mut parser = SseParser::new();
        assert!(parser.feed(b": only a comment\n\n").is_empty());
        assert!(parser.finish().is_none());
    }

    #[test]
    fn serialize_round_trips_events_with_fields() {
        let events = vec![
            SseEvent {
                data: "line one\nline two".to_string(),
                event: Some("delta".to_string()),
                id: Some("evt_9".to_string()),
                retry: Some(1500),
            },
            SseEvent {
                data: "[DONE]".to_string(),
                event: None,
                id: None,
                retry: None,
            },
        ];
        let text = serialize_sse_events(&events);
        assert_eq!(
            text,
            "event: delta\nid: evt_9\nretry: 1500\ndata: line one\ndata: line two\n\ndata: [DONE]\n\n"
        );
        let mut parser = SseParser::new();
        let reparsed = parser.feed(text.as_bytes());
        assert_eq!(reparsed, events);
    }

    #[test]
    fn v2_marker_round_trips_and_v1_stays_distinct() {
        let events = vec![SseEvent {
            data: "hi".to_string(),
            event: Some("greet".to_string()),
            id: None,
            retry: None,
        }];
        let payload = sse_marker_payload_v2(events.clone());
        assert_eq!(sse_events_from_payload(&payload), Some(events));
        assert_eq!(sse_chunks_from_payload(&payload), None);

        let legacy = sse_marker_payload(vec!["a".to_string()]);
        assert_eq!(
            sse_chunks_from_payload(&legacy),
            Some(vec!["a".to_string()])
        );
        assert_eq!(sse_events_from_payload(&legacy), None);
        assert_eq!(sse_events_from_payload(&json!({"ok": true})), None);
    }

    #[test]
    fn scrub_sse_data_redacts_json_secrets_and_keeps_done() {
        let scrubber = chronostrace_scrub::SecretScrubber::new();
        let scrubbed = scrub_sse_data(
            &scrubber,
            "{\"token\": \"sk-proj-abcdefghijklmnopqrstuvwx\"}",
        );
        assert!(!scrubbed.contains("sk-proj-"));
        assert_eq!(scrub_sse_data(&scrubber, "[DONE]"), "[DONE]");
    }
}
