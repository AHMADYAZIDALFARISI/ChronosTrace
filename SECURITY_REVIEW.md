# SECURITY_REVIEW.md — Remediation Audit Cycle

## Threat model (unchanged from `SECURITY_THREAT_MODEL.md`, restated briefly)
The proxy replays recorded AI-agent traffic. The assets are: recorded payloads (which may contain secrets if upstream systems leak them into traffic), replay integrity (a wrong replayed response can cause a wrong real-world action), and the proxy process itself (long-lived, network-facing on localhost/test networks). Attackers in scope: a client that can send crafted replay requests, and a malicious or corrupt cassette. Out of scope: the upstream model provider itself, and the operator's own machine.

## What this cycle improves
- **Replay integrity (fixed):** CT-CORE-01 closed the MCP structural-fallback bypass. A replay request can no longer receive a response recorded for different critical arguments.
- **Availability (fixed):** CT-RUN-02 caps session state (1,024 sessions, 256-char keys, 429 admission). The unbounded-header memory vector is closed at the admission point, before allocation.
- **Durability (partial):** CT-CORE-05 adds directory sync after rename, closing the crash window for the rename itself.
- **Auditability (API):** CT-CHAOS-01/03 give chaos decisions a stable identity and an audit event. Events are returned, not yet persisted — see residual.
- **Bounds (fixed):** CT-CORE-04 bounds DAG validation (100k nodes / depth 2,048) on both load and save paths.

## What remains exposed (do not understate)
1. **Boolean/null critical fields (High, CT-CORE-02).** The safety gate does not see them. A tool whose critical semantics live in a boolean (approve, enabled, admin) is not protected by the gate today.
2. **Unkeyed short secrets (High by severity, by-design limit, CT-SEC-01).** Scrubbing cannot catch them without false positives. Defense must be upstream (never put secrets in free-text fields), not in this crate.
3. **Scrub proof not wired (Medium, CT-SEC-02).** The proof API exists; the save path does not use it yet. Until wired, a scrubber regression could still persist silently.
4. **No advisory scanning (Medium, CT-BUILD-02).** Dependencies are unchecked in CI.
5. **Mutex poisoning (Low, CT-RUN-05).** A panic while holding the state lock wedges the proxy into per-request panics until restart.
6. **Hung MCP child (Medium, CT-MCP-02).** No request/idle timeout in the stdio wrapper.
7. **Fuzzy replay semantics (Medium by design, CT-CORE-03).** Near-miss prompts within threshold match as reconciled drift. Operators should not treat fuzzy matches as exact evidence.

## Secrets handling in this cycle
- No real secrets were introduced. Test fixtures use obviously fake values (`FAKE`-repeated strings); the final canary check (`git grep -I "sk-live-"`, excluding tests) is run at commit time per the payload's step 5.
- No credentials, tokens, or keys appear in any deliverable in this branch. The HMAC/session-token design from the hardening cycle is untouched.
- The logo asset rule and the `AUDIT_BASELINE.md` read-only rule were respected: neither was modified.

## Supply chain note
No new crates were introduced. The only dependency changes are crate-level additions of `serde`/`sha2` (scrub) and `sha2` (chaos), all already workspace dependencies at pinned versions. `Cargo.lock` changed only by the two new dependency edges; no crate versions changed.
