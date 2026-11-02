# ARCHITECTURE_REVIEW.md — Remediation Audit Cycle (base `a05d571`)

## System shape (as found, not as wished)
- **Workspace crates**: `chronostrace-core` (cassette, DAG, matching, SimHash, diff), `chronostrace-proxy` (Axum HTTP replay/record + MCP stdio wrapper), `chronostrace-chaos` (fault profiles), `chronostrace-scrub` (secret redaction), `chronostrace-cli` (replay/mcp-wrap/diff/verify).
- **Cassette format**: YAML, schema `ctrace-v1` ("1.0"). Nodes carry `exact_request_hash`, `structural_tool_hash`, `safety_critical_hash`, `prompt_simhash`, request/response payloads, and (since hardening) HTTP status + safe response headers. Schema version unchanged by this cycle.
- **Matching (3-tier, core)**: exact request hash → MCP structural fallback → fuzzy SimHash for non-MCP kinds. The safety-critical hash gates tiers 2 and 3. This cycle's core fix (CT-CORE-01) closed the one tier that did not consult it.
- **Proxy concurrency**: one global state mutex for short critical sections + one async mutex per session held for the whole interaction (including SSE pump lifetime). Same-session requests serialize by design; sessions are independent. Session state is now capped (1,024 sessions, 256-char keys) with 429 admission.
- **Chaos**: sequential xorshift engine (`apply`) plus, new this cycle, a hash-derived deterministic decision API (`apply_deterministic`) with audit events. Both exist; only the new path is order-independent.
- **Scrubbing**: pattern + JWT + Shannon-entropy detector with custom patterns. New this cycle: a fail-closed persistence API returning a proof (policy version + SHA-256 of stored bytes). The record path still scrubs inline; the proof API is not yet the save path.
- **Diff**: graph-aware alignment (kind + target + parent counterpart, matched-set, order-based for duplicates). New this cycle: occurrence indices in output and a severity → CI exit-code mapping.

## Architectural judgments
1. **The safety hash is the load-bearing wall.** Almost every matching guarantee routes through it, and it is near-exact (critical fields + all strings + all numbers). That makes the MCP fix strong but also means the "structural fallback" is now very narrow (booleans/nulls/key renames only). If a wider structural fallback is ever wanted, it must be designed as a new, explicitly named mode — not by loosening the safety hash.
2. **Fail-closed beats fail-open, but only where wired.** The scrub proof and the session cap are both fail-closed at their enforcement points. The scrub proof's enforcement point is not yet the save path, so its value today is as an available API, not a guarantee. Do not describe it as a guarantee until wired.
3. **No TTL eviction for sessions is the right call for now.** Expiring a session mid-chain corrupts replays silently; an explicit 429 for new sessions past the cap is observable and debuggable. If the cap ever binds in production, raise the constant with data, not silently.
4. **Two implementations of diff (Rust + TypeScript) will drift.** The TS SDK reimplements canonicalization, SimHash, and diff. This cycle fixed Rust only; TS parity (occurrence indices, severity) is open work (R-16).
5. **CI covers behavior, not supply chain or toolchain floor.** Fmt/clippy/schema/multi-OS tests/SDK E2E all exist. Advisory scanning and an MSRV job do not. Those are the cheapest high-value additions available to this repo.

## What did not change (deliberately)
- Cassette schema version, wire protocols, CLI flags, SDK public APIs, and the release workflow. No breaking API change was introduced; the one behavior change is MCP replay matching (see `MIGRATION_GUIDE.md`).
