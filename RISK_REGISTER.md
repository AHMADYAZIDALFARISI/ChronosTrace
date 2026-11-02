# RISK_REGISTER.md — Remediation Audit Cycle (`remediation/audit-hardening-v0.1`, base `a05d571`)

Living register for risks found in this cycle. "Owner" is the role that must close the item; nothing here is assigned to a person. Risks marked CLOSED were closed by a landed, tested change on this branch. Everything else stays visible until its evidence exists.

| ID | Risk | Severity | Status | Evidence / mitigation | Owner |
|---|---|---|---|---|---|
| R-01 | MCP structural fallback replays responses for changed critical arguments | High | CLOSED | Safety-hash gate in `match_request()` + regression test (CT-CORE-01) | Core |
| R-02 | Novel critical fields in boolean/null form escape the safety gate | High | OPEN | Fixed key list + all strings/numbers collected; booleans/nulls not collected (CT-CORE-02). Needs per-tool critical-path policy. | Core |
| R-03 | Short unkeyed secrets survive scrubbing by design | High | OPEN (by design) | Documented + pinned by test; never rely on scrubbing alone for free-text secrets (CT-SEC-01). | Security |
| R-04 | Scrub proof exists but is not yet wired into the save path | Medium | OPEN | `scrub_json_for_persistence()` landed + tested; proxy still calls `scrub_json` directly (CT-SEC-02). | Proxy |
| R-05 | Legacy chaos `apply()` still order-dependent | Medium | OPEN | Deterministic API landed; legacy path retained for compatibility (CT-CHAOS-01). | Chaos |
| R-06 | Diff performance at scale unquantified | Medium | OPEN | Alignment is O(n·m) worst case; no diff benchmark exists (CT-DIFF-03). | Core |
| R-07 | No explicit request resource limits in proxy code | Medium | OPEN | Session cap landed (CT-RUN-02); body/concurrency limits still implicit only (CT-RUN-04). | Proxy |
| R-08 | Mutex poisoning wedges the proxy into per-request panics | Low | OPEN | Pattern present; blast radius is per-request task, not process death (CT-RUN-05). | Proxy |
| R-09 | Hung MCP child stalls a replay (no request/idle timeout) | Medium | OPEN | Only the 5s shutdown wait timeout exists (CT-MCP-02). | MCP |
| R-10 | SDK behavior drift without shared fixtures | Low | OPEN | Separate harnesses; both test against the same release binary in CI (CT-SDK-02). | SDK |
| R-11 | MSRV 1.78 build never actually run | Medium | OPEN | Only known 1.82+ API removed; 1.78 toolchain unavailable in this VM (CT-BUILD-01). Needs a CI MSRV job. | Build |
| R-12 | Dependency advisories unchecked in CI | Medium | OPEN | No audit/deny step in `ci.yml` (CT-BUILD-02/03). | Build |
| R-13 | v0.1.0 release outcome unverified | Low | OPEN (external) | Release run `41423882765` still `queued` at final check; GitHub Release + PyPI/npm outcomes pending (CT-BUILD-04). CI run `41422717962` for `a05d571` is `completed / success`. | Release |
| R-14 | Concurrent same-target writers last-win on rename | Low | OPEN | Temp-name collision fixed earlier; dir sync added this cycle; rename race remains (CT-CORE-05). Single-writer proxy contract is the mitigation. | Core |
| R-15 | Fuzzy replay matches near-miss prompts within threshold | Medium | OPEN (by design) | Gated by safety hash + threshold; treat fuzzy matches as reconciled drift, not exact evidence (CT-CORE-03). | Core |
| R-16 | TS SDK diff lacks occurrence indices / severity mapping | Low | OPEN | Rust core fixed (CT-DIFF-01/02); TS `diffCassettes` is a separate implementation and was not changed. | SDK |

## Risks explicitly NOT carried (closed at HEAD, evidence in `VERIFIED_AUDIT_FINDINGS.md`)
Per-session serialization (CT-RUN-01), per-session ordering (CT-RUN-03), MCP lifecycle crashes (CT-MCP-01), SDK env restore (CT-SDK-01), fault-profile coverage (CT-CHAOS-02), release mechanism (CT-BUILD-04 mechanism only — its outcome is R-13).
