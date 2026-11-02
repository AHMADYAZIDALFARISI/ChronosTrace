# EXECUTIVE_SUMMARY.md — Remediation Audit Cycle

## What was asked
Run a 26-finding audit remediation (payload IDs `CT-CORE-01` s/d `CT-BUILD-04`) against ChronosTrace at `a05d571` (tag `v0.1.0`), with real verification, no fabricated test results, no deleted tests, and a safe (non-force) push.

## What was actually done
- Worked on branch `remediation/audit-hardening-v0.1`, cut from `a05d571`. `master` and tag `v0.1.0` were **not** touched.
- Phase 0 baseline at `a05d571`: `cargo fmt --check`, `cargo clippy --workspace --all-targets`, and `cargo test --workspace` all passed (84 tests, 0 failed) before any change.
- Phase 1 verified all 26 findings against code actually read. Result: 9 CONFIRMED, 10 PARTIALLY CONFIRMED, 6 NOT REPRODUCED, 1 NEEDS INVESTIGATION. One honest caveat runs through the whole cycle: the 26 IDs do not exist in any finding register inside this repo (the repo's register is `CT-001`–`CT-016`, already closed), and the original audit document was not opened. Findings are therefore verified against the code property each ID implies, and the document says so wherever the claim text was unavailable, instead of inventing it.
- 6 findings fully fixed with regression tests; 4 partially fixed (API landed, a follow-up remains); the rest are documented as open, by-design, or not reproduced — not claimed closed.

## The one finding that mattered most
**CT-CORE-01 (High, CONFIRMED, now CLOSED).** The MCP replay fallback returned an earlier response for any arguments sharing a tool schema — argument identity was never checked on that path. A replay with a changed critical argument could receive the wrong response. The fix gates the fallback on the safety-critical hash, with a regression test proving an `amount` 50 → 500 change now misses while a non-critical boolean change still matches structurally.

## What is NOT done (read before merging)
- The safety gate still misses boolean/null-typed novel critical fields (CT-CORE-02, High, OPEN).
- The scrub-proof API exists but is not yet wired into the cassette save path (CT-SEC-02, OPEN).
- No dependency advisory scanning and no MSRV job in CI (CT-BUILD-01/02, OPEN); the MSRV fix itself was not verified with a real 1.78 toolchain.
- The v0.1.0 release workflow run (`41423882765`) was still `queued` at final check — GitHub Release and PyPI/npm outcomes are unverified. This branch does not change that.
- This branch changes MCP replay behavior: replays that previously matched structurally with different critical arguments now miss (see `MIGRATION_GUIDE.md`). That is the intended fix, but it is a behavior change.

## Verification for this branch
`cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace` all pass: **91 tests, 0 failed** (84 baseline + 7 new regression tests). SDK suites (TypeScript, Python) were not re-run because no SDK files changed. Full evidence in `TEST_EVIDENCE.md`.

## Readiness
Ready for review as a branch. **Not** ready to merge to `master` as a release without the owner's decision on the behavior change above and on the open High residual (CT-CORE-02). No release, tag, or publish step was performed or is implied by this branch.
