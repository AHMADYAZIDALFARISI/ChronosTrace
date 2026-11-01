# AUDIT_BASELINE.md — ChronosTrace Phase 0

**Status: baseline only. No source file was modified in this phase.**
The only file added by Phase 0 is this document itself. All other working-tree
changes listed below predate Phase 0 (approved fixes from the 2026-11-01
evening session, still uncommitted). Nothing was pushed, published, or
released in this phase, per the recovery prompt rule 12.

## 1. Baseline identity

| Item | Value |
|---|---|
| Repository | https://github.com/AHMADYAZIDALFARISI/project-genesis |
| Remote HEAD (verified via `git ls-remote origin HEAD`, 2026-11-01 ~21:45 WIB) | `439fc973d86aab479a72617ebfe9cbb59a0d1838` |
| Local HEAD | `439fc97` (identical to remote HEAD) |
| HEAD commit date | 2026-11-01 17:04:57 +0700 |
| HEAD commit subject | `repair: restore truncated lines and layout-indentation from original sources` |
| Default branch | `master` (local branch tracks `origin/master`) |
| Tags | none locally; no release has been cut through the release workflow |
| Baseline date | 2026-11-01 evening WIB |

Note on fetch reliability: one `git fetch origin` attempt during this phase
failed transiently (`kex_exchange_identification: read: Connection reset by
peer` via the egress proxy). A retry with `git ls-remote origin HEAD`
succeeded and confirmed remote HEAD = local HEAD, so the baseline commit is
current. No fetch failure is hidden behind this note.

### Toolchain used for this baseline

| Tool | Version |
|---|---|
| rustc / cargo | 1.99.1 (installed 2026-11-01 via rustup; `~/.cargo/bin`) |
| rustfmt | 1.10.0-stable |
| clippy | 0.1.99 |
| python3 | 3.12.3 (pytest **not** installed in this environment) |
| node | v24.22.1 (npm registry reachable through the VM proxy; 87 packages installed in 33 s for the TS check copy) |

Workspace declares `rust-version = "1.78"` (MSRV claim). MSRV compliance
was **not** verified — no 1.78 toolchain was available. All Rust results
below are on 1.99.1.

### Working tree vs HEAD (state at baseline time)

```
 M crates/chronostrace-core/src/diff.rs
 M crates/chronostrace-core/src/lib.rs
 M crates/chronostrace-scrub/src/lib.rs
?? Cargo.lock
?? phase0_dump.txt
?? target/
```

The three modified files are the 2026-11-01 evening fixes (diff.rs
`},` restoration, lib.rs import/`allow`/stub cleanup, scrub dead-field
removal). They are **uncommitted** and are **not** part of HEAD.
`Cargo.lock`, `phase0_dump.txt`, and `target/` are local build/dump
byproducts. There is **no `.gitignore`** in the repository, so a clean
checkout that builds will show the same pollution.

Because of this split, baseline commands were run **twice**: once on a
clean detached worktree at HEAD (`/tmp/ct-baseline`, removed after use),
and once on the working tree. Both sets of numbers are reported below.
Where they differ, the finding is marked accordingly.

## 2. Repository map (27 tracked files, `git ls-files`)

```
.github/workflows/ci.yml, release.yml
Cargo.toml, LICENSE, README.md
crates/chronostrace-core/{Cargo.toml, src/lib.rs, src/simhash.rs, src/diff.rs}
crates/chronostrace-scrub/{Cargo.toml, src/lib.rs}
crates/chronostrace-chaos/{Cargo.toml, src/lib.rs}
crates/chronostrace-proxy/{Cargo.toml, src/lib.rs, src/mcp_stdio.rs}
crates/chronostrace-cli/{Cargo.toml, src/main.rs}
schemas/ctrace-v1.schema.json
sdks/python/pyproject.toml, sdks/python/pytest_chronostrace/plugin.py
sdks/typescript/{package.json, tsconfig.json, src/index.ts, src/bin-path.ts,
  tests/chronostrace.e2e.test.ts}
examples/langgraph-refund-agent/src/agent.ts
```

Not present (all checked): `CONTRIBUTING.md`, `SECURITY.md`,
`CODE_OF_CONDUCT.md`, `CHANGELOG.md`, issue/PR templates, `.gitignore`,
any `*.ctrace` fixture, any Python test file, any Rust `#[test]`.

### Workspace and packages

- Cargo workspace (resolver 2, edition 2021, version `0.1.0` for all
  members): `chronostrace-core`, `chronostrace-scrub`,
  `chronostrace-chaos`, `chronostrace-proxy`, `chronostrace-cli`.
- Binary targets: exactly one — the `chronostrace-cli` package builds a
  binary named **`chronostrace-cli`** (no `[[bin]]` name override).
  The CLI's clap command name is `chronostrace`, but the produced
  executable file is `chronostrace-cli`. This mismatch drives CT-015.
- Workspace `repository` field: `https://github.com/chronostrace/chronostrace`
  — **wrong organization** (actual repo is `AHMADYAZIDALFARISI/project-genesis`).
- Python package: `pytest-chronostrace` 0.1.0, maturin `bindings = "bin"`,
  manifest `crates/chronostrace-cli/Cargo.toml`, pytest11 entry point
  `chronostrace = "pytest_chronostrace.plugin"`.
- TypeScript package: `@chronostrace/test` 0.1.0, `main`/`module`/`types`
  under `dist/` (built by tsup, never built here), `optionalDependencies`
  on five `@chronostrace/cli-*` 0.1.0 platform packages, **no
  `package-lock.json` tracked** (so CI's `npm ci` cannot run).
- Schema: `schemas/ctrace-v1.schema.json`, draft 2020-12, `$id`
  `https://chronostrace.dev/schemas/ctrace-v1.schema.json`, cassette
  `version` const `"1.0"`.
- Examples: one file, `examples/langgraph-refund-agent/src/agent.ts`
  (no `package.json`/`tsconfig.json` of its own). `examples/anthropic-mcp-filesystem/`
  exists only as an empty directory (tree entry, no code).

## 3. Baseline commands and exit codes

### 3a. Clean worktree at HEAD (`/tmp/ct-baseline`, detached 439fc97)

| Command | Exit | Evidence |
|---|---|---|
| `cargo fmt --all -- --check` | 1 | formatting diffs (e.g. `chronostrace-chaos/src/lib.rs`) |
| `cargo check --workspace --all-targets --all-features` | 101 | `error: this file contains an unclosed delimiter` → `could not compile chronostrace-core (lib)` |
| `cargo test --workspace --all-features` | 101 | same compile error blocks all test targets |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 101 | same compile error |

### 3b. Working tree (with the three uncommitted evening fixes)

| Command | Exit | Evidence |
|---|---|---|
| `cargo fmt --all -- --check` | 1 | formatting diffs only; deliberately not reformatted in this phase |
| `cargo check --workspace --all-targets --all-features` | 0 | full workspace compiles |
| `cargo test --workspace --all-features` | 0 | 9 suites, **0 tests each** — green is vacuous (see §6) |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 0 | after the lib.rs + scrub cleanups |

### 3c. CLI binary probes (built from the working tree, `cargo build --package chronostrace-cli`)

Binary: `target/debug/chronostrace-cli` (24,956,968 bytes).

| Invocation | Exit | Observed behavior |
|---|---|---|
| `--help` | 0 | only `replay` and `verify` subcommands exist |
| `--version` | 2 | `error: unexpected argument '--version' found` (no version flag defined) |
| `replay /nonexistent.ctrace` | 0 | success with no work performed, even for a missing file |
| `verify /nonexistent.ctrace` | 0 | same — success with no work performed |
| `replay --cassette x.ctrace --bind 127.0.0.1:18787` (exact Python/TS SDK invocation) | 2 | `error: unexpected argument '--cassette' found`; `replay` takes a positional `<CASSETTE>` only |
| `diff a.ctrace b.ctrace --format json` | 2 | `error: unrecognized subcommand 'diff'` |
| `mcp-wrap --mode replay --cassette x.ctrace` | 2 | `error: unrecognized subcommand 'mcp-wrap'` |

### 3d. Python SDK

| Check | Exit | Evidence |
|---|---|---|
| `python3 -m py_compile sdks/python/pytest_chronostrace/plugin.py` | 1 | `SyntaxError: unterminated string literal (detected at line 10)` |
| `python3 -m pytest --version` | n/a | `No module named pytest` — pytest is not installed here, so no pytest run was possible in this environment (CI installs it; the plugin syntax error would still break collection there) |
| `python3 -c "import jsonschema"` | 0 | jsonschema is available; used for the §4 CT-013 reproduction |

### 3e. TypeScript SDK (checked on a `/tmp` copy to avoid polluting the repo)

`npm install` succeeded (87 packages, 33 s), so the registry is reachable.

| Check | Exit | Evidence |
|---|---|---|
| `tsc --noEmit` (strict, per `tsconfig.json`) | 2 | 16 errors, **all** in `examples/langgraph-refund-agent/src/agent.ts` and `sdks/typescript/tests/chronostrace.e2e.test.ts`; zero errors in `src/` |
| `vitest run` | 1 | 0 tests run; suite fails at transform: `e2e.test.ts:19:118: ERROR: Unterminated string literal` |

### 3f. JSON Schema validation

- Schema file itself parses as JSON (CI's only schema step is
  `python3 -c "import json; json.load(...)"` — a syntax check, not a
  validation).
- Reproduction with a structurally valid cassette (correct
  `structural_tool_hash`, 16-hex simhash, `scrubbed: true`):
  `jsonschema.validate` → **INVALID**: `'structural_ tool_hash' is a
  required property`. The typo is a live defect, not a cosmetic one.
- Validation "against all cassette fixtures" could not run: **zero
  `*.ctrace` fixtures exist** in the repository.

### 3g. Commands not run, with reasons

- `pytest` suite: no test files exist under `sdks/python/`, and pytest
  is not installed here.
- Full `vitest` suite beyond the transform failure: blocked by the
  parse errors above; even if parsed, all 5 tests spawn CLI commands
  that do not exist (see CT-012).
- `cargo audit`: not run (requires the audit database; CI runs it via
  `rustsec/audit-check`). Not claimed either way.
- Release packaging (`cargo zigbuild`, maturin wheels, npm publish):
  not run — publishing is out of scope for Phase 0 and forbidden
  without explicit user approval.
- MSRV check on Rust 1.78: toolchain unavailable; not claimed.

## 4. Audit finding verification (CT-001 – CT-016)

Statuses: `CONFIRMED` / `NOT REPRODUCED` / `ALREADY FIXED` /
`PARTIALLY CONFIRMED` / `NEEDS INVESTIGATION`. "At HEAD" means the
published commit; "working tree" means HEAD plus the three uncommitted
evening fixes. Reproduction commands are the §3 commands unless noted.

### CT-001 — Core matching function unimplemented (P0): CONFIRMED

- `crates/chronostrace-core/src/lib.rs`, `match_request()` (line ~130):
  body is a comment (`// Placeholder returning matched node wrapper in
  real build`) followed by `unimplemented!()` (line 140, the only
  `unimplemented!()` in the repo; no `todo!()`/`panic!()`/`unsafe`
  blocks anywhere).
- Consequence: any call panics. The README's three-tier DAG matching
  has no implementation behind it. `Cassette::record_node()` exists
  (hashing + node construction), but no lookup/matching path exists.

### CT-002 — Cassette metadata hardcoded / overstated (P0/P1): CONFIRMED

- `crates/chronostrace-core/src/lib.rs` `Cassette::new()`:
  `created_at: "2026-11-01T00:00:00Z".to_string()` (line 75) and
  `scrubbed: true` (line 77) — both unconditional, no clock read, no
  scrubbing call on this path.
- `sdks/typescript/src/index.ts` `CassetteBuilder.toJSON()`:
  `created_at: "2026-11-01T00:00:00Z"` (line 569),
  `scrubbed: true` (line 571), same pattern.
- `schemas/ctrace-v1.schema.json` bakes the overclaim in:
  `"scrubbed": { "type": "boolean", "const": true }`.
- The CI dogfood fixture (`.github/workflows/ci.yml` ~line 201) repeats
  the same hardcoded pair; as test data that is acceptable, but it
  shows the values were never treated as runtime-derived anywhere.

### CT-003 — Cassette writes not crash-safe (P1): CONFIRMED

- `crates/chronostrace-core/src/lib.rs` `save_to_file()`:
  `fs::write(path, yaml)?` (line 95). No temp file, no fsync, no
  atomic rename, no locking. A crash mid-write leaves a truncated
  cassette. No concurrent-write policy exists.

### CT-004 — SimHash must not authorize unsafe semantic replay (P1): PARTIALLY CONFIRMED

- Algorithm exists (`crates/chronostrace-core/src/simhash.rs`):
  3-word shingles, FNV-1a 64-bit, similarity `1 - hamming/64`.
  Normalization is exactly as the audit described: non-alphanumeric →
  space, ASCII-only lowercasing (`to_ascii_lowercase`), so Unicode
  case-folding is absent and Unicode letters are kept as-is.
- The authorization risk itself is **latent, not active**: there is no
  matcher (CT-001), so nothing currently authorizes replay on SimHash.
  `TrajectoryDiffReport` carries `prompt_similarity` as a display
  metric only.
- The configured threshold is never enforced: `similarity_threshold`
  is stored in `CassetteMetadata` and in the schema (0.5–1.0) but no
  code path reads it for a classification decision (`diff.rs` kind
  logic uses hashes only).
- New divergence found in this phase (see N-01): the TypeScript
  "bit-exact" SimHash port is not bit-exact for non-ASCII input, and
  the TS SHA-256 canonicalization differs from Rust's for floats.

### CT-005 — Trajectory diff alignment and classification (P0/P1): CONFIRMED, with one sub-item ALREADY FIXED

- Alignment: `crates/chronostrace-core/src/diff.rs`
  `diff_cassettes()` iterates `for idx in 0..max_len` and pairs
  `baseline.nodes.get(idx)` with `candidate.nodes.get(idx)` — pure
  array-index alignment. Reordered or branched DAGs are miscompared
  by construction. No parent-identity matching exists.
- Classification: `compare_node_pair()` (line 152) classifies
  `structural_tool_hash` change as `ToolArgumentMutated` even when
  the change is a tool schema/identity change; there is no separate
  class for schema, identity, model/provider, or output mutation.
  Compatibility (`is_compatible()`) is computed from counts, not from
  a safety policy, and never inspects `prompt_similarity`.
- Malformed `format!()`: **at HEAD, CONFIRMED** — the `target_name`
  line in `compare_node_pair` lacked the closing `},`, producing the
  §3a check-101 failure (`unclosed delimiter`). **In the working
  tree, ALREADY FIXED** (uncommitted evening fix; walkthrough section
  of the source document contains the equivalent correct
  `display_target` form, confirming intended semantics). The three
  `(None, None) => unreachable!()` arms (diff.rs lines 132, 232, 244)
  are logically sound (loop bounds guarantee one side exists) and are
  not defects.

### CT-006 — Chaos fault injection incomplete (P0): CONFIRMED

- `crates/chronostrace-chaos/src/lib.rs` `ChaosEngine::apply()`:
  only `FaultKind::Http429RateLimit` produces an injected outcome;
  `LatencySpike`, `TruncatedJsonPayload`, `DropSchemaField`, and
  `ServiceUnavailable503` all fall through to
  `ChaosOutcome::PassThrough` — i.e. configuring them silently does
  nothing. The enum variants, `DelayThenPassThrough`, `InjectedError`,
  and `MutatedPayload` outcome shapes exist but are never constructed
  except for 429.
- Further gaps: the 429 body carries no Retry-After metadata
  (`InjectedError` has no headers field at all, so the audit's
  "correct status/body/headers and retry metadata" requirement cannot
  be met by this shape); `probability` is never range-validated
  (values outside [0,1] silently alter behavior); seed 0 silently
  becomes `0xDEAD_BEEF` (documented nowhere in-repo).

### CT-007 — Secret scrubbing does not meet documented claims (P0): CONFIRMED, with one sub-item ALREADY FIXED

- `crates/chronostrace-scrub/src/lib.rs` `scrub_str()` applies only
  three regex replacements (OpenAI-style `sk-…`, GitHub tokens,
  AWS access-key IDs). There is no entropy scanning, no header
  scrubbing, no encoded-secret handling, no PII handling.
  `scrub_json()` recurses objects/arrays and redacts
  `api_key`/`secret`/`password` keys — that part is real but narrow.
- The "configured but unused entropy threshold" sub-claim: **at HEAD,
  CONFIRMED** (field `entropy_threshold`, line 7, initialized to
  `4.35`, never read). **In the working tree, ALREADY FIXED by
  removal** (uncommitted evening fix; verified no reader exists
  anywhere in the workspace). Removal does not add entropy scanning —
  the feature gap itself remains CONFIRMED.
- The three regexes use `.unwrap()` on compile-time literals
  (lines 17–19 at HEAD). Low risk (patterns are constants), recorded
  for completeness; they are the only `.unwrap()` calls in Rust code.
  No `.expect()` calls exist in Rust code.

### CT-008 — HTTP proxy stub-level (P0): CONFIRMED

- `crates/chronostrace-proxy/src/lib.rs` (8 lines total):
  `build_router()` exposes exactly one route,
  `/v1/chat/completions`, with an empty handler
  (`post(|| async {})`). No upstream configuration, no forwarding,
  no capture, no replay, no SSE, no timeouts, no header handling.
  `ProxyState` is an empty struct. The promised LLM interception does
  not exist.

### CT-009 — MCP stdio wrapper stub-level (P0): CONFIRMED

- `crates/chronostrace-proxy/src/mcp_stdio.rs` (13 lines):
  `run_mcp_stdio_wrapper(_config)` returns `Ok(())` immediately,
  ignoring its config (cassette path, threshold, chaos engine). No
  subprocess spawn, no pipe forwarding, no JSON-RPC parsing, no
  request correlation, no stderr handling, no shutdown logic.

### CT-010 — CLI parses but does not dispatch (P0): CONFIRMED

- `crates/chronostrace-cli/src/main.rs`: subcommands are only
  `Replay { cassette }` and `Verify { cassette }`. `main()` does
  `let _cli = Cli::parse();` then returns `ExitCode::SUCCESS`
  unconditionally. Proven by §3c: `replay`/`verify` on a nonexistent
  file exit 0; `diff` and `mcp-wrap` are unrecognized subcommands;
  `--version` does not exist. The README documents `chronostrace
  diff` and `chronostrace mcp-wrap` (README lines 83, 246, 270);
  neither exists in the binary.

### CT-011 — Python SDK lifecycle and CLI contract (P1): CONFIRMED

- `sdks/python/pytest_chronostrace/plugin.py`:
  - Syntax: `SyntaxError: unterminated string literal (detected at
    line 10)` — the plugin cannot even be imported (py_compile exit 1).
  - CLI contract: spawns `chronostrace replay --cassette <path>
    --bind <addr>` plus optional `--chaos-rate-limit-prob`. The Rust
    CLI has no `--cassette`, `--bind`, or `--chaos-rate-limit-prob`
    flags (proven by the §3c probe, exit 2), and the binary on PATH
    would need to be named `chronostrace` while cargo builds
    `chronostrace-cli`.
  - Startup: fixed `time.sleep(0.15)` with no readiness polling and
    no early-exit detection.
  - Lifecycle: `stdout`/`stderr` are `PIPE`d but never drained
    (pipe-buffer deadlock risk on verbose children);
    `finally: proc.terminate(); proc.wait(timeout=3)` raises
    `TimeoutExpired` on a stubborn child and leaves it running.
  - Environment: `monkeypatch.setenv` for `OPENAI_BASE_URL`,
    `ANTHROPIC_BASE_URL`, `CHRONOSTRACE_MCP_PROXY` (lines 32–34).
    Nuance vs the audit: monkeypatch reverts after the test, so the
    mutation is test-scoped, but it is still process-global during
    the test (parallel-test cross-talk risk stands).
  - No test files exist under `sdks/python/`; the CI job's
    `pytest -v` would exit 5 (no tests collected) even if the plugin
    imported.

### CT-012 — TypeScript SDK concurrency and lifecycle (P1): CONFIRMED, with nuances

- `sdks/typescript/src/index.ts`:
  - Global env mutation is **default-on** (`injectEnv !== false`):
    `process.env.OPENAI_BASE_URL` / `ANTHROPIC_BASE_URL` /
    `CHRONOSTRACE_MCP_PROXY` are overwritten and restored in
    `finally` (lines 157–159, 170–172, 187–190). Restoration is real,
    but the mutation is process-global during the callback, so
    parallel tests in one process can cross-contaminate. Client-scoped
    configuration does not exist.
  - Ephemeral-port race: `getEphemeralPort()` binds port 0, closes,
    returns the number, and the child binds later — a textbook
    TOCTOU window. (`waitForPortReady` polling exists and is good;
    it does not close the race.)
  - No request timeout: `McpStdioTestClient.request()` awaits a
    pending promise that is only rejected on child exit. A hung
    server hangs the caller forever. No cancellation API exists.
  - `close()` resolves after a 500 ms timer even if the child is
    still alive (no SIGKILL fallback), unlike `withChronosTrace`
    which does SIGKILL after 1000 ms.
  - `CassetteBuilder.toJSON()` hardcodes `created_at` and
    `scrubbed: true` (lines 569, 571) — same CT-002 pattern.
  - CLI contract: all three entry points invoke commands that do
    not exist — `replay --cassette … --bind …`
    (`withChronosTrace`), `mcp-wrap --cassette … --mode …
    --similarity-threshold …` (`McpStdioTestClient`),
    `diff … --format json` (`diffCassettes`). Proven by §3c.
  - `bin-path.ts` resolves the binary from
    `CHRONOSTRACE_BIN_PATH`, else the `@chronostrace/cli-*`
    optional dependency, else bare `chronostrace` on PATH. The
    platform packages do not exist (see CT-015), so in practice
    only the env var or a PATH install works.
- `sdks/typescript/tests/chronostrace.e2e.test.ts`: 5 tests, all
  dependent on the missing CLI and on unimplemented matching
  (test 2 assumes Tier-2 SimHash matching; test 4 assumes MCP 503
  chaos). None can run today (see §3e).

### CT-013 — Cassette schema typo / validation gaps (P1): CONFIRMED

- `schemas/ctrace-v1.schema.json`, nodes `required` array contains
  `"structural_ tool_hash"` (with a space). The `properties` block
  correctly defines `structural_tool_hash`. Reproduced with
  `jsonschema` (§3f): a structurally valid cassette is reported
  INVALID solely for the missing mistyped property.
- Further gaps: CI's schema step only parses the JSON (no cassette
  is ever validated); zero fixtures exist; schema requires
  `request_payload`/`response_payload` to be `object` while the Rust
  model is `serde_json::Value` (any JSON value); schema `$id` points
  to `chronostrace.dev`, unrelated to this repo; `scrubbed` is
  `const: true`, which forbids honestly-unscrubbed cassettes.

### CT-014 — CI branch mismatch (P1): CONFIRMED

- `.github/workflows/ci.yml` triggers on `push`/`pull_request`
  branches `["main"]` (lines 3–7). Default branch is `master`.
  Push validation therefore does not run on the default branch.
  (`release.yml` triggers on tags + `workflow_dispatch` and is
  branch-agnostic, so it is not affected by this specific mismatch.)

### CT-015 — Release and package metadata inconsistencies (P1): CONFIRMED

- Binary name: cargo builds `target/release/chronostrace-cli`, but
  (a) CI's rust-test job uploads artifact path
  `target/release/chronostrace`, (b) release.yml copies
  `target/${{ matrix.target }}/release/${{ matrix.bin_name }}` with
  `bin_name: chronostrace` / `chronostrace.exe`, (c) downstream CI
  jobs `chmod +x /usr/local/bin/chronostrace` and run
  `chronostrace --version` (a flag that does not exist, §3c).
  Every one of these steps fails on a real run; no rename step
  exists anywhere.
- npm platform packages: `sdks/typescript/package.json`
  `optionalDependencies` reference five `@chronostrace/cli-*`
  0.1.0 packages, and release.yml generates their `package.json`
  files with `repository: chronostrace/chronostrace` (wrong org).
  The packages have never been published (no tags exist), so a
  fresh `npm install -D @chronostrace/test` cannot resolve the
  binary at all.
- `npm ci` in the TS CI job cannot run: no `package-lock.json` is
  tracked (`git ls-files sdks/typescript` lists only `package.json`,
  `tsconfig.json`, sources, tests).
- Python: maturin builds the `chronostrace-cli` binary into the
  wheel, while the plugin executes bare `chronostrace` — same name
  mismatch on the Python side.
- Cargo publishing is absent from release.yml (no `cargo publish`
  step), while the README badges link to crates.io
  `chronostrace-cli`, PyPI `pytest-chronostrace`, and npm
  `@chronostrace/test` as if published.
- Version source of truth is fragmented: workspace `0.1.0`,
  release.yml derives versions from git tags (`vX.Y.Z`), the TS
  publish step rewrites `package.json` via `npm version`, and the
  Python package carries its own `0.1.0`. No single source is
  enforced.

### CT-016 — Documentation and maintainer readiness (P2): CONFIRMED

- README contains pasted generation-conversation material
  (README ~lines 325–334): an `<ElicitationsGroup>` block
  ("Next steps for ChronosTrace or the Project Genesis portfolio",
  including "Initiate Phase 9 build for Candidate #2 (AegisMCP)"),
  a verbatim `User prompt: Now make a full file .Zip ready to
  download i want push on github`, and an assistant `Response: Here
  is the complete, production-ready source tree structure…`
  preamble. This is internal conversation content in a public README.
- The pasted tree claims
  `crates/chronostrace-cli/src/diff_render.rs`, a file that does
  not exist in the repo.
- Badges (README lines 7–10) point to `chronostrace/chronostrace`
  CI and to unpublished crates.io / PyPI / npm packages.
- Unproven performance/feature claims: "100% Offline, Sub-2ms CI
  Replay" (line 47), "executes in < 15ms" (line 90),
  "3-Tier DAG Matcher", "`mcp-wrap`", "`chronostrace diff`",
  "Automatic Secret & High-Entropy Scrubbing" — none has a passing
  test behind it today (see §6–§7).
- Missing maintainer files (verified via `git ls-files` and root
  listing): `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`,
  `CHANGELOG.md`, issue/PR templates, `.gitignore`, supported-
  platform matrix, compatibility policy, troubleshooting guide.
- LICENSE is the full Apache-2.0 text, copyright 2026
  AHMADYAZIDALFARISI — that part is in order.

## 5. New findings beyond the audit (found in this phase)

### N-01 — TypeScript/Rust hash divergence ("bit-exact" claim is false for two input classes)

- SimHash normalization differs: Rust keeps Unicode letters
  (`is_alphanumeric()` is Unicode-aware; `to_ascii_lowercase()` is a
  no-op on them) and replaces other chars with space. TypeScript
  strips everything outside `[a-zA-Z0-9\s]` — i.e. it **deletes**
  Unicode letters entirely — then applies Unicode-aware
  `toLowerCase()`. Example: `"café"` normalizes to `"café"` in
  Rust but `"caf"` in TypeScript → different shingles → different
  fingerprints. The `computeSimHashHex` docstring claim of a
  "bit-exact port" holds only for pure-ASCII input.
- Float serialization differs: Rust `serde_json::to_vec` renders
  `50.0` as `50.0`; TypeScript `JSON.stringify` renders it as `50`.
  `sha256CanonicalJson` therefore disagrees on any payload
  containing a float, so a cassette built by the TS builder and one
  built by the Rust core hash the "same" request differently.
  (Key sorting agrees for ASCII keys: Rust `BTreeMap` byte order vs
  JS `.sort()` UTF-16 order coincide there.)

### N-02 — TypeScript example and test files are syntactically invalid

- `examples/langgraph-refund-agent/src/agent.ts`: `tsc` reports
  `TS1002: Unterminated string literal` at line 31, with cascade
  errors at lines 198/200/214. The file also contains visibly
  truncated template literals around lines 172 and 198.
- `sdks/typescript/tests/chronostrace.e2e.test.ts`:
  `TS1002` at lines 19 and 185 (both truncated string literals).
- Net: `tsc --noEmit` exit 2 with 16 errors, all in these two
  files; `src/` itself is clean. `vitest run` exits 1 with 0 tests
  executed (transform failure at e2e line 19).

### N-03 — No `.gitignore`; build artifacts pollute the tree

A fresh clone that runs any build immediately shows `target/` and
`Cargo.lock` as untracked (plus `node_modules/` for the TS side).
`Cargo.lock` is currently untracked at HEAD; for a workspace that
ships a binary, committing the lockfile is the standard choice, but
that decision belongs to Phase 1, not to this baseline.

### N-04 — CLI has no `--version` flag

`chronostrace --version` exits 2. This independently breaks the CI
Python job's `chronostrace --version` step even if the binary-name
problem (CT-015) were fixed.

### N-05 — Test inventory is empty where it matters most

See §6. The Rust suites are vacuous, the Python suite does not
exist, and the TS suite cannot parse. No test in the repo exercises
matching, replay, proxying, MCP wrapping, chaos injection, or
scrubbing end-to-end.

### N-06 — Repository URL is wrong in two places

Workspace `Cargo.toml` `repository` and the release.yml-generated
npm platform `package.json` files both point to
`github.com/chronostrace/chronostrace`. The actual repo is
`github.com/AHMADYAZIDALFARISI/project-genesis`. README badges
use the wrong org as well.

### N-07 — CI's schema step validates nothing

`.github/workflows/ci.yml` "Validate .ctrace JSON Schema syntax"
only parses the schema file as JSON. It would pass with the typo
present (and does — the file is valid JSON). No cassette is ever
validated against the schema in CI or anywhere else.

## 6. Test inventory

| Suite | Files | Tests | Result at baseline |
|---|---|---|---|
| Rust workspace (`cargo test`) | 9 test targets (5 lib/bins incl. lib-test variants) | 0 in every target | exit 0 on working tree (vacuous); exit 101 at HEAD (compile error) |
| Python (`pytest`) | none | 0 | cannot run: no test files; plugin unimportable (syntax error); pytest not installed here |
| TypeScript (`vitest run`) | 1 file, 5 tests (e2e refund suite) | 0 executed | exit 1: transform failure (unterminated string literal) |
| Schema validation | 0 fixtures | 0 | cannot run: no `*.ctrace` fixtures; manual `jsonschema` demo fails on the typo |
| CI (5 jobs) | `ci.yml` | — | every job is blocked by ≥1 defect: lint (fmt fails even on working tree), rust-test (fails at HEAD), python-e2e (artifact path + `--version` + no tests + plugin syntax), typescript-e2e (`npm ci` without lockfile + typecheck fails), dogfood (missing binary + `diff`/`mcp-wrap` commands do not exist) |

## 7. Feature-to-code evidence matrix (README claims vs code)

| README claim | Code evidence | Verdict |
|---|---|---|
| Rust core engine | 5 crates compile (working tree); 0 tests | Partial |
| Cassette recording (`record_node`, YAML save) | exists; hardcoded timestamp + `scrubbed: true`; non-atomic write | Partial |
| Exact canonical hashing | `sha256_canonical_json` implemented (BTreeMap sort + SHA-256) | Implemented (Rust only; TS port diverges on floats, N-01) |
| Three-tier DAG matching | `match_request()` is `unimplemented!()`; no DAG traversal | Not implemented |
| SimHash | algorithm implemented (Rust + TS port) | Implemented, with N-01 divergences; no threshold enforcement |
| Trajectory diff | index-aligned diff with 5 kinds; `diff` CLI missing | Partial; needs graph-aware redesign |
| HTTP replay proxy | one empty route, no upstream | Stub |
| MCP stdio wrapper | `Ok(())` stub | Stub |
| HTTP 429 injection | implemented in `ChaosEngine::apply()` (no retry headers) | Partial |
| HTTP 503 / latency / truncated JSON / drop field | enum variants exist; `apply()` passes through | Not implemented |
| Pattern secret scrubbing | 3 regexes + key-based JSON redaction | Partial |
| Shannon entropy filter | no scanning code; threshold field removed in working tree | Not implemented |
| Python SDK | unimportable plugin (syntax error) + wrong CLI contract | Broken |
| TypeScript SDK | scaffolding + builder + clients exist; unparseable example/test; wrong CLI contract | Partial / broken at test layer |
| CLI `diff` / `mcp-wrap` | neither subcommand exists | Not implemented |
| CI/CD | 5 jobs defined; all blocked (see §6) | Misconfigured |
| Install via pip / npm / cargo install | packages never published; no tags | Not installable |
| Sub-2ms / <15ms replay | no benchmark, no replay implementation | Unproven |
| `chronostrace diff` sample output in README | sample output is illustrative; command does not exist | Illustrative only |

## 8. Blockers and decisions needed before Phase 1

1. **Binary name (single source of truth).** Cargo builds
   `chronostrace-cli`; CI, release, and both SDKs expect
   `chronostrace`. Decide one name and rename everywhere (or add an
   explicit `[[bin]] name = "chronostrace"`). Until decided, every
   downstream job stays broken regardless of code fixes.
2. **Default branch for CI.** `ci.yml` targets `main`; the repo
   default is `master`. Either retarget CI or rename the branch
   (rename requires user approval; retargeting CI is the smaller
   change).
3. **Version source of truth.** Workspace `0.1.0`, tag-derived
   release versions, and per-package rewrites currently disagree.
   Decide whether tags drive versions before any publish step is
   wired.
4. **Verbatim-doc vs rewrite policy.** Much of this code was
   extracted verbatim from a source document, including its defects
   (truncated literals, the schema typo, the missing `},`). Phase 1
   fixes will necessarily deviate from the document. The standing
   rule for this repo so far has been "verbatim unless a repair is
   explicitly approved" — confirm that Phase 1+ repairs may deviate
   where the document is wrong, with each deviation disclosed.
5. **`.gitignore` + `Cargo.lock` policy.** Needed before the first
   Phase 1 commit, or every commit will drag build artifacts.
6. **README disposition.** The pasted conversation block and the
   `diff_render.rs` tree entry must be removed, and every
   performance/install claim either proven or deleted. Decide
   whether the README is rewritten in Phase 1 or quarantined until
   Phase 10.
7. **Test strategy for the empty suites.** Rust has 0 tests, Python
   has 0 test files, TS has 5 tests that cannot parse. Phase 1
   needs a decision on where the first real tests land (the recovery
   prompt requires regression tests per fix).

## 9. Phase 0 scope statement

- Inspected: all 27 tracked files, both workflows, all manifests,
  both SDKs, the schema, the example, and the README (395 lines).
- Searched repo-wide (excluding `target/`): `unimplemented!`
  (1 hit), `todo!` (0), `panic!` (0), `unsafe` (0), `unwrap()` (3,
  regex literals only), `.expect(` in Rust (0), `allow(` (1 — the
  evening fix itself), hardcoded timestamps (3 code sites + 1 CI
  fixture), `scrubbed` (code + schema + fixture), `fs::write` (1),
  `TODO`/`FIXME`/`XXX` (0), env mutations (TS + Python, §4).
- Ran: all §3 commands with exit codes recorded. No command output
  was invented; every exit code above comes from an actual run in
  this session.
- Changed in this phase: **only** `AUDIT_BASELINE.md` (this file).
  The three modified source files, `Cargo.lock`, `phase0_dump.txt`,
  and `target/` predate Phase 0 and remain uncommitted.
- Not done in this phase (by design, per the recovery prompt's FIRST
  ACTION): no refactoring, no new features, no test authoring, no
  commit, no push, no publish, no release. The baseline is presented
  for review; Phase 1 starts only on user approval.

---

*Next recommended step: user review of this baseline, then Phase 1
(foundation & build stabilization) scoped to §8 decisions 1–5 plus
the CT-013 schema typo and the CT-016 README quarantine.*
