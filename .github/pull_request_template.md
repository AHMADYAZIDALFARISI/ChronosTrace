## Description

Describe what this pull request changes and why. Link any related
issue (for example: `Fixes #123`).

## Type of Change

- [ ] Bug fix
- [ ] New feature
- [ ] Performance improvement
- [ ] Documentation only
- [ ] Refactoring (no behavior change)

## Verification Checklist

Before requesting review, run the full verification suite locally
from the repository root and confirm every command exits with code `0`:

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `cargo test --workspace --all-features`
- [ ] `pytest` (Python SDK, from `sdks/python`)
- [ ] `npm test` (TypeScript SDK, from `sdks/typescript`, with
      `npm run typecheck` passing)

Paste or summarize the test evidence (commands run and their results)
below:

```text
```

## Additional Notes

- [ ] `CHANGELOG.md` updated (for user-visible behavior changes)
- [ ] Documentation updated (`README.md`, `docs/`, or SDK notes) if a
      command, option, or matching rule changed
- [ ] No real API keys, tokens, or unscrubbed production credentials
      are included in fixtures, cassettes, or documentation
- [ ] No `todo!()`, `unimplemented!()`, empty handlers, or
      fake-success stubs were introduced
