# Security Policy

## Supported versions

| Version | Supported |
|---|---|
| `0.1.x` | Yes |

## Reporting a vulnerability

Please do **not** report security vulnerabilities through public GitHub
issues. Instead, report them privately via GitHub Security Advisories:

https://github.com/AHMADYAZIDALFARISI/project-genesis/security/advisories

Include, where possible:

1. A description of the vulnerability and the affected crate or SDK
   (`chronostrace-core`, `chronostrace-scrub`, `chronostrace-proxy`,
   `chronostrace-cli`, the Python plugin, or the TypeScript SDK).
2. Minimal reproduction steps or a synthetic test fixture. Never include
   real credentials, tokens, or production cassettes.
3. Expected versus actual behavior.

## Threat model

See [`SECURITY_THREAT_MODEL.md`](./SECURITY_THREAT_MODEL.md) for the
project threat model, including trust boundaries, safety-critical replay
constraints, the two-stage scrubbing approach (sensitive field names,
key patterns, and Shannon entropy filtering), and residual risks.

In particular: regex plus entropy filtering cannot guarantee finding
short, low-entropy secrets. Treat any recorded cassette as potentially
sensitive until it has been scrubbed and reviewed, and use test-only
credentials when recording.

## No package publication

This repository does not publish packages to PyPI, npm, or crates.io,
and does not create GitHub Releases or tags as part of its normal
workflow. Security reports should not assume a published artifact
channel.
