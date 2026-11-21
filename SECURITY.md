# Security Policy

## Supported Versions

| Version | Supported |
| ------- | --------- |
| `main` (latest) | ✅ |
| older commits | ❌ |

## Reporting a Vulnerability

**Do not open a public issue for security vulnerabilities.**

Report it privately through GitHub's private vulnerability reporting for this repository:
**[Report a vulnerability](https://github.com/masterfabric/masterfabric-spark-rust-bench/security/advisories/new)** (Security tab → "Report a vulnerability").
Include:

- a description of the vulnerability and its impact,
- steps to reproduce (OS, container runtime, commit, exact command),
- a suggested fix, if you have one,
- how we can contact you.

For critical issues start the advisory title with `[URGENT]`.

### Response timeline

- **Initial response:** within 48 hours
- **Assessment / status update:** within 7 days
- **Fix:** prioritized and released as soon as possible

## Scope

This repository benchmarks JVM-based Apache Spark against Rust-based single-node engines on identical queries and data, with every engine run in a container under the same CPU and memory limits, and verifies that every engine returns the same answers.

Not in scope: theoretical issues without a proof of concept, social engineering, physical
attacks, and automated scanner output without analysis.

## Notes for operators

- The default credentials in `.env.example` exist only for a laptop lab bound to localhost.
  Change every one of them before exposing any service on a network.
- Never commit `.env`, access keys, tokens, kubeconfigs or model weights.

## Legal

Copyright (c) 2026 MasterFabric. Licensed under the GNU AGPL v3.0, see [LICENSE](LICENSE) and
[NOTICE](NOTICE). Security fixes and contributions are licensed under the same terms.
