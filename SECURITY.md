# Security Policy

GaussFlow is a **Technology Preview (Alpha)** from Gaussian Technologies. It is **not yet
suitable for production or for processing sensitive data.** This document describes how to report
vulnerabilities and the current security posture.

## Reporting a vulnerability

Please report security issues privately — do **not** open a public issue.

- Email: **security@gaussian.technology** (or the maintainers listed in the repository).
- Include: affected version/commit, a description, reproduction steps, and impact.
- We aim to acknowledge reports within 3 business days and to provide a remediation timeline
  after triage.

Please do not run automated scanners against any hosted GaussFlow instance you do not own.

## Supported versions

While in Technology Preview, only the latest `main` is supported. There are no security
backports to older commits.

## Credential handling

GaussFlow sources all credentials from the environment — **no secrets are committed to the
source tree.** A previously committed SurrealDB password and a default JWT secret were removed
in the Phase 0 hardening pass; if you cloned an earlier revision, rotate any credentials that
matched those values.

Relevant environment variables:

| Variable | Purpose | Default (local dev only) |
|---|---|---|
| `GAUSSFLOW_SURREAL_URL` | SurrealDB endpoint | `ws://127.0.0.1:8000/rpc` (runtime) / `http://127.0.0.1:8000` (cli) |
| `GAUSSFLOW_DB_USER` | SurrealDB user | `root` |
| `GAUSSFLOW_DB_PASS` | SurrealDB password | `root` — **set this for any non-local deployment** |
| `GAUSSFLOW_DB_NS` | SurrealDB namespace | `gaussflow` |
| `GAUSSFLOW_DB_NAME` | SurrealDB database | `gaussflow` |
| `GAUSSFLOW_JWT_SECRET` | JWT signing secret | *empty* — `Config::validate()` rejects an empty secret, so this must be set to enable auth |
| `OPENAI_API_KEY` | OpenAI LLM node | *unset* |

The local-dev fallbacks exist only so the project runs out of the box against a throwaway local
SurrealDB. They are intentionally insecure and must be overridden in any shared or hosted
environment. See `.env.example`.

## Known limitations (Alpha)

These are tracked in [`docs/PRODUCTION_ROADMAP.md`](docs/PRODUCTION_ROADMAP.md) (notably Phase 5,
Security & multi-tenancy):

- The web API does **not** enforce authentication/authorization yet (RBAC types exist but are not
  wired in).
- PII detection/redaction is configuration-only; no enforcement.
- Dependency advisories are surfaced by `cargo audit` in CI but are **not** yet gating.
- Audit logging and tamper-evidence are not implemented.

Do not expose a GaussFlow instance to untrusted networks or users during the preview.
