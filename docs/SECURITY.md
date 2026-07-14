# Security

Fala is local-first, but adapters still cross trust boundaries.

Rules:

- subprocess commands are argument lists, not shell strings
- subprocess effectors receive manifests and must not write SQLite directly
- adapter env supports `${env:NAME}` references
- resolved secret values are redacted from subprocess stdout/stderr and effector output
- reaction paths are resolved inside the reaction store root
- web/API infrastructure is not part of core
- runtime mutations should go through command APIs

Do not put secrets in event payloads, reaction metadata, exported traces, or
HTML reports.
