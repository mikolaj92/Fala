# Security

Fala is local-first, but adapters cross a process trust boundary.

Rules:

- subprocess commands are argument lists, never shell strings;
- subprocess effectors receive manifests and must not write a journal or SQLite
  directly;
- adapter environment values may use `${env:NAME}` references;
- resolved secret values are redacted from captured subprocess stdout/stderr
  only; the structured `output/result.json` object is not secret-redacted, but
  is semantically canonicalized (equivalent JSON values are preserved while
  whitespace and object-key ordering may change);
- reaction source files may originate outside the CAS root; resolved `fala-reaction://` blob paths stay inside the reaction-store root;
- web/API infrastructure is not part of core;
- runtime mutations go through JournalPort/backend command APIs.

Do not put secrets in event payloads, reaction metadata, exported traces, or
HTML reports. See [`ADAPTER_CONTRACTS.md`](ADAPTER_CONTRACTS.md) for the wire
boundary and [`REACTIONS_AND_REFERENCES.md`](REACTIONS_AND_REFERENCES.md) for
reaction storage and foreign-journal references.
