# Security

Fala is local-first. Adapters cross a process trust boundary.

- Subprocess commands are argument lists, never shell strings.
- Subprocess effectors receive manifests and must not write a journal or
  SQLite file directly.
- Adapter environment values may use `${env:NAME}` references.
- Resolved secret values are redacted from captured stdout/stderr only. The
  structured `output/result.json` object is not secret-redacted; it is
  semantically canonicalized (equivalent JSON is preserved; whitespace and
  object-key order may change).
- Reaction source files may originate outside the CAS root; resolved
  `fala-reaction://` blob paths stay inside the reaction-store root.
- No web/API infrastructure is part of core.
- Runtime mutations go through journal/backend command APIs, not adapter
  writes.

External effects under automatic retry are **at-least-once**. Effectors must
durably deduplicate by stable `execution_id` before performing the effect;
`attempt` is only a physical try. Use `retry_policy = "none"` when durable
deduplication cannot be guaranteed.

For effects that can be authoritatively observed, `effect_protocol.mojo` is
an ordinary Fala composition: persist a typed intent, observe, act only when
absent, observe again, confirm with evidence. Resume always returns to
observe. This is not exactly-once execution and has no provider-specific
GitHub, document, or messaging semantics.

Durable subprocess cancellation polls the journal while retaining the live
process-host handle. `cancel_requested` records the operator request, then
the host sends SIGTERM to the private process group, waits a bounded grace
period, and escalates to SIGKILL. `native_function` calls cannot be
preempted. `manual_homeostat` has no live child and is cancelled in the
journal. After driver death, recovery can only reclaim the lease: OS process
ownership is not reconstructed from an untrusted stale PID.

Do not put secrets in event payloads, reaction metadata, exported traces, or
`explain` output. See [`ADAPTER_CONTRACTS.md`](ADAPTER_CONTRACTS.md) and
[`REACTIONS_AND_REFERENCES.md`](REACTIONS_AND_REFERENCES.md).
