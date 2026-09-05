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

External effects under automatic retry are **at-least-once**: a timeout or crash
may leave an effect completed before its runtime result is committed. Effectors
must durably deduplicate by stable `execution_id` before performing the effect;
`attempt` identifies only a physical try and is not an idempotency key. Use
`retry_policy = "none"` when durable deduplication cannot be guaranteed.

For effects that can be authoritatively observed, `effect_protocol.mojo`
provides an **effectively-once confirmed effect** contract. It is an ordinary
Fala composition: persist a typed intent (`idempotency_key`, desired identity,
capability), observe the world, act only when absent, observe again, then
confirm the authoritative identity with an evidence reference. Resume always
returns to observe. A matching existing effect confirms without another act;
a conflicting observation or evidence-free confirmation fails closed. This is
not an exactly-once execution guarantee and contains no provider-specific
GitHub, document, or messaging semantics.

Durable subprocess cancellation polls the journal while retaining the live
process-host handle. `cancel_requested` records the operator request, then the
host sends SIGTERM to the private process group, waits a bounded grace period,
and escalates the whole group to SIGKILL when needed. Signal/escalation and the
single `cancelled` terminal are journal events; replaying the cancel key is
idempotent. A race with natural completion observes one durable terminal.
`native_function` calls cannot be preempted and finish cooperatively;
`manual_homeostat` has no live child and is cancelled directly in the journal.
After driver death, recovery can only reclaim/terminalize the lease: OS process
ownership is intentionally not reconstructed from an untrusted stale PID.

Do not put secrets in event payloads, reaction metadata, exported traces, or
HTML reports. See [`ADAPTER_CONTRACTS.md`](ADAPTER_CONTRACTS.md) for the wire
boundary and [`REACTIONS_AND_REFERENCES.md`](REACTIONS_AND_REFERENCES.md) for
reaction storage and foreign-journal references.
