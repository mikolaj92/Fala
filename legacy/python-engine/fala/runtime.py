from __future__ import annotations

import functools
import inspect
from datetime import datetime
from pathlib import Path
from typing import Callable

from fala.reactions import ReactionStore, create_reaction_store
from fala.driver import RunCorrelationPathResult, RunUntilIdleResult, run_correlation_path, run_until_idle
from fala.correlation_paths import (
    CorrelationPathAdvance,
    CorrelationPathInstance,
    advance_correlation_path,
    instantiate_correlation_path,
)
from fala.journal import (
    InMemoryJournal,
    Journal,
    JournalBackedBackend,
    JsonlJournal,
    SqliteJournal,
)
from fala.models import CorrelationPathSpec, JournalConfig, RuntimeConfigSpec
from fala.runtime_backend import (
    RuntimeReactionStore,
    RuntimeJournalMaintenancePlan,
    Reaction,
    WaitGraphDiagnostic,
    ProcessStatus,
    RunStatus,
    Impulse,
    ImpulseRelation,
    ImpulseType,
    CommandSubmission,
    Homeostat,
    HomeostatStatus,
    Association,
    Process,
    Projection,
    Run,
    RunBoundary,
    RuntimeBackend,
    RuntimeBackendService,
    RuntimeEvent,
    Correlator,
)


def open_journal(
    config: JournalConfig | RuntimeConfigSpec | str | Path | Journal,
) -> Journal:
    """Open a Journal from config, path, or an existing Journal instance."""
    if isinstance(config, (InMemoryJournal, SqliteJournal, JsonlJournal)):
        return config
    # Structural Journal Protocol instances (duck-typed)
    if hasattr(config, "append_batch") and hasattr(config, "claim_next"):
        return config  # type: ignore[return-value]
    if isinstance(config, RuntimeConfigSpec):
        if config.journal is not None:
            return open_journal(config.journal)
        if config.backend is not None:
            return open_journal(
                JournalConfig(kind=config.backend.kind, path=config.backend.path)
            )
        raise ValueError("runtime config has neither journal nor backend")
    if isinstance(config, JournalConfig):
        if config.kind == "memory":
            return InMemoryJournal()
        if config.kind == "sqlite":
            assert config.path is not None
            return SqliteJournal(config.path)
        if config.kind == "jsonl":
            assert config.path is not None
            return JsonlJournal(config.path)
        raise ValueError(f"Unsupported journal kind: {config.kind!r}")
    # str | Path → sqlite path
    return SqliteJournal(config)


class AutonomousCorrelator:
    """Impulse-first embedded runtime facade.

    This module is intentionally independent from HTTP, CLI, and web UI modules.
    Prefer :meth:`from_journal` / :func:`open_journal`; :meth:`sqlite` remains
    a compatibility shim.
    """

    def __init__(self, backend: RuntimeBackend, *, journal: Journal | None = None) -> None:
        self.service = RuntimeBackendService(backend)
        self.backend = backend
        self.journal = journal

    @classmethod
    def from_journal(cls, journal: Journal | JournalConfig | str | Path) -> "AutonomousCorrelator":
        opened = open_journal(journal)
        backend = JournalBackedBackend(opened)
        return cls(backend, journal=opened)

    @classmethod
    def sqlite(cls, path: str | Path) -> "AutonomousCorrelator":
        """Compatibility shim — equivalent to ``from_journal(path)`` for SQLite."""
        return cls.from_journal(path)

    @property
    def runtime_uri(self) -> str:
        if self.journal is not None:
            return self.journal.runtime_uri
        backend = self.backend
        if isinstance(backend, Correlator):
            return f"sqlite://{Path(backend.path).expanduser().resolve()}"
        if isinstance(backend, JournalBackedBackend):
            return backend.runtime_uri
        return "unknown://"

    def scope(self, run_id: str) -> "RunScope":
        return RunScope(self, run_id)

    async def create_run(
        self,
        run: Run,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Run, CommandSubmission]:
        return await self.service.create_run(
            run,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def set_run_status(
        self,
        *,
        run_id: str,
        status: RunStatus,
        idempotency_key: str,
        reason: str | None = None,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Run, CommandSubmission]:
        return await self.service.set_run_status(
            run_id=run_id,
            status=status,
            idempotency_key=idempotency_key,
            reason=reason,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def cancel_run(
        self,
        *,
        run_id: str,
        idempotency_key: str,
        reason: str | None = None,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Run, CommandSubmission]:
        return await self.service.cancel_run(
            run_id=run_id,
            idempotency_key=idempotency_key,
            reason=reason,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def accept_impulse(
        self,
        impulse: Impulse,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Impulse, CommandSubmission]:
        return await self.service.accept_impulse(
            impulse,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def register_impulse_type(
        self,
        impulse_type: ImpulseType,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[ImpulseType, CommandSubmission]:
        return await self.service.register_impulse_type(
            impulse_type,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def record_impulse_relation(
        self,
        relation: ImpulseRelation,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[ImpulseRelation, CommandSubmission]:
        return await self.service.record_impulse_relation(
            relation,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def record_association(
        self,
        association: Association,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Association, CommandSubmission]:
        return await self.service.record_association(
            association,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def record_reaction(
        self,
        reaction: Reaction,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Reaction, CommandSubmission]:
        return await self.service.record_reaction(
            reaction,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def record_file_reaction(
        self,
        *,
        run_id: str,
        kind: str,
        path: str | Path,
        impulse_id: str | None = None,
        media_type: str | None = None,
        reaction_id: str | None = None,
        reaction_store: ReactionStore | str | Path | None = None,
        metadata: dict | None = None,
        idempotency_key: str | None = None,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Reaction, CommandSubmission]:
        store = (
            reaction_store
            if hasattr(reaction_store, "put_file")
            else create_reaction_store(
                reaction_store or _default_impulse_reaction_root(self.backend)
            )
        )
        ref = store.put_file(
            kind=kind,
            path=path,
            reaction_id=reaction_id,
            metadata=metadata,
        )
        ref_metadata = dict(ref.metadata)
        digest = ref_metadata.get("sha256")
        reaction = Reaction(
            id=ref.id,
            run_id=run_id,
            kind=kind,
            uri=ref.uri,
            impulse_id=impulse_id,
            media_type=media_type,
            size_bytes=ref_metadata.get("size_bytes"),
            content_hash=f"sha256:{digest}" if isinstance(digest, str) else None,
            metadata={**ref_metadata, "reaction_store": store.location},
        )
        return await self.record_reaction(
            reaction,
            idempotency_key=idempotency_key or f"reaction.record:{ref.id}",
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def schedule_process(
        self,
        process: Process,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        return await self.service.schedule_process(
            process,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def ready_process(
        self,
        *,
        run_id: str,
        process_id: str,
        input: dict | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        return await self.service.ready_process(
            run_id=run_id,
            process_id=process_id,
            input=input,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def claim_next_ready_process(
        self,
        *,
        worker_id: str,
        run_id: str | None = None,
        lease_seconds: float = 300.0,
        all_runs: bool = False,
    ) -> Process | None:
        return await self.service.claim_next_ready_process(
            worker_id=worker_id,
            run_id=run_id,
            lease_seconds=lease_seconds,
            all_runs=all_runs,
        )

    async def wait_process(
        self,
        *,
        run_id: str,
        process_id: str,
        output: dict | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        return await self.service.wait_process(
            run_id=run_id,
            process_id=process_id,
            output=output,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def instantiate_correlation_path(
        self,
        *,
        run_id: str,
        correlation_path: CorrelationPathSpec,
        correlation_path_id: str | None = None,
        impulse_id: str | None = None,
        effector_inputs: dict[str, dict] | None = None,
        effector_configs: dict[str, dict] | None = None,
        max_attempts: int = 1,
        priority: int = 0,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        capability_output_schemas: dict[str, dict] | None = None,
        accepted_reaction_kinds_by_effector: dict[str, list[str]] | None = None,
        max_attempts_by_effector: dict[str, int] | None = None,
        auto_advance: bool = True,
        regulation_by_effector: dict[str, dict] | None = None,
    ) -> CorrelationPathInstance:
        return await instantiate_correlation_path(
            self.service,
            run_id=run_id,
            correlation_path=correlation_path,
            correlation_path_id=correlation_path_id,
            impulse_id=impulse_id,
            effector_inputs=effector_inputs,
            effector_configs=effector_configs,
            max_attempts=max_attempts,
            priority=priority,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            capability_output_schemas=capability_output_schemas,
            accepted_reaction_kinds_by_effector=accepted_reaction_kinds_by_effector,
            auto_advance=auto_advance,
            regulation_by_effector=regulation_by_effector,
            max_attempts_by_effector=max_attempts_by_effector,
        )

    async def advance_correlation_path(
        self,
        *,
        run_id: str,
        correlation_path_id: str,
        actor: str | None = None,
    ) -> CorrelationPathAdvance:
        return await advance_correlation_path(
            self.service,
            run_id=run_id,
            correlation_path_id=correlation_path_id,
            actor=actor,
        )
    async def run_until_idle(
        self,
        *,
        worker_id: str,
        run_id: str | None = None,
        lease_seconds: float = 300.0,
        max_ticks: int = 100,
        work_dir: str | Path | None = None,
        advance_correlation_paths: bool = True,
        should_stop: Callable[[], bool] | None = None,
        all_runs: bool = False,
    ) -> RunUntilIdleResult:
        return await run_until_idle(
            self.service,
            worker_id=worker_id,
            run_id=run_id,
            lease_seconds=lease_seconds,
            max_ticks=max_ticks,
            work_dir=work_dir,
            advance_correlation_paths=advance_correlation_paths,
            should_stop=should_stop,
            all_runs=all_runs,
        )

    async def run_correlation_path(
        self,
        *,
        run: Run,
        correlation_path: CorrelationPathSpec,
        worker_id: str,
        correlation_path_id: str | None = None,
        effector_inputs: dict[str, dict] | None = None,
        effector_configs: dict[str, dict] | None = None,
        work_dir: str | Path | None = None,
        max_ticks: int = 100,
        lease_seconds: float = 300.0,
        actor: str | None = None,
        max_attempts_by_effector: dict[str, int] | None = None,
        regulation_by_effector: dict[str, dict] | None = None,
    ) -> RunCorrelationPathResult:
        return await run_correlation_path(
            self.service,
            run=run,
            correlation_path=correlation_path,
            worker_id=worker_id,
            correlation_path_id=correlation_path_id,
            effector_inputs=effector_inputs,
            effector_configs=effector_configs,
            work_dir=work_dir,
            max_ticks=max_ticks,
            lease_seconds=lease_seconds,
            actor=actor,
            regulation_by_effector=regulation_by_effector,
            max_attempts_by_effector=max_attempts_by_effector,
        )

    async def complete_process(
        self,
        *,
        run_id: str,
        process_id: str,
        output: dict | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        return await self.service.complete_process(
            run_id=run_id,
            process_id=process_id,
            output=output,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def fail_process(
        self,
        *,
        run_id: str,
        process_id: str,
        error: dict | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        return await self.service.fail_process(
            run_id=run_id,
            process_id=process_id,
            error=error,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def cancel_process(
        self,
        *,
        run_id: str,
        process_id: str,
        error: dict | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        return await self.service.cancel_process(
            run_id=run_id,
            process_id=process_id,
            error=error,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def timeout_process(
        self,
        *,
        run_id: str,
        process_id: str,
        error: dict | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        return await self.service.timeout_process(
            run_id=run_id,
            process_id=process_id,
            error=error,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def retry_process(
        self,
        *,
        run_id: str,
        process_id: str,
        idempotency_key: str,
        available_at: datetime | None = None,
        error: dict | None = None,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        return await self.service.retry_process(
            run_id=run_id,
            process_id=process_id,
            available_at=available_at,
            error=error,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def save_homeostat(
        self,
        homeostat: Homeostat,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Homeostat, CommandSubmission]:
        return await self.service.save_homeostat(
            homeostat,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def open_homeostat(
        self,
        homeostat: Homeostat,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Homeostat, CommandSubmission]:
        return await self.service.open_homeostat(
            homeostat,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def complete_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        values: dict | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Homeostat, CommandSubmission]:
        return await self.service.complete_homeostat(
            run_id=run_id,
            homeostat_id=homeostat_id,
            values=values,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def cancel_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        values: dict | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Homeostat, CommandSubmission]:
        return await self.service.cancel_homeostat(
            run_id=run_id,
            homeostat_id=homeostat_id,
            values=values,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def expire_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        values: dict | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Homeostat, CommandSubmission]:
        return await self.service.expire_homeostat(
            run_id=run_id,
            homeostat_id=homeostat_id,
            values=values,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def save_projection(
        self,
        projection: Projection,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Projection, CommandSubmission]:
        return await self.service.save_projection(
            projection,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def rebuild_projections(
        self,
        *,
        run_id: str,
        names: list[str] | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[list[Projection], CommandSubmission]:
        return await self.service.rebuild_projections(
            run_id=run_id,
            names=names,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def get_projection(self, *, run_id: str, name: str) -> Projection | None:
        return await self.service.get_projection(run_id=run_id, name=name)

    async def observe_run(self, *, run_id: str) -> RunBoundary:
        return await self.service.observe_run(run_id=run_id)

    async def list_events(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        after_sequence: int | None = None,
        limit: int | None = None,
    ) -> list[RuntimeEvent]:
        return await self.backend.list_events(
            run_id=run_id,
            impulse_id=impulse_id,
            after_sequence=after_sequence,
            limit=limit,
        )

    async def list_runs(
        self,
        *,
        status: RunStatus | None = None,
        limit: int | None = None,
    ) -> list[Run]:
        return await self.service.list_runs(status=status, limit=limit)

    async def maintain_journal(
        self,
        *,
        older_than_days: float,
        keep_last: int | None = None,
        vacuum: bool = True,
        dry_run: bool = True,
        reaction_store: RuntimeReactionStore | None = None,
    ) -> RuntimeJournalMaintenancePlan:
        return await self.service.maintain_journal(
            older_than_days=older_than_days,
            keep_last=keep_last,
            vacuum=vacuum,
            dry_run=dry_run,
            reaction_store=reaction_store,
        )

    async def list_impulse_types(self, *, run_id: str) -> list[ImpulseType]:
        return await self.service.list_impulse_types(run_id=run_id)

    async def list_impulse_relations(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        relation_type: str | None = None,
    ) -> list[ImpulseRelation]:
        return await self.service.list_impulse_relations(
            run_id=run_id,
            impulse_id=impulse_id,
            relation_type=relation_type,
        )

    async def list_reactions(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        kind: str | None = None,
    ) -> list[Reaction]:
        return await self.service.list_reactions(
            run_id=run_id,
            impulse_id=impulse_id,
            kind=kind,
        )

    async def list_processes(
        self,
        *,
        run_id: str,
        status: ProcessStatus | None = None,
        impulse_id: str | None = None,
    ) -> list[Process]:
        return await self.service.list_processes(
            run_id=run_id,
            status=status,
            impulse_id=impulse_id,
        )

    async def list_homeostats(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        status: HomeostatStatus | None = None,
    ) -> list[Homeostat]:
        return await self.service.list_homeostats(
            run_id=run_id,
            impulse_id=impulse_id,
            status=status,
        )

    async def diagnose_waits(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
    ) -> WaitGraphDiagnostic:
        return await self.service.diagnose_waits(
            run_id=run_id,
            impulse_id=impulse_id,
        )


class RunScope:
    """An :class:`AutonomousCorrelator` view bound to a single run.

    Methods whose signature takes ``run_id`` get the scope's run injected;
    methods that take a model (``accept_impulse``, ``record_association``,
    ...) are checked so a payload addressed at another run cannot leak
    through the boundary.
    """

    def __init__(self, runtime: AutonomousCorrelator, run_id: str) -> None:
        self.runtime = runtime
        self.run_id = run_id

    def __getattr__(self, name: str):
        attr = getattr(self.runtime, name)
        if not callable(attr):
            return attr
        parameters = inspect.signature(attr).parameters

        @functools.wraps(attr)
        def bound(*args, **kwargs):
            if "run_id" in parameters:
                kwargs.setdefault("run_id", self.run_id)
            for value in args:
                value_run_id = getattr(value, "run_id", None)
                if value_run_id is not None and value_run_id != self.run_id:
                    raise ValueError(
                        f"{name} payload is addressed to run {value_run_id!r}, "
                        f"but this scope is bound to run {self.run_id!r}"
                    )
            if kwargs.get("run_id") != self.run_id and "run_id" in parameters:
                raise ValueError(
                    f"{name} run_id {kwargs.get('run_id')!r} does not match "
                    f"scope run {self.run_id!r}"
                )
            return attr(*args, **kwargs)

        return bound


def _default_impulse_reaction_root(backend: RuntimeBackend) -> Path:
    if isinstance(backend, JournalBackedBackend):
        journal = backend.journal
        if isinstance(journal, SqliteJournal):
            return Path(journal.path).expanduser().resolve().parent / "reactions"
    if isinstance(backend, Correlator):
        return backend.path.parent / "reactions"
    return Path(".fala") / "reactions"


__all__ = ["AutonomousCorrelator", "RunScope", "open_journal"]
