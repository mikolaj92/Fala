from __future__ import annotations

from typing import Annotated, Any, Literal
from uuid import uuid4

from jsonschema import Draft202012Validator, SchemaError
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

RUNTIME_ID_PATTERN = r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$"
RuntimeId = Annotated[str, Field(pattern=RUNTIME_ID_PATTERN)]
EffectorAdapterKind = Literal[
    "fala_runtime",
    "manual_homeostat",
    "python_function",
    "subprocess",
]


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"


def validate_json_schema(schema: dict[str, Any], *, label: str) -> None:
    if not schema:
        return
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        raise ValueError(f"{label} is not a valid JSON Schema: {exc.message}") from exc


class ReactionRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId = Field(default_factory=lambda: new_id("reaction"))
    kind: str
    uri: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class ReactionKindSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    media_types: list[str] = Field(default_factory=list)
    value_schema: dict[str, Any] = Field(default_factory=dict)
    metadata_schema: dict[str, Any] = Field(default_factory=dict)


class ImpulseTypeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    media_types: list[str] = Field(default_factory=list)
    value_schema: dict[str, Any] = Field(default_factory=dict)
    metadata_schema: dict[str, Any] = Field(default_factory=dict)


class ImpulseRelationSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    source_impulse_types: list[RuntimeId] = Field(default_factory=list)
    target_impulse_types: list[RuntimeId] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_impulse_types(self) -> "ImpulseRelationSpec":
        _validate_unique_values(
            f"Impulse relation {self.id!r} source_impulse_types",
            self.source_impulse_types,
        )
        _validate_unique_values(
            f"Impulse relation {self.id!r} target_impulse_types",
            self.target_impulse_types,
        )
        return self


class AssociationKindSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    value_schema: dict[str, Any] = Field(default_factory=dict)
    metadata_schema: dict[str, Any] = Field(default_factory=dict)


class CapabilitySpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    accepts_impulse_types: list[RuntimeId] = Field(default_factory=list)
    accepts_reaction_kinds: list[RuntimeId] = Field(default_factory=list)
    emits_impulse_types: list[RuntimeId] = Field(default_factory=list)
    emits_reaction_kinds: list[RuntimeId] = Field(default_factory=list)
    emits_association_kinds: list[RuntimeId] = Field(default_factory=list)
    config_schema: dict[str, Any] = Field(default_factory=dict)
    output_schema: dict[str, Any] = Field(default_factory=dict)


class EffectorAdapterSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: EffectorAdapterKind
    command: list[str] | None = None
    ref: str | None = None
    runtime_ref: str | None = None
    cwd: str | None = None
    env: dict[str, str] = Field(default_factory=dict)
    # Names of caller environment variables passed through to subprocess
    # effectors. Everything else (beyond a minimal base like PATH/HOME) is
    # withheld, so an effector's environment is an explicit, replayable contract.
    inherit_env: list[str] = Field(default_factory=list)
    timeout_seconds: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_adapter_boundary(self) -> "EffectorAdapterSpec":
        if self.kind == "subprocess":
            if not self.command:
                raise ValueError("subprocess adapter requires non-empty command")
            if self.ref is not None:
                raise ValueError("subprocess adapter cannot define ref")
            if self.runtime_ref is not None:
                raise ValueError("subprocess adapter cannot define runtime_ref")
            return self

        if self.inherit_env:
            raise ValueError(f"{self.kind} adapter cannot define inherit_env")

        if self.kind == "python_function":
            if not self.ref:
                raise ValueError("python_function adapter requires ref")
            if self.command is not None:
                raise ValueError("python_function adapter cannot define command")
            if self.runtime_ref is not None:
                raise ValueError("python_function adapter cannot define runtime_ref")
            if self.cwd is not None:
                raise ValueError("python_function adapter cannot define cwd")
            if self.env:
                raise ValueError("python_function adapter cannot define env")
            return self

        if self.kind == "manual_homeostat":
            if self.command is not None:
                raise ValueError("manual_homeostat adapter cannot define command")
            if self.ref is not None:
                raise ValueError("manual_homeostat adapter cannot define ref")
            if self.runtime_ref is not None:
                raise ValueError("manual_homeostat adapter cannot define runtime_ref")
            if self.cwd is not None:
                raise ValueError("manual_homeostat adapter cannot define cwd")
            if self.env:
                raise ValueError("manual_homeostat adapter cannot define env")
            if self.timeout_seconds is not None:
                raise ValueError("manual_homeostat adapter cannot define timeout_seconds")
            return self

        if self.kind == "fala_runtime":
            if not self.runtime_ref:
                raise ValueError("fala_runtime adapter requires runtime_ref")
            if self.command is not None:
                raise ValueError("fala_runtime adapter cannot define command")
            if self.ref is not None:
                raise ValueError("fala_runtime adapter cannot define ref")
            if self.cwd is not None:
                raise ValueError("fala_runtime adapter cannot define cwd")
            if self.env:
                raise ValueError("fala_runtime adapter cannot define env")
            return self

        return self


class EffectorSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    capability: RuntimeId
    adapter: EffectorAdapterSpec
    conduction: list[RuntimeId] = Field(default_factory=list)
    timeout_seconds: float | None = Field(default=None, gt=0)
    config: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_conduction(self) -> "EffectorSpec":
        _validate_unique_values(f"Impulse correlation_path effector {self.id!r} conduction", self.conduction)
        if self.id in self.conduction:
            raise ValueError(f"Impulse correlation_path effector {self.id!r} cannot depend on itself")
        return self


class CorrelationPathSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    effectors: list[EffectorSpec] = Field(min_length=1)
    allow_feedback_cycles: bool = False
    # Opt-in transitive reaction visibility: when True, each effector is readied with an
    # ``upstream_reactions`` input holding the ``reactions`` of *every* transitive
    # ancestor (not just direct conduction), in topological order. Off by default so the
    # deliberate direct-conduction-only dataflow stays the norm.
    accumulate_upstream_reactions: bool = False

    @model_validator(mode="after")
    def validate_effectors(self) -> "CorrelationPathSpec":
        _validate_unique_ids(f"Impulse correlation_path {self.id!r} effector", self.effectors)
        known = {effector.id for effector in self.effectors}
        for effector in self.effectors:
            _validate_known_refs(
                f"Impulse correlation_path {self.id!r} effector {effector.id!r} conduction",
                effector.conduction,
                known,
            )
        if not self.allow_feedback_cycles:
            _validate_acyclic(self.effectors)
        return self


class RuntimeBackendConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["sqlite"] = "sqlite"
    path: str


class JournalConfig(BaseModel):
    """Event-stream journal sink config (preferred over ``backend``).

    ``backend`` remains a legacy alias for SQLite path configs.
    """

    model_config = ConfigDict(extra="forbid")

    kind: Literal["sqlite", "memory"] = "sqlite"
    path: str | None = None

    @model_validator(mode="after")
    def validate_path_for_sqlite(self) -> "JournalConfig":
        if self.kind == "sqlite" and not self.path:
            raise ValueError("journal.kind 'sqlite' requires path")
        return self


class ReactionStoreConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["filesystem"] = "filesystem"
    root: str


class RuntimeConfigSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    backend: RuntimeBackendConfig | None = None
    journal: JournalConfig | None = None
    reaction_store: ReactionStoreConfig

    @model_validator(mode="after")
    def require_backend_or_journal(self) -> "RuntimeConfigSpec":
        if self.backend is None and self.journal is None:
            raise ValueError("runtime requires backend or journal")
        return self


class FalaPackageSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    version: str = "2"
    impulse_types: list[ImpulseTypeSpec] = Field(default_factory=list)
    impulse_relations: list[ImpulseRelationSpec] = Field(default_factory=list)
    association_kinds: list[AssociationKindSpec] = Field(default_factory=list)
    reaction_kinds: list[ReactionKindSpec] = Field(default_factory=list)
    capabilities: list[CapabilitySpec] = Field(default_factory=list)
    correlation_paths: list[CorrelationPathSpec] = Field(min_length=1)
    runtime: RuntimeConfigSpec | None = None

    @field_validator("version", mode="before")
    @classmethod
    def coerce_version(cls, value: object) -> object:
        if isinstance(value, int):
            return str(value)
        return value

    @model_validator(mode="after")
    def validate_fala_package(self) -> "FalaPackageSpec":
        _validate_unique_ids("fala package impulse type", self.impulse_types)
        _validate_unique_ids("fala package impulse relation", self.impulse_relations)
        _validate_unique_ids("fala package association kind", self.association_kinds)
        _validate_unique_ids("fala package reaction kind", self.reaction_kinds)
        _validate_unique_ids("fala package capability", self.capabilities)
        _validate_unique_ids("fala package correlation_path", self.correlation_paths)

        impulse_type_ids = {item.id for item in self.impulse_types}
        reaction_kind_ids = {item.id for item in self.reaction_kinds}
        association_kind_ids = {item.id for item in self.association_kinds}
        capability_ids = {item.id for item in self.capabilities}

        for impulse_type in self.impulse_types:
            validate_json_schema(
                impulse_type.value_schema,
                label=f"Impulse type {impulse_type.id!r} value_schema",
            )
            validate_json_schema(
                impulse_type.metadata_schema,
                label=f"Impulse type {impulse_type.id!r} metadata_schema",
            )

        for association_kind in self.association_kinds:
            validate_json_schema(
                association_kind.value_schema,
                label=f"Association kind {association_kind.id!r} value_schema",
            )
            validate_json_schema(
                association_kind.metadata_schema,
                label=f"Association kind {association_kind.id!r} metadata_schema",
            )

        for reaction_kind in self.reaction_kinds:
            validate_json_schema(
                reaction_kind.value_schema,
                label=f"Reaction kind {reaction_kind.id!r} value_schema",
            )
            validate_json_schema(
                reaction_kind.metadata_schema,
                label=f"Reaction kind {reaction_kind.id!r} metadata_schema",
            )

        for relation in self.impulse_relations:
            _validate_known_refs(
                f"Impulse relation {relation.id!r} source_impulse_types",
                relation.source_impulse_types,
                impulse_type_ids,
            )
            _validate_known_refs(
                f"Impulse relation {relation.id!r} target_impulse_types",
                relation.target_impulse_types,
                impulse_type_ids,
            )

        for capability in self.capabilities:
            validate_json_schema(
                capability.config_schema,
                label=f"Impulse capability {capability.id!r} config_schema",
            )
            validate_json_schema(
                capability.output_schema,
                label=f"Impulse capability {capability.id!r} output_schema",
            )
            _validate_known_refs(
                f"Impulse capability {capability.id!r} accepts_impulse_types",
                capability.accepts_impulse_types,
                impulse_type_ids,
            )
            _validate_known_refs(
                f"Impulse capability {capability.id!r} accepts_reaction_kinds",
                capability.accepts_reaction_kinds,
                reaction_kind_ids,
            )
            _validate_known_refs(
                f"Impulse capability {capability.id!r} emits_impulse_types",
                capability.emits_impulse_types,
                impulse_type_ids,
            )
            _validate_known_refs(
                f"Impulse capability {capability.id!r} emits_reaction_kinds",
                capability.emits_reaction_kinds,
                reaction_kind_ids,
            )
            _validate_known_refs(
                f"Impulse capability {capability.id!r} emits_association_kinds",
                capability.emits_association_kinds,
                association_kind_ids,
            )

        for correlation_path in self.correlation_paths:
            for effector in correlation_path.effectors:
                _validate_known_refs(
                    f"Impulse correlation_path {correlation_path.id!r} effector {effector.id!r} capability",
                    [effector.capability],
                    capability_ids,
                )

        return self


def _validate_acyclic(effectors: list[EffectorSpec]) -> None:
    graph = {effector.id: list(effector.conduction) for effector in effectors}
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visited:
            return
        if node in visiting:
            raise ValueError(f"Impulse correlation_path contains a cycle at effector {node!r}")
        visiting.add(node)
        for dependency in graph[node]:
            visit(dependency)
        visiting.remove(node)
        visited.add(node)

    for effector in effectors:
        visit(effector.id)


def _validate_unique_ids(label: str, items: list[Any]) -> None:
    ids = [item.id for item in items]
    duplicates = sorted({item for item in ids if ids.count(item) > 1})
    if duplicates:
        raise ValueError(f"Duplicate {label} id(s): {', '.join(duplicates)}")


def _validate_unique_values(label: str, values: list[str]) -> None:
    duplicates = sorted({item for item in values if values.count(item) > 1})
    if duplicates:
        raise ValueError(f"Duplicate {label} value(s): {', '.join(duplicates)}")


def _validate_known_refs(label: str, refs: list[str], known: set[str]) -> None:
    missing = sorted(set(refs) - known)
    if missing:
        raise ValueError(f"{label} reference unknown id(s): {', '.join(missing)}")


__all__ = [
    "ReactionKindSpec",
    "ReactionRef",
    "EffectorAdapterKind",
    "EffectorAdapterSpec",
    "ReactionStoreConfig",
    "CapabilitySpec",
    "CorrelationPathSpec",
    "EffectorSpec",
    "ImpulseRelationSpec",
    "JournalConfig",
    "RuntimeBackendConfig",
    "RuntimeConfigSpec",
    "ImpulseTypeSpec",
    "FalaPackageSpec",
    "AssociationKindSpec",
    "RUNTIME_ID_PATTERN",
    "RuntimeId",
    "new_id",
]
