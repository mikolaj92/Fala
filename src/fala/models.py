from __future__ import annotations

from typing import Annotated, Any, Literal
from uuid import uuid4

from jsonschema import Draft202012Validator, SchemaError
from pydantic import BaseModel, ConfigDict, Field, model_validator

RUNTIME_ID_PATTERN = r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$"
RuntimeId = Annotated[str, Field(pattern=RUNTIME_ID_PATTERN)]
CarrierAdapterKind = Literal[
    "fala_runtime",
    "manual_gate",
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


class ArtifactRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId = Field(default_factory=lambda: new_id("artifact"))
    kind: str
    uri: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactKindSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    media_types: list[str] = Field(default_factory=list)
    value_schema: dict[str, Any] = Field(default_factory=dict)
    metadata_schema: dict[str, Any] = Field(default_factory=dict)


class CarrierTypeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    media_types: list[str] = Field(default_factory=list)
    value_schema: dict[str, Any] = Field(default_factory=dict)
    metadata_schema: dict[str, Any] = Field(default_factory=dict)


class CarrierRelationSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    source_carrier_types: list[RuntimeId] = Field(default_factory=list)
    target_carrier_types: list[RuntimeId] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_carrier_types(self) -> "CarrierRelationSpec":
        _validate_unique_values(
            f"Carrier relation {self.id!r} source_carrier_types",
            self.source_carrier_types,
        )
        _validate_unique_values(
            f"Carrier relation {self.id!r} target_carrier_types",
            self.target_carrier_types,
        )
        return self


class ObservationKindSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    value_schema: dict[str, Any] = Field(default_factory=dict)
    metadata_schema: dict[str, Any] = Field(default_factory=dict)


class CarrierCapabilitySpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    accepts_carrier_types: list[RuntimeId] = Field(default_factory=list)
    accepts_artifact_kinds: list[RuntimeId] = Field(default_factory=list)
    emits_carrier_types: list[RuntimeId] = Field(default_factory=list)
    emits_artifact_kinds: list[RuntimeId] = Field(default_factory=list)
    emits_observation_kinds: list[RuntimeId] = Field(default_factory=list)
    config_schema: dict[str, Any] = Field(default_factory=dict)
    output_schema: dict[str, Any] = Field(default_factory=dict)


class CarrierAdapterSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: CarrierAdapterKind
    command: list[str] | None = None
    ref: str | None = None
    runtime_ref: str | None = None
    cwd: str | None = None
    env: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_adapter_boundary(self) -> "CarrierAdapterSpec":
        if self.kind == "subprocess":
            if not self.command:
                raise ValueError("subprocess adapter requires non-empty command")
            if self.ref is not None:
                raise ValueError("subprocess adapter cannot define ref")
            if self.runtime_ref is not None:
                raise ValueError("subprocess adapter cannot define runtime_ref")
            return self

        if self.kind == "python_function":
            if not self.ref:
                raise ValueError("python_function adapter requires ref")
            if self.command is not None:
                raise ValueError("python_function adapter cannot define command")
            if self.runtime_ref is not None:
                raise ValueError("python_function adapter cannot define runtime_ref")
            if self.cwd is not None:
                raise ValueError("python_function adapter cannot define cwd")
            return self

        if self.kind == "manual_gate":
            if self.command is not None:
                raise ValueError("manual_gate adapter cannot define command")
            if self.ref is not None:
                raise ValueError("manual_gate adapter cannot define ref")
            if self.runtime_ref is not None:
                raise ValueError("manual_gate adapter cannot define runtime_ref")
            if self.cwd is not None:
                raise ValueError("manual_gate adapter cannot define cwd")
            if self.env:
                raise ValueError("manual_gate adapter cannot define env")
            if self.timeout_seconds is not None:
                raise ValueError("manual_gate adapter cannot define timeout_seconds")
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


class CarrierFlowStepSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    capability: RuntimeId
    adapter: CarrierAdapterSpec
    needs: list[RuntimeId] = Field(default_factory=list)
    timeout_seconds: float | None = Field(default=None, gt=0)
    config: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_needs(self) -> "CarrierFlowStepSpec":
        _validate_unique_values(f"Carrier flow step {self.id!r} needs", self.needs)
        if self.id in self.needs:
            raise ValueError(f"Carrier flow step {self.id!r} cannot depend on itself")
        return self


class CarrierFlowSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    steps: list[CarrierFlowStepSpec] = Field(min_length=1)
    allow_feedback_cycles: bool = False

    @model_validator(mode="after")
    def validate_steps(self) -> "CarrierFlowSpec":
        _validate_unique_ids(f"Carrier flow {self.id!r} step", self.steps)
        known = {step.id for step in self.steps}
        for step in self.steps:
            _validate_known_refs(
                f"Carrier flow {self.id!r} step {step.id!r} needs",
                step.needs,
                known,
            )
        if not self.allow_feedback_cycles:
            _validate_acyclic(self.steps)
        return self


class CarrierRuntimeBackendConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["sqlite"] = "sqlite"
    path: str


class CarrierArtifactStoreConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["filesystem"] = "filesystem"
    root: str


class CarrierRuntimeConfigSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    backend: CarrierRuntimeBackendConfig
    artifact_store: CarrierArtifactStoreConfig


class CarrierWorkflowPackageSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    version: str = "2"
    carrier_types: list[CarrierTypeSpec] = Field(default_factory=list)
    carrier_relations: list[CarrierRelationSpec] = Field(default_factory=list)
    observation_kinds: list[ObservationKindSpec] = Field(default_factory=list)
    artifact_kinds: list[ArtifactKindSpec] = Field(default_factory=list)
    capabilities: list[CarrierCapabilitySpec] = Field(default_factory=list)
    flows: list[CarrierFlowSpec] = Field(min_length=1)
    runtime: CarrierRuntimeConfigSpec | None = None

    @model_validator(mode="after")
    def validate_carrier_package(self) -> "CarrierWorkflowPackageSpec":
        _validate_unique_ids("carrier package carrier type", self.carrier_types)
        _validate_unique_ids("carrier package carrier relation", self.carrier_relations)
        _validate_unique_ids("carrier package observation kind", self.observation_kinds)
        _validate_unique_ids("carrier package artifact kind", self.artifact_kinds)
        _validate_unique_ids("carrier package capability", self.capabilities)
        _validate_unique_ids("carrier package flow", self.flows)

        carrier_type_ids = {item.id for item in self.carrier_types}
        artifact_kind_ids = {item.id for item in self.artifact_kinds}
        observation_kind_ids = {item.id for item in self.observation_kinds}
        capability_ids = {item.id for item in self.capabilities}

        for carrier_type in self.carrier_types:
            validate_json_schema(
                carrier_type.value_schema,
                label=f"Carrier type {carrier_type.id!r} value_schema",
            )
            validate_json_schema(
                carrier_type.metadata_schema,
                label=f"Carrier type {carrier_type.id!r} metadata_schema",
            )

        for observation_kind in self.observation_kinds:
            validate_json_schema(
                observation_kind.value_schema,
                label=f"Observation kind {observation_kind.id!r} value_schema",
            )
            validate_json_schema(
                observation_kind.metadata_schema,
                label=f"Observation kind {observation_kind.id!r} metadata_schema",
            )

        for artifact_kind in self.artifact_kinds:
            validate_json_schema(
                artifact_kind.value_schema,
                label=f"Artifact kind {artifact_kind.id!r} value_schema",
            )
            validate_json_schema(
                artifact_kind.metadata_schema,
                label=f"Artifact kind {artifact_kind.id!r} metadata_schema",
            )

        for relation in self.carrier_relations:
            _validate_known_refs(
                f"Carrier relation {relation.id!r} source_carrier_types",
                relation.source_carrier_types,
                carrier_type_ids,
            )
            _validate_known_refs(
                f"Carrier relation {relation.id!r} target_carrier_types",
                relation.target_carrier_types,
                carrier_type_ids,
            )

        for capability in self.capabilities:
            validate_json_schema(
                capability.config_schema,
                label=f"Carrier capability {capability.id!r} config_schema",
            )
            validate_json_schema(
                capability.output_schema,
                label=f"Carrier capability {capability.id!r} output_schema",
            )
            _validate_known_refs(
                f"Carrier capability {capability.id!r} accepts_carrier_types",
                capability.accepts_carrier_types,
                carrier_type_ids,
            )
            _validate_known_refs(
                f"Carrier capability {capability.id!r} accepts_artifact_kinds",
                capability.accepts_artifact_kinds,
                artifact_kind_ids,
            )
            _validate_known_refs(
                f"Carrier capability {capability.id!r} emits_carrier_types",
                capability.emits_carrier_types,
                carrier_type_ids,
            )
            _validate_known_refs(
                f"Carrier capability {capability.id!r} emits_artifact_kinds",
                capability.emits_artifact_kinds,
                artifact_kind_ids,
            )
            _validate_known_refs(
                f"Carrier capability {capability.id!r} emits_observation_kinds",
                capability.emits_observation_kinds,
                observation_kind_ids,
            )

        for flow in self.flows:
            for step in flow.steps:
                _validate_known_refs(
                    f"Carrier flow {flow.id!r} step {step.id!r} capability",
                    [step.capability],
                    capability_ids,
                )

        return self


def _validate_acyclic(steps: list[CarrierFlowStepSpec]) -> None:
    graph = {step.id: list(step.needs) for step in steps}
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visited:
            return
        if node in visiting:
            raise ValueError(f"Carrier flow contains a cycle at step {node!r}")
        visiting.add(node)
        for dependency in graph[node]:
            visit(dependency)
        visiting.remove(node)
        visited.add(node)

    for step in steps:
        visit(step.id)


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
    "ArtifactKindSpec",
    "ArtifactRef",
    "CarrierAdapterKind",
    "CarrierAdapterSpec",
    "CarrierArtifactStoreConfig",
    "CarrierCapabilitySpec",
    "CarrierFlowSpec",
    "CarrierFlowStepSpec",
    "CarrierRelationSpec",
    "CarrierRuntimeBackendConfig",
    "CarrierRuntimeConfigSpec",
    "CarrierTypeSpec",
    "CarrierWorkflowPackageSpec",
    "ObservationKindSpec",
    "RUNTIME_ID_PATTERN",
    "RuntimeId",
    "new_id",
]
