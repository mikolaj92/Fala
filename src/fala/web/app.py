from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from fala.auth import RuntimeAccessPolicy
from fala.project import project_pipeline_dir, resolve_project_yaml
from fala.queue_bridge import QueueBrokerTransport, create_queue_broker_transport
from fala.registry import PipelineRegistry
from fala.routes import create_runtime_router
from fala.service import RuntimeService
from fala.store import StateStore
from fala.store_factory import create_state_store, default_state_store_target
from fala.web.routes import create_runtime_web_router

STATIC_DIR = Path(__file__).resolve().parent / "static"


def create_runtime_web_app(
    *,
    service: RuntimeService | None = None,
    registry: PipelineRegistry | None = None,
    store: StateStore | None = None,
    pipeline_dir: str | os.PathLike[str] | None = None,
    db: str | os.PathLike[str] | None = None,
    artifact_roots: list[str | Path] | None = None,
    artifact_store: str | os.PathLike[str] | None = None,
    artifact_store_root: str | os.PathLike[str] | None = None,
    queue_broker: str | os.PathLike[str] | None = None,
    queue_db: str | os.PathLike[str] | None = None,
    project_dir: str | os.PathLike[str] | None = None,
    project_yaml: str | os.PathLike[str] | None = None,
    title: str = "Fala",
    access_policy: RuntimeAccessPolicy | None = None,
) -> FastAPI:
    resolved_project_yaml = resolve_project_yaml(
        project_dir=project_dir or os.environ.get("FALA_PROJECT_DIR"),
        project_yaml=project_yaml or os.environ.get("FALA_PROJECT_YAML"),
    )
    if service is None:
        if registry is None:
            if pipeline_dir is None and resolved_project_yaml is not None:
                pipeline_dir = project_pipeline_dir(resolved_project_yaml)
            pipeline_root = Path(
                pipeline_dir
                or os.environ.get("FALA_PIPELINE_DIR")
                or "examples/pipelines"
            )
            registry = PipelineRegistry.from_directory(pipeline_root)
        if store is None:
            store = create_state_store(default_state_store_target(db))
        service = RuntimeService(
            registry=registry,
            store=store,
            artifact_roots=artifact_roots,
            artifact_store_root=artifact_store or artifact_store_root,
        )

    policy = access_policy or RuntimeAccessPolicy.from_env()
    queue_transport = _queue_transport_from_target(
        queue_broker=queue_broker,
        queue_db=queue_db,
    )
    app = FastAPI(title=title)
    mount_runtime_web(
        app,
        service=service,
        title=title,
        access_policy=policy,
        queue_transport=queue_transport,
        project_yaml=resolved_project_yaml,
    )
    app.include_router(
        create_runtime_router(
            service,
            access_policy=policy,
            project_yaml=resolved_project_yaml,
        ),
        prefix="/api",
    )
    return app


def mount_runtime_web(
    app: FastAPI,
    *,
    service: RuntimeService,
    title: str = "Fala",
    static_path: str = "/static",
    access_policy: RuntimeAccessPolicy | None = None,
    queue_broker: str | os.PathLike[str] | None = None,
    queue_db: str | os.PathLike[str] | None = None,
    queue_transport: QueueBrokerTransport | None = None,
    project_yaml: str | os.PathLike[str] | None = None,
) -> None:
    app.mount(static_path, StaticFiles(directory=str(STATIC_DIR)), name="static")
    app.include_router(
        create_runtime_web_router(
            service,
            title=title,
            static_path=static_path,
            access_policy=access_policy,
            queue_transport=queue_transport
            or _queue_transport_from_target(
                queue_broker=queue_broker,
                queue_db=queue_db,
            ),
            project_yaml=project_yaml,
        )
    )


def _queue_transport_from_target(
    *,
    queue_broker: str | os.PathLike[str] | None,
    queue_db: str | os.PathLike[str] | None,
) -> QueueBrokerTransport | None:
    target = (
        queue_broker
        or queue_db
        or os.environ.get("FALA_QUEUE_BROKER")
        or os.environ.get("FALA_QUEUE_DB")
    )
    if not target:
        return None
    return create_queue_broker_transport(target)
