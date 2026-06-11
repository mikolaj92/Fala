from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlencode, urlparse

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import Environment, FileSystemLoader, select_autoescape
from starlette.datastructures import UploadFile

from fala.auth import (
    RuntimeAccessPolicy,
    RuntimeAuthError,
    principal_from_request,
    web_permission_for_request,
)
from fala.blueprints import (
    get_scaffold_blueprint,
    list_scaffold_blueprints,
    scaffold_blueprint_summary,
)
from fala.package_registry import build_workflow_readiness_report
from fala.project import (
    build_project_alert_report,
    build_project_bootstrap_check,
    build_project_bootstrap_commands,
    build_project_lifecycle_report,
    build_project_operations_report,
    build_project_run_history,
    build_project_readiness_report,
    build_project_runtime_run_input,
    build_project_spec_report,
    build_project_supervision_report,
)
from fala.models import (
    ProcessAction,
    ProcessOutput,
    ProcessStatus,
    RunStatus,
    RuntimeDocumentInput,
    RuntimeDocumentStatus,
    RuntimeRunInput,
)
from fala.queue_bridge import QueueBrokerTransport
from fala.service import RuntimeService
from fala.store_factory import runtime_db_diagnostics, state_store_diagnostics_target

TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"


def create_runtime_web_router(
    service: RuntimeService,
    *,
    title: str = "Fala",
    static_path: str = "/static",
    access_policy: RuntimeAccessPolicy | None = None,
    queue_transport: QueueBrokerTransport | None = None,
    project_yaml: str | Path | None = None,
) -> APIRouter:
    policy = access_policy or RuntimeAccessPolicy.disabled()

    async def authorize_request(request: Request) -> None:
        try:
            principal = policy.require(request, web_permission_for_request(request))
            run_id = request.path_params.get("run_id")
            if isinstance(run_id, str):
                run = await service.get_run(run_id)
                if run is not None:
                    policy.require_run_metadata(principal, run.metadata)
        except RuntimeAuthError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

    router = APIRouter(dependencies=[Depends(authorize_request)])

    async def visible_run_summaries(
        request: Request,
        limit: int,
        *,
        project_id: str | None = None,
    ) -> list[dict[str, Any]]:
        runs = await service.list_run_summaries(limit=limit)
        principal = principal_from_request(request)
        visible: list[dict[str, Any]] = []
        for run_summary in runs:
            run = await service.get_run(str(run_summary["run_id"]))
            run_metadata = run.metadata if run is not None else {}
            try:
                policy.require_run_metadata(
                    principal,
                    run_metadata,
                )
            except RuntimeAuthError:
                continue
            if project_id and not _run_metadata_matches_project(
                run_metadata,
                project_id,
            ):
                continue
            visible.append(run_summary)
        return visible

    def project_report() -> dict[str, Any] | None:
        if project_yaml is None:
            return None
        return build_project_readiness_report(project_yaml, registry=service.registry)

    def collect_project_bootstrap_report(
        *,
        base_url: str,
        run_id: str | None,
    ) -> dict[str, Any] | None:
        if project_yaml is None:
            return None
        db_target = state_store_diagnostics_target(service.store)
        db_report = (
            runtime_db_diagnostics(db_target, ensure_schema=False)
            if db_target is not None
            else None
        )
        check = build_project_bootstrap_check(
            project_yaml,
            registry=service.registry,
            base_url=base_url,
            run_id=run_id,
            db=db_report,
        )
        return {
            "ok": check["ok"],
            "project_id": check.get("project_id"),
            "db_configured": db_target is not None,
            "db_target": db_target,
            "check": check,
            "db": db_report,
            "commands": build_project_bootstrap_commands(
                project_yaml=project_yaml,
                db_target=db_target,
                base_url=base_url,
                run_id=check["run_id"],
            ),
        }

    async def collect_project_supervision_report(
        request: Request,
        *,
        package_id: str | None,
        pipeline_id: str | None,
        document_type: str | None,
        operation_type: str | None,
        stuck_status: ProcessStatus | None,
        waiting_after_seconds: float,
        queued_after_seconds: float,
        running_after_seconds: float,
        consumer_id: str | None,
        min_lag: int,
        over_limit: bool | None,
        limit: int,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        report = project_report()
        if report is None:
            return None, None
        project_id = str(report.get("project_id") or "") or None
        runs = await visible_run_summaries(
            request,
            500,
            project_id=project_id,
        )
        history = build_project_run_history(
            project_id=project_id,
            registry=service.registry,
            runs=runs,
            package_id=package_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            limit=None,
        )
        dead_letter_pages: list[dict[str, Any]] = []
        stuck_work_pages: list[dict[str, Any]] = []
        stream_lag_pages: list[dict[str, Any]] = []
        for run in history["runs"]:
            run_id = str(run["run_id"])
            dead_letter_pages.append(
                (
                    await service.dead_letter_queue(
                        run_id,
                        pipeline_id=pipeline_id,
                        document_type=document_type,
                        operation_type=operation_type,
                        limit=1000,
                    )
                ).model_dump(mode="json")
            )
            stuck_work_pages.append(
                (
                    await service.stuck_work(
                        run_id,
                        status=stuck_status,
                        pipeline_id=pipeline_id,
                        document_type=document_type,
                        operation_type=operation_type,
                        waiting_after_seconds=waiting_after_seconds,
                        queued_after_seconds=queued_after_seconds,
                        running_after_seconds=running_after_seconds,
                        limit=1000,
                    )
                ).model_dump(mode="json")
            )
            stream_lag_pages.append(
                (
                    await service.stream_lag(
                        run_id,
                        pipeline_id=pipeline_id,
                        document_type=document_type,
                        operation_type=operation_type,
                        consumer_id=consumer_id,
                        min_lag=min_lag,
                        over_limit=over_limit,
                        limit=1000,
                    )
                ).model_dump(mode="json")
            )
        supervision = build_project_supervision_report(
            project_id=project_id,
            registry=service.registry,
            runs=history["runs"],
            dead_letter_pages=dead_letter_pages,
            stuck_work_pages=stuck_work_pages,
            stream_lag_pages=stream_lag_pages,
            package_id=package_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            operation_type=operation_type,
            limit=limit,
        )
        supervision["filters"].update(
            {
                "stuck_status": (
                    stuck_status.value if stuck_status is not None else None
                ),
                "waiting_after_seconds": waiting_after_seconds,
                "queued_after_seconds": queued_after_seconds,
                "running_after_seconds": running_after_seconds,
                "operation_type": operation_type,
                "consumer_id": consumer_id,
                "min_lag": min_lag,
                "over_limit": over_limit,
            }
        )
        return report, supervision

    async def collect_project_operations_report(
        request: Request,
        *,
        package_id: str | None,
        pipeline_id: str | None,
        document_type: str | None,
        operation_type: str | None,
        stuck_status: ProcessStatus | None,
        waiting_after_seconds: float,
        queued_after_seconds: float,
        running_after_seconds: float,
        consumer_id: str | None,
        min_lag: int,
        over_limit: bool | None,
        stale_after_seconds: float,
        limit: int,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        report = project_report()
        if report is None:
            return None, None
        project_id = str(report.get("project_id") or "") or None
        runs = await visible_run_summaries(
            request,
            500,
            project_id=project_id,
        )
        history = build_project_run_history(
            project_id=project_id,
            registry=service.registry,
            runs=runs,
            package_id=package_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            limit=None,
        )
        _supervision_project, supervision = await collect_project_supervision_report(
            request,
            package_id=package_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            operation_type=operation_type,
            stuck_status=stuck_status,
            waiting_after_seconds=waiting_after_seconds,
            queued_after_seconds=queued_after_seconds,
            running_after_seconds=running_after_seconds,
            consumer_id=consumer_id,
            min_lag=min_lag,
            over_limit=over_limit,
            limit=limit,
        )
        health_reports: list[dict[str, Any]] = []
        for run in history["runs"]:
            health_reports.append(
                (
                    await service.run_health(
                        str(run["run_id"]),
                        stale_after_seconds=stale_after_seconds,
                    )
                ).model_dump(mode="json")
            )
        operations = build_project_operations_report(
            project_id=project_id,
            registry=service.registry,
            runs=history["runs"],
            health_reports=health_reports,
            supervision=supervision,
            package_id=package_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            operation_type=operation_type,
            limit=limit,
        )
        operations["filters"].update(
            {
                "stuck_status": (
                    stuck_status.value if stuck_status is not None else None
                ),
                "waiting_after_seconds": waiting_after_seconds,
                "queued_after_seconds": queued_after_seconds,
                "running_after_seconds": running_after_seconds,
                "operation_type": operation_type,
                "consumer_id": consumer_id,
                "min_lag": min_lag,
                "over_limit": over_limit,
                "stale_after_seconds": stale_after_seconds,
            }
        )
        return report, operations

    async def collect_project_alert_report(
        request: Request,
        *,
        package_id: str | None,
        pipeline_id: str | None,
        document_type: str | None,
        operation_type: str | None,
        stuck_status: ProcessStatus | None,
        waiting_after_seconds: float,
        queued_after_seconds: float,
        running_after_seconds: float,
        consumer_id: str | None,
        min_lag: int,
        over_limit: bool | None,
        stale_after_seconds: float,
        limit: int,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None]:
        report, operations = await collect_project_operations_report(
            request,
            package_id=package_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            operation_type=operation_type,
            stuck_status=stuck_status,
            waiting_after_seconds=waiting_after_seconds,
            queued_after_seconds=queued_after_seconds,
            running_after_seconds=running_after_seconds,
            consumer_id=consumer_id,
            min_lag=min_lag,
            over_limit=over_limit,
            stale_after_seconds=stale_after_seconds,
            limit=limit,
        )
        if report is None or operations is None or project_yaml is None:
            return report, None, operations
        return (
            report,
            build_project_alert_report(project_yaml, operations=operations),
            operations,
        )

    async def collect_project_lifecycle_report(
        request: Request,
        *,
        package_id: str | None,
        pipeline_id: str | None,
        document_type: str | None,
        before: str | None,
        older_than_days: float | None,
        statuses: list[RunStatus] | None,
        include_artifact_gc: bool,
        limit: int,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        report = project_report()
        if report is None or project_yaml is None:
            return report, None
        project_id = str(report.get("project_id") or "") or None
        runs = await visible_run_summaries(
            request,
            500,
            project_id=project_id,
        )
        history = build_project_run_history(
            project_id=project_id,
            registry=service.registry,
            runs=runs,
            package_id=package_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            limit=None,
        )
        artifact_gc = (
            (await service.artifact_gc(dry_run=True)).model_dump(mode="json")
            if include_artifact_gc
            else None
        )
        lifecycle = build_project_lifecycle_report(
            project_yaml,
            runs=history["runs"],
            before=before,
            older_than_days=older_than_days,
            statuses=statuses,
            artifact_gc=artifact_gc,
            dry_run=True,
            limit=limit,
        )
        lifecycle["filters"].update(
            {
                "package_id": package_id,
                "pipeline_id": pipeline_id,
                "document_type": document_type,
                "include_artifact_gc": include_artifact_gc,
            }
        )
        return report, lifecycle

    templates = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape(("html", "xml")),
        cache_size=0,
    )

    def render(template_name: str, context: dict[str, Any]) -> HTMLResponse:
        context.setdefault("app_title", title)
        context.setdefault("static_path", static_path.rstrip("/"))
        context.setdefault("project_configured", project_yaml is not None)
        html = templates.get_template(template_name).render(context)
        return HTMLResponse(html)

    def operator_actor(request: Request) -> str:
        principal = principal_from_request(request)
        if principal is not None:
            return principal.actor
        return (
            request.headers.get("x-fala-actor")
            or request.headers.get("x-operator-id")
            or request.headers.get("x-user-email")
            or "web-panel"
        )

    def operator_source(request: Request) -> str:
        principal = principal_from_request(request)
        if principal is not None:
            return principal.source
        return request.headers.get("x-fala-source") or "web"

    async def audit(
        request: Request,
        *,
        action: str,
        run_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        target: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        await service.record_operator_audit(
            actor=operator_actor(request),
            source=operator_source(request),
            action=action,
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            target=target,
            data=data,
        )

    async def run_state_or_404(
        run_id: str,
        *,
        include_events: bool = False,
    ) -> dict[str, Any]:
        state = await service.load_state(run_id, include_events=include_events)
        if not state.get("documents"):
            run_ids = {row["run_id"] for row in await service.store.list_runs(limit=None)}
            if run_id not in run_ids:
                raise HTTPException(status_code=404, detail="Run not found")
        return state

    async def run_exists_or_404(run_id: str) -> None:
        if await service.get_run(run_id) is not None:
            return
        run_ids = {row["run_id"] for row in await service.store.list_runs(limit=None)}
        if run_id not in run_ids:
            raise HTTPException(status_code=404, detail="Run not found")

    async def render_runtime_partial(request: Request, run_id: str) -> HTMLResponse:
        runtime = await run_state_or_404(run_id, include_events=True)
        metrics = await service.queue_metrics(run_id)
        health = await service.run_health(run_id)
        trace = await service.process_trace(run_id)
        workers = await service.worker_health(run_id)
        _prepare_runtime_documents(runtime)
        return render(
            "run_process_runtime_partial.html",
            {
                "request": request,
                "run_id": run_id,
                "runtime": runtime,
                "metrics": metrics.model_dump(mode="json"),
                "health": health.model_dump(mode="json"),
                "trace": trace.model_dump(mode="json"),
                "workers": [worker.model_dump(mode="json") for worker in workers],
            },
        )

    async def broker_queue_context(
        *,
        queue: str | None = None,
        state: str | None = "dead_letter",
        limit: int = 100,
        message: str | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        broker: dict[str, Any] = {
            "configured": queue_transport is not None,
            "records": [],
            "stats": None,
            "filters": {
                "queue": queue,
                "state": state,
                "limit": limit,
            },
            "message": message,
            "error": error,
        }
        if queue_transport is not None:
            records = await queue_transport.list_work_records(
                queue=queue or None,
                state=state or None,
                limit=limit,
                include_payload=True,
            )
            broker["records"] = [
                record.model_dump(mode="json", exclude_none=True)
                for record in records
            ]
            broker["stats"] = await queue_transport.stats()
        return broker

    async def render_broker_queue_partial(
        request: Request,
        *,
        queue: str | None = None,
        state: str | None = "dead_letter",
        limit: int = 100,
        message: str | None = None,
        error: str | None = None,
    ) -> HTMLResponse:
        broker = await broker_queue_context(
            queue=queue,
            state=state,
            limit=limit,
            message=message,
            error=error,
        )
        return render(
            "queue_broker_partial.html",
            {
                "request": request,
                "broker": broker,
                "queue_states": [
                    "dead_letter",
                    "ready",
                    "leased",
                    "completed",
                    "failed",
                ],
            },
        )

    async def render_lineage_partial(request: Request, run_id: str) -> HTMLResponse:
        await run_state_or_404(run_id)
        lineage = await service.document_lineage(run_id)
        return render(
            "run_process_runtime_lineage_partial.html",
            {
                "request": request,
                "run_id": run_id,
                "lineage": lineage.model_dump(mode="json"),
            },
        )

    async def render_results_partial(
        request: Request,
        run_id: str,
        *,
        pipeline_id: str | None = None,
        process_id: str | None = None,
        document_id: str | None = None,
        document_type: str | None = None,
        limit: int = 50,
    ) -> HTMLResponse:
        await run_state_or_404(run_id)
        results = await service.run_results(
            run_id,
            pipeline_id=pipeline_id,
            process_id=process_id,
            document_id=document_id,
            document_type=document_type,
        )
        return render(
            "run_process_runtime_results_partial.html",
            {
                "request": request,
                "run_id": run_id,
                "results": results.model_dump(mode="json"),
                "visible_results": [
                    item.model_dump(mode="json") for item in results.results[:limit]
                ],
                "limit": limit,
            },
        )

    async def render_output_documents_partial(
        request: Request,
        run_id: str,
        *,
        pipeline_id: str | None = None,
        process_id: str | None = None,
        document_id: str | None = None,
        source_document_type: str | None = None,
        output_document_id: str | None = None,
        document_type: str | None = None,
        relation: str | None = None,
        media_type: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> HTMLResponse:
        await run_state_or_404(run_id)
        page = await service.output_documents(
            run_id,
            pipeline_id=pipeline_id,
            process_id=process_id,
            document_id=document_id,
            source_document_type=source_document_type,
            output_document_id=output_document_id,
            document_type=document_type,
            relation=relation,
            media_type=media_type,
            limit=limit,
            offset=offset,
        )
        return render(
            "run_process_runtime_output_documents_partial.html",
            {
                "request": request,
                "run_id": run_id,
                "page": page.model_dump(mode="json"),
            },
        )

    async def render_documents_partial(
        request: Request,
        run_id: str,
        *,
        status: RuntimeDocumentStatus | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        relation: str | None = None,
        parent_document_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> HTMLResponse:
        await run_exists_or_404(run_id)
        page = await service.document_registry(
            run_id,
            status=status,
            pipeline_id=pipeline_id,
            document_type=document_type,
            relation=relation,
            parent_document_id=parent_document_id,
            limit=limit,
            offset=offset,
        )
        filters = {
            key: value
            for key, value in {
                "status": status.value if status is not None else None,
                "pipeline_id": pipeline_id,
                "document_type": document_type,
                "relation": relation,
                "parent_document_id": parent_document_id,
            }.items()
            if value
        }
        base_params = {**filters, "limit": str(limit)}
        previous_url = None
        if offset > 0:
            previous_url = (
                f"/runs/{run_id}/process-runtime/documents?"
                + urlencode({**base_params, "offset": str(max(0, offset - limit))})
            )
        next_url = None
        if page.has_more:
            next_url = (
                f"/runs/{run_id}/process-runtime/documents?"
                + urlencode({**base_params, "offset": str(offset + limit)})
            )
        return render(
            "run_process_runtime_documents_partial.html",
            {
                "request": request,
                "run_id": run_id,
                "page": page.model_dump(mode="json"),
                "filters": filters,
                "status_choices": [item.value for item in RuntimeDocumentStatus],
                "limit": limit,
                "offset": offset,
                "previous_url": previous_url,
                "next_url": next_url,
            },
        )

    async def render_processes_partial(
        request: Request,
        run_id: str,
        *,
        status: ProcessStatus | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        parent_document_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        capability: str | None = None,
        operation_type: str | None = None,
        adapter_kind: str | None = None,
        resource_pool: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> HTMLResponse:
        await run_exists_or_404(run_id)
        page = await service.process_registry(
            run_id,
            status=status,
            pipeline_id=pipeline_id,
            document_type=document_type,
            parent_document_id=parent_document_id,
            document_id=document_id,
            process_id=process_id,
            capability=capability,
            operation_type=operation_type,
            adapter_kind=adapter_kind,
            resource_pool=resource_pool,
            limit=limit,
            offset=offset,
        )
        filters = {
            key: value
            for key, value in {
                "status": status.value if status is not None else None,
                "pipeline_id": pipeline_id,
                "document_type": document_type,
                "parent_document_id": parent_document_id,
                "document_id": document_id,
                "process_id": process_id,
                "capability": capability,
                "operation_type": operation_type,
                "adapter_kind": adapter_kind,
                "resource_pool": resource_pool,
            }.items()
            if value
        }
        base_params = {**filters, "limit": str(limit)}
        previous_url = None
        if offset > 0:
            previous_url = (
                f"/runs/{run_id}/process-runtime/processes?"
                + urlencode({**base_params, "offset": str(max(0, offset - limit))})
            )
        next_url = None
        if page.has_more:
            next_url = (
                f"/runs/{run_id}/process-runtime/processes?"
                + urlencode({**base_params, "offset": str(offset + limit)})
            )
        return render(
            "run_process_runtime_processes_partial.html",
            {
                "request": request,
                "run_id": run_id,
                "page": page.model_dump(mode="json"),
                "filters": filters,
                "status_choices": [item.value for item in ProcessStatus],
                "limit": limit,
                "offset": offset,
                "previous_url": previous_url,
                "next_url": next_url,
            },
        )

    async def render_dead_letter_partial(
        request: Request,
        run_id: str,
        *,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        parent_document_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        capability: str | None = None,
        operation_type: str | None = None,
        adapter_kind: str | None = None,
        resource_pool: str | None = None,
        limit: int = 50,
        offset: int = 0,
        message: str | None = None,
        error: str | None = None,
    ) -> HTMLResponse:
        await run_exists_or_404(run_id)
        page = await service.dead_letter_queue(
            run_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            parent_document_id=parent_document_id,
            document_id=document_id,
            process_id=process_id,
            capability=capability,
            operation_type=operation_type,
            adapter_kind=adapter_kind,
            resource_pool=resource_pool,
            limit=limit,
            offset=offset,
        )
        filters = {
            key: value
            for key, value in {
                "pipeline_id": pipeline_id,
                "document_type": document_type,
                "parent_document_id": parent_document_id,
                "document_id": document_id,
                "process_id": process_id,
                "capability": capability,
                "operation_type": operation_type,
                "adapter_kind": adapter_kind,
                "resource_pool": resource_pool,
            }.items()
            if value
        }
        base_params = {**filters, "limit": str(limit)}
        refresh_url = (
            f"/runs/{run_id}/process-runtime/dead-letter?"
            + urlencode({**base_params, "offset": str(offset)})
        )
        previous_url = None
        if offset > 0:
            previous_url = (
                f"/runs/{run_id}/process-runtime/dead-letter?"
                + urlencode({**base_params, "offset": str(max(0, offset - limit))})
            )
        next_url = None
        if page.has_more:
            next_url = (
                f"/runs/{run_id}/process-runtime/dead-letter?"
                + urlencode({**base_params, "offset": str(offset + limit)})
            )
        return render(
            "run_process_runtime_dead_letter_partial.html",
            {
                "request": request,
                "run_id": run_id,
                "page": page.model_dump(mode="json"),
                "filters": filters,
                "limit": limit,
                "offset": offset,
                "previous_url": previous_url,
                "next_url": next_url,
                "refresh_url": refresh_url,
                "message": message,
                "error": error,
            },
        )

    async def render_stuck_work_partial(
        request: Request,
        run_id: str,
        *,
        status: ProcessStatus | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        parent_document_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        capability: str | None = None,
        operation_type: str | None = None,
        adapter_kind: str | None = None,
        resource_pool: str | None = None,
        waiting_after_seconds: float = 3600.0,
        queued_after_seconds: float = 600.0,
        running_after_seconds: float = 1800.0,
        limit: int = 50,
        offset: int = 0,
        message: str | None = None,
        error: str | None = None,
    ) -> HTMLResponse:
        await run_exists_or_404(run_id)
        page = await service.stuck_work(
            run_id,
            status=status,
            pipeline_id=pipeline_id,
            document_type=document_type,
            parent_document_id=parent_document_id,
            document_id=document_id,
            process_id=process_id,
            capability=capability,
            operation_type=operation_type,
            adapter_kind=adapter_kind,
            resource_pool=resource_pool,
            waiting_after_seconds=waiting_after_seconds,
            queued_after_seconds=queued_after_seconds,
            running_after_seconds=running_after_seconds,
            limit=limit,
            offset=offset,
        )
        filters = {
            key: value
            for key, value in {
                "status": status.value if status is not None else None,
                "pipeline_id": pipeline_id,
                "document_type": document_type,
                "parent_document_id": parent_document_id,
                "document_id": document_id,
                "process_id": process_id,
                "capability": capability,
                "operation_type": operation_type,
                "adapter_kind": adapter_kind,
                "resource_pool": resource_pool,
            }.items()
            if value
        }
        threshold_params = {
            "waiting_after_seconds": str(waiting_after_seconds),
            "queued_after_seconds": str(queued_after_seconds),
            "running_after_seconds": str(running_after_seconds),
        }
        base_params = {**filters, **threshold_params, "limit": str(limit)}
        refresh_url = (
            f"/runs/{run_id}/process-runtime/stuck-work?"
            + urlencode({**base_params, "offset": str(offset)})
        )
        previous_url = None
        if offset > 0:
            previous_url = (
                f"/runs/{run_id}/process-runtime/stuck-work?"
                + urlencode({**base_params, "offset": str(max(0, offset - limit))})
            )
        next_url = None
        if page.has_more:
            next_url = (
                f"/runs/{run_id}/process-runtime/stuck-work?"
                + urlencode({**base_params, "offset": str(offset + limit)})
            )
        return render(
            "run_process_runtime_stuck_work_partial.html",
            {
                "request": request,
                "run_id": run_id,
                "page": page.model_dump(mode="json"),
                "filters": filters,
                "status_choices": [
                    ProcessStatus.waiting.value,
                    ProcessStatus.queued.value,
                    ProcessStatus.running.value,
                ],
                "thresholds": {
                    "waiting_after_seconds": waiting_after_seconds,
                    "queued_after_seconds": queued_after_seconds,
                    "running_after_seconds": running_after_seconds,
                },
                "limit": limit,
                "offset": offset,
                "previous_url": previous_url,
                "next_url": next_url,
                "refresh_url": refresh_url,
                "message": message,
                "error": error,
            },
        )

    async def render_stream_lag_partial(
        request: Request,
        run_id: str,
        *,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        parent_document_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        capability: str | None = None,
        operation_type: str | None = None,
        adapter_kind: str | None = None,
        resource_pool: str | None = None,
        stream_id: str | None = None,
        consumer_id: str | None = None,
        min_lag: int = 1,
        over_limit: bool | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> HTMLResponse:
        await run_exists_or_404(run_id)
        page = await service.stream_lag(
            run_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            parent_document_id=parent_document_id,
            document_id=document_id,
            process_id=process_id,
            capability=capability,
            operation_type=operation_type,
            adapter_kind=adapter_kind,
            resource_pool=resource_pool,
            stream_id=stream_id,
            consumer_id=consumer_id,
            min_lag=min_lag,
            over_limit=over_limit,
            limit=limit,
            offset=offset,
        )
        filters = {
            key: value
            for key, value in {
                "pipeline_id": pipeline_id,
                "document_type": document_type,
                "parent_document_id": parent_document_id,
                "document_id": document_id,
                "process_id": process_id,
                "capability": capability,
                "operation_type": operation_type,
                "adapter_kind": adapter_kind,
                "resource_pool": resource_pool,
                "stream_id": stream_id,
                "consumer_id": consumer_id,
                "over_limit": "true" if over_limit is True else None,
            }.items()
            if value
        }
        base_params = {
            **filters,
            "min_lag": str(min_lag),
            "limit": str(limit),
        }
        refresh_url = (
            f"/runs/{run_id}/process-runtime/stream-lag?"
            + urlencode({**base_params, "offset": str(offset)})
        )
        previous_url = None
        if offset > 0:
            previous_url = (
                f"/runs/{run_id}/process-runtime/stream-lag?"
                + urlencode({**base_params, "offset": str(max(0, offset - limit))})
            )
        next_url = None
        if page.has_more:
            next_url = (
                f"/runs/{run_id}/process-runtime/stream-lag?"
                + urlencode({**base_params, "offset": str(offset + limit)})
            )
        return render(
            "run_process_runtime_stream_lag_partial.html",
            {
                "request": request,
                "run_id": run_id,
                "page": page.model_dump(mode="json"),
                "filters": filters,
                "min_lag": min_lag,
                "over_limit": over_limit,
                "limit": limit,
                "offset": offset,
                "previous_url": previous_url,
                "next_url": next_url,
                "refresh_url": refresh_url,
            },
        )

    async def render_capability_demands_partial(
        request: Request,
        run_id: str,
    ) -> HTMLResponse:
        await run_exists_or_404(run_id)
        demands = await service.capability_demands(run_id)
        return render(
            "run_process_runtime_capability_demands_partial.html",
            {
                "request": request,
                "run_id": run_id,
                "demands": demands.model_dump(mode="json"),
            },
        )

    async def render_provenance_partial(request: Request, run_id: str) -> HTMLResponse:
        await run_exists_or_404(run_id)
        page = await service.run_provenance(run_id)
        provenance = page["provenance"] if isinstance(page["provenance"], dict) else {}
        route_report = provenance.get("route_report")
        if not isinstance(route_report, dict):
            route_report = None
        plan = provenance.get("plan")
        if not isinstance(plan, dict):
            plan = {}
        contracts = provenance.get("pipeline_contracts")
        if not isinstance(contracts, dict):
            contracts = {}
        document_summary = provenance.get("document_summary")
        if not isinstance(document_summary, dict):
            document_summary = {}
        append_batches = provenance.get("append_batches")
        if not isinstance(append_batches, list):
            append_batches = []
        pipeline_ids = provenance.get("pipeline_ids")
        if not isinstance(pipeline_ids, list):
            pipeline_ids = []
        contract_drift = page.get("contract_drift")
        if not isinstance(contract_drift, dict):
            contract_drift = {}
        contract_drift_rows = contract_drift.get("pipelines")
        if not isinstance(contract_drift_rows, list):
            contract_drift_rows = []
        route_documents = (
            route_report.get("documents", [])
            if isinstance(route_report, dict)
            else []
        )
        if not isinstance(route_documents, list):
            route_documents = []
        contract_rows = []
        for pipeline_id, contract in sorted(contracts.items()):
            contract_payload = contract if isinstance(contract, dict) else {}
            contract_rows.append(
                {
                    "pipeline_id": pipeline_id,
                    "step_count": len(contract_payload.get("steps", []) or []),
                    "combine_count": len(contract_payload.get("combines", []) or []),
                    "reduce_count": len(contract_payload.get("reduces", []) or []),
                    "package_id": contract_payload.get("package_id"),
                    "source": contract_payload.get("source"),
                }
            )
        return render(
            "run_process_runtime_provenance_partial.html",
            {
                "request": request,
                "run_id": run_id,
                "page": page,
                "provenance": provenance,
                "route_report": route_report,
                "plan": plan,
                "contracts": contracts,
                "document_summary": document_summary,
                "append_batches": append_batches,
                "pipeline_ids": pipeline_ids,
                "contract_drift": contract_drift,
                "contract_drift_rows": contract_drift_rows,
                "route_documents": route_documents,
                "contract_rows": contract_rows,
                "process_rows": plan.get("processes", []) or [],
                "worker_demands": plan.get("worker_demands", []) or [],
                "resource_pools": plan.get("resource_pools", []) or [],
            },
        )

    async def render_reductions_partial(
        request: Request,
        run_id: str,
        *,
        pipeline_id: str | None = None,
        reduce_id: str | None = None,
    ) -> HTMLResponse:
        await run_state_or_404(run_id)
        reductions = await service.run_reductions(
            run_id,
            pipeline_id=pipeline_id,
            reduce_id=reduce_id,
        )
        return render(
            "run_process_runtime_reductions_partial.html",
            {
                "request": request,
                "run_id": run_id,
                "reductions": reductions.model_dump(mode="json"),
            },
        )

    async def render_audit_partial(
        request: Request,
        run_id: str,
        *,
        limit: int = 50,
    ) -> HTMLResponse:
        await run_state_or_404(run_id)
        audit_page = await service.operator_audit(
            run_id=run_id,
            limit=limit,
            descending=True,
        )
        return render(
            "run_process_runtime_audit_partial.html",
            {
                "request": request,
                "run_id": run_id,
                "audit": audit_page.model_dump(mode="json"),
                "limit": limit,
            },
        )

    async def render_run_events_partial(
        request: Request,
        run_id: str,
        *,
        document_id: str | None = None,
        process_id: str | None = None,
        operation_type: str | None = None,
        limit: int = 50,
    ) -> HTMLResponse:
        await run_state_or_404(run_id)
        try:
            events = await service.list_process_events(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                operation_type=operation_type,
                limit=limit,
                descending=True,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        events = list(reversed(events))
        event_count = await service.count_process_events(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            operation_type=operation_type,
        )
        refresh_params = []
        if document_id:
            refresh_params.append(("document_id", document_id))
        if process_id:
            refresh_params.append(("process_id", process_id))
        if operation_type:
            refresh_params.append(("operation_type", operation_type))
        refresh_params.append(("limit", str(limit)))
        stream_params = []
        if process_id:
            stream_params.append(("process_id", process_id))
        if operation_type:
            stream_params.append(("operation_type", operation_type))
        stream_url = f"/api/runs/{run_id}/process-runtime/events/stream"
        if stream_params:
            stream_url += "?" + urlencode(stream_params)
        return render(
            "run_process_runtime_run_events_partial.html",
            {
                "request": request,
                "run_id": run_id,
                "document_id": document_id,
                "process_id": process_id,
                "operation_type": operation_type,
                "events": [event.model_dump(mode="json") for event in events],
                "event_count": event_count,
                "has_more": event_count > len(events),
                "limit": limit,
                "refresh_url": (
                    f"/runs/{run_id}/process-runtime/events?"
                    + urlencode(refresh_params)
                ),
                "stream_url": stream_url,
            },
        )

    async def render_manual_partial(
        request: Request,
        run_id: str,
        *,
        error: str | None = None,
        message: str | None = None,
    ) -> HTMLResponse:
        runtime = await run_state_or_404(run_id, include_events=True)
        return render(
            "run_process_runtime_manual_partial.html",
            {
                "request": request,
                "run_id": run_id,
                "manual_items": _manual_work_items(runtime),
                "error": error,
                "message": message,
            },
        )

    @router.get("/", response_class=HTMLResponse)
    async def home(request: Request, limit: int = Query(default=50, ge=1, le=500)):
        return await runs_page(request, limit)

    @router.get("/runs", response_class=HTMLResponse)
    async def runs_page(request: Request, limit: int = Query(default=50, ge=1, le=500)):
        runs = await visible_run_summaries(request, limit)
        queue = _queue_summary(runs)
        return render(
            "runs.html",
            {
                "request": request,
                "title": "Runs",
                "runs": runs,
                "queue": queue,
                "limit": limit,
            },
        )

    @router.get("/project", response_class=HTMLResponse)
    async def project_page(
        request: Request,
        status: str | None = Query(default=None),
        package_id: str | None = Query(default=None),
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        limit: int = Query(default=20, ge=1, le=100),
    ):
        report = project_report()
        all_runs = await visible_run_summaries(
            request,
            500,
            project_id=str(report.get("project_id") or "") if report else None,
        )
        history = (
            build_project_run_history(
                project_id=str(report.get("project_id") or "") if report else None,
                registry=service.registry,
                runs=all_runs,
                status=status,
                package_id=package_id,
                pipeline_id=pipeline_id,
                document_type=document_type,
                limit=limit,
            )
            if report
            else None
        )
        return render(
            "project.html",
            {
                "request": request,
                "title": "Project",
                "project": report,
                "history": history,
                "runs": history["runs"] if history else [],
                "queue": _queue_summary(history["runs"] if history else []),
                "limit": limit,
                "filters": {
                    "status": status,
                    "package_id": package_id,
                    "pipeline_id": pipeline_id,
                    "document_type": document_type,
                },
                "error": None,
            },
        )

    @router.get("/project/spec", response_class=HTMLResponse)
    async def project_spec_page(
        request: Request,
        base_url: str = Query(default="http://localhost:8000"),
        run_id: str | None = Query(default=None),
    ):
        report = project_report()
        spec = (
            build_project_spec_report(
                project_yaml,
                registry=service.registry,
                base_url=base_url,
                run_id=run_id,
            )
            if project_yaml is not None
            else None
        )
        return render(
            "project_spec.html",
            {
                "request": request,
                "title": "Project spec",
                "project": report,
                "spec": spec,
                "base_url": base_url,
                "run_id": run_id,
                "error": None,
            },
        )

    @router.get("/project/bootstrap", response_class=HTMLResponse)
    async def project_bootstrap_page(
        request: Request,
        base_url: str = Query(default="http://localhost:8000"),
        run_id: str | None = Query(default=None),
    ):
        report = project_report()
        bootstrap = collect_project_bootstrap_report(
            base_url=base_url,
            run_id=run_id,
        )
        return render(
            "project_bootstrap.html",
            {
                "request": request,
                "title": "Project bootstrap",
                "project": report,
                "bootstrap": bootstrap,
                "base_url": base_url,
                "run_id": run_id,
                "error": None,
            },
        )

    @router.get("/project/supervision", response_class=HTMLResponse)
    async def project_supervision_page(
        request: Request,
        package_id: str | None = Query(default=None),
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        stuck_status: str | None = Query(default=None),
        waiting_after_seconds: float = Query(default=3600.0, ge=0),
        queued_after_seconds: float = Query(default=600.0, ge=0),
        running_after_seconds: float = Query(default=1800.0, ge=0),
        consumer_id: str | None = Query(default=None),
        min_lag: int = Query(default=1, ge=0),
        over_limit: bool | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
    ):
        try:
            selected_stuck_status = (
                ProcessStatus(stuck_status) if stuck_status else None
            )
            report, supervision = await collect_project_supervision_report(
                request,
                package_id=package_id,
                pipeline_id=pipeline_id,
                document_type=document_type,
                operation_type=operation_type,
                stuck_status=selected_stuck_status,
                waiting_after_seconds=waiting_after_seconds,
                queued_after_seconds=queued_after_seconds,
                running_after_seconds=running_after_seconds,
                consumer_id=consumer_id,
                min_lag=min_lag,
                over_limit=over_limit,
                limit=limit,
            )
            error = None
        except Exception as exc:
            report = project_report()
            supervision = None
            error = str(exc)
        return render(
            "project_supervision.html",
            {
                "request": request,
                "title": "Project supervision",
                "project": report,
                "supervision": supervision,
                "filters": {
                    "package_id": package_id,
                    "pipeline_id": pipeline_id,
                    "document_type": document_type,
                    "operation_type": operation_type,
                    "stuck_status": stuck_status,
                    "waiting_after_seconds": waiting_after_seconds,
                    "queued_after_seconds": queued_after_seconds,
                    "running_after_seconds": running_after_seconds,
                    "consumer_id": consumer_id,
                    "min_lag": min_lag,
                    "over_limit": over_limit,
                },
                "limit": limit,
                "stuck_status_choices": [
                    ProcessStatus.waiting.value,
                    ProcessStatus.queued.value,
                    ProcessStatus.running.value,
                ],
                "error": error,
            },
        )

    @router.get("/project/operations", response_class=HTMLResponse)
    async def project_operations_page(
        request: Request,
        package_id: str | None = Query(default=None),
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        stuck_status: str | None = Query(default=None),
        waiting_after_seconds: float = Query(default=3600.0, ge=0),
        queued_after_seconds: float = Query(default=600.0, ge=0),
        running_after_seconds: float = Query(default=1800.0, ge=0),
        consumer_id: str | None = Query(default=None),
        min_lag: int = Query(default=1, ge=0),
        over_limit: bool | None = Query(default=None),
        stale_after_seconds: float = Query(default=60.0, gt=0, le=3600.0),
        limit: int = Query(default=50, ge=1, le=500),
    ):
        try:
            selected_stuck_status = (
                ProcessStatus(stuck_status) if stuck_status else None
            )
            report, operations = await collect_project_operations_report(
                request,
                package_id=package_id,
                pipeline_id=pipeline_id,
                document_type=document_type,
                operation_type=operation_type,
                stuck_status=selected_stuck_status,
                waiting_after_seconds=waiting_after_seconds,
                queued_after_seconds=queued_after_seconds,
                running_after_seconds=running_after_seconds,
                consumer_id=consumer_id,
                min_lag=min_lag,
                over_limit=over_limit,
                stale_after_seconds=stale_after_seconds,
                limit=limit,
            )
            error = None
        except Exception as exc:
            report = project_report()
            operations = None
            error = str(exc)
        return render(
            "project_operations.html",
            {
                "request": request,
                "title": "Project operations",
                "project": report,
                "operations": operations,
                "filters": {
                    "package_id": package_id,
                    "pipeline_id": pipeline_id,
                    "document_type": document_type,
                    "operation_type": operation_type,
                    "stuck_status": stuck_status,
                    "waiting_after_seconds": waiting_after_seconds,
                    "queued_after_seconds": queued_after_seconds,
                    "running_after_seconds": running_after_seconds,
                    "consumer_id": consumer_id,
                    "min_lag": min_lag,
                    "over_limit": over_limit,
                    "stale_after_seconds": stale_after_seconds,
                },
                "limit": limit,
                "error": error,
            },
        )

    @router.get("/project/alerts", response_class=HTMLResponse)
    async def project_alerts_page(
        request: Request,
        package_id: str | None = Query(default=None),
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        stuck_status: str | None = Query(default=None),
        waiting_after_seconds: float = Query(default=3600.0, ge=0),
        queued_after_seconds: float = Query(default=600.0, ge=0),
        running_after_seconds: float = Query(default=1800.0, ge=0),
        consumer_id: str | None = Query(default=None),
        min_lag: int = Query(default=1, ge=0),
        over_limit: bool | None = Query(default=None),
        stale_after_seconds: float = Query(default=60.0, gt=0, le=3600.0),
        limit: int = Query(default=50, ge=1, le=500),
    ):
        try:
            selected_stuck_status = (
                ProcessStatus(stuck_status) if stuck_status else None
            )
            report, alerts, operations = await collect_project_alert_report(
                request,
                package_id=package_id,
                pipeline_id=pipeline_id,
                document_type=document_type,
                operation_type=operation_type,
                stuck_status=selected_stuck_status,
                waiting_after_seconds=waiting_after_seconds,
                queued_after_seconds=queued_after_seconds,
                running_after_seconds=running_after_seconds,
                consumer_id=consumer_id,
                min_lag=min_lag,
                over_limit=over_limit,
                stale_after_seconds=stale_after_seconds,
                limit=limit,
            )
            error = None
        except Exception as exc:
            report = project_report()
            alerts = None
            operations = None
            error = str(exc)
        return render(
            "project_alerts.html",
            {
                "request": request,
                "title": "Project alerts",
                "project": report,
                "alerts": alerts,
                "operations": operations,
                "filters": {
                    "package_id": package_id,
                    "pipeline_id": pipeline_id,
                    "document_type": document_type,
                    "operation_type": operation_type,
                    "stuck_status": stuck_status,
                    "waiting_after_seconds": waiting_after_seconds,
                    "queued_after_seconds": queued_after_seconds,
                    "running_after_seconds": running_after_seconds,
                    "consumer_id": consumer_id,
                    "min_lag": min_lag,
                    "over_limit": over_limit,
                    "stale_after_seconds": stale_after_seconds,
                },
                "limit": limit,
                "error": error,
            },
        )

    @router.get("/project/lifecycle", response_class=HTMLResponse)
    async def project_lifecycle_page(
        request: Request,
        package_id: str | None = Query(default=None),
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        before: str | None = Query(default=None),
        older_than_days: float | None = Query(default=None, gt=0),
        status: list[RunStatus] = Query(default=[]),
        include_artifact_gc: bool = Query(default=True),
        limit: int = Query(default=50, ge=1, le=500),
    ):
        try:
            report, lifecycle = await collect_project_lifecycle_report(
                request,
                package_id=package_id,
                pipeline_id=pipeline_id,
                document_type=document_type,
                before=before,
                older_than_days=older_than_days,
                statuses=status or None,
                include_artifact_gc=include_artifact_gc,
                limit=limit,
            )
            error = None
        except Exception as exc:
            report = project_report()
            lifecycle = None
            error = str(exc)
        return render(
            "project_lifecycle.html",
            {
                "request": request,
                "title": "Project lifecycle",
                "project": report,
                "lifecycle": lifecycle,
                "filters": {
                    "package_id": package_id,
                    "pipeline_id": pipeline_id,
                    "document_type": document_type,
                    "before": before,
                    "older_than_days": older_than_days,
                    "status": [item.value for item in status],
                    "include_artifact_gc": include_artifact_gc,
                },
                "status_choices": [item.value for item in RunStatus],
                "limit": limit,
                "error": error,
            },
        )

    @router.post("/project/runs", response_class=HTMLResponse)
    async def create_project_run_from_panel(request: Request):
        if project_yaml is None:
            return render(
                "project.html",
                {
                    "request": request,
                    "title": "Project",
                    "project": None,
                    "history": None,
                    "runs": [],
                    "queue": _queue_summary([]),
                    "limit": 20,
                    "filters": {},
                    "error": "Fala project manifest is not configured.",
                },
            )
        form = await request.form()
        try:
            run_input, route_report = build_project_runtime_run_input(
                project_yaml,
                registry=service.registry,
                run_id=str(form.get("run_id") or "").strip() or None,
                title=str(form.get("title") or "").strip() or None,
                existing_run_policy=str(form.get("existing_run_policy") or "error"),
                existing_document_policy=str(
                    form.get("existing_document_policy") or "error"
                ),
                metadata=policy.stamp_run_metadata(
                    principal_from_request(request),
                    {},
                ),
            )
            run, _schedules = await service.create_run_with_documents(
                run_input,
                route_report=route_report,
            )
            await audit(
                request,
                action="project.run.create",
                run_id=run.id,
                target=f"run:{run.id}",
                data={
                    "project_yaml": str(project_yaml),
                    "document_count": len(run_input.documents),
                    "from": "web_project_panel",
                },
            )
        except Exception as exc:
            report = project_report()
            project_id = str(report.get("project_id") or "") if report else None
            runs = await visible_run_summaries(
                request,
                500,
                project_id=project_id,
            )
            history = (
                build_project_run_history(
                    project_id=project_id,
                    registry=service.registry,
                    runs=runs,
                    limit=20,
                )
                if report
                else None
            )
            return render(
                "project.html",
                {
                    "request": request,
                    "title": "Project",
                    "project": report,
                    "history": history,
                    "runs": history["runs"] if history else [],
                    "queue": _queue_summary(history["runs"] if history else []),
                    "limit": 20,
                    "filters": {},
                    "error": str(exc),
                },
            )
        return RedirectResponse(f"/runs/{run.id}", status_code=303)

    @router.get("/runs/new", response_class=HTMLResponse)
    async def new_run_page(request: Request):
        return render(
            "run_new.html",
            {
                "request": request,
                "title": "New run",
                "pipelines": _pipeline_options(service),
                "form": {},
                "error": None,
            },
        )

    @router.post("/runs/new", response_class=HTMLResponse)
    async def create_run_from_panel(request: Request):
        form, uploads = await _parse_run_form(request)
        try:
            run_input, route_report = await _runtime_run_input_from_web_form(
                form,
                uploads=uploads,
                service=service,
            )
            run_input = run_input.model_copy(
                update={
                    "metadata": policy.stamp_run_metadata(
                        principal_from_request(request),
                        run_input.metadata,
                    )
                }
            )
            run, _schedules = await service.create_run_with_documents(
                run_input,
                route_report=route_report,
            )
            await audit(
                request,
                action="run.create",
                run_id=run.id,
                target=f"run:{run.id}",
                data={
                    "title": run.title,
                    "pipeline_id": run_input.pipeline_id,
                    "document_count": len(run_input.documents),
                    "from": "web_new_run_form",
                },
            )
        except Exception as exc:
            return render(
                "run_new.html",
                {
                    "request": request,
                    "title": "New run",
                    "pipelines": _pipeline_options(service),
                    "form": form,
                    "error": str(exc),
                },
            )
        return RedirectResponse(f"/runs/{run.id}", status_code=303)

    @router.get("/queue", response_class=HTMLResponse)
    async def queue_page(
        request: Request,
        limit: int = Query(default=100, ge=1, le=500),
        broker_queue: str | None = Query(default=None),
        broker_state: str | None = Query(default="dead_letter"),
    ):
        runs = await visible_run_summaries(request, limit)
        queue = _queue_summary(runs)
        queue["executions"] = [
            run
            for run in runs
            if run["status"] in {"running", "queued", "waiting", "paused"}
        ]
        broker = await broker_queue_context(
            queue=broker_queue,
            state=broker_state,
            limit=limit,
        )
        return render(
            "queue.html",
            {
                "request": request,
                "title": "Queue",
                "queue": queue,
                "broker": broker,
                "queue_states": [
                    "dead_letter",
                    "ready",
                    "leased",
                    "completed",
                    "failed",
                ],
            },
        )

    @router.get("/queue/broker", response_class=HTMLResponse)
    async def broker_queue_partial(
        request: Request,
        queue: str | None = Query(default=None),
        state: str | None = Query(default="dead_letter"),
        limit: int = Query(default=100, ge=1, le=500),
    ):
        return await render_broker_queue_partial(
            request,
            queue=queue,
            state=state,
            limit=limit,
        )

    @router.post("/queue/broker/{work_id}/requeue", response_class=HTMLResponse)
    async def broker_queue_requeue(
        request: Request,
        work_id: str,
        queue: str | None = Query(default=None),
        state: str | None = Query(default="dead_letter"),
        limit: int = Query(default=100, ge=1, le=500),
    ):
        if queue_transport is None:
            return await render_broker_queue_partial(
                request,
                queue=queue,
                state=state,
                limit=limit,
                error="Queue broker is not configured.",
            )
        form = await request.form()
        keep_delivery_count = str(form.get("keep_delivery_count") or "").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        work = await queue_transport.requeue_work(
            work_id,
            reset_delivery_count=not keep_delivery_count,
        )
        if work is None:
            return await render_broker_queue_partial(
                request,
                queue=queue,
                state=state,
                limit=limit,
                error=f"Queue work not found: {work_id}",
            )
        await audit(
            request,
            action="queue.work.requeue",
            run_id=work.run_id,
            document_id=work.document_id,
            process_id=work.process_id,
            target=f"queue-work:{work_id}",
            data={
                "queue": work.queue,
                "reset_delivery_count": not keep_delivery_count,
                "from": "broker_queue_panel",
            },
        )
        return await render_broker_queue_partial(
            request,
            queue=queue,
            state=state,
            limit=limit,
            message=f"Requeued {work_id}",
        )

    @router.get("/process-runtime/pipelines", response_class=HTMLResponse)
    async def pipelines_page(request: Request):
        readiness = build_workflow_readiness_report(service.registry)
        readiness_by_package = {
            package.package_id: package.model_dump(mode="json")
            for package in readiness.packages
        }
        packages = [
            {
                **package.model_dump(mode="json"),
                "readiness": readiness_by_package.get(package.id),
            }
            for package in service.registry.packages()
        ]
        pipelines = [
            {
                "package_id": service.registry.pipeline_package_id(pipeline.id),
                **pipeline.model_dump(mode="json"),
            }
            for pipeline in service.registry.all()
        ]
        return render(
            "pipelines.html",
            {
                "request": request,
                "title": "Pipelines",
                "packages": packages,
                "pipelines": pipelines,
                "readiness": readiness.model_dump(mode="json"),
            },
        )

    @router.get("/process-runtime/blueprints", response_class=HTMLResponse)
    async def blueprints_page(
        request: Request,
        blueprint: str | None = Query(default=None),
        query: str | None = Query(default=None),
    ):
        blueprints = list_scaffold_blueprints(query=query)
        selected = None
        if blueprint:
            selected_blueprint = get_scaffold_blueprint(blueprint)
            if selected_blueprint is None:
                raise HTTPException(status_code=404, detail="Blueprint not found")
            selected = scaffold_blueprint_summary(selected_blueprint)
        return render(
            "blueprints.html",
            {
                "request": request,
                "title": "Blueprints",
                "blueprints": blueprints,
                "selected": selected,
                "query": query,
            },
        )

    @router.get("/runs/{run_id}", response_class=HTMLResponse)
    async def run_detail_page(request: Request, run_id: str):
        state = await run_state_or_404(run_id)
        run = await service.get_run(run_id)
        summary = _state_summary(state, run=run.model_dump(mode="json") if run else None)
        return render(
            "run_detail.html",
            {
                "request": request,
                "title": run_id,
                "run_id": run_id,
                "run": run.model_dump(mode="json") if run is not None else None,
                "runtime": state,
                "summary": summary,
            },
        )

    @router.post("/runs/{run_id}/actions/{action}")
    async def run_action(
        request: Request,
        run_id: str,
        action: str,
        reason: str | None = Query(default=None),
        allow_contract_drift: bool = Query(default=False),
    ):
        await run_state_or_404(run_id)
        form = await request.form()
        allow_contract_drift = allow_contract_drift or _truthy_form_value(
            str(form.get("allow_contract_drift") or "")
        )
        try:
            if action == "pause":
                await service.pause_run(run_id, reason=reason or "web panel")
            elif action == "resume":
                await service.resume_run(
                    run_id,
                    reason=reason or "web panel",
                    allow_contract_drift=allow_contract_drift,
                )
            elif action == "cancel":
                await service.cancel_run(run_id, reason=reason or "web panel")
            else:
                raise ValueError(f"Unknown run action: {action}")
            await audit(
                request,
                action=f"run.{action}",
                run_id=run_id,
                target=f"run:{run_id}",
                data={
                    "reason": reason or "web panel",
                    "allow_contract_drift": allow_contract_drift,
                },
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return RedirectResponse(f"/runs/{run_id}", status_code=303)

    @router.get("/runs/{run_id}/process-runtime", response_class=HTMLResponse)
    async def run_process_runtime(request: Request, run_id: str):
        return await render_runtime_partial(request, run_id)

    @router.get("/runs/{run_id}/process-runtime/documents", response_class=HTMLResponse)
    async def run_process_runtime_documents(
        request: Request,
        run_id: str,
        status: RuntimeDocumentStatus | None = Query(default=None),
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        relation: str | None = Query(default=None),
        parent_document_id: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ):
        return await render_documents_partial(
            request,
            run_id,
            status=status,
            pipeline_id=pipeline_id,
            document_type=document_type,
            relation=relation,
            parent_document_id=parent_document_id,
            limit=limit,
            offset=offset,
        )

    @router.get("/runs/{run_id}/process-runtime/processes", response_class=HTMLResponse)
    async def run_process_runtime_processes(
        request: Request,
        run_id: str,
        status: ProcessStatus | None = Query(default=None),
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        parent_document_id: str | None = Query(default=None),
        document_id: str | None = Query(default=None),
        process_id: str | None = Query(default=None),
        capability: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        adapter_kind: str | None = Query(default=None),
        resource_pool: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ):
        return await render_processes_partial(
            request,
            run_id,
            status=status,
            pipeline_id=pipeline_id,
            document_type=document_type,
            parent_document_id=parent_document_id,
            document_id=document_id,
            process_id=process_id,
            capability=capability,
            operation_type=operation_type,
            adapter_kind=adapter_kind,
            resource_pool=resource_pool,
            limit=limit,
            offset=offset,
        )

    @router.get("/runs/{run_id}/process-runtime/dead-letter", response_class=HTMLResponse)
    async def run_process_runtime_dead_letter(
        request: Request,
        run_id: str,
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        parent_document_id: str | None = Query(default=None),
        document_id: str | None = Query(default=None),
        process_id: str | None = Query(default=None),
        capability: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        adapter_kind: str | None = Query(default=None),
        resource_pool: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ):
        return await render_dead_letter_partial(
            request,
            run_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            parent_document_id=parent_document_id,
            document_id=document_id,
            process_id=process_id,
            capability=capability,
            operation_type=operation_type,
            adapter_kind=adapter_kind,
            resource_pool=resource_pool,
            limit=limit,
            offset=offset,
        )

    @router.post(
        "/runs/{run_id}/process-runtime/dead-letter/{document_id:path}"
        "/processes/{process_id}/replay",
        response_class=HTMLResponse,
    )
    async def run_process_runtime_dead_letter_replay(
        request: Request,
        run_id: str,
        document_id: str,
        process_id: str,
        pipeline_id: str | None = Query(default=None),
    ):
        await run_exists_or_404(run_id)
        form = await request.form()
        reason = str(form.get("reason") or "web dead-letter replay")
        allow_contract_drift = _truthy_form_value(
            str(form.get("allow_contract_drift") or "")
        )
        try:
            result = await service.control_process(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                pipeline_id=pipeline_id,
                action=ProcessAction.retry,
                reason=reason,
                allow_contract_drift=allow_contract_drift,
            )
            await audit(
                request,
                action="process.dead_letter.replay",
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                target=f"run:{run_id}/document:{document_id}/process:{process_id}",
                data={
                    "pipeline_id": pipeline_id,
                    "reason": reason,
                    "affected": result.affected,
                    "queued_count": len(result.schedule.queued),
                    "waiting_count": len(result.schedule.waiting),
                    "from": "dead_letter_panel",
                    "allow_contract_drift": allow_contract_drift,
                },
            )
        except Exception as exc:
            return await render_dead_letter_partial(request, run_id, error=str(exc))
        return await render_dead_letter_partial(
            request,
            run_id,
            message=f"Replayed {document_id} / {process_id}",
        )

    @router.get("/runs/{run_id}/process-runtime/stuck-work", response_class=HTMLResponse)
    async def run_process_runtime_stuck_work(
        request: Request,
        run_id: str,
        status: ProcessStatus | None = Query(default=None),
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        parent_document_id: str | None = Query(default=None),
        document_id: str | None = Query(default=None),
        process_id: str | None = Query(default=None),
        capability: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        adapter_kind: str | None = Query(default=None),
        resource_pool: str | None = Query(default=None),
        waiting_after_seconds: float = Query(default=3600.0, ge=0),
        queued_after_seconds: float = Query(default=600.0, ge=0),
        running_after_seconds: float = Query(default=1800.0, ge=0),
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ):
        return await render_stuck_work_partial(
            request,
            run_id,
            status=status,
            pipeline_id=pipeline_id,
            document_type=document_type,
            parent_document_id=parent_document_id,
            document_id=document_id,
            process_id=process_id,
            capability=capability,
            operation_type=operation_type,
            adapter_kind=adapter_kind,
            resource_pool=resource_pool,
            waiting_after_seconds=waiting_after_seconds,
            queued_after_seconds=queued_after_seconds,
            running_after_seconds=running_after_seconds,
            limit=limit,
            offset=offset,
        )

    @router.get("/runs/{run_id}/process-runtime/stream-lag", response_class=HTMLResponse)
    async def run_process_runtime_stream_lag(
        request: Request,
        run_id: str,
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        parent_document_id: str | None = Query(default=None),
        document_id: str | None = Query(default=None),
        process_id: str | None = Query(default=None),
        capability: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        adapter_kind: str | None = Query(default=None),
        resource_pool: str | None = Query(default=None),
        stream_id: str | None = Query(default=None),
        consumer_id: str | None = Query(default=None),
        min_lag: int = Query(default=1, ge=0),
        over_limit: bool | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ):
        return await render_stream_lag_partial(
            request,
            run_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            parent_document_id=parent_document_id,
            document_id=document_id,
            process_id=process_id,
            capability=capability,
            operation_type=operation_type,
            adapter_kind=adapter_kind,
            resource_pool=resource_pool,
            stream_id=stream_id,
            consumer_id=consumer_id,
            min_lag=min_lag,
            over_limit=over_limit,
            limit=limit,
            offset=offset,
        )

    @router.post(
        "/runs/{run_id}/process-runtime/stuck-work/{document_id:path}"
        "/processes/{process_id}/actions/{action}",
        response_class=HTMLResponse,
    )
    async def run_process_runtime_stuck_work_action(
        request: Request,
        run_id: str,
        document_id: str,
        process_id: str,
        action: ProcessAction,
        pipeline_id: str | None = Query(default=None),
        allow_contract_drift: bool = Query(default=False),
    ):
        await run_exists_or_404(run_id)
        form = await request.form()
        reason = str(form.get("reason") or "web stuck-work action")
        allow_contract_drift = allow_contract_drift or _truthy_form_value(
            str(form.get("allow_contract_drift") or "")
        )
        try:
            await service.control_process(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                pipeline_id=pipeline_id,
                action=action,
                reason=reason,
                allow_contract_drift=allow_contract_drift,
            )
            await audit(
                request,
                action=f"process.stuck_work.{action.value}",
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                target=f"run:{run_id}/document:{document_id}/process:{process_id}",
                data={
                    "pipeline_id": pipeline_id,
                    "reason": reason,
                    "from": "stuck_work_panel",
                    "allow_contract_drift": allow_contract_drift,
                },
            )
        except Exception as exc:
            return await render_stuck_work_partial(request, run_id, error=str(exc))
        return await render_stuck_work_partial(
            request,
            run_id,
            message=f"{action.value} requested for {document_id} / {process_id}",
        )

    @router.get(
        "/runs/{run_id}/process-runtime/capability-demands",
        response_class=HTMLResponse,
    )
    async def run_process_runtime_capability_demands(request: Request, run_id: str):
        return await render_capability_demands_partial(request, run_id)

    @router.get("/runs/{run_id}/process-runtime/provenance", response_class=HTMLResponse)
    async def run_process_runtime_provenance(request: Request, run_id: str):
        return await render_provenance_partial(request, run_id)

    @router.get("/runs/{run_id}/process-runtime/lineage", response_class=HTMLResponse)
    async def run_process_runtime_lineage(request: Request, run_id: str):
        return await render_lineage_partial(request, run_id)

    @router.get("/runs/{run_id}/process-runtime/results", response_class=HTMLResponse)
    async def run_process_runtime_results(
        request: Request,
        run_id: str,
        pipeline_id: str | None = Query(default=None),
        process_id: str | None = Query(default=None),
        document_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
    ):
        return await render_results_partial(
            request,
            run_id,
            pipeline_id=pipeline_id,
            process_id=process_id,
            document_id=document_id,
            document_type=document_type,
            limit=limit,
        )

    @router.get(
        "/runs/{run_id}/process-runtime/output-documents",
        response_class=HTMLResponse,
    )
    async def run_process_runtime_output_documents(
        request: Request,
        run_id: str,
        pipeline_id: str | None = Query(default=None),
        process_id: str | None = Query(default=None),
        document_id: str | None = Query(default=None),
        source_document_type: str | None = Query(default=None),
        output_document_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        relation: str | None = Query(default=None),
        media_type: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ):
        return await render_output_documents_partial(
            request,
            run_id,
            pipeline_id=pipeline_id,
            process_id=process_id,
            document_id=document_id,
            source_document_type=source_document_type,
            output_document_id=output_document_id,
            document_type=document_type,
            relation=relation,
            media_type=media_type,
            limit=limit,
            offset=offset,
        )

    @router.get("/runs/{run_id}/process-runtime/reductions", response_class=HTMLResponse)
    async def run_process_runtime_reductions(
        request: Request,
        run_id: str,
        pipeline_id: str | None = Query(default=None),
        reduce_id: str | None = Query(default=None),
    ):
        return await render_reductions_partial(
            request,
            run_id,
            pipeline_id=pipeline_id,
            reduce_id=reduce_id,
        )

    @router.get("/runs/{run_id}/process-runtime/audit", response_class=HTMLResponse)
    async def run_process_runtime_audit(
        request: Request,
        run_id: str,
        limit: int = Query(default=50, ge=1, le=500),
    ):
        return await render_audit_partial(request, run_id, limit=limit)

    @router.get("/runs/{run_id}/process-runtime/events", response_class=HTMLResponse)
    async def run_process_runtime_run_events(
        request: Request,
        run_id: str,
        document_id: str | None = Query(default=None),
        process_id: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
    ):
        return await render_run_events_partial(
            request,
            run_id,
            document_id=document_id,
            process_id=process_id,
            operation_type=operation_type,
            limit=limit,
        )

    @router.get("/runs/{run_id}/process-runtime/manual", response_class=HTMLResponse)
    async def run_process_runtime_manual(request: Request, run_id: str):
        return await render_manual_partial(request, run_id)

    @router.post(
        "/runs/{run_id}/process-runtime/{document_id:path}/processes/{process_id}/manual-complete",
        response_class=HTMLResponse,
    )
    async def run_process_runtime_manual_complete(
        request: Request,
        run_id: str,
        document_id: str,
        process_id: str,
        pipeline_id: str | None = Query(default=None),
    ):
        await run_state_or_404(run_id)
        form = await request.form()
        try:
            output = _process_output_from_web_form(form)
            completed, _refreshed, _schedule, _spawned = (
                await service.complete_process_output(
                    run_id=run_id,
                    document_id=document_id,
                    process_id=process_id,
                    output=output,
                    pipeline_id=pipeline_id,
                    worker_id="web-panel",
                )
            )
            await audit(
                request,
                action="process.output.put",
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                target=f"run:{run_id}/document:{document_id}/process:{process_id}",
                data={
                    "pipeline_id": pipeline_id,
                    "worker_id": "web-panel",
                    "value_keys": sorted(completed.values),
                    "artifact_count": len(completed.artifacts),
                    "from": "manual_complete_form",
                },
            )
        except Exception as exc:
            return await render_manual_partial(request, run_id, error=str(exc))
        value_keys = ", ".join(sorted(completed.values)) or "no values"
        return await render_manual_partial(
            request,
            run_id,
            message=f"Completed {document_id} / {process_id} ({value_keys})",
        )

    @router.post(
        "/runs/{run_id}/process-runtime/{document_id:path}/processes/{process_id}/actions/{action}",
        response_class=HTMLResponse,
    )
    async def run_process_action(
        request: Request,
        run_id: str,
        document_id: str,
        process_id: str,
        action: ProcessAction,
        reason: str | None = Query(default=None),
        allow_contract_drift: bool = Query(default=False),
    ):
        await run_state_or_404(run_id)
        try:
            await service.control_process(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                action=action,
                reason=reason or "web panel",
                allow_contract_drift=allow_contract_drift,
            )
            await audit(
                request,
                action=f"process.{action.value}",
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                target=f"run:{run_id}/document:{document_id}/process:{process_id}",
                data={
                    "reason": reason or "web panel",
                    "allow_contract_drift": allow_contract_drift,
                },
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return await render_runtime_partial(request, run_id)

    @router.get(
        "/runs/{run_id}/process-runtime/{document_id:path}/events",
        response_class=HTMLResponse,
    )
    async def run_process_runtime_events(
        request: Request,
        run_id: str,
        document_id: str,
        process_id: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        limit: int = Query(default=12, ge=1, le=100),
    ):
        await run_state_or_404(run_id)
        try:
            events = await service.list_process_events(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                operation_type=operation_type,
                limit=limit,
                descending=True,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        events = list(reversed(events))
        event_count = await service.count_process_events(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            operation_type=operation_type,
        )
        state = await service.load_state(run_id)
        document = next(
            (
                item
                for item in state.get("documents", [])
                if item.get("document_id") == document_id
            ),
            {},
        )
        event_log_id = _runtime_event_log_id(document_id, process_id)
        return render(
            "run_process_runtime_events_partial.html",
            {
                "request": request,
                "run_id": run_id,
                "document_id": document_id,
                "process_id": process_id,
                "operation_type": operation_type,
                "event_log_id": event_log_id,
                "steps": document.get("steps", []),
                "events": [event.model_dump(mode="json") for event in events],
                "event_count": event_count,
                "has_more": event_count > len(events),
                "limit": limit,
            },
        )

    @router.get("/health", response_class=HTMLResponse)
    async def health_page(request: Request):
        return render(
            "health.html",
            {
                "request": request,
                "title": "Health",
                "packages_count": len(service.registry.packages()),
                "pipelines_count": len(service.registry.all()),
            },
        )

    return router


def _queue_summary(runs: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "pending_count": sum(1 for run in runs if run["status"] in {"queued", "waiting"}),
        "running": any(run["status"] == "running" for run in runs),
        "paused_count": sum(1 for run in runs if run["status"] == "paused"),
    }


def _run_metadata_matches_project(
    metadata: dict[str, Any],
    project_id: str,
) -> bool:
    if not project_id:
        return True
    if metadata.get("project_id") == project_id:
        return True
    process_runtime = metadata.get("process_runtime")
    if not isinstance(process_runtime, dict):
        return False
    project = process_runtime.get("project")
    return isinstance(project, dict) and project.get("project_id") == project_id


def _state_summary(state: dict[str, Any], *, run: dict[str, Any] | None = None) -> dict[str, Any]:
    summary = state.get("summary", {})
    status_counts = summary.get("status_counts") or {}
    if run and run.get("status") == "paused":
        runtime_status = "paused"
    else:
        for status in ("running", "queued", "waiting", "failed", "cancelled"):
            if status_counts.get(status, 0) > 0:
                runtime_status = status
                break
        else:
            runtime_status = "completed" if summary.get("process_count", 0) else "empty"
    return {"status": runtime_status, **summary}


def _manual_work_items(runtime: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for document in runtime.get("documents", []):
        doc = document.get("document") or {}
        for step in document.get("steps", []):
            if step.get("adapter_kind") != "manual":
                continue
            items.append(
                {
                    "document_id": document.get("document_id"),
                    "document_title": doc.get("title") or document.get("document_id"),
                    "pipeline_id": document.get("pipeline_id"),
                    "process_id": step.get("id"),
                    "process_title": step.get("title") or step.get("id"),
                    "status": step.get("status"),
                    "description": step.get("description"),
                    "tags": step.get("tags") or [],
                    "event_count": document.get("event_count", 0),
                    "ready": step.get("status") == "waiting",
                }
            )
    return sorted(
        items,
        key=lambda item: (
            0 if item["ready"] else 1,
            str(item.get("document_id") or ""),
            str(item.get("process_id") or ""),
        ),
    )


def _pipeline_options(service: RuntimeService) -> list[dict[str, str | None]]:
    return [
        {
            "id": pipeline.id,
            "title": pipeline.title,
            "package_id": service.registry.pipeline_package_id(pipeline.id),
        }
        for pipeline in service.registry.all()
    ]


async def _parse_run_form(request: Request) -> tuple[dict[str, str], list[UploadFile]]:
    content_type = request.headers.get("content-type") or ""
    if "multipart/form-data" not in content_type:
        return _parse_urlencoded_body(await request.body()), []

    data = await request.form()
    form: dict[str, str] = {}
    uploads: list[UploadFile] = []
    for key, value in data.multi_items():
        if isinstance(value, UploadFile):
            if value.filename:
                uploads.append(value)
            continue
        form[key] = str(value)
    return form, uploads


def _parse_urlencoded_body(body: bytes) -> dict[str, str]:
    parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True)
    return {key: values[-1] if values else "" for key, values in parsed.items()}


async def _runtime_run_input_from_web_form(
    form: dict[str, str],
    *,
    uploads: list[UploadFile],
    service: RuntimeService,
) -> tuple[RuntimeRunInput, dict[str, Any] | None]:
    pipeline_id = (form.get("pipeline_id") or "").strip()
    auto_route = _truthy_form_value(form.get("auto_route"))
    if not pipeline_id and not auto_route:
        raise ValueError("Pipeline is required")
    metadata = _parse_key_value_lines(form.get("metadata") or "")
    document_type = (form.get("document_type") or "").strip() or None
    media_type = (form.get("media_type") or "").strip() or None
    documents = _runtime_documents_from_web_form(
        raw=form.get("documents") or "",
        document_type=document_type,
        media_type=media_type,
        metadata=metadata,
    )
    documents.extend(
        await _runtime_documents_from_uploads(
            uploads=uploads,
            service=service,
            document_type=document_type,
            media_type=media_type,
            metadata=metadata,
            existing_document_ids={document.document_id for document in documents},
        )
    )
    if not documents:
        raise ValueError("At least one document is required")
    route_report = None
    if auto_route:
        documents, route_report = service.route_runtime_document_inputs_with_report(
            documents,
            auto_route=True,
        )
    return (
        RuntimeRunInput(
            run_id=(form.get("run_id") or "").strip() or None,
            title=(form.get("title") or "").strip() or None,
            pipeline_id=pipeline_id or None,
            metadata=metadata,
            documents=documents,
        ),
        route_report,
    )


def _truthy_form_value(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def _runtime_documents_from_web_form(
    *,
    raw: str,
    document_type: str | None,
    media_type: str | None,
    metadata: dict[str, Any],
) -> list[RuntimeDocumentInput]:
    documents: list[RuntimeDocumentInput] = []
    for index, line in enumerate(raw.splitlines(), start=1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        document_id: str | None = None
        source_value = line
        if "=" in line:
            left, right = line.split("=", 1)
            document_id = left.strip() or None
            source_value = right.strip()
        source_uri = _source_uri_from_web_value(source_value)
        if document_id is None:
            document_id = _document_id_from_source(source_value, index=index)
        if not document_id:
            raise ValueError(f"Document line {index} has empty document id")
        documents.append(
            RuntimeDocumentInput(
                document_id=document_id,
                document_type=document_type,
                media_type=media_type,
                source_uri=source_uri,
                metadata=dict(metadata),
            )
        )
    return documents


async def _runtime_documents_from_uploads(
    *,
    uploads: list[UploadFile],
    service: RuntimeService,
    document_type: str | None,
    media_type: str | None,
    metadata: dict[str, Any],
    existing_document_ids: set[str],
) -> list[RuntimeDocumentInput]:
    documents: list[RuntimeDocumentInput] = []
    seen = set(existing_document_ids)
    for index, upload in enumerate(uploads, start=1):
        filename = Path(upload.filename or f"upload_{index}").name
        if not filename:
            filename = f"upload_{index}"
        document_id = _unique_document_id(filename, seen)
        seen.add(document_id)
        try:
            await upload.seek(0)
            content_type = upload.content_type or media_type
            artifact = service.artifact_store.put_fileobj(
                kind=document_type or "source_document",
                fileobj=upload.file,
                filename=filename,
                metadata={
                    **metadata,
                    "uploaded": True,
                    **({"media_type": content_type} if content_type else {}),
                },
            )
        finally:
            await upload.close()
        document_metadata = {
            **metadata,
            **artifact.metadata,
            "uploaded": True,
        }
        documents.append(
            RuntimeDocumentInput(
                document_id=document_id,
                title=filename,
                document_type=document_type,
                media_type=content_type,
                source_uri=artifact.uri,
                metadata=document_metadata,
            )
        )
    return documents


def _unique_document_id(value: str, existing: set[str]) -> str:
    stem = value or "document"
    if stem not in existing:
        return stem
    base = stem
    suffix = 2
    while f"{base}-{suffix}" in existing:
        suffix += 1
    return f"{base}-{suffix}"


def _parse_key_value_lines(raw: str) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for index, line in enumerate(raw.splitlines(), start=1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"Metadata line {index} must use key=value")
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Metadata line {index} has empty key")
        values[key] = value.strip()
    return values


def _process_output_from_web_form(form: Any) -> ProcessOutput:
    raw_values = str(form.get("values") or "").strip()
    if raw_values:
        try:
            values = json.loads(raw_values)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Output values must be valid JSON: {exc.msg}") from exc
    else:
        values = {}
    if not isinstance(values, dict):
        raise ValueError("Output values JSON must be an object")
    metadata = _parse_key_value_lines(str(form.get("metadata") or ""))
    return ProcessOutput(values=values, metadata=metadata)


def _source_uri_from_web_value(value: str) -> str | None:
    value = value.strip()
    if not value:
        return None
    parsed = urlparse(value)
    if parsed.scheme:
        return value
    if value.startswith(("/", ".", "~")):
        return Path(value).expanduser().resolve().as_uri()
    return None


def _document_id_from_source(value: str, *, index: int) -> str:
    value = value.strip()
    if not value:
        return f"doc_{index}"
    parsed = urlparse(value)
    path_name = Path(unquote(parsed.path)).name if parsed.path else ""
    if path_name:
        return path_name
    if parsed.netloc:
        return parsed.netloc
    return value


def _prepare_runtime_documents(runtime: dict[str, Any]) -> None:
    for document in runtime.get("documents", []):
        document_id = str(document.get("document_id") or "")
        document["event_log_id"] = _runtime_event_log_id(document_id, None)
        events = document.get("events") or []
        document["events"] = events[-12:]


def _runtime_event_log_id(document_id: str, process_id: str | None) -> str:
    raw = f"{document_id}\0{process_id or ''}".encode("utf-8")
    digest = hashlib.sha1(raw).hexdigest()[:12]
    return f"runtime-events-{digest}"
