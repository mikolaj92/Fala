from __future__ import annotations

import csv
import hashlib
import io
import json
import mimetypes
import shlex
import tarfile
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

from fala.intake import (
    auto_document_routes_from_registry,
    coerce_document_routes,
    route_runtime_documents_with_report,
)
from fala.models import (
    ExistingDocumentPolicy,
    ExistingRunPolicy,
    RunStatus,
    RuntimeDocumentInput,
    RuntimeRunInput,
)
from fala.package_registry import (
    build_workflow_readiness_report,
    build_workflow_registry_index,
)
from fala.registry import PipelineRegistry
from fala.service import RuntimeService
from fala.store import InMemoryStateStore

DEFAULT_PROJECT_ALERT_RULES = [
    {
        "id": "project_status_critical",
        "metric": "status",
        "operator": "==",
        "threshold": "critical",
        "severity": "critical",
        "message": "Project operations status is critical.",
    },
    {
        "id": "project_status_warning",
        "metric": "status",
        "operator": "==",
        "threshold": "warning",
        "severity": "warning",
        "message": "Project operations status is warning.",
    },
    {
        "id": "dead_letter_present",
        "metric": "supervision.dead_letter_count",
        "operator": ">",
        "threshold": 0,
        "severity": "critical",
        "message": "Project has dead-lettered process instances.",
    },
    {
        "id": "worker_deficit_present",
        "metric": "queue.worker_deficit_count",
        "operator": ">",
        "threshold": 0,
        "severity": "critical",
        "message": "Project needs more healthy workers for queued work.",
    },
    {
        "id": "critical_stuck_work_present",
        "metric": "supervision.critical_stuck_work_count",
        "operator": ">",
        "threshold": 0,
        "severity": "critical",
        "message": "Project has critically stuck work.",
    },
    {
        "id": "stream_lag_present",
        "metric": "supervision.stream_lag_count",
        "operator": ">",
        "threshold": 0,
        "severity": "warning",
        "message": "Project has stream lag.",
    },
    {
        "id": "stale_workers_present",
        "metric": "stale_worker_count",
        "operator": ">",
        "threshold": 0,
        "severity": "warning",
        "message": "Project has stale or unhealthy workers.",
    },
]

PROJECT_ALERT_OPERATORS = {">", ">=", "==", "!=", "<", "<="}
PROJECT_ALERT_SEVERITIES = {"info", "warning", "critical"}
DEFAULT_PROJECT_LIFECYCLE_POLICY = {
    "run_retention": {
        "enabled": True,
        "older_than_days": 30.0,
        "statuses": [
            RunStatus.completed.value,
            RunStatus.failed.value,
            RunStatus.cancelled.value,
        ],
    },
    "artifact_gc": {
        "enabled": True,
    },
}


def resolve_project_yaml(
    *,
    project_dir: str | Path | None = None,
    project_yaml: str | Path | None = None,
) -> Path | None:
    if project_yaml is not None:
        return Path(project_yaml).expanduser().resolve()
    root = Path(project_dir).expanduser() if project_dir is not None else Path.cwd()
    candidate = root / "fala-project.yaml"
    if candidate.is_file():
        return candidate.resolve()
    return None


def project_pipeline_dir(project_yaml: str | Path) -> Path:
    path = Path(project_yaml).expanduser().resolve()
    project = read_project_manifest(path)
    return project_path(path.parent, project.get("pipeline_dir") or "pipelines")


def read_project_manifest(project_yaml: str | Path) -> dict[str, Any]:
    return _read_yaml_object(Path(project_yaml), label="Fala project")


def build_project_readiness_report(
    project_yaml: str | Path,
    *,
    registry: PipelineRegistry | None = None,
) -> dict[str, Any]:
    project_yaml = Path(project_yaml).expanduser().resolve()
    issues: list[dict[str, Any]] = []
    project: dict[str, Any] = {}
    root = project_yaml.parent
    readiness: dict[str, Any] | None = None
    mixed_run_input: dict[str, Any] | None = None
    alert_policy: dict[str, Any] = {"enabled": True, "rule_count": 0, "rules": []}

    if not project_yaml.is_file():
        issues.append(
            project_issue(
                "error",
                "missing_project_manifest",
                f"Project manifest is missing: {project_yaml}",
                path=str(project_yaml),
            )
        )
    else:
        try:
            project = read_project_manifest(project_yaml)
        except Exception as exc:
            issues.append(
                project_issue(
                    "error",
                    "invalid_project_manifest",
                    f"Project manifest cannot be read: {exc}",
                    path=str(project_yaml),
                )
            )

    pipeline_dir = project_path(root, project.get("pipeline_dir") or "pipelines")
    source_list = project_path(root, project.get("source_list") or "source-list.example.csv")
    routes = project_path(root, project.get("routes") or "document-routes.example.yaml")
    sample_files = {
        "project_yaml": project_yaml.is_file(),
        "readme": (root / "README.md").is_file(),
        "makefile": (root / "Makefile").is_file(),
        "pipeline_dir": pipeline_dir.is_dir(),
        "source_list": source_list.is_file(),
        "routes": routes.is_file(),
    }

    for key, exists in sample_files.items():
        if not exists:
            issues.append(
                project_issue(
                    "error" if key in {"project_yaml", "pipeline_dir"} else "warning",
                    f"missing_{key}",
                    f"Project file is missing: {key}",
                )
            )

    if registry is None and pipeline_dir.is_dir():
        try:
            registry = PipelineRegistry.from_directory(pipeline_dir)
        except Exception as exc:
            issues.append(
                project_issue(
                    "error",
                    "pipeline_dir_not_loadable",
                    f"Project pipeline directory cannot be loaded: {exc}",
                    path=str(pipeline_dir),
                )
            )

    if registry is not None:
        readiness_model = build_workflow_readiness_report(registry)
        readiness = readiness_model.model_dump(mode="json")
        if not readiness_model.ok:
            issues.append(
                project_issue(
                    "error",
                    "package_readiness_failed",
                    "One or more workflow packages are not ready.",
                )
            )

    if registry is not None and source_list.is_file():
        try:
            run_input, route_report = project_mixed_run_input(
                root=root,
                project=project,
                registry=registry,
            )
            preview = RuntimeService(
                registry=registry,
                store=InMemoryStateStore(),
            ).preview_runtime_run_input(run_input)
            mixed_run_input = {
                "ok": True,
                "run_id": run_input.run_id,
                "document_count": len(run_input.documents),
                "pipeline_counts": preview["document_summary"]["pipeline_counts"],
                "document_type_counts": preview["document_summary"][
                    "document_type_counts"
                ],
                "route_report": route_report,
            }
        except Exception as exc:
            issues.append(
                project_issue(
                    "error",
                    "mixed_run_input_invalid",
                    f"Mixed source-list cannot be routed and validated: {exc}",
                    path=str(source_list),
                )
            )

    try:
        alert_policy = project_alert_policy_summary(project)
    except Exception as exc:
        issues.append(
            project_issue(
                "error",
                "invalid_project_alerts",
                f"Project alert policy is invalid: {exc}",
                path=str(project_yaml),
            )
        )

    try:
        lifecycle_policy = project_lifecycle_policy_summary(project)
    except Exception as exc:
        lifecycle_policy = {"enabled": False, "error": str(exc)}
        issues.append(
            project_issue(
                "error",
                "invalid_project_lifecycle",
                f"Project lifecycle policy is invalid: {exc}",
                path=str(project_yaml),
            )
        )

    error_count = sum(1 for issue in issues if issue["severity"] == "error")
    warning_count = sum(1 for issue in issues if issue["severity"] == "warning")
    return {
        "ok": error_count == 0,
        "project_id": project.get("project") or project.get("project_id"),
        "adapter_kind": project.get("adapter_kind"),
        "project_yaml": str(project_yaml),
        "pipeline_dir": str(pipeline_dir),
        "source_list": str(source_list),
        "routes": str(routes),
        "sample_files": sample_files,
        "package_count": len(project.get("packages") or []),
        "packages": list(project.get("packages") or []),
        "alerts": alert_policy,
        "lifecycle": lifecycle_policy,
        "readiness": readiness,
        "mixed_run_input": mixed_run_input,
        "issue_count": len(issues),
        "error_count": error_count,
        "warning_count": warning_count,
        "issues": issues,
    }


def build_project_runtime_run_input(
    project_yaml: str | Path,
    *,
    registry: PipelineRegistry,
    run_id: str | None = None,
    title: str | None = None,
    existing_run_policy: ExistingRunPolicy = "error",
    existing_document_policy: ExistingDocumentPolicy = "error",
    metadata: dict[str, Any] | None = None,
) -> tuple[RuntimeRunInput, dict[str, Any]]:
    project_yaml = Path(project_yaml).expanduser().resolve()
    project = read_project_manifest(project_yaml)
    root = project_yaml.parent
    run_input, route_report = project_mixed_run_input(
        root=root,
        project=project,
        registry=registry,
    )
    project_id = str(project.get("project") or project.get("project_id") or "")
    payload = run_input.model_dump(mode="python")
    payload.update(
        {
            "run_id": run_id or run_input.run_id,
            "title": title or f"{project_id} mixed intake".strip(),
            "existing_run_policy": existing_run_policy,
            "existing_document_policy": existing_document_policy,
            "metadata": project_run_metadata(
                project_yaml=project_yaml,
                project=project,
                metadata=metadata or {},
            ),
        }
    )
    return (
        RuntimeRunInput.model_validate(payload),
        route_report,
    )


def build_project_run_history(
    *,
    project_id: str | None,
    registry: PipelineRegistry,
    runs: list[dict[str, Any]],
    status: str | None = None,
    package_id: str | None = None,
    pipeline_id: str | None = None,
    document_type: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    matched: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    process_status_counts: Counter[str] = Counter()
    pipeline_counts: Counter[str] = Counter()
    package_counts: Counter[str] = Counter()
    document_type_counts: Counter[str] = Counter()
    document_count = 0
    process_count = 0

    for run in runs:
        if project_id and run.get("project_id") != project_id:
            continue
        if status and run.get("status") != status:
            continue
        run_pipeline_counts = _run_pipeline_counts(run)
        run_document_type_counts = _run_document_type_counts(run)
        if pipeline_id and pipeline_id not in run_pipeline_counts:
            continue
        if document_type and document_type not in run_document_type_counts:
            continue
        filtered_pipeline_counts = _filtered_pipeline_counts(
            run_pipeline_counts,
            registry=registry,
            package_id=package_id,
            pipeline_id=pipeline_id,
        )
        if (package_id or pipeline_id) and not filtered_pipeline_counts:
            continue

        run_package_counts = _package_counts(
            filtered_pipeline_counts or run_pipeline_counts,
            registry=registry,
        )
        row_document_count = (
            sum(filtered_pipeline_counts.values())
            if (package_id or pipeline_id)
            else _run_summary_int(run, "document_count")
        )
        row_process_count = _run_summary_int(run, "process_count")
        row = {
            **run,
            "matched_document_count": row_document_count,
            "matched_process_count": row_process_count,
            "matched_pipeline_counts": dict(filtered_pipeline_counts or run_pipeline_counts),
            "package_counts": dict(run_package_counts),
            "document_type_counts": dict(run_document_type_counts),
        }
        matched.append(row)

        status_counts[str(run.get("status") or "unknown")] += 1
        process_status_counts.update(
            {
                str(key): int(value)
                for key, value in (run.get("status_counts") or {}).items()
            }
        )
        pipeline_counts.update(row["matched_pipeline_counts"])
        package_counts.update(run_package_counts)
        if document_type:
            document_type_counts[document_type] += run_document_type_counts[document_type]
        else:
            document_type_counts.update(run_document_type_counts)
        document_count += row_document_count
        process_count += row_process_count

    rows = matched[:limit] if limit is not None else matched
    return {
        "ok": True,
        "project_id": project_id,
        "filters": {
            "status": status,
            "package_id": package_id,
            "pipeline_id": pipeline_id,
            "document_type": document_type,
            "limit": limit,
        },
        "run_count": len(matched),
        "shown_count": len(rows),
        "document_count": document_count,
        "process_count": process_count,
        "status_counts": _sorted_counter(status_counts),
        "process_status_counts": _sorted_counter(process_status_counts),
        "pipeline_counts": _sorted_counter(pipeline_counts),
        "package_counts": _sorted_counter(package_counts),
        "document_type_counts": _sorted_counter(document_type_counts),
        "runs": rows,
    }


def build_project_supervision_report(
    *,
    project_id: str | None,
    registry: PipelineRegistry,
    runs: list[dict[str, Any]],
    dead_letter_pages: list[dict[str, Any]],
    stuck_work_pages: list[dict[str, Any]],
    stream_lag_pages: list[dict[str, Any]],
    package_id: str | None = None,
    pipeline_id: str | None = None,
    document_type: str | None = None,
    operation_type: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    dead_letter_items = _project_supervision_items(
        dead_letter_pages,
        registry=registry,
        package_id=package_id,
        pipeline_id=pipeline_id,
        document_type=document_type,
        operation_type=operation_type,
    )
    stuck_work_items = _project_supervision_items(
        stuck_work_pages,
        registry=registry,
        package_id=package_id,
        pipeline_id=pipeline_id,
        document_type=document_type,
        operation_type=operation_type,
    )
    stream_lag_items = _project_supervision_items(
        stream_lag_pages,
        registry=registry,
        package_id=package_id,
        pipeline_id=pipeline_id,
        document_type=document_type,
        operation_type=operation_type,
    )
    dead_letter_items.sort(
        key=lambda item: str(
            item.get("dead_lettered_at")
            or item.get("last_event_at")
            or item.get("status_updated_at")
            or ""
        ),
        reverse=True,
    )
    stuck_work_items.sort(
        key=lambda item: (
            1 if item.get("severity") == "critical" else 0,
            float(item.get("status_age_seconds") or 0),
        ),
        reverse=True,
    )
    stream_lag_items.sort(
        key=lambda item: int(item.get("lag") or 0),
        reverse=True,
    )
    all_items = dead_letter_items + stuck_work_items + stream_lag_items
    return {
        "ok": True,
        "project_id": project_id,
        "filters": {
            "package_id": package_id,
            "pipeline_id": pipeline_id,
            "document_type": document_type,
            "operation_type": operation_type,
            "limit": limit,
        },
        "run_count": len(runs),
        "dead_letter_count": len(dead_letter_items),
        "stuck_work_count": len(stuck_work_items),
        "stream_lag_count": len(stream_lag_items),
        "issue_count": len(all_items),
        "critical_stuck_work_count": sum(
            1 for item in stuck_work_items if item.get("severity") == "critical"
        ),
        "warning_stuck_work_count": sum(
            1 for item in stuck_work_items if item.get("severity") == "warning"
        ),
        "over_limit_stream_count": sum(
            1 for item in stream_lag_items if item.get("over_limit") is True
        ),
        "uncheckpointed_stream_count": sum(
            1 for item in stream_lag_items if item.get("uncheckpointed") is True
        ),
        "max_stream_lag": max(
            (int(item.get("lag") or 0) for item in stream_lag_items),
            default=0,
        ),
        "package_counts": _counter_for_items(all_items, "package_id"),
        "pipeline_counts": _counter_for_items(all_items, "pipeline_id"),
        "document_type_counts": _counter_for_items(all_items, "document_type"),
        "operation_type_counts": _counter_for_items(all_items, "operation_type"),
        "dead_letter": _project_supervision_section(
            dead_letter_items,
            limit=limit,
            source_has_more=_pages_have_more(dead_letter_pages),
        ),
        "stuck_work": _project_supervision_section(
            stuck_work_items,
            limit=limit,
            source_has_more=_pages_have_more(stuck_work_pages),
        ),
        "stream_lag": _project_supervision_section(
            stream_lag_items,
            limit=limit,
            source_has_more=_pages_have_more(stream_lag_pages),
        ),
    }


def build_project_operations_report(
    *,
    project_id: str | None,
    registry: PipelineRegistry,
    runs: list[dict[str, Any]],
    health_reports: list[dict[str, Any]],
    supervision: dict[str, Any] | None = None,
    package_id: str | None = None,
    pipeline_id: str | None = None,
    document_type: str | None = None,
    operation_type: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    status_counts: Counter[str] = Counter()
    issue_code_counts: Counter[str] = Counter()
    demand_rows: list[dict[str, Any]] = []
    issue_rows: list[dict[str, Any]] = []
    run_rows: list[dict[str, Any]] = []
    totals: Counter[str] = Counter()
    worker_count = 0
    healthy_worker_count = 0
    stale_worker_count = 0

    health_by_run_id = {
        str(report.get("run_id")): report
        for report in health_reports
        if isinstance(report, dict) and report.get("run_id") is not None
    }
    for run in runs:
        run_id = str(run.get("run_id") or "")
        if not run_id:
            continue
        health = health_by_run_id.get(run_id)
        if health is None:
            continue
        status_counts[str(health.get("status") or "unknown")] += 1
        metrics = health.get("metrics") if isinstance(health.get("metrics"), dict) else {}
        worker_count += _int_value(health.get("worker_count"))
        healthy_worker_count += _int_value(health.get("healthy_worker_count"))
        stale_worker_count += _int_value(health.get("stale_worker_count"))
        for key in (
            "document_count",
            "process_instance_count",
            "waiting_count",
            "queued_count",
            "running_count",
            "failed_count",
            "retry_backoff_count",
            "missing_worker_count",
            "missing_worker_process_count",
            "resource_blocked_count",
            "resource_blocked_process_count",
            "saturated_process_count",
            "worker_demand_process_count",
            "worker_deficit_count",
        ):
            totals[key] += _int_value(metrics.get(key))

        run_issues = _filtered_project_operations_issues(
            health.get("issues"),
            run_id=run_id,
            registry=registry,
            package_id=package_id,
            pipeline_id=pipeline_id,
            operation_type=operation_type,
        )
        for issue in run_issues:
            issue_code_counts[str(issue.get("code") or "unknown")] += _int_value(
                issue.get("count"),
                default=1,
            )
        issue_rows.extend(run_issues)

        run_demands = _filtered_project_operations_demands(
            metrics.get("worker_demands") if isinstance(metrics, dict) else None,
            run_id=run_id,
            registry=registry,
            package_id=package_id,
            pipeline_id=pipeline_id,
            operation_type=operation_type,
        )
        demand_rows.extend(run_demands)
        run_rows.append(
            {
                "run_id": run_id,
                "status": health.get("status") or "unknown",
                "issue_count": len(run_issues),
                "critical_count": sum(
                    1 for item in run_issues if item.get("severity") == "critical"
                ),
                "warning_count": sum(
                    1 for item in run_issues if item.get("severity") == "warning"
                ),
                "queued_count": sum(
                    _int_value(item.get("queued_count")) for item in run_demands
                ),
                "running_count": sum(
                    _int_value(item.get("running_count")) for item in run_demands
                ),
                "worker_deficit_count": sum(
                    _int_value(item.get("worker_deficit_count"))
                    for item in run_demands
                ),
                "matched_document_count": _int_value(
                    run.get("matched_document_count")
                ),
                "matched_pipeline_counts": run.get("matched_pipeline_counts") or {},
            }
        )

    issue_rows.sort(
        key=lambda item: (
            1 if item.get("severity") == "critical" else 0,
            _int_value(item.get("count"), default=1),
            str(item.get("code") or ""),
        ),
        reverse=True,
    )
    demand_rows.sort(
        key=lambda item: (
            _int_value(item.get("worker_deficit_count")),
            _int_value(item.get("claimable_queued_count")),
            _int_value(item.get("queued_count")),
            str(item.get("capability") or ""),
        ),
        reverse=True,
    )
    supervision_summary = _project_operations_supervision_summary(supervision)
    critical_count = sum(
        _int_value(item.get("count"), default=1)
        for item in issue_rows
        if item.get("severity") == "critical"
    )
    warning_count = sum(
        _int_value(item.get("count"), default=1)
        for item in issue_rows
        if item.get("severity") == "warning"
    )
    critical_count += supervision_summary["dead_letter_count"]
    critical_count += supervision_summary["critical_stuck_work_count"]
    warning_count += supervision_summary["warning_stuck_work_count"]
    warning_count += supervision_summary["stream_lag_count"]
    status = "healthy"
    if warning_count:
        status = "warning"
    if critical_count:
        status = "critical"
    shown_issues = issue_rows[:limit]
    shown_demands = demand_rows[:limit]
    return {
        "ok": True,
        "project_id": project_id,
        "status": status,
        "filters": {
            "package_id": package_id,
            "pipeline_id": pipeline_id,
            "document_type": document_type,
            "operation_type": operation_type,
            "limit": limit,
        },
        "run_count": len(runs),
        "health_run_count": len(health_reports),
        "issue_count": len(issue_rows) + supervision_summary["issue_count"],
        "critical_count": critical_count,
        "warning_count": warning_count,
        "worker_count": worker_count,
        "healthy_worker_count": healthy_worker_count,
        "stale_worker_count": stale_worker_count,
        "queue": {
            "document_count": totals["document_count"],
            "process_instance_count": totals["process_instance_count"],
            "waiting_count": totals["waiting_count"],
            "queued_count": totals["queued_count"],
            "running_count": totals["running_count"],
            "failed_count": totals["failed_count"],
            "retry_backoff_count": totals["retry_backoff_count"],
            "missing_worker_count": totals["missing_worker_count"],
            "missing_worker_process_count": totals["missing_worker_process_count"],
            "resource_blocked_count": totals["resource_blocked_count"],
            "resource_blocked_process_count": totals["resource_blocked_process_count"],
            "saturated_process_count": totals["saturated_process_count"],
            "worker_demand_process_count": totals["worker_demand_process_count"],
            "worker_deficit_count": totals["worker_deficit_count"],
        },
        "status_counts": _sorted_counter(status_counts),
        "issue_code_counts": _sorted_counter(issue_code_counts),
        "supervision": supervision_summary,
        "capability_demands": {
            "count": len(demand_rows),
            "shown_count": len(shown_demands),
            "has_more": len(demand_rows) > len(shown_demands),
            "queued_count": sum(_int_value(item.get("queued_count")) for item in demand_rows),
            "claimable_queued_count": sum(
                _int_value(item.get("claimable_queued_count")) for item in demand_rows
            ),
            "target_worker_count": sum(
                _int_value(item.get("target_worker_count")) for item in demand_rows
            ),
            "worker_deficit_count": sum(
                _int_value(item.get("worker_deficit_count")) for item in demand_rows
            ),
            "items": shown_demands,
        },
        "issues": {
            "count": len(issue_rows),
            "shown_count": len(shown_issues),
            "has_more": len(issue_rows) > len(shown_issues),
            "items": shown_issues,
        },
        "runs": run_rows[:limit],
    }


def build_project_alert_report(
    project_yaml: str | Path,
    *,
    operations: dict[str, Any],
) -> dict[str, Any]:
    project = read_project_manifest(project_yaml)
    rules = project_alert_rules(project)
    alerts: list[dict[str, Any]] = []
    for rule in rules:
        value = _metric_value(operations, rule["metric"])
        if _alert_rule_matches(value, rule["operator"], rule["threshold"]):
            alerts.append(
                {
                    "rule_id": rule["id"],
                    "severity": rule["severity"],
                    "message": rule["message"],
                    "metric": rule["metric"],
                    "operator": rule["operator"],
                    "threshold": rule["threshold"],
                    "value": value,
                    "labels": dict(rule.get("labels") or {}),
                    "runbook": rule.get("runbook"),
                    "status": "firing",
                }
            )
    alerts.sort(
        key=lambda item: (
            _severity_rank(str(item.get("severity") or "")),
            str(item.get("rule_id") or ""),
        ),
        reverse=True,
    )
    critical_count = sum(1 for item in alerts if item["severity"] == "critical")
    warning_count = sum(1 for item in alerts if item["severity"] == "warning")
    info_count = sum(1 for item in alerts if item["severity"] == "info")
    status = "healthy"
    if warning_count or info_count:
        status = "warning"
    if critical_count:
        status = "critical"
    return {
        "ok": not alerts,
        "project_id": operations.get("project_id"),
        "status": status,
        "enabled": True,
        "rule_count": len(rules),
        "firing_count": len(alerts),
        "critical_count": critical_count,
        "warning_count": warning_count,
        "info_count": info_count,
        "filters": dict(operations.get("filters") or {}),
        "alerts": alerts,
        "rules": rules,
    }


def build_project_lifecycle_report(
    project_yaml: str | Path,
    *,
    runs: list[dict[str, Any]],
    before: str | datetime | None = None,
    older_than_days: float | None = None,
    statuses: list[RunStatus | str] | None = None,
    artifact_gc: dict[str, Any] | None = None,
    dry_run: bool = True,
    deleted_run_ids: set[str] | None = None,
    row_counts: dict[str, int] | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    project = read_project_manifest(project_yaml)
    policy = project_lifecycle_policy_summary(project)
    retention_policy = policy["run_retention"]
    artifact_policy = policy["artifact_gc"]
    selected_statuses = [
        status.value if isinstance(status, RunStatus) else str(status)
        for status in (statuses or retention_policy["statuses"])
    ]
    cutoff = _project_lifecycle_cutoff(
        before=before,
        older_than_days=older_than_days,
        default_older_than_days=retention_policy["older_than_days"],
    )
    deleted_run_ids = deleted_run_ids or set()
    row_counts = row_counts or {}
    candidate_items: list[dict[str, Any]] = []
    skipped_counts: Counter[str] = Counter()
    for run in runs:
        run_id = str(run.get("run_id") or "")
        if not run_id:
            continue
        status = str(run.get("status") or "unknown")
        updated_at = _datetime_value(run.get("updated_at")) or _datetime_value(
            run.get("finished_at")
        )
        matched = False
        reason: str | None = None
        if not retention_policy["enabled"]:
            reason = "retention_disabled"
        elif updated_at is None:
            reason = "missing_updated_at"
        elif updated_at >= cutoff:
            reason = "not_before_cutoff"
        elif status not in selected_statuses:
            reason = "status_not_selected"
        else:
            matched = True
            reason = "matched"
        if not matched:
            skipped_counts[reason or "not_matched"] += 1
            continue
        candidate_items.append(
            {
                "run_id": run_id,
                "status": status,
                "title": run.get("title"),
                "updated_at": updated_at.isoformat() if updated_at else None,
                "finished_at": run.get("finished_at"),
                "matched": True,
                "deleted": run_id in deleted_run_ids,
                "reason": reason,
                "matched_document_count": _int_value(
                    run.get("matched_document_count")
                ),
                "matched_pipeline_counts": dict(
                    run.get("matched_pipeline_counts") or {}
                ),
            }
        )
    candidate_items.sort(
        key=lambda item: str(item.get("updated_at") or ""),
    )
    shown_items = candidate_items[:limit]
    deleted_run_count = sum(1 for item in candidate_items if item["deleted"])
    return {
        "ok": True,
        "project_id": project.get("project") or project.get("project_id"),
        "dry_run": dry_run,
        "policy": policy,
        "filters": {
            "before": cutoff.isoformat(),
            "older_than_days": older_than_days,
            "statuses": selected_statuses,
            "limit": limit,
        },
        "run_count": len(runs),
        "candidate_count": len(candidate_items),
        "deleted_run_count": deleted_run_count,
        "row_counts": dict(sorted(row_counts.items())),
        "skipped_counts": _sorted_counter(skipped_counts),
        "retention": {
            "enabled": retention_policy["enabled"],
            "before": cutoff.isoformat(),
            "statuses": selected_statuses,
            "candidate_count": len(candidate_items),
            "shown_count": len(shown_items),
            "has_more": len(candidate_items) > len(shown_items),
            "deleted_run_count": deleted_run_count,
            "runs": shown_items,
        },
        "artifact_gc": {
            "enabled": artifact_policy["enabled"],
            "plan": artifact_gc if artifact_policy["enabled"] else None,
        },
    }


def build_project_secret_inventory(
    *,
    project_id: str | None,
    registry: PipelineRegistry,
) -> dict[str, Any]:
    secret_rows: list[dict[str, Any]] = []
    package_rows: list[dict[str, Any]] = []
    env_rows: dict[str, dict[str, Any]] = {}

    for package in sorted(registry.packages(), key=lambda item: item.id):
        worker_ids_by_secret: dict[str, list[str]] = {}
        for worker in package.workers:
            for secret_id in worker.secrets:
                worker_ids_by_secret.setdefault(secret_id, []).append(worker.id)

        package_secrets: list[dict[str, Any]] = []
        for secret in sorted(package.secrets, key=lambda item: (item.env_var, item.id)):
            worker_ids = sorted(set(worker_ids_by_secret.get(secret.id, [])))
            row = {
                "package_id": package.id,
                "secret_id": secret.id,
                "title": secret.title,
                "description": secret.description,
                "env_var": secret.env_var,
                "required": secret.required,
                "kubernetes_secret_name": secret.kubernetes_secret_name,
                "kubernetes_secret_key": secret.kubernetes_secret_key,
                "worker_ids": worker_ids,
                "used": bool(worker_ids),
            }
            secret_rows.append(row)
            package_secrets.append(row)

            env_row = env_rows.setdefault(
                secret.env_var,
                {
                    "env_var": secret.env_var,
                    "required": False,
                    "secret_count": 0,
                    "package_ids": set(),
                    "secret_ids": set(),
                    "worker_ids": set(),
                    "kubernetes_refs": set(),
                    "descriptions": set(),
                },
            )
            env_row["required"] = bool(env_row["required"] or secret.required)
            env_row["secret_count"] = int(env_row["secret_count"]) + 1
            env_row["package_ids"].add(package.id)
            env_row["secret_ids"].add(secret.id)
            env_row["worker_ids"].update(worker_ids)
            if secret.kubernetes_secret_name:
                env_row["kubernetes_refs"].add(
                    f"{secret.kubernetes_secret_name}:{secret.kubernetes_secret_key}"
                )
            if secret.description:
                env_row["descriptions"].add(secret.description)

        package_rows.append(
            {
                "package_id": package.id,
                "title": package.title,
                "secret_count": len(package_secrets),
                "required_count": sum(1 for item in package_secrets if item["required"]),
                "secrets": package_secrets,
            }
        )

    env_var_rows = []
    for env_row in env_rows.values():
        env_var_rows.append(
            {
                "env_var": env_row["env_var"],
                "required": env_row["required"],
                "secret_count": env_row["secret_count"],
                "package_ids": sorted(env_row["package_ids"]),
                "secret_ids": sorted(env_row["secret_ids"]),
                "worker_ids": sorted(env_row["worker_ids"]),
                "kubernetes_refs": sorted(env_row["kubernetes_refs"]),
                "descriptions": sorted(env_row["descriptions"]),
            }
        )
    env_var_rows.sort(key=lambda item: (not item["required"], item["env_var"]))

    required_count = sum(1 for item in secret_rows if item["required"])
    return {
        "ok": True,
        "project_id": project_id,
        "package_count": len(package_rows),
        "secret_count": len(secret_rows),
        "required_count": required_count,
        "optional_count": len(secret_rows) - required_count,
        "env_var_count": len(env_var_rows),
        "env_vars": env_var_rows,
        "secrets": secret_rows,
        "packages": package_rows,
    }


def render_project_env_template(
    inventory: dict[str, Any],
    *,
    include_auth_placeholders: bool = True,
) -> str:
    lines = [
        "# Generated by `fala project-secrets`.",
        "# Fill values locally or map them from your deployment secret manager.",
        "",
    ]
    env_vars = inventory.get("env_vars")
    if isinstance(env_vars, list) and env_vars:
        for item in env_vars:
            if not isinstance(item, dict):
                continue
            env_var = str(item.get("env_var") or "")
            if not env_var:
                continue
            required = bool(item.get("required"))
            package_ids = ", ".join(str(value) for value in item.get("package_ids") or [])
            secret_ids = ", ".join(str(value) for value in item.get("secret_ids") or [])
            marker = "required" if required else "optional"
            lines.append(f"# {marker}; packages: {package_ids}; secrets: {secret_ids}")
            lines.append(f"{env_var}=")
            lines.append("")
    else:
        lines.extend(
            [
                "# No package worker secrets declared yet.",
                "",
            ]
        )

    if include_auth_placeholders:
        lines.extend(
            [
                "# Optional built-in control-plane auth.",
                "# FALA_API_KEYS=operator-secret:operator,worker-secret:worker",
                "# FALA_API_KEY=worker-secret",
                "",
            ]
        )
    return "\n".join(lines)


def build_project_bootstrap_check(
    project_yaml: str | Path,
    *,
    registry: PipelineRegistry,
    base_url: str = "http://localhost:8000",
    run_id: str | None = None,
    db: dict[str, Any] | None = None,
    bundle: dict[str, Any] | None = None,
) -> dict[str, Any]:
    project_yaml = Path(project_yaml).expanduser().resolve()
    project = read_project_manifest(project_yaml)
    project_id = str(project.get("project") or project.get("project_id") or "") or None
    readiness = build_project_readiness_report(project_yaml, registry=registry)
    spec = build_project_spec_report(
        project_yaml,
        registry=registry,
        base_url=base_url,
        run_id=run_id,
    )
    secrets = build_project_secret_inventory(
        project_id=project_id,
        registry=registry,
    )
    checks = [
        {
            "id": "project_readiness",
            "ok": bool(readiness.get("ok")),
            "status": "passed" if readiness.get("ok") else "failed",
            "summary": {
                "error_count": readiness.get("error_count"),
                "warning_count": readiness.get("warning_count"),
                "package_count": readiness.get("readiness", {}).get("package_count"),
            },
        },
        {
            "id": "project_spec",
            "ok": bool(spec.get("ok")),
            "status": "passed" if spec.get("ok") else "failed",
            "summary": {
                "package_count": spec.get("package_index", {}).get("package_count"),
                "document_count": spec.get("intake", {}).get("document_count"),
                "route_count": spec.get("routes", {}).get("count"),
                "worker_count": spec.get("worker_commands", {}).get("worker_count"),
            },
        },
        {
            "id": "project_secrets",
            "ok": True,
            "status": "passed",
            "summary": {
                "secret_count": secrets["secret_count"],
                "required_count": secrets["required_count"],
                "env_var_count": secrets["env_var_count"],
            },
        },
    ]
    if db is not None:
        checks.append(
            {
                "id": "runtime_database",
                "ok": bool(db.get("ok")),
                "status": "passed" if db.get("ok") else "failed",
                "summary": {
                    "store_kind": db.get("store_kind"),
                    "missing_table_count": len(db.get("schema", {}).get("missing_tables") or []),
                    "current_version": db.get("schema", {}).get("current_version"),
                    "latest_version": db.get("schema", {}).get("latest_version"),
                    "missing_migration_count": (
                        db.get("schema", {})
                        .get("migrations", {})
                        .get("missing_count")
                    ),
                    "run_count": db.get("counts", {}).get("runs"),
                    "document_count": db.get("counts", {}).get("documents"),
                    "process_count": db.get("counts", {}).get("processes"),
                },
            }
        )
    if bundle is not None:
        checks.append(
            {
                "id": "project_bundle",
                "ok": bool(bundle.get("ok")),
                "status": "passed" if bundle.get("ok") else "failed",
                "summary": {
                    "bundle_name": bundle.get("bundle_name"),
                    "file_count": bundle.get("file_count"),
                    "checked_file_count": bundle.get("checked_file_count"),
                    "error_count": bundle.get("error_count"),
                    "warning_count": bundle.get("warning_count"),
                },
            }
        )
    failed_checks = [check for check in checks if not check["ok"]]
    return {
        "ok": not failed_checks,
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "project_id": project_id,
        "project_yaml": str(project_yaml),
        "base_url": base_url,
        "run_id": spec["run_id"],
        "check_count": len(checks),
        "failed_check_count": len(failed_checks),
        "checks": checks,
        "readiness": readiness,
        "spec_summary": {
            "ok": spec.get("ok"),
            "package_count": spec.get("package_index", {}).get("package_count"),
            "document_count": spec.get("intake", {}).get("document_count"),
            "route_count": spec.get("routes", {}).get("count"),
            "worker_count": spec.get("worker_commands", {}).get("worker_count"),
            "secret_count": spec.get("secrets", {}).get("secret_count"),
        },
        "secrets": secrets,
        "db": db,
        "bundle": bundle,
    }


PROJECT_BUNDLE_GENERATED_FILES = {
    ".env.example",
    "bundle-manifest.json",
    "package-index.json",
    "project-alerts.json",
    "project-doctor.json",
    "project-lifecycle.json",
    "project-operations.json",
    "project-secrets.json",
    "project-spec.json",
    "project-supervision.json",
    "worker-commands.json",
}

PROJECT_BUNDLE_EXCLUDED_DIRS = {
    ".fala",
    ".flow-runs",
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "build",
    "dist",
}

PROJECT_BUNDLE_EXCLUDED_FILES = {
    ".env",
    "fala.db",
    "runtime.db",
    "deployment.docker-compose.json",
    "run-input.mixed.json",
    }


def build_project_bootstrap_commands(
    *,
    project_yaml: str | Path,
    db_target: str | None,
    base_url: str = "http://localhost:8000",
    run_id: str,
) -> list[dict[str, Any]]:
    project_arg = f"--project-yaml {shlex.quote(str(Path(project_yaml)))}"
    db_check_arg = (
        f"--db {shlex.quote(db_target)} --ensure-schema"
        if db_target is not None
        else "--db <runtime.db> --ensure-schema"
    )
    db_run_arg = (
        f"--db {shlex.quote(db_target)}" if db_target is not None else "--db <runtime.db>"
    )
    commands: list[dict[str, Any]] = [
        {
            "id": "project_check",
            "title": "Project check",
            "available": db_target is not None,
            "shell": (
                "fala project-check "
                f"{project_arg} {db_check_arg} "
                f"--base-url {shlex.quote(base_url)} "
                f"--run-id {shlex.quote(run_id)}"
            ),
        },
        {
            "id": "project_smoke",
            "title": "Project smoke",
            "available": db_target is not None,
            "shell": (
                "fala project-smoke "
                f"{project_arg} {db_run_arg} "
                f"--run-id {shlex.quote(run_id)}"
            ),
        },
    ]
    if db_target is not None:
        commands.insert(
            0,
            {
                "id": "db_doctor",
                "title": "DB doctor",
                "available": True,
                "shell": f"fala db-doctor --db {shlex.quote(db_target)} --ensure-schema",
            },
        )
    return commands


def build_project_bundle_manifest(
    project_yaml: str | Path,
    *,
    registry: PipelineRegistry,
    base_url: str = "http://localhost:8000",
    run_id: str | None = None,
    bundle_name: str | None = None,
) -> dict[str, Any]:
    project_yaml = Path(project_yaml).expanduser().resolve()
    project = read_project_manifest(project_yaml)
    root = project_yaml.parent
    pipeline_dir = project_path(root, project.get("pipeline_dir") or "pipelines")
    project_id = str(project.get("project") or project.get("project_id") or "")
    selected_run_id = run_id or str(project.get("run_id") or "run_mixed_sample")
    selected_bundle_name = bundle_name or _project_bundle_name(project_id, root)
    files = _project_bundle_files(
        project_yaml=project_yaml,
        project=project,
        pipeline_dir=pipeline_dir,
    )
    generated = _project_bundle_generated_payloads(
        project_yaml=project_yaml,
        registry=registry,
        base_url=base_url,
        run_id=selected_run_id,
        project_id=project_id or None,
    )
    return {
        "ok": True,
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "project_id": project_id or None,
        "bundle_name": selected_bundle_name,
        "project_yaml": str(project_yaml),
        "root": str(root),
        "pipeline_dir": str(pipeline_dir),
        "run_id": selected_run_id,
        "base_url": base_url,
        "file_count": len(files),
        "generated_file_count": len(generated),
        "files": [
            _project_bundle_file_entry(root=root, path=path)
            for path in files
        ],
        "generated_files": [
            _project_bundle_generated_entry(path, content)
            for path, content in sorted(generated.items())
        ],
        "excludes": {
            "dirs": sorted(PROJECT_BUNDLE_EXCLUDED_DIRS),
            "files": sorted(PROJECT_BUNDLE_EXCLUDED_FILES),
            "generated_files": sorted(PROJECT_BUNDLE_GENERATED_FILES),
        },
    }


def write_project_bundle(
    project_yaml: str | Path,
    *,
    registry: PipelineRegistry,
    output: str | Path,
    base_url: str = "http://localhost:8000",
    run_id: str | None = None,
    bundle_name: str | None = None,
) -> dict[str, Any]:
    project_yaml = Path(project_yaml).expanduser().resolve()
    root = project_yaml.parent
    manifest = build_project_bundle_manifest(
        project_yaml,
        registry=registry,
        base_url=base_url,
        run_id=run_id,
        bundle_name=bundle_name,
    )
    generated = _project_bundle_generated_payloads(
        project_yaml=project_yaml,
        registry=registry,
        base_url=base_url,
        run_id=str(manifest["run_id"]),
        project_id=manifest.get("project_id"),
    )
    manifest["generated_files"] = [
        _project_bundle_generated_entry(path, content)
        for path, content in sorted(generated.items())
    ]
    manifest["generated_file_count"] = len(generated)
    generated["bundle-manifest.json"] = (
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    output_path = Path(output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_prefix = str(manifest["bundle_name"]).strip("/")
    with tarfile.open(output_path, "w:gz") as archive:
        for entry in manifest["files"]:
            source = root / str(entry["path"])
            archive.add(source, arcname=f"{bundle_prefix}/{entry['path']}")
        for relative_path, content in sorted(generated.items()):
            _add_text_to_tar(
                archive,
                arcname=f"{bundle_prefix}/{relative_path}",
                text=content,
            )
    return {
        "ok": True,
        "output": str(output_path),
        "archive_size_bytes": output_path.stat().st_size,
        "project_id": manifest.get("project_id"),
        "bundle_name": bundle_prefix,
        "file_count": manifest["file_count"],
        "generated_file_count": len(generated),
        "manifest": manifest,
    }


def verify_project_bundle(bundle: str | Path) -> dict[str, Any]:
    bundle_path = Path(bundle).expanduser().resolve()
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    try:
        archive = tarfile.open(bundle_path, "r:gz")
    except (OSError, tarfile.TarError) as exc:
        return {
            "ok": False,
            "archive": str(bundle_path),
            "error_count": 1,
            "warning_count": 0,
            "errors": [
                {
                    "code": "bundle_open_failed",
                    "message": str(exc),
                }
            ],
            "warnings": [],
        }

    with archive:
        members = archive.getmembers()
        file_members: dict[str, tarfile.TarInfo] = {}
        manifest_members: list[tarfile.TarInfo] = []
        bundle_name: str | None = None

        for member in members:
            if member.issym() or member.islnk():
                errors.append(
                    {
                        "code": "bundle_link_entry",
                        "path": member.name,
                        "message": "Bundle entries must not be links.",
                    }
                )
                continue
            safe_name = _safe_bundle_member_name(member.name)
            if safe_name is None:
                errors.append(
                    {
                        "code": "bundle_unsafe_path",
                        "path": member.name,
                        "message": "Bundle entry path is unsafe.",
                    }
                )
                continue
            if safe_name.name == "bundle-manifest.json":
                manifest_members.append(member)
            if not member.isfile():
                continue
            parts = safe_name.parts
            if len(parts) < 2:
                errors.append(
                    {
                        "code": "bundle_missing_prefix",
                        "path": member.name,
                        "message": "Bundle file must live under top-level directory.",
                    }
                )
                continue
            if bundle_name is None:
                bundle_name = parts[0]
            elif bundle_name != parts[0]:
                errors.append(
                    {
                        "code": "bundle_multiple_prefixes",
                        "path": member.name,
                        "message": "Bundle contains more than one top-level directory.",
                    }
                )
            relative = PurePosixPath(*parts[1:])
            if _project_bundle_runtime_excluded(relative):
                errors.append(
                    {
                        "code": "bundle_runtime_state_included",
                        "path": relative.as_posix(),
                        "message": "Bundle includes runtime state or local secret file.",
                    }
                )
            relative_key = relative.as_posix()
            if relative_key in file_members:
                errors.append(
                    {
                        "code": "bundle_duplicate_path",
                        "path": relative_key,
                        "message": "Bundle contains duplicate file path.",
                    }
                )
            file_members[relative_key] = member

        manifest: dict[str, Any] = {}
        if len(manifest_members) != 1:
            errors.append(
                {
                    "code": "bundle_manifest_count",
                    "message": "Bundle must contain exactly one bundle-manifest.json.",
                    "count": len(manifest_members),
                }
            )
        else:
            try:
                handle = archive.extractfile(manifest_members[0])
                if handle is None:
                    raise ValueError("bundle-manifest.json is not readable")
                manifest = json.loads(handle.read().decode("utf-8"))
            except Exception as exc:
                errors.append(
                    {
                        "code": "bundle_manifest_invalid",
                        "message": str(exc),
                    }
                )

        manifest_bundle_name = (
            str(manifest.get("bundle_name") or "")
            if isinstance(manifest, dict)
            else ""
        )
        if manifest_bundle_name and bundle_name and manifest_bundle_name != bundle_name:
            errors.append(
                {
                    "code": "bundle_name_mismatch",
                    "message": "Bundle prefix does not match manifest bundle_name.",
                    "prefix": bundle_name,
                    "bundle_name": manifest_bundle_name,
                }
            )

        required_paths = {
            ".env.example",
            "bundle-manifest.json",
            "fala-project.yaml",
            "package-index.json",
            "project-secrets.json",
            "project-spec.json",
        }
        missing_required = sorted(path for path in required_paths if path not in file_members)
        for path in missing_required:
            errors.append(
                {
                    "code": "bundle_required_file_missing",
                    "path": path,
                    "message": "Bundle required file is missing.",
                }
            )

        checked_paths: set[str] = set()
        for entry in _project_bundle_manifest_file_entries(manifest):
            path = str(entry.get("path") or "")
            member = file_members.get(path)
            if member is None:
                errors.append(
                    {
                        "code": "bundle_manifest_file_missing",
                        "path": path,
                        "message": "Manifest file entry is missing from archive.",
                    }
                )
                continue
            checked_paths.add(path)
            _verify_bundle_member_digest(
                archive=archive,
                member=member,
                entry=entry,
                errors=errors,
            )

        declared_paths = {
            str(entry.get("path") or "")
            for entry in _project_bundle_manifest_file_entries(manifest)
        }
        extra_paths = sorted(
            path
            for path in file_members
            if path not in declared_paths and path != "bundle-manifest.json"
        )
        for path in extra_paths:
            warnings.append(
                {
                    "code": "bundle_extra_file",
                    "path": path,
                    "message": "Bundle file is not listed in bundle manifest.",
                }
            )

        return {
            "ok": not errors,
            "archive": str(bundle_path),
            "bundle_name": manifest_bundle_name or bundle_name,
            "project_id": manifest.get("project_id") if isinstance(manifest, dict) else None,
            "file_count": len(file_members),
            "checked_file_count": len(checked_paths),
            "error_count": len(errors),
            "warning_count": len(warnings),
            "errors": errors,
            "warnings": warnings,
            "manifest": manifest,
        }


def _project_bundle_manifest_file_entries(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(manifest, dict):
        return []
    entries: list[dict[str, Any]] = []
    for key in ("files", "generated_files"):
        raw_entries = manifest.get(key)
        if not isinstance(raw_entries, list):
            continue
        entries.extend(item for item in raw_entries if isinstance(item, dict))
    return entries


def _verify_bundle_member_digest(
    *,
    archive: tarfile.TarFile,
    member: tarfile.TarInfo,
    entry: dict[str, Any],
    errors: list[dict[str, Any]],
) -> None:
    path = str(entry.get("path") or member.name)
    expected_size = entry.get("size_bytes")
    if isinstance(expected_size, int) and member.size != expected_size:
        errors.append(
            {
                "code": "bundle_file_size_mismatch",
                "path": path,
                "expected": expected_size,
                "actual": member.size,
            }
        )
    expected_sha256 = entry.get("sha256")
    if isinstance(expected_sha256, str) and expected_sha256:
        handle = archive.extractfile(member)
        if handle is None:
            errors.append(
                {
                    "code": "bundle_file_unreadable",
                    "path": path,
                    "message": "Bundle file is not readable.",
                }
            )
            return
        actual = hashlib.sha256(handle.read()).hexdigest()
        if actual != expected_sha256:
            errors.append(
                {
                    "code": "bundle_file_sha256_mismatch",
                    "path": path,
                    "expected": expected_sha256,
                    "actual": actual,
                }
            )


def _safe_bundle_member_name(name: str) -> PurePosixPath | None:
    if not name or name.startswith("/"):
        return None
    path = PurePosixPath(name)
    if path.is_absolute():
        return None
    if any(part in {"", ".", ".."} for part in path.parts):
        return None
    return path


def _project_bundle_runtime_excluded(relative: PurePosixPath) -> bool:
    if any(part in PROJECT_BUNDLE_EXCLUDED_DIRS for part in relative.parts):
        return True
    return relative.name in PROJECT_BUNDLE_EXCLUDED_FILES


def _project_bundle_generated_payloads(
    *,
    project_yaml: Path,
    registry: PipelineRegistry,
    base_url: str,
    run_id: str,
    project_id: str | None,
) -> dict[str, str]:
    package_index = build_workflow_registry_index(registry).model_dump(mode="json")
    secrets = build_project_secret_inventory(
        project_id=project_id,
        registry=registry,
    )
    spec = build_project_spec_report(
        project_yaml,
        registry=registry,
        base_url=base_url,
        run_id=run_id,
    )
    return {
        "package-index.json": json.dumps(package_index, indent=2, sort_keys=True) + "\n",
        "project-secrets.json": json.dumps(secrets, indent=2, sort_keys=True) + "\n",
        "project-spec.json": json.dumps(spec, indent=2, sort_keys=True) + "\n",
        ".env.example": render_project_env_template(secrets),
    }


def _project_bundle_files(
    *,
    project_yaml: Path,
    project: dict[str, Any],
    pipeline_dir: Path,
) -> list[Path]:
    root = project_yaml.parent
    candidates: list[Path] = [
        project_yaml,
        root / "README.md",
        root / "Makefile",
        project_path(root, project.get("source_list") or "source-list.example.csv"),
        project_path(root, project.get("routes") or "document-routes.example.yaml"),
    ]
    files: dict[str, Path] = {}
    for path in candidates:
        if path.is_file() and not path.is_symlink():
            relative = path.resolve().relative_to(root)
            if not _project_bundle_excluded(relative):
                files[relative.as_posix()] = path.resolve()
    if pipeline_dir.is_dir():
        for path in sorted(pipeline_dir.rglob("*")):
            if not path.is_file() or path.is_symlink():
                continue
            relative = path.resolve().relative_to(root)
            if not _project_bundle_excluded(relative):
                files[relative.as_posix()] = path.resolve()
    return [files[key] for key in sorted(files)]


def _project_bundle_excluded(relative: Path) -> bool:
    if any(part in PROJECT_BUNDLE_EXCLUDED_DIRS for part in relative.parts):
        return True
    if relative.name in PROJECT_BUNDLE_EXCLUDED_FILES:
        return True
    if relative.name in PROJECT_BUNDLE_GENERATED_FILES:
        return True
    if relative.suffix in {".pyc", ".pyo"}:
        return True
    return False


def _project_bundle_file_entry(*, root: Path, path: Path) -> dict[str, Any]:
    relative = path.relative_to(root)
    return {
        "path": relative.as_posix(),
        "size_bytes": path.stat().st_size,
        "sha256": _file_sha256(path),
        "kind": "source",
    }


def _project_bundle_generated_entry(path: str, content: str) -> dict[str, Any]:
    data = content.encode("utf-8")
    return {
        "path": path,
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "kind": "generated",
    }


def _add_text_to_tar(archive: tarfile.TarFile, *, arcname: str, text: str) -> None:
    data = text.encode("utf-8")
    info = tarfile.TarInfo(arcname)
    info.size = len(data)
    info.mtime = int(datetime.now(timezone.utc).timestamp())
    archive.addfile(info, io.BytesIO(data))


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _project_bundle_name(project_id: str, root: Path) -> str:
    source = project_id or root.name or "fala-project"
    value = "".join(
        character if character.isalnum() or character in {"-", "_", "."} else "-"
        for character in source.strip()
    ).strip("-_.")
    return value or "fala-project"


def build_project_spec_report(
    project_yaml: str | Path,
    *,
    registry: PipelineRegistry,
    base_url: str = "http://localhost:8000",
    run_id: str | None = None,
) -> dict[str, Any]:
    project_yaml = Path(project_yaml).expanduser().resolve()
    project = read_project_manifest(project_yaml)
    root = project_yaml.parent
    pipeline_dir = project_path(root, project.get("pipeline_dir") or "pipelines")
    source_list = project_path(
        root,
        project.get("source_list") or "source-list.example.csv",
    )
    routes_path = project_path(
        root,
        project.get("routes") or "document-routes.example.yaml",
    )
    selected_run_id = run_id or str(project.get("run_id") or "run_mixed_sample")
    readiness = build_project_readiness_report(project_yaml, registry=registry)
    package_index = build_workflow_registry_index(registry).model_dump(mode="json")
    routes = _project_spec_routes(routes_path)
    source_documents = _project_spec_source_documents(source_list)
    worker_commands = _project_spec_worker_commands(
        registry=registry,
        pipeline_dir=pipeline_dir,
        base_url=base_url,
        run_id=selected_run_id,
    )
    secret_inventory = build_project_secret_inventory(
        project_id=str(project.get("project") or project.get("project_id") or "") or None,
        registry=registry,
    )
    return {
        "ok": readiness["ok"],
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "project_id": project.get("project") or project.get("project_id"),
        "project_yaml": str(project_yaml),
        "root": str(root),
        "pipeline_dir": str(pipeline_dir),
        "source_list": str(source_list),
        "routes_path": str(routes_path),
        "run_id": selected_run_id,
        "base_url": base_url,
        "manifest": project,
        "readiness": readiness,
        "package_index": package_index,
        "routes": routes,
        "intake": {
            "source_list": str(source_list),
            "document_count": len(source_documents),
            "documents": source_documents,
        },
        "alerts": project_alert_policy_summary(project),
        "lifecycle": project_lifecycle_policy_summary(project),
        "secrets": secret_inventory,
        "worker_commands": worker_commands,
        "bootstrap": {
            "commands": [
                "fala project-doctor --project-dir . --output project-doctor.json",
                "fala db-doctor --db runtime.db --ensure-schema --output db-doctor.json",
                "fala project-secrets --project-dir . --output project-secrets.json --env-output .env.example",
                "fala project-bundle --project-dir . --output fala-project-bundle.tar.gz",
                "fala --pipeline-dir pipelines package-index --output package-index.json",
                (
                    "fala create-project-run --project-dir . --db runtime.db "
                    f"--run-id {shlex.quote(selected_run_id)}"
                ),
                "fala serve --project-dir . --pipeline-dir pipelines --db runtime.db",
            ],
        },
        "web": {
            "pages": [
                "/project",
                "/project/operations",
                "/project/alerts",
                "/project/lifecycle",
                "/project/supervision",
                "/runs",
            ],
            "api": [
                "/api/process-runtime/project",
                "/api/process-runtime/project/spec",
                "/api/process-runtime/project/runs",
                "/api/process-runtime/project/operations",
                "/api/process-runtime/project/alerts",
                "/api/process-runtime/project/lifecycle",
                "/api/process-runtime/project/supervision",
            ],
        },
    }


def project_alert_policy_summary(project: dict[str, Any]) -> dict[str, Any]:
    rules = project_alert_rules(project)
    return {
        "enabled": _project_alerts_enabled(project),
        "rule_count": len(rules),
        "rules": rules,
    }


def project_lifecycle_policy_summary(project: dict[str, Any]) -> dict[str, Any]:
    lifecycle = project.get("lifecycle")
    if lifecycle is None:
        lifecycle = {}
    if not isinstance(lifecycle, dict):
        raise ValueError("Project lifecycle must be an object")
    run_retention = lifecycle.get("run_retention") or {}
    if not isinstance(run_retention, dict):
        raise ValueError("Project lifecycle.run_retention must be an object")
    artifact_gc = lifecycle.get("artifact_gc") or {}
    if not isinstance(artifact_gc, dict):
        raise ValueError("Project lifecycle.artifact_gc must be an object")
    older_than_days = run_retention.get(
        "older_than_days",
        DEFAULT_PROJECT_LIFECYCLE_POLICY["run_retention"]["older_than_days"],
    )
    if not isinstance(older_than_days, int | float) or older_than_days <= 0:
        raise ValueError("Project lifecycle.run_retention.older_than_days must be > 0")
    statuses_raw = run_retention.get(
        "statuses",
        DEFAULT_PROJECT_LIFECYCLE_POLICY["run_retention"]["statuses"],
    )
    if not isinstance(statuses_raw, list) or not statuses_raw:
        raise ValueError("Project lifecycle.run_retention.statuses must be a non-empty list")
    statuses: list[str] = []
    valid_statuses = {status.value for status in RunStatus}
    for item in statuses_raw:
        status = str(item)
        if status not in valid_statuses:
            raise ValueError(
                f"Project lifecycle.run_retention.statuses has unsupported status: {status}"
            )
        statuses.append(status)
    return {
        "enabled": True,
        "run_retention": {
            "enabled": run_retention.get("enabled") is not False,
            "older_than_days": float(older_than_days),
            "statuses": statuses,
        },
        "artifact_gc": {
            "enabled": artifact_gc.get("enabled") is not False,
        },
    }


def project_alert_rules(project: dict[str, Any]) -> list[dict[str, Any]]:
    if not _project_alerts_enabled(project):
        return []
    alerts = project.get("alerts")
    raw_rules: Any
    if alerts is None:
        raw_rules = DEFAULT_PROJECT_ALERT_RULES
    elif isinstance(alerts, list):
        raw_rules = alerts
    elif isinstance(alerts, dict):
        raw_rules = alerts.get("rules", DEFAULT_PROJECT_ALERT_RULES)
    else:
        raise ValueError("Project alerts must be an object or list of rules")
    if raw_rules is None:
        return []
    if not isinstance(raw_rules, list):
        raise ValueError("Project alerts.rules must be a list")
    return [
        _project_alert_rule(raw_rule, index=index)
        for index, raw_rule in enumerate(raw_rules, start=1)
    ]


def project_run_metadata(
    *,
    project_yaml: Path,
    project: dict[str, Any],
    metadata: dict[str, Any],
) -> dict[str, Any]:
    root = project_yaml.parent
    project_id = str(project.get("project") or project.get("project_id") or "")
    merged = dict(metadata)
    if project_id:
        merged.setdefault("project_id", project_id)
    namespace = merged.get("process_runtime")
    runtime_metadata = dict(namespace) if isinstance(namespace, dict) else {}
    if namespace is not None and not isinstance(namespace, dict):
        runtime_metadata["user_metadata"] = namespace
    runtime_metadata["project"] = {
        "schema_version": 1,
        "project_id": project_id or None,
        "project_yaml": str(project_yaml),
        "pipeline_dir": str(project_path(root, project.get("pipeline_dir") or "pipelines")),
        "source_list": str(project_path(root, project.get("source_list") or "source-list.example.csv")),
        "routes": str(project_path(root, project.get("routes") or "document-routes.example.yaml")),
        "package_count": len(project.get("packages") or []),
    }
    merged["process_runtime"] = runtime_metadata
    return merged


def project_mixed_run_input(
    *,
    root: Path,
    project: dict[str, Any],
    registry: PipelineRegistry,
) -> tuple[RuntimeRunInput, dict[str, Any]]:
    source_list = project_path(root, project.get("source_list") or "source-list.example.csv")
    routes_path = project_path(root, project.get("routes") or "document-routes.example.yaml")
    documents = runtime_document_inputs_from_source_list(source_list)
    routes = read_document_routes([routes_path]) if routes_path.is_file() else []
    documents, route_report = route_runtime_documents_with_report(
        documents,
        routes=routes,
        auto_routes=auto_document_routes_from_registry(registry),
    )
    return (
        RuntimeRunInput(
            run_id=project.get("run_id") or "run_mixed_sample",
            documents=documents,
        ),
        route_report,
    )


def runtime_document_inputs_from_source_list(source: str | Path) -> list[RuntimeDocumentInput]:
    path = Path(source).expanduser().resolve()
    if not path.exists() or not path.is_file():
        raise ValueError(f"Source list does not exist: {path}")
    delimiter = "\t" if path.suffix.lower() == ".tsv" else ","
    documents: list[RuntimeDocumentInput] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        if not reader.fieldnames:
            raise ValueError(f"Source list has no header row: {path}")
        for row_number, row in enumerate(reader, start=2):
            documents.append(
                runtime_document_input_from_source_row(
                    row={
                        str(key or "").strip(): (value or "")
                        for key, value in row.items()
                    },
                    row_number=row_number,
                    source_list=path,
                )
            )
    return documents


def runtime_document_input_from_source_row(
    *,
    row: dict[str, str],
    row_number: int,
    source_list: Path,
) -> RuntimeDocumentInput:
    source_uri = row.get("source_uri", "").strip()
    source_path = (row.get("path") or row.get("source_path") or "").strip()
    if not source_uri and source_path:
        local_path = Path(source_path).expanduser()
        if not local_path.is_absolute():
            local_path = source_list.parent / local_path
        source_uri = local_path.resolve().as_uri()
    if not source_uri:
        raise ValueError(
            f"Source list row {row_number} requires source_uri, path, or source_path"
        )

    document_id = (row.get("document_id") or "").strip() or f"row_{row_number}"
    row_values = {
        key.removeprefix("value."): parse_source_list_cell(value)
        for key, value in row.items()
        if key.startswith("value.") and value != ""
    }
    row_metadata = {
        key.removeprefix("metadata."): parse_source_list_cell(value)
        for key, value in row.items()
        if key.startswith("metadata.") and value != ""
    }
    source_sha256 = (row.get("source_sha256") or row.get("sha256") or "").strip()
    metadata = {
        **row_metadata,
        "source_list": str(source_list),
        "source_list_row": row_number,
    }
    if source_sha256:
        metadata["source_sha256"] = source_sha256
    media_type = (
        (row.get("media_type") or "").strip()
        or mimetypes.guess_type(source_uri)[0]
        or "application/octet-stream"
    )
    return RuntimeDocumentInput(
        document_id=document_id,
        pipeline_id=(row.get("pipeline_id") or row.get("pipeline") or "").strip()
        or None,
        title=(row.get("title") or "").strip() or document_id,
        document_type=(row.get("document_type") or "").strip() or None,
        relation=(row.get("relation") or "").strip() or None,
        parent_document_id=(row.get("parent_document_id") or "").strip()
        or None,
        parent_process_id=(row.get("parent_process_id") or "").strip()
        or None,
        media_type=media_type,
        source_uri=source_uri,
        values=row_values,
        metadata=metadata,
    )


def read_document_routes(paths: list[str | Path]) -> list[dict[str, Any]]:
    routes: list[dict[str, Any]] = []
    for path in paths:
        data = _read_yaml_value(Path(path), label="document route")
        routes.extend(coerce_document_routes(data, source=f"Document route {path!r}"))
    return routes


def parse_source_list_cell(value: str) -> Any:
    text = value.strip()
    if not text:
        return ""
    try:
        parsed = yaml.safe_load(text)
    except yaml.YAMLError:
        return value
    return text if parsed is None and text.lower() not in {"null", "~"} else parsed


def project_path(root: Path, value: Any) -> Path:
    path = Path(str(value)).expanduser()
    if path.is_absolute():
        return path
    return (root / path).resolve()


def project_issue(
    severity: str,
    code: str,
    message: str,
    *,
    path: str | None = None,
) -> dict[str, Any]:
    issue = {
        "severity": severity,
        "code": code,
        "message": message,
    }
    if path is not None:
        issue["path"] = path
    return issue


def _run_pipeline_counts(run: dict[str, Any]) -> dict[str, int]:
    counts = run.get("pipeline_counts")
    if not isinstance(counts, dict):
        summary = run.get("summary")
        counts = summary.get("pipeline_counts") if isinstance(summary, dict) else {}
    return {
        str(key): int(value)
        for key, value in (counts or {}).items()
        if isinstance(value, int | float)
    }


def _run_document_type_counts(run: dict[str, Any]) -> dict[str, int]:
    counts = run.get("document_type_counts")
    return {
        str(key): int(value)
        for key, value in (counts or {}).items()
        if isinstance(value, int | float)
    }


def _filtered_pipeline_counts(
    counts: dict[str, int],
    *,
    registry: PipelineRegistry,
    package_id: str | None,
    pipeline_id: str | None,
) -> dict[str, int]:
    filtered: dict[str, int] = {}
    for item_pipeline_id, count in counts.items():
        if pipeline_id and item_pipeline_id != pipeline_id:
            continue
        item_package_id = registry.pipeline_package_id(item_pipeline_id)
        if package_id and item_package_id != package_id:
            continue
        filtered[item_pipeline_id] = count
    return filtered


def _package_counts(
    pipeline_counts: dict[str, int],
    *,
    registry: PipelineRegistry,
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for pipeline_id, count in pipeline_counts.items():
        counts[registry.pipeline_package_id(pipeline_id) or "<unpackaged>"] += count
    return _sorted_counter(counts)


def _project_supervision_items(
    pages: list[dict[str, Any]],
    *,
    registry: PipelineRegistry,
    package_id: str | None,
    pipeline_id: str | None,
    document_type: str | None,
    operation_type: str | None,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for page in pages:
        run_id = page.get("run_id")
        raw_items = page.get("items") if isinstance(page, dict) else None
        if not isinstance(raw_items, list):
            continue
        for raw_item in raw_items:
            if not isinstance(raw_item, dict):
                continue
            item = dict(raw_item)
            item.setdefault("run_id", run_id)
            item_pipeline_id = _string_or_none(item.get("pipeline_id"))
            item_document_type = _string_or_none(item.get("document_type"))
            item_operation_type = _string_or_none(item.get("operation_type"))
            if pipeline_id and item_pipeline_id != pipeline_id:
                continue
            if document_type and item_document_type != document_type:
                continue
            if operation_type and item_operation_type != operation_type:
                continue
            item_package_id = (
                registry.pipeline_package_id(item_pipeline_id)
                if item_pipeline_id is not None
                else None
            )
            if package_id and item_package_id != package_id:
                continue
            item["package_id"] = item_package_id
            items.append(item)
    return items


def _project_supervision_section(
    items: list[dict[str, Any]],
    *,
    limit: int,
    source_has_more: bool,
) -> dict[str, Any]:
    shown = items[:limit]
    return {
        "count": len(items),
        "shown_count": len(shown),
        "has_more": source_has_more or len(items) > len(shown),
        "items": shown,
    }


def _counter_for_items(
    items: list[dict[str, Any]],
    key: str,
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for item in items:
        value = _string_or_none(item.get(key))
        counts[value or "<none>"] += 1
    return _sorted_counter(counts)


def _filtered_project_operations_issues(
    raw_issues: Any,
    *,
    run_id: str,
    registry: PipelineRegistry,
    package_id: str | None,
    pipeline_id: str | None,
    operation_type: str | None,
) -> list[dict[str, Any]]:
    if not isinstance(raw_issues, list):
        return []
    items: list[dict[str, Any]] = []
    for raw_issue in raw_issues:
        if not isinstance(raw_issue, dict):
            continue
        item = dict(raw_issue)
        item_pipeline_id = _string_or_none(item.get("pipeline_id"))
        item_operation_type = _string_or_none(item.get("operation_type"))
        if pipeline_id and item_pipeline_id != pipeline_id:
            continue
        if operation_type and item_operation_type != operation_type:
            continue
        item_package_id = (
            registry.pipeline_package_id(item_pipeline_id)
            if item_pipeline_id is not None
            else None
        )
        if package_id and item_package_id != package_id:
            continue
        item["run_id"] = run_id
        item["package_id"] = item_package_id
        items.append(item)
    return items


def _filtered_project_operations_demands(
    raw_demands: Any,
    *,
    run_id: str,
    registry: PipelineRegistry,
    package_id: str | None,
    pipeline_id: str | None,
    operation_type: str | None,
) -> list[dict[str, Any]]:
    if not isinstance(raw_demands, list):
        return []
    items: list[dict[str, Any]] = []
    for raw_demand in raw_demands:
        if not isinstance(raw_demand, dict):
            continue
        item = dict(raw_demand)
        item_pipeline_id = _string_or_none(item.get("pipeline_id"))
        item_operation_type = _string_or_none(item.get("operation_type"))
        if pipeline_id and item_pipeline_id != pipeline_id:
            continue
        if operation_type and item_operation_type != operation_type:
            continue
        item_package_id = (
            registry.pipeline_package_id(item_pipeline_id)
            if item_pipeline_id is not None
            else None
        )
        if package_id and item_package_id != package_id:
            continue
        item["run_id"] = run_id
        item["package_id"] = item_package_id
        items.append(item)
    return items


def _project_operations_supervision_summary(
    supervision: dict[str, Any] | None,
) -> dict[str, int]:
    if not isinstance(supervision, dict):
        return {
            "issue_count": 0,
            "dead_letter_count": 0,
            "stuck_work_count": 0,
            "stream_lag_count": 0,
            "critical_stuck_work_count": 0,
            "warning_stuck_work_count": 0,
            "over_limit_stream_count": 0,
            "uncheckpointed_stream_count": 0,
            "max_stream_lag": 0,
        }
    return {
        "issue_count": _int_value(supervision.get("issue_count")),
        "dead_letter_count": _int_value(supervision.get("dead_letter_count")),
        "stuck_work_count": _int_value(supervision.get("stuck_work_count")),
        "stream_lag_count": _int_value(supervision.get("stream_lag_count")),
        "critical_stuck_work_count": _int_value(
            supervision.get("critical_stuck_work_count")
        ),
        "warning_stuck_work_count": _int_value(
            supervision.get("warning_stuck_work_count")
        ),
        "over_limit_stream_count": _int_value(
            supervision.get("over_limit_stream_count")
        ),
        "uncheckpointed_stream_count": _int_value(
            supervision.get("uncheckpointed_stream_count")
        ),
        "max_stream_lag": _int_value(supervision.get("max_stream_lag")),
    }


def _project_spec_routes(routes_path: Path) -> dict[str, Any]:
    if not routes_path.is_file():
        return {
            "path": str(routes_path),
            "ok": False,
            "count": 0,
            "items": [],
            "error": "routes file is missing",
        }
    try:
        routes = read_document_routes([routes_path])
    except Exception as exc:
        return {
            "path": str(routes_path),
            "ok": False,
            "count": 0,
            "items": [],
            "error": str(exc),
        }
    return {
        "path": str(routes_path),
        "ok": True,
        "count": len(routes),
        "items": routes,
    }


def _project_spec_source_documents(source_list: Path) -> list[dict[str, Any]]:
    if not source_list.is_file():
        return []
    try:
        documents = runtime_document_inputs_from_source_list(source_list)
    except Exception:
        return []
    return [document.model_dump(mode="json") for document in documents]


def _project_spec_worker_commands(
    *,
    registry: PipelineRegistry,
    pipeline_dir: Path,
    base_url: str,
    run_id: str,
) -> dict[str, Any]:
    workers: list[dict[str, Any]] = []
    for package in sorted(registry.packages(), key=lambda item: item.id):
        for worker in package.workers:
            argv = [
                "process-runtime-worker",
                "--pipeline-dir",
                str(pipeline_dir),
                "--base-url",
                base_url,
                "--run-id",
                run_id,
                "--package-id",
                package.id,
                "--package-worker",
                worker.id,
            ]
            workers.append(
                {
                    "package_id": package.id,
                    "worker_id": worker.id,
                    "pipeline_id": worker.pipeline_id,
                    "process_id": worker.process_id,
                    "capabilities": list(worker.capabilities),
                    "resources": worker.resources.model_dump(mode="json"),
                    "secrets": list(worker.secrets),
                    "sandbox": worker.sandbox.model_dump(mode="json"),
                    "adapter_kind": worker.adapter_kind,
                    "argv": argv,
                    "shell": " ".join(shlex.quote(part) for part in argv),
                }
            )
    return {
        "base_url": base_url,
        "run_id": run_id,
        "worker_count": len(workers),
        "workers": workers,
    }


def _project_alerts_enabled(project: dict[str, Any]) -> bool:
    alerts = project.get("alerts")
    if isinstance(alerts, dict) and alerts.get("enabled") is False:
        return False
    return True


def _project_alert_rule(raw_rule: Any, *, index: int) -> dict[str, Any]:
    if not isinstance(raw_rule, dict):
        raise ValueError(f"Project alert rule {index} must be an object")
    metric = _string_or_none(raw_rule.get("metric"))
    if metric is None:
        raise ValueError(f"Project alert rule {index} requires metric")
    operator = str(raw_rule.get("operator") or raw_rule.get("op") or ">")
    if operator not in PROJECT_ALERT_OPERATORS:
        raise ValueError(
            f"Project alert rule {index} has unsupported operator: {operator}"
        )
    severity = str(raw_rule.get("severity") or "warning")
    if severity not in PROJECT_ALERT_SEVERITIES:
        raise ValueError(
            f"Project alert rule {index} has unsupported severity: {severity}"
        )
    if "threshold" not in raw_rule:
        raise ValueError(f"Project alert rule {index} requires threshold")
    labels = raw_rule.get("labels") or {}
    if not isinstance(labels, dict):
        raise ValueError(f"Project alert rule {index} labels must be an object")
    rule_id = _string_or_none(raw_rule.get("id")) or f"rule_{index}"
    message = (
        _string_or_none(raw_rule.get("message"))
        or f"{metric} {operator} {raw_rule['threshold']}"
    )
    runbook = raw_rule.get("runbook")
    return {
        "id": rule_id,
        "metric": metric,
        "operator": operator,
        "threshold": raw_rule["threshold"],
        "severity": severity,
        "message": message,
        "labels": {str(key): value for key, value in labels.items()},
        "runbook": runbook if isinstance(runbook, str) and runbook else None,
    }


def _metric_value(payload: dict[str, Any], path: str) -> Any:
    value: Any = payload
    for part in path.split("."):
        if isinstance(value, dict) and part in value:
            value = value[part]
        else:
            return None
    return value


def _alert_rule_matches(
    value: Any,
    operator: str,
    threshold: Any,
) -> bool:
    if operator in {"==", "!="}:
        result = value == threshold
        return result if operator == "==" else not result
    value_number = _number_or_none(value)
    threshold_number = _number_or_none(threshold)
    if value_number is None or threshold_number is None:
        return False
    if operator == ">":
        return value_number > threshold_number
    if operator == ">=":
        return value_number >= threshold_number
    if operator == "<":
        return value_number < threshold_number
    if operator == "<=":
        return value_number <= threshold_number
    return False


def _number_or_none(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _severity_rank(severity: str) -> int:
    return {"info": 0, "warning": 1, "critical": 2}.get(severity, -1)


def _project_lifecycle_cutoff(
    *,
    before: str | datetime | None,
    older_than_days: float | None,
    default_older_than_days: float,
) -> datetime:
    if before is not None and older_than_days is not None:
        raise ValueError("Use only one of before or older_than_days")
    if before is not None:
        parsed = _datetime_value(before)
        if parsed is None:
            raise ValueError(f"Invalid lifecycle before datetime: {before}")
        return parsed
    days = older_than_days if older_than_days is not None else default_older_than_days
    if days <= 0:
        raise ValueError("older_than_days must be greater than zero")
    return datetime.now(timezone.utc) - timedelta(days=days)


def _datetime_value(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str) and value:
        text = value.replace("Z", "+00:00")
        try:
            result = datetime.fromisoformat(text)
        except ValueError:
            return None
    else:
        return None
    if result.tzinfo is None:
        return result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _pages_have_more(pages: list[dict[str, Any]]) -> bool:
    return any(bool(page.get("has_more")) for page in pages if isinstance(page, dict))


def _string_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _int_value(value: Any, *, default: int = 0) -> int:
    return int(value) if isinstance(value, int | float) else default


def _run_summary_int(run: dict[str, Any], key: str) -> int:
    summary = run.get("summary")
    if not isinstance(summary, dict):
        return 0
    value = summary.get(key)
    return int(value) if isinstance(value, int | float) else 0


def _sorted_counter(counter: Counter[str] | dict[str, int]) -> dict[str, int]:
    return {
        key: int(counter[key])
        for key in sorted(counter)
    }


def _read_yaml_object(source: Path, *, label: str) -> dict[str, Any]:
    data = _read_yaml_value(source, label=label)
    if not isinstance(data, dict):
        raise ValueError(f"{label} must contain an object: {source}")
    return data


def _read_yaml_value(source: Path, *, label: str) -> Any:
    try:
        data = yaml.safe_load(source.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid {label} {str(source)!r}: {exc}") from exc
    return data
