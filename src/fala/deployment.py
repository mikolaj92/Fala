from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Literal

import yaml

from fala.models import ResourceSpec, WorkerSandboxSpec, WorkflowSecretSpec
from fala.supervisor import SupervisedWorkerSpec

DeploymentFormat = Literal["docker-compose", "kubernetes"]
CONTROL_PLANE_SERVICE_NAME = "fala-control-plane"
POSTGRES_SERVICE_NAME = "fala-postgres"
DEFAULT_DATA_VOLUME = "fala-data"
DEFAULT_POSTGRES_VOLUME = "fala-postgres-data"
DEFAULT_DATA_MOUNT_PATH = "/data"
DEFAULT_ARTIFACT_STORE_ROOT = "/data/artifact-store"
DEFAULT_ARTIFACT_CACHE_ROOT = "/data/artifact-cache"
DEFAULT_PROCESS_ARTIFACT_ROOT = "/data/process-artifacts"
DEFAULT_QUEUE_DB = "/data/queue.sqlite"
DEFAULT_SQLITE_DB = "/data/fala.db"


def render_worker_deployment_manifest(
    workers: list[SupervisedWorkerSpec],
    *,
    format: DeploymentFormat,
    image: str,
    replicas: int = 1,
    namespace: str | None = None,
    env: dict[str, str] | None = None,
    container_pipeline_dir: str | None = None,
    container_workdir: str | None = None,
    mount_pipeline_dir: bool | None = None,
) -> str:
    if replicas < 1:
        raise ValueError("replicas must be greater than zero")
    if not workers:
        raise ValueError("No workers selected for deployment manifest")
    base_env = env or {}
    render_workers = [
        _container_worker_spec(
            worker,
            container_pipeline_dir=container_pipeline_dir,
            container_workdir=container_workdir,
        )
        for worker in workers
    ]
    if format == "docker-compose":
        return _render_docker_compose(
            render_workers,
            image=image,
            env=base_env,
            pipeline_volume=_compose_pipeline_volume(
                workers,
                container_pipeline_dir=container_pipeline_dir,
                mount_pipeline_dir=(
                    container_pipeline_dir is not None
                    if mount_pipeline_dir is None
                    else mount_pipeline_dir
                ),
            ),
        )
    if format == "kubernetes":
        return _render_kubernetes(
            render_workers,
            image=image,
            replicas=replicas,
            namespace=namespace,
            env=base_env,
        )
    raise ValueError(f"Unsupported deployment format: {format}")


def render_control_plane_deployment_manifest(
    workers: list[SupervisedWorkerSpec],
    *,
    format: DeploymentFormat,
    image: str,
    worker_image: str | None = None,
    namespace: str | None = None,
    env: dict[str, str] | None = None,
    worker_env: dict[str, str] | None = None,
    host_port: int = 8000,
    container_port: int = 8000,
    control_plane_replicas: int = 1,
    worker_replicas: int = 1,
    pipeline_dir: str | Path | None = None,
    container_pipeline_dir: str | None = None,
    container_workdir: str | None = None,
    mount_pipeline_dir: bool | None = None,
    database_url: str | None = None,
    sqlite_db: str = DEFAULT_SQLITE_DB,
    queue_broker: str | None = None,
    queue_db: str | None = DEFAULT_QUEUE_DB,
    artifact_store: str | None = None,
    artifact_store_root: str = DEFAULT_ARTIFACT_STORE_ROOT,
    artifact_cache_root: str = DEFAULT_ARTIFACT_CACHE_ROOT,
    process_artifact_root: str = DEFAULT_PROCESS_ARTIFACT_ROOT,
    data_volume: str = DEFAULT_DATA_VOLUME,
    include_postgres: bool = False,
    postgres_image: str = "postgres:16",
    postgres_database: str = "fala",
    postgres_user: str = "fala",
    postgres_password: str = "${FALA_POSTGRES_PASSWORD:-fala}",
    postgres_volume: str = DEFAULT_POSTGRES_VOLUME,
) -> str:
    if control_plane_replicas < 1:
        raise ValueError("control_plane_replicas must be greater than zero")
    if worker_replicas < 1:
        raise ValueError("worker_replicas must be greater than zero")
    if host_port < 1 or container_port < 1:
        raise ValueError("ports must be greater than zero")

    render_workers = [
        _container_worker_spec(
            worker,
            container_pipeline_dir=container_pipeline_dir,
            container_workdir=container_workdir,
        )
        for worker in workers
    ]
    control_env = _control_plane_environment(
        env or {},
        container_pipeline_dir=container_pipeline_dir,
        database_url=(
            database_url
            or (
                f"postgresql://{postgres_user}:{postgres_password}@"
                f"{POSTGRES_SERVICE_NAME}:5432/{postgres_database}"
                if include_postgres
                else None
            )
        ),
        sqlite_db=sqlite_db,
        queue_broker=queue_broker,
        queue_db=queue_db,
        artifact_store=artifact_store,
        artifact_store_root=artifact_store_root,
    )
    base_worker_env: dict[str, str] = {
        "FALA_ARTIFACT_CACHE_ROOT": artifact_cache_root,
        "PROCESS_RUNTIME_ARTIFACT_ROOT": process_artifact_root,
    }
    if artifact_store:
        base_worker_env["FALA_ARTIFACT_STORE"] = artifact_store
    else:
        base_worker_env["FALA_ARTIFACT_STORE_ROOT"] = artifact_store_root
    if container_pipeline_dir is not None:
        base_worker_env["FALA_PIPELINE_DIR"] = container_pipeline_dir
    render_worker_env = {**base_worker_env, **(worker_env or {})}
    if format == "docker-compose":
        return _render_control_plane_docker_compose(
            render_workers,
            image=image,
            worker_image=worker_image or image,
            env=control_env,
            worker_env=render_worker_env,
            host_port=host_port,
            container_port=container_port,
            pipeline_volume=_compose_pipeline_volume_for_path(
                pipeline_dir,
                container_pipeline_dir=container_pipeline_dir,
                mount_pipeline_dir=(
                    container_pipeline_dir is not None
                    if mount_pipeline_dir is None
                    else mount_pipeline_dir
                ),
            ),
            data_volume=data_volume,
            data_mount_path=DEFAULT_DATA_MOUNT_PATH,
            include_postgres=include_postgres,
            postgres_image=postgres_image,
            postgres_database=postgres_database,
            postgres_user=postgres_user,
            postgres_password=postgres_password,
            postgres_volume=postgres_volume,
        )
    if format == "kubernetes":
        return _render_control_plane_kubernetes(
            render_workers,
            image=image,
            worker_image=worker_image or image,
            namespace=namespace,
            env=control_env,
            worker_env=render_worker_env,
            container_port=container_port,
            control_plane_replicas=control_plane_replicas,
            worker_replicas=worker_replicas,
            data_volume=data_volume,
            data_mount_path=DEFAULT_DATA_MOUNT_PATH,
            include_postgres=include_postgres,
            postgres_image=postgres_image,
            postgres_database=postgres_database,
            postgres_user=postgres_user,
            postgres_password=postgres_password,
            postgres_volume=postgres_volume,
        )
    raise ValueError(f"Unsupported deployment format: {format}")


def _container_worker_spec(
    worker: SupervisedWorkerSpec,
    *,
    container_pipeline_dir: str | None,
    container_workdir: str | None,
) -> SupervisedWorkerSpec:
    if container_pipeline_dir is None and container_workdir is None:
        return worker
    argv = list(worker.argv)
    host_pipeline_dir = _argv_value(argv, "--pipeline-dir")
    if container_pipeline_dir is not None:
        _replace_argv_value(argv, "--pipeline-dir", container_pipeline_dir)
    cwd = container_workdir
    if cwd is None:
        cwd = _mapped_container_path(
            worker.cwd,
            host_root=host_pipeline_dir,
            container_root=container_pipeline_dir,
        )
    return worker.model_copy(update={"argv": argv, "cwd": cwd})


def _argv_value(argv: list[str], flag: str) -> str | None:
    try:
        index = argv.index(flag)
    except ValueError:
        return None
    value_index = index + 1
    if value_index >= len(argv):
        return None
    return argv[value_index]


def _replace_argv_value(argv: list[str], flag: str, value: str) -> None:
    try:
        index = argv.index(flag)
    except ValueError:
        return
    value_index = index + 1
    if value_index < len(argv):
        argv[value_index] = value


def _mapped_container_path(
    value: str | None,
    *,
    host_root: str | None,
    container_root: str | None,
) -> str | None:
    if value is None:
        return None
    if host_root is None or container_root is None:
        return value
    if not value.startswith("/"):
        return value
    normalized_host = str(Path(host_root).expanduser().resolve()).rstrip("/")
    normalized_value = str(Path(value).expanduser().resolve())
    if normalized_value == normalized_host:
        return container_root.rstrip("/") or "/"
    prefix = f"{normalized_host}/"
    if not normalized_value.startswith(prefix):
        return value
    relative = normalized_value[len(prefix):]
    return f"{container_root.rstrip('/')}/{relative}" if relative else container_root


def render_worker_autoscaling_manifest(
    workers: list[SupervisedWorkerSpec],
    *,
    run_id: str,
    prometheus_server: str,
    min_replicas: int = 0,
    max_replicas: int = 10,
    target_value: int = 1,
    namespace: str | None = None,
) -> str:
    if min_replicas < 0:
        raise ValueError("min_replicas must be greater than or equal to zero")
    if max_replicas < 1:
        raise ValueError("max_replicas must be greater than zero")
    if max_replicas < min_replicas:
        raise ValueError("max_replicas must be greater than or equal to min_replicas")
    if target_value < 1:
        raise ValueError("target_value must be greater than zero")
    if not workers:
        raise ValueError("No workers selected for autoscaling manifest")
    manifests = [
        _keda_scaled_object(
            worker,
            run_id=run_id,
            prometheus_server=prometheus_server,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            target_value=target_value,
            namespace=namespace,
        )
        for worker in workers
    ]
    return "\n---\n".join(_yaml_dump(manifest).strip() for manifest in manifests) + "\n"


def _render_docker_compose(
    workers: list[SupervisedWorkerSpec],
    *,
    image: str,
    env: dict[str, str],
    pipeline_volume: str | None,
) -> str:
    return _yaml_dump(
        {
            "services": _docker_compose_worker_services(
                workers,
                image=image,
                env=env,
                pipeline_volume=pipeline_volume,
            ),
        }
    )


def _docker_compose_worker_services(
    workers: list[SupervisedWorkerSpec],
    *,
    image: str,
    env: dict[str, str],
    pipeline_volume: str | None,
) -> dict[str, Any]:
    services: dict[str, Any] = {}
    for worker in workers:
        service_name = _service_name(worker)
        service: dict[str, Any] = {
            "image": image,
            "command": worker.argv,
            "restart": "unless-stopped",
            "environment": _compose_environment(
                {
                    **env,
                    **worker.env,
                    **_compose_secret_environment(worker.secrets),
                }
            ),
            "labels": [
                f"fala.worker.id={worker.id}",
                f"fala.package.id={worker.package_id}",
                f"fala.pipeline.id={worker.pipeline_id}",
            ],
            "x-fala": _worker_metadata(worker),
        }
        if worker.cwd:
            service["working_dir"] = worker.cwd
        if pipeline_volume is not None:
            service["volumes"] = [pipeline_volume]
        services[service_name] = service
    return services


def _render_control_plane_docker_compose(
    workers: list[SupervisedWorkerSpec],
    *,
    image: str,
    worker_image: str,
    env: dict[str, str],
    worker_env: dict[str, str],
    host_port: int,
    container_port: int,
    pipeline_volume: str | None,
    data_volume: str,
    data_mount_path: str,
    include_postgres: bool,
    postgres_image: str,
    postgres_database: str,
    postgres_user: str,
    postgres_password: str,
    postgres_volume: str,
) -> str:
    data_mount = f"{data_volume}:{data_mount_path}"
    services: dict[str, Any] = {
        CONTROL_PLANE_SERVICE_NAME: {
            "image": image,
            "command": _control_plane_command(container_port),
            "restart": "unless-stopped",
            "ports": [f"{host_port}:{container_port}"],
            "environment": _compose_environment(env),
            "volumes": [data_mount],
            "labels": [
                "fala.component=control-plane",
            ],
            "x-fala": {
                "component": "control-plane",
                "web_url": f"http://localhost:{host_port}",
                "api_base_url": f"http://{CONTROL_PLANE_SERVICE_NAME}:{container_port}",
            },
        }
    }
    if pipeline_volume is not None:
        services[CONTROL_PLANE_SERVICE_NAME]["volumes"].append(pipeline_volume)
    if include_postgres:
        services[CONTROL_PLANE_SERVICE_NAME]["depends_on"] = [POSTGRES_SERVICE_NAME]
        services[POSTGRES_SERVICE_NAME] = {
            "image": postgres_image,
            "restart": "unless-stopped",
            "environment": _compose_environment(
                {
                    "POSTGRES_DB": postgres_database,
                    "POSTGRES_PASSWORD": postgres_password,
                    "POSTGRES_USER": postgres_user,
                }
            ),
            "volumes": [f"{postgres_volume}:/var/lib/postgresql/data"],
            "healthcheck": {
                "test": [
                    "CMD-SHELL",
                    f"pg_isready -U {postgres_user} -d {postgres_database}",
                ],
                "interval": "10s",
                "timeout": "5s",
                "retries": 5,
            },
        }

    worker_services = _docker_compose_worker_services(
        workers,
        image=worker_image,
        env=worker_env,
        pipeline_volume=pipeline_volume,
    )
    for service in worker_services.values():
        service.setdefault("depends_on", [CONTROL_PLANE_SERVICE_NAME])
        volumes = service.setdefault("volumes", [])
        if data_mount not in volumes:
            volumes.append(data_mount)
    services.update(worker_services)

    volumes: dict[str, Any] = {data_volume: {}}
    if include_postgres:
        volumes[postgres_volume] = {}
    return _yaml_dump({"services": services, "volumes": volumes})


def _control_plane_environment(
    env: dict[str, str],
    *,
    container_pipeline_dir: str | None,
    database_url: str | None,
    sqlite_db: str,
    queue_broker: str | None,
    queue_db: str | None,
    artifact_store: str | None,
    artifact_store_root: str,
) -> dict[str, str]:
    result: dict[str, str] = {}
    if artifact_store:
        result["FALA_ARTIFACT_STORE"] = artifact_store
    else:
        result["FALA_ARTIFACT_STORE_ROOT"] = artifact_store_root
    if container_pipeline_dir is not None:
        result["FALA_PIPELINE_DIR"] = container_pipeline_dir
    if database_url:
        result["FALA_DATABASE_URL"] = database_url
    else:
        result["FALA_DB"] = sqlite_db
    if queue_broker:
        result["FALA_QUEUE_BROKER"] = queue_broker
    elif queue_db:
        result["FALA_QUEUE_DB"] = queue_db
    return {**result, **env}


def _control_plane_command(container_port: int) -> list[str]:
    return [
        "fala",
        "serve",
        "--host",
        "0.0.0.0",
        "--port",
        str(container_port),
    ]


def _compose_pipeline_volume(
    workers: list[SupervisedWorkerSpec],
    *,
    container_pipeline_dir: str | None,
    mount_pipeline_dir: bool,
) -> str | None:
    if not mount_pipeline_dir:
        return None
    if container_pipeline_dir is None:
        raise ValueError(
            "container_pipeline_dir is required when mounting pipeline directory"
        )
    host_dirs = sorted(
        {
            *_worker_pipeline_dirs(workers),
        }
    )
    return _compose_pipeline_volume_from_dirs(
        host_dirs,
        container_pipeline_dir=container_pipeline_dir,
    )


def _compose_pipeline_volume_for_path(
    pipeline_dir: str | Path | None,
    *,
    container_pipeline_dir: str | None,
    mount_pipeline_dir: bool,
) -> str | None:
    if not mount_pipeline_dir:
        return None
    if container_pipeline_dir is None:
        raise ValueError(
            "container_pipeline_dir is required when mounting pipeline directory"
        )
    if pipeline_dir is None:
        return None
    return _compose_pipeline_volume_from_dirs(
        [str(Path(pipeline_dir).expanduser().resolve())],
        container_pipeline_dir=container_pipeline_dir,
    )


def _worker_pipeline_dirs(workers: list[SupervisedWorkerSpec]) -> list[str]:
    return [
        str(Path(value).expanduser().resolve())
        for worker in workers
        for value in [_argv_value(worker.argv, "--pipeline-dir")]
        if value
    ]


def _compose_pipeline_volume_from_dirs(
    host_dirs: list[str],
    *,
    container_pipeline_dir: str,
) -> str | None:
    if not host_dirs:
        return None
    if len(host_dirs) > 1:
        raise ValueError(
            "Cannot mount multiple worker pipeline directories into one "
            "docker-compose manifest"
        )
    return f"{host_dirs[0]}:{container_pipeline_dir}:ro"


def _render_kubernetes(
    workers: list[SupervisedWorkerSpec],
    *,
    image: str,
    replicas: int,
    namespace: str | None,
    env: dict[str, str],
) -> str:
    manifests = _k8s_worker_manifests(
        workers,
        image=image,
        replicas=replicas,
        namespace=namespace,
        env=env,
    )
    return "\n---\n".join(_yaml_dump(manifest).strip() for manifest in manifests) + "\n"


def _k8s_worker_manifests(
    workers: list[SupervisedWorkerSpec],
    *,
    image: str,
    replicas: int,
    namespace: str | None,
    env: dict[str, str],
) -> list[dict[str, Any]]:
    manifests: list[dict[str, Any]] = []
    for worker in workers:
        name = _k8s_name(worker)
        labels = {
            "app.kubernetes.io/name": "fala-worker",
            "app.kubernetes.io/component": "worker",
            "app.kubernetes.io/instance": name,
            "fala.worker.id": worker.id,
            "fala.package.id": worker.package_id,
            "fala.pipeline.id": worker.pipeline_id,
        }
        container: dict[str, Any] = {
            "name": "worker",
            "image": image,
            "command": [worker.argv[0]],
            "args": worker.argv[1:],
            "env": [
                *_k8s_env({**env, **worker.env}),
                *_k8s_secret_env(worker.secrets),
            ],
            "securityContext": _k8s_security_context(worker.sandbox),
        }
        resources = _k8s_resources(worker.resources)
        if resources:
            container["resources"] = resources
        manifest: dict[str, Any] = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": name,
                "labels": labels,
                "annotations": {
                    "fala.dev/worker": yaml.safe_dump(
                        _worker_metadata(worker),
                        sort_keys=True,
                    ).strip()
                },
            },
            "spec": {
                "replicas": replicas,
                "selector": {"matchLabels": {"app.kubernetes.io/instance": name}},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "containers": [container],
                    },
                },
            },
        }
        if namespace:
            manifest["metadata"]["namespace"] = namespace
        if worker.cwd:
            container["workingDir"] = worker.cwd
        manifests.append(manifest)
    return manifests


def _render_control_plane_kubernetes(
    workers: list[SupervisedWorkerSpec],
    *,
    image: str,
    worker_image: str,
    namespace: str | None,
    env: dict[str, str],
    worker_env: dict[str, str],
    container_port: int,
    control_plane_replicas: int,
    worker_replicas: int,
    data_volume: str,
    data_mount_path: str,
    include_postgres: bool,
    postgres_image: str,
    postgres_database: str,
    postgres_user: str,
    postgres_password: str,
    postgres_volume: str,
) -> str:
    manifests: list[dict[str, Any]] = [
        _k8s_persistent_volume_claim(data_volume, namespace=namespace),
    ]
    if include_postgres:
        manifests.extend(
            _k8s_postgres_manifests(
                namespace=namespace,
                image=postgres_image,
                database=postgres_database,
                user=postgres_user,
                password=postgres_password,
                volume=postgres_volume,
            )
        )
    control_plane = _k8s_control_plane_deployment(
        image=image,
        namespace=namespace,
        env=env,
        container_port=container_port,
        replicas=control_plane_replicas,
    )
    _add_k8s_pvc_mount(
        control_plane,
        claim_name=data_volume,
        mount_path=data_mount_path,
    )
    manifests.extend(
        [
            control_plane,
            _k8s_control_plane_service(
                namespace=namespace,
                container_port=container_port,
            ),
        ]
    )

    worker_manifests = _k8s_worker_manifests(
        workers,
        image=worker_image,
        replicas=worker_replicas,
        namespace=namespace,
        env=worker_env,
    )
    for manifest in worker_manifests:
        _add_k8s_pvc_mount(
            manifest,
            claim_name=data_volume,
            mount_path=data_mount_path,
        )
    manifests.extend(worker_manifests)
    return "\n---\n".join(_yaml_dump(manifest).strip() for manifest in manifests) + "\n"


def _k8s_control_plane_deployment(
    *,
    image: str,
    namespace: str | None,
    env: dict[str, str],
    container_port: int,
    replicas: int,
) -> dict[str, Any]:
    labels = {
        "app.kubernetes.io/name": "fala",
        "app.kubernetes.io/component": "control-plane",
        "app.kubernetes.io/instance": CONTROL_PLANE_SERVICE_NAME,
    }
    manifest: dict[str, Any] = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": CONTROL_PLANE_SERVICE_NAME,
            "labels": labels,
        },
        "spec": {
            "replicas": replicas,
            "selector": {"matchLabels": {"app.kubernetes.io/instance": CONTROL_PLANE_SERVICE_NAME}},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "containers": [
                        {
                            "name": "web",
                            "image": image,
                            "command": ["fala"],
                            "args": _control_plane_command(container_port)[1:],
                            "env": _k8s_env(env),
                            "ports": [{"name": "http", "containerPort": container_port}],
                        }
                    ],
                },
            },
        },
    }
    if namespace:
        manifest["metadata"]["namespace"] = namespace
    return manifest


def _k8s_control_plane_service(
    *,
    namespace: str | None,
    container_port: int,
) -> dict[str, Any]:
    labels = {
        "app.kubernetes.io/name": "fala",
        "app.kubernetes.io/component": "control-plane",
        "app.kubernetes.io/instance": CONTROL_PLANE_SERVICE_NAME,
    }
    manifest: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": CONTROL_PLANE_SERVICE_NAME,
            "labels": labels,
        },
        "spec": {
            "selector": {"app.kubernetes.io/instance": CONTROL_PLANE_SERVICE_NAME},
            "ports": [
                {
                    "name": "http",
                    "port": container_port,
                    "targetPort": "http",
                }
            ],
        },
    }
    if namespace:
        manifest["metadata"]["namespace"] = namespace
    return manifest


def _k8s_postgres_manifests(
    *,
    namespace: str | None,
    image: str,
    database: str,
    user: str,
    password: str,
    volume: str,
) -> list[dict[str, Any]]:
    labels = {
        "app.kubernetes.io/name": "postgres",
        "app.kubernetes.io/component": "database",
        "app.kubernetes.io/instance": POSTGRES_SERVICE_NAME,
    }
    deployment: dict[str, Any] = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": POSTGRES_SERVICE_NAME,
            "labels": labels,
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app.kubernetes.io/instance": POSTGRES_SERVICE_NAME}},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "containers": [
                        {
                            "name": "postgres",
                            "image": image,
                            "env": _k8s_env(
                                {
                                    "POSTGRES_DB": database,
                                    "POSTGRES_PASSWORD": password,
                                    "POSTGRES_USER": user,
                                }
                            ),
                            "ports": [{"name": "postgres", "containerPort": 5432}],
                        }
                    ],
                },
            },
        },
    }
    _add_k8s_pvc_mount(
        deployment,
        claim_name=volume,
        mount_path="/var/lib/postgresql/data",
    )
    service: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": POSTGRES_SERVICE_NAME,
            "labels": labels,
        },
        "spec": {
            "selector": {"app.kubernetes.io/instance": POSTGRES_SERVICE_NAME},
            "ports": [{"name": "postgres", "port": 5432, "targetPort": "postgres"}],
        },
    }
    pvc = _k8s_persistent_volume_claim(volume, namespace=namespace)
    if namespace:
        deployment["metadata"]["namespace"] = namespace
        service["metadata"]["namespace"] = namespace
    return [pvc, deployment, service]


def _k8s_persistent_volume_claim(
    name: str,
    *,
    namespace: str | None,
) -> dict[str, Any]:
    manifest: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": name,
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "10Gi"}},
        },
    }
    if namespace:
        manifest["metadata"]["namespace"] = namespace
    return manifest


def _add_k8s_pvc_mount(
    deployment: dict[str, Any],
    *,
    claim_name: str,
    mount_path: str,
) -> None:
    volume_name = _slug(claim_name, separator="-")
    template_spec = deployment["spec"]["template"]["spec"]
    container = template_spec["containers"][0]
    container.setdefault("volumeMounts", []).append(
        {"name": volume_name, "mountPath": mount_path}
    )
    template_spec.setdefault("volumes", []).append(
        {
            "name": volume_name,
            "persistentVolumeClaim": {"claimName": claim_name},
        }
    )


def _keda_scaled_object(
    worker: SupervisedWorkerSpec,
    *,
    run_id: str,
    prometheus_server: str,
    min_replicas: int,
    max_replicas: int,
    target_value: int,
    namespace: str | None,
) -> dict[str, Any]:
    target_name = _k8s_name(worker)
    query = (
        "max("
        f'fala_runtime_worker_target_workers{{run_id="{_promql_escape(run_id)}",'
        f'package_worker_id="{_promql_escape(worker.id)}"}}'
        ")"
    )
    metadata: dict[str, Any] = {
        "name": f"{target_name}-autoscale",
        "labels": {
            "app.kubernetes.io/name": "fala-worker",
            "app.kubernetes.io/component": "autoscaler",
            "app.kubernetes.io/instance": target_name,
            "fala.worker.id": worker.id,
            "fala.package.id": worker.package_id,
            "fala.pipeline.id": worker.pipeline_id,
        },
        "annotations": {
            "fala.dev/worker": yaml.safe_dump(
                _worker_metadata(worker),
                sort_keys=True,
            ).strip()
        },
    }
    if namespace:
        metadata["namespace"] = namespace
    return {
        "apiVersion": "keda.sh/v1alpha1",
        "kind": "ScaledObject",
        "metadata": metadata,
        "spec": {
            "scaleTargetRef": {
                "name": target_name,
            },
            "minReplicaCount": min_replicas,
            "maxReplicaCount": max_replicas,
            "triggers": [
                {
                    "type": "prometheus",
                    "metadata": {
                        "serverAddress": prometheus_server,
                        "metricName": "fala_runtime_worker_target_workers",
                        "query": query,
                        "threshold": str(target_value),
                    },
                }
            ],
        },
    }


def _worker_metadata(worker: SupervisedWorkerSpec) -> dict[str, Any]:
    return {
        "worker_id": worker.id,
        "package_id": worker.package_id,
        "pipeline_id": worker.pipeline_id,
        "process_id": worker.process_id,
        "capabilities": worker.capabilities,
        "resources": worker.resources.model_dump(mode="json"),
        "secrets": [
            {
                "id": secret.id,
                "env_var": secret.env_var,
                "required": secret.required,
            }
            for secret in worker.secrets
        ],
        "sandbox": worker.sandbox.model_dump(mode="json"),
    }


def _compose_secret_environment(secrets: list[WorkflowSecretSpec]) -> dict[str, str]:
    return {
        secret.env_var: (
            f"${{{secret.env_var}:?Fala secret {secret.id} is required}}"
            if secret.required
            else f"${{{secret.env_var}:-}}"
        )
        for secret in secrets
    }


def _compose_environment(env: dict[str, str]) -> dict[str, str]:
    return {key: str(value) for key, value in sorted(env.items())}


def _k8s_env(env: dict[str, str]) -> list[dict[str, str]]:
    return [
        {"name": key, "value": str(value)}
        for key, value in sorted(env.items())
    ]


def _k8s_secret_env(secrets: list[WorkflowSecretSpec]) -> list[dict[str, Any]]:
    return [
        {
            "name": secret.env_var,
            "valueFrom": {
                "secretKeyRef": {
                    "name": secret.kubernetes_secret_name or _slug(secret.id, separator="-"),
                    "key": secret.kubernetes_secret_key,
                    "optional": not secret.required,
                }
            },
        }
        for secret in sorted(secrets, key=lambda item: item.env_var)
    ]


def _k8s_security_context(sandbox: WorkerSandboxSpec) -> dict[str, Any]:
    context: dict[str, Any] = {
        "allowPrivilegeEscalation": sandbox.allow_privilege_escalation,
        "readOnlyRootFilesystem": sandbox.read_only_root_filesystem,
        "runAsNonRoot": sandbox.run_as_non_root,
    }
    if sandbox.drop_capabilities:
        context["capabilities"] = {"drop": list(sandbox.drop_capabilities)}
    if sandbox.seccomp_profile:
        context["seccompProfile"] = {"type": sandbox.seccomp_profile}
    return context


def _k8s_resources(resources: ResourceSpec) -> dict[str, Any]:
    requests: dict[str, str] = {}
    limits: dict[str, str] = {}
    if resources.cpu_cores is not None:
        requests["cpu"] = str(resources.cpu_cores)
    if resources.memory_mb is not None:
        requests["memory"] = f"{resources.memory_mb}Mi"
    if resources.gpu_count is not None:
        requests["nvidia.com/gpu"] = str(resources.gpu_count)
        limits["nvidia.com/gpu"] = str(resources.gpu_count)
    result: dict[str, Any] = {}
    if requests:
        result["requests"] = requests
    if limits:
        result["limits"] = limits
    return result


def _service_name(worker: SupervisedWorkerSpec) -> str:
    return _slug(f"fala-{worker.package_id}-{worker.id}", separator="_")


def _k8s_name(worker: SupervisedWorkerSpec) -> str:
    return _slug(f"fala-{worker.package_id}-{worker.id}", separator="-")


def _slug(value: str, *, separator: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", separator, value).strip(separator).lower()
    return slug or "fala-worker"


def _promql_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _yaml_dump(value: dict[str, Any]) -> str:
    return yaml.safe_dump(value, sort_keys=False)
