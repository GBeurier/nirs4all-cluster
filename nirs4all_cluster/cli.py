"""``n4cluster`` command-line interface.

Subcommands:

    n4cluster server   --host 0.0.0.0 --port 8765 --state ./cluster-state [auth options] [--allow-python-jobs]
    n4cluster worker   --server URL [auth options] [--labels k=v,...] [--slots N] [--allow-python]
    n4cluster submit   job.yaml [--server URL] [auth options] [--wait] [--out DIR]
    n4cluster run      --pipeline P.yaml --dataset DATA [--param k=v] [--wait]
    n4cluster status   JOB_ID
    n4cluster jobs     [--status S] [--name N] [--limit L]
    n4cluster logs     JOB_ID
    n4cluster cancel   JOB_ID
    n4cluster artifacts JOB_ID --out ./results
    n4cluster workers

The server also serves a live ops dashboard at ``/ui``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import yaml

from .client import ClusterClient, build_nirs4all_run_request
from .client_errors import (
    ClusterAuthError,
    ClusterConnectionError,
    ClusterError,
    ClusterPermissionError,
    ClusterVersionError,
)


def _parse_labels(raw: str | None) -> dict[str, str]:
    labels: dict[str, str] = {}
    if not raw:
        return labels
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair:
            continue
        key, _, value = pair.partition("=")
        labels[key.strip()] = value.strip()
    return labels


def _client(args: argparse.Namespace) -> ClusterClient:
    server = getattr(args, "server", None) or os.environ.get("N4CLUSTER_SERVER", "http://127.0.0.1:8765")
    token = getattr(args, "token", None) or os.environ.get("N4CLUSTER_TOKEN")
    return ClusterClient(server, token=token)


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #


def _csv_list(values: list[str] | None) -> list[str]:
    out: list[str] = []
    for value in values or []:
        out.extend(p.strip() for p in value.split(",") if p.strip())
    return out


def _parse_key_values(raw: list[str] | None, *, option: str) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for item in raw or []:
        key, sep, value = item.partition("=")
        key = key.strip()
        if not sep or not key:
            raise ValueError(f"{option} expects KEY=VALUE (got {item!r})")
        parsed[key] = yaml.safe_load(value)
    return parsed


def _parse_string_key_values(raw: list[str] | None, *, option: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for item in raw or []:
        key, sep, value = item.partition("=")
        key = key.strip()
        if not sep or not key:
            raise ValueError(f"{option} expects KEY=VALUE (got {item!r})")
        parsed[key] = value.strip()
    return parsed


def _run_job_from_args(args: argparse.Namespace) -> Any:
    pipelines = args.pipeline if len(args.pipeline) > 1 else None
    pipeline = None if pipelines is not None else args.pipeline[0]
    datasets = args.dataset if len(args.dataset) > 1 else None
    dataset = None if datasets is not None else args.dataset[0]
    labels = _parse_string_key_values(args.require_label, option="--require-label")
    requirements: dict[str, Any] | None = {"labels": labels} if labels else None
    outputs = {
        "export_best_model": not args.no_export_best_model,
        "keep_task_workspace": args.keep_task_workspace,
    }
    return build_nirs4all_run_request(
        pipeline=pipeline,
        pipelines=pipelines,
        dataset=dataset,
        datasets=datasets,
        params=_parse_key_values(args.param, option="--param"),
        n_jobs=args.n_jobs,
        inner_n_jobs=args.inner_n_jobs,
        name=args.name,
        priority=args.priority,
        requirements=requirements,
        outputs=outputs,
        rank_metric=args.rank_metric,
        rank_mode=args.rank_mode,
        idempotency_key=args.idempotency_key,
    )


def _load_principals(specs: list[str] | None, auth_file: str | None) -> list[Any]:
    """Build RBAC principals from ``--principal`` entries and/or ``--auth-file`` JSON.

    Raises ``ValueError`` on a malformed spec or unknown role so the server fails
    fast at startup rather than silently granting nothing.
    """
    from .server.auth import Principal, rights_from_roles

    principals: list[Any] = []
    for spec in specs or []:
        name, sep, rest = spec.partition(":")
        token, sep2, roles_csv = rest.partition(":")
        if not (name and sep and token):
            raise ValueError(f"RBAC principal spec must contain name, token, and roles separated by ':' (got {spec!r})")
        roles = [r.strip() for r in roles_csv.split(",") if r.strip()]
        if not roles:
            raise ValueError(f"RBAC principal {name!r} needs at least one role")
        principals.append(Principal(name=name, token=token, rights=rights_from_roles(roles)))
    if auth_file:
        entries = json.loads(Path(auth_file).read_text(encoding="utf-8"))
        for entry in entries:
            principals.append(
                Principal(
                    name=entry["name"],
                    token=entry["token"],
                    rights=rights_from_roles(entry.get("roles", [])),
                )
            )
    return principals


def cmd_server(args: argparse.Namespace) -> int:
    import uvicorn

    from .logging_setup import configure_logging
    from .server.app import ServerConfig, create_app

    configure_logging(args.log_level, args.log_file)
    token = args.token or os.environ.get("N4CLUSTER_TOKEN")
    try:
        principals = _load_principals(args.principal, args.auth_file)
    except (ValueError, KeyError, OSError, json.JSONDecodeError) as exc:
        print(f"[n4cluster] invalid principals config: {exc}")
        return 2
    config = ServerConfig(
        state_dir=args.state,
        token=token,
        principals=principals,
        allow_python_jobs=args.allow_python_jobs,
        lease_ttl_s=args.lease_ttl,
        cors_origins=_csv_list(args.cors_origin),
    )
    app = create_app(config)
    auth_mode = "rbac" if principals else ("token" if token else "off")
    print(f"[n4cluster] server on http://{args.host}:{args.port}  state={args.state}  "
          f"auth={auth_mode}  python_jobs={'on' if args.allow_python_jobs else 'off'}")
    print(f"[n4cluster] dashboard: http://{args.host}:{args.port}/ui")
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)
    return 0


def cmd_worker(args: argparse.Namespace) -> int:
    from .logging_setup import configure_logging
    from .versioning import ClusterVersionError
    from .worker.agent import WorkerAgent

    configure_logging(args.log_level, args.log_file)
    server = args.server or os.environ.get("N4CLUSTER_SERVER", "http://127.0.0.1:8765")
    token = args.token or os.environ.get("N4CLUSTER_TOKEN")
    capabilities: dict[str, Any] = {}
    if args.memory_gb is not None:
        capabilities["memory_gb"] = args.memory_gb
    agent = WorkerAgent(
        server,
        token=token,
        state_dir=args.state,
        labels=_parse_labels(args.labels),
        capabilities=capabilities,
        slots=args.slots,
        allow_python=args.allow_python,
        name=args.name,
        poll_interval=args.poll_interval,
        gpu_count=args.gpus,
    )
    try:
        worker_id = agent.register()
    except ClusterVersionError as exc:
        print(f"[n4cluster] cannot register: {exc}")
        return 2
    print(f"[n4cluster] worker {worker_id} registered to {server}  slots={args.slots}  "
          f"labels={agent.labels}  (Ctrl-C to stop)")
    try:
        agent.serve()
    except KeyboardInterrupt:
        print("\n[n4cluster] worker stopping...")
    return 0


def _load_job_file(path: str) -> dict[str, Any]:
    text = Path(path).read_text(encoding="utf-8")
    if path.endswith((".yaml", ".yml")):
        return yaml.safe_load(text)
    return json.loads(text)


def cmd_submit(args: argparse.Namespace) -> int:
    spec = _load_job_file(args.job_file)
    with _client(args) as client:
        job = client.submit(spec)
        print(f"submitted job {job.id}  status={job.status.value}  tasks={job.aggregate.num_tasks}")
        if args.wait:
            job = client.wait(job.id, timeout=args.timeout)
            print(f"finished  status={job.status.value}  "
                  f"best[{spec.get('rank_metric', 'best_rmse')}]={job.aggregate.best_metric}")
            _print_ranking(job)
            if args.out:
                written = client.download_all_artifacts(job.id, args.out)
                print(f"downloaded {len(written)} artifact(s) to {args.out}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    req = _run_job_from_args(args)
    with _client(args) as client:
        job = client.submit(req)
        scope = req.parity.scope if req.parity else "unknown"
        print(f"submitted nirs4all.run job {job.id}  status={job.status.value}  "
              f"tasks={job.aggregate.num_tasks}  parity={scope}")
        if args.wait:
            job = client.wait(job.id, timeout=args.timeout)
            print(f"finished  status={job.status.value}  best[{req.rank_metric}]={job.aggregate.best_metric}")
            _print_ranking(job)
            if args.out:
                written = client.download_all_artifacts(job.id, args.out)
                print(f"downloaded {len(written)} artifact(s) to {args.out}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    with _client(args) as client:
        job = client.get_job(args.job_id)
        agg = job.aggregate
        print(f"job {job.id}  status={job.status.value}  name={job.name}")
        print(f"tasks: {agg.num_succeeded} ok / {agg.num_failed} failed / "
              f"{agg.num_running} running / {agg.num_queued} queued  (total {agg.num_tasks})")
        if agg.best_metric is not None:
            print(f"best metric: {agg.best_metric}  (task {agg.best_task_id})")
        _print_ranking(job)
        if agg.errors:
            print("errors:")
            for task_id, err in agg.errors.items():
                print(f"  {task_id}: {err.splitlines()[0] if err else ''}")
    return 0


def cmd_logs(args: argparse.Namespace) -> int:
    with _client(args) as client:
        for ev in client.get_events(args.job_id, limit=args.limit):
            ts = f"{ev.ts:.0f}"
            print(f"[{ts}] {ev.level.value:<7} {ev.type:<20} {ev.message}")
    return 0


def cmd_cancel(args: argparse.Namespace) -> int:
    with _client(args) as client:
        job = client.cancel(args.job_id)
        print(f"job {job.id}  status={job.status.value}")
    return 0


def cmd_artifacts(args: argparse.Namespace) -> int:
    with _client(args) as client:
        arts = client.list_artifacts(args.job_id)
        for art in arts:
            print(f"{art['role']:<12} {art['id']}  {art.get('filename')}  {art['size_bytes']} bytes")
        if args.out:
            written = client.download_all_artifacts(args.job_id, args.out)
            print(f"downloaded {len(written)} artifact(s) to {args.out}")
    return 0


def cmd_jobs(args: argparse.Namespace) -> int:
    with _client(args) as client:
        jobs = client.list_jobs(limit=args.limit, status=args.status, name=args.name)
        if not jobs:
            print("(no jobs)")
        for j in jobs:
            a = j.aggregate
            best = "" if a.best_metric is None else f"best={a.best_metric}"
            print(f"{j.id}  {j.status.value:<10}  {a.num_succeeded}/{a.num_tasks} ok  "
                  f"{a.num_failed} failed  {best}  {j.name or ''}".rstrip())
    return 0


def cmd_workers(args: argparse.Namespace) -> int:
    with _client(args) as client:
        for w in client.list_workers():
            diverge = "  !version" if w.get("version_divergent") else ""
            ver = w.get("cluster_version") or "?"
            print(f"{w['id']}  {w['status']:<5}  slots={w['slots_used']}/{w['slots_total']}  "
                  f"labels={w['labels']}  v={ver}{diverge}  name={w.get('name')}")
    return 0


def _print_ranking(job: Any) -> None:
    ranking = job.aggregate.ranking
    if not ranking:
        return
    print("ranking:")
    for i, entry in enumerate(ranking[:10], 1):
        metric_keys = {k: v for k, v in entry.items() if k not in ("task_id", "dataset", "pipeline", "metrics")}
        metric_str = "  ".join(f"{k}={v}" for k, v in metric_keys.items())
        print(f"  {i:>2}. {entry.get('pipeline')} x {entry.get('dataset')}  {metric_str}")


# --------------------------------------------------------------------------- #
# Parser
# --------------------------------------------------------------------------- #


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="n4cluster", description="nirs4all-cluster CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_conn(p: argparse.ArgumentParser) -> None:
        p.add_argument("--server", help="server base URL (or $N4CLUSTER_SERVER)")
        p.add_argument("--token", help="auth token (or $N4CLUSTER_TOKEN)")

    p_server = sub.add_parser("server", help="run the cluster server")
    p_server.add_argument("--host", default="127.0.0.1")
    p_server.add_argument("--port", type=int, default=8765)
    p_server.add_argument("--state", default="./cluster-state")
    p_server.add_argument(
        "--token",
        help="legacy single all-rights admin token (or $N4CLUSTER_TOKEN); for RBAC use --principal",
    )
    p_server.add_argument(
        "--principal",
        action="append",
        metavar="PRINCIPAL_SPEC",
        help=(
            "credential-bound RBAC principal as name/token/roles separated by ':' "
            "(roles: submitter,executor,viewer,admin; comma-separate to combine; repeatable)"
        ),
    )
    p_server.add_argument(
        "--auth-file",
        help='JSON file of [{"name","token","roles":[...]}] principals (alternative to --principal)',
    )
    p_server.add_argument("--allow-python-jobs", action="store_true")
    p_server.add_argument("--lease-ttl", type=float, default=60.0)
    p_server.add_argument("--log-level", default="info")
    p_server.add_argument("--log-file", default=None, help="also write logs to this file")
    p_server.add_argument(
        "--cors-origin",
        action="append",
        help="allow this browser origin to call the API (repeatable; off by default)",
    )
    p_server.set_defaults(func=cmd_server)

    p_worker = sub.add_parser("worker", help="run a worker that polls the server")
    p_worker.add_argument("--server", help="server base URL (or $N4CLUSTER_SERVER)")
    p_worker.add_argument("--token", help="auth token (or $N4CLUSTER_TOKEN)")
    p_worker.add_argument("--state", default="./worker-state")
    p_worker.add_argument("--labels", help="comma-separated k=v capability labels")
    p_worker.add_argument("--slots", type=int, default=1)
    p_worker.add_argument("--memory-gb", type=float, default=None)
    p_worker.add_argument(
        "--gpus",
        type=int,
        default=None,
        help="force declared GPU count (default: auto-detect via nvidia-smi; 0 hides GPUs)",
    )
    p_worker.add_argument("--allow-python", action="store_true", help="permit python_entrypoint pipelines")
    p_worker.add_argument("--name", default=None)
    p_worker.add_argument("--poll-interval", type=float, default=2.0)
    p_worker.add_argument("--log-level", default="info")
    p_worker.add_argument("--log-file", default=None, help="also write logs to this file")
    p_worker.set_defaults(func=cmd_worker)

    p_submit = sub.add_parser("submit", help="submit a job YAML/JSON")
    p_submit.add_argument("job_file")
    p_submit.add_argument("--wait", action="store_true")
    p_submit.add_argument("--timeout", type=float, default=None)
    p_submit.add_argument("--out", help="download artifacts here after completion")
    add_conn(p_submit)
    p_submit.set_defaults(func=cmd_submit)

    p_run = sub.add_parser("run", help="submit a nirs4all.run job without a job file")
    p_run.add_argument("--pipeline", action="append", required=True, help="pipeline YAML/JSON path on workers")
    p_run.add_argument("--dataset", action="append", required=True, help="dataset path on workers")
    p_run.add_argument("--param", action="append", help="nirs4all.run parameter as KEY=YAML_VALUE")
    p_run.add_argument("--n-jobs", type=int, default=None, help="local nirs4all.run n_jobs mapped to inner_n_jobs")
    p_run.add_argument("--inner-n-jobs", type=int, default=None, help="worker-local nirs4all.run n_jobs")
    p_run.add_argument("--name", default=None)
    p_run.add_argument("--priority", type=int, default=0)
    p_run.add_argument("--rank-metric", default="best_rmse")
    p_run.add_argument("--rank-mode", choices=["min", "max"], default="min")
    p_run.add_argument("--idempotency-key", default=None)
    p_run.add_argument("--require-label", action="append", help="worker label requirement as KEY=VALUE")
    p_run.add_argument("--no-export-best-model", action="store_true")
    p_run.add_argument("--keep-task-workspace", action="store_true")
    p_run.add_argument("--wait", action="store_true")
    p_run.add_argument("--timeout", type=float, default=None)
    p_run.add_argument("--out", help="download artifacts here after completion")
    add_conn(p_run)
    p_run.set_defaults(func=cmd_run)

    p_status = sub.add_parser("status", help="show job status + ranking")
    p_status.add_argument("job_id")
    add_conn(p_status)
    p_status.set_defaults(func=cmd_status)

    p_logs = sub.add_parser("logs", help="print job events")
    p_logs.add_argument("job_id")
    p_logs.add_argument("--limit", type=int, default=500)
    add_conn(p_logs)
    p_logs.set_defaults(func=cmd_logs)

    p_cancel = sub.add_parser("cancel", help="cancel a job")
    p_cancel.add_argument("job_id")
    add_conn(p_cancel)
    p_cancel.set_defaults(func=cmd_cancel)

    p_art = sub.add_parser("artifacts", help="list/download job artifacts")
    p_art.add_argument("job_id")
    p_art.add_argument("--out", help="download artifacts to this directory")
    add_conn(p_art)
    p_art.set_defaults(func=cmd_artifacts)

    p_jobs = sub.add_parser("jobs", help="list jobs (filter by status/name)")
    p_jobs.add_argument("--status", help="filter by status (queued/running/succeeded/failed/...)")
    p_jobs.add_argument("--name", help="filter by name substring")
    p_jobs.add_argument("--limit", type=int, default=50)
    add_conn(p_jobs)
    p_jobs.set_defaults(func=cmd_jobs)

    p_workers = sub.add_parser("workers", help="list registered workers")
    add_conn(p_workers)
    p_workers.set_defaults(func=cmd_workers)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except ClusterPermissionError as exc:
        missing = ", ".join(sorted(exc.missing_rights)) or "?"
        who = f" (as {exc.principal})" if exc.principal else ""
        print(f"[n4cluster] forbidden{who}: your credential lacks the right(s): {missing}", file=sys.stderr)
        return 3
    except ClusterAuthError:
        print("[n4cluster] unauthorized: provide a valid authentication credential", file=sys.stderr)
        return 3
    except ClusterConnectionError as exc:
        print(f"[n4cluster] cannot reach server: {exc.message}", file=sys.stderr)
        return 4
    except ClusterVersionError as exc:
        print(f"[n4cluster] {exc}", file=sys.stderr)
        return 2
    except (ValueError, OSError, json.JSONDecodeError, yaml.YAMLError) as exc:
        print(f"[n4cluster] invalid input: {exc}", file=sys.stderr)
        return 2
    except ClusterError as exc:
        print(f"[n4cluster] error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
