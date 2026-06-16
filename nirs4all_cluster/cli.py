"""``n4cluster`` command-line interface.

Subcommands:

    n4cluster server   --host 0.0.0.0 --port 8765 --state ./cluster-state [--token ${N4CLUSTER_TOKEN} [--allow-python-jobs]
    n4cluster worker   --server URL [--token ${N4CLUSTER_TOKEN} [--labels k=v,...] [--slots N] [--allow-python]
    n4cluster submit   job.yaml [--server URL] [--token ${N4CLUSTER_TOKEN} [--wait] [--out DIR]
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

from .client import ClusterClient


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


def cmd_server(args: argparse.Namespace) -> int:
    import uvicorn

    from .logging_setup import configure_logging
    from .server.app import ServerConfig, create_app

    configure_logging(args.log_level, args.log_file)
    token = args.token or os.environ.get("N4CLUSTER_TOKEN")
    config = ServerConfig(
        state_dir=args.state,
        token=token,
        allow_python_jobs=args.allow_python_jobs,
        lease_ttl_s=args.lease_ttl,
        cors_origins=_csv_list(args.cors_origin),
    )
    app = create_app(config)
    print(f"[n4cluster] server on http://{args.host}:{args.port}  state={args.state}  "
          f"auth={'on' if token else 'off'}  python_jobs={'on' if args.allow_python_jobs else 'off'}")
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
    p_server.add_argument("--token", help="static auth token (or $N4CLUSTER_TOKEN)")
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
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
