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

import httpx
import yaml

from .client import ClusterClient, build_nirs4all_run_request
from .client_errors import (
    ClusterAuthError,
    ClusterConnectionError,
    ClusterError,
    ClusterPermissionError,
    ClusterVersionError,
)
from .logging_setup import LOG_LEVELS


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
    token = _credential(args)
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


def _credential(args: argparse.Namespace) -> str | None:
    path = getattr(args, "token_file", None)
    if path:
        from .credentials import read_bearer_file

        return read_bearer_file(path)
    return os.environ.get("NIRS4ALL_PROJECT_TOKEN") or os.environ.get("N4CLUSTER_TOKEN")


def cmd_init_state(args: argparse.Namespace) -> int:
    from .server.state import bootstrap_state

    bootstrap_state(args.state, ())
    print("New project-services state initialized. Provision a token before startup.")
    return 0


def cmd_token_create(args: argparse.Namespace) -> int:
    from nirs4all_access import ProjectPrincipal, issue_credential, write_credential_store

    from .server.auth import rights_from_roles
    from .server.state import state_credentials, validate_state

    state = validate_state(args.state)
    records = state_credentials(state)
    p = ProjectPrincipal(
        args.project, args.principal, frozenset({"cluster"}), frozenset(rights_from_roles(_csv_list(args.role)))
    )
    token, record = issue_credential(p, expires_at=args.expires_at)
    write_credential_store(state / "auth" / "credentials.json", (*records, record))
    print("Credential provisioned; restart server for changes to apply.", file=sys.stderr)
    print(token)  # Intentional one-time secret delivery. Never written to auth state.
    return 0


def cmd_token_revoke(args: argparse.Namespace) -> int:
    from dataclasses import replace

    from nirs4all_access import write_credential_store

    from .server.state import state_credentials, validate_state

    state = validate_state(args.state)
    records = state_credentials(state)
    matches = [r for r in records if r.key_id == args.key_id]
    if len(matches) != 1:
        raise ValueError("unknown credential key ID; provide the exact configured key_id")
    write_credential_store(
        state / "auth" / "credentials.json",
        tuple(replace(r, revoked=True) if r.key_id == args.key_id else r for r in records),
    )
    print("Credential revoked in hash-only configuration; restart server to apply this snapshot.")
    return 0


def cmd_server(args: argparse.Namespace) -> int:
    import uvicorn
    from nirs4all_access import require_secure_transport

    from .logging_setup import QUIET_UVICORN_PROFILE, configure_logging
    from .server.app import ServerConfig, create_app

    require_secure_transport(
        args.host, tls=bool(args.ssl_keyfile and args.ssl_certfile), trusted_proxy=args.trusted_tls_proxy
    )
    config = ServerConfig(
        state_dir=args.state,
        allow_python_jobs=args.allow_python_jobs,
        lease_ttl_s=args.lease_ttl,
        cors_origins=_csv_list(args.cors_origin),
    )
    configure_logging(args.log_level, args.log_file)
    app = create_app(config)
    print(f"[n4cluster] project-token server on {args.host}:{args.port}")
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        **QUIET_UVICORN_PROFILE,
        ssl_keyfile=args.ssl_keyfile,
        ssl_certfile=args.ssl_certfile,
    )
    return 0


def cmd_worker(args: argparse.Namespace) -> int:
    from .logging_setup import configure_logging
    from .versioning import ClusterVersionError
    from .worker.agent import WorkerAgent

    configure_logging(args.log_level, args.log_file)
    server = args.server or os.environ.get("N4CLUSTER_SERVER", "http://127.0.0.1:8765")
    token = _credential(args)
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
        allowed_roots=tuple(Path(p).resolve() for p in args.allow_path_root or []),
        python_exe=args.python_exe,
    )
    try:
        worker_id = agent.register()
    except ClusterVersionError as exc:
        print(f"[n4cluster] cannot register: {exc}")
        return 2
    print(
        f"[n4cluster] worker {worker_id} registered to {server}  slots={args.slots}  "
        f"labels={agent.labels}  (Ctrl-C to stop)"
    )
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
            print(
                f"finished  status={job.status.value}  "
                f"best[{spec.get('rank_metric', 'best_rmse')}]={job.aggregate.best_metric}"
            )
            _print_ranking(job)
            if job.status.value != "succeeded" or job.aggregate.completeness == "incomplete":
                return 1
            if args.out:
                written = client.download_all_artifacts(job.id, args.out)
                print(f"downloaded {len(written)} artifact(s) to {args.out}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    req = _run_job_from_args(args)
    with _client(args) as client:
        job = client.submit(req)
        scope = req.parity.scope if req.parity else "unknown"
        print(
            f"submitted nirs4all.run job {job.id}  status={job.status.value}  "
            f"tasks={job.aggregate.num_tasks}  parity={scope}"
        )
        if args.wait:
            job = client.wait(job.id, timeout=args.timeout)
            print(f"finished  status={job.status.value}  best[{req.rank_metric}]={job.aggregate.best_metric}")
            _print_ranking(job)
            if job.status.value != "succeeded" or job.aggregate.completeness == "incomplete":
                return 1
            if args.out:
                written = client.download_all_artifacts(job.id, args.out)
                print(f"downloaded {len(written)} artifact(s) to {args.out}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    with _client(args) as client:
        job = client.get_job(args.job_id)
        agg = job.aggregate
        print(f"job {job.id}  status={job.status.value}  name={job.name}")
        print(
            f"tasks: {agg.num_succeeded} ok / {agg.num_failed} failed / "
            f"{agg.num_running} running / {agg.num_queued} queued  (total {agg.num_tasks})"
        )
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
            print(
                f"{j.id}  {j.status.value:<10}  {a.num_succeeded}/{a.num_tasks} ok  "
                f"{a.num_failed} failed  {best}  {j.name or ''}".rstrip()
            )
    return 0


def cmd_workers(args: argparse.Namespace) -> int:
    with _client(args) as client:
        for w in client.list_workers():
            diverge = "  !version" if w.get("version_divergent") else ""
            ver = w.get("cluster_version") or "?"
            print(
                f"{w['id']}  {w['status']:<5}  slots={w['slots_used']}/{w['slots_total']}  "
                f"labels={w['labels']}  v={ver}{diverge}  name={w.get('name')}"
            )
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


class _Parser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        kwargs["allow_abbrev"] = False
        super().__init__(*args, **kwargs)

    def error(self, message):
        if not message.startswith("legacy credential options"):
            message = "invalid command arguments; use --help. Project clients use --token-file or NIRS4ALL_PROJECT_TOKEN; provision new state with init-state."
        super().error(message)

    def parse_known_args(self, args=None, namespace=None):
        values = list(args) if args is not None else sys.argv[1:]
        # Refuse before argparse can echo either values or unknown arguments.
        provisioning = self.prog.endswith("token-create") or (self.prog == "n4cluster" and "token-create" in values[:1])
        forbidden = {"--token", "--auth-file"} | (set() if provisioning else {"--principal"})
        if any(value.split("=", 1)[0] in forbidden for value in values):
            self.error(
                "legacy credential options are refused; use project init-state in a NEW empty directory and token-create; clients use --token-file or NIRS4ALL_PROJECT_TOKEN"
            )
        return super().parse_known_args(values, namespace)


def build_parser() -> argparse.ArgumentParser:
    parser = _Parser(prog="n4cluster", description="nirs4all-cluster CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_conn(p: argparse.ArgumentParser) -> None:
        p.add_argument("--server", help="server base URL (or $N4CLUSTER_SERVER)")
        p.add_argument("--token-file", help="private credential file (or $NIRS4ALL_PROJECT_TOKEN)")

    p_init = sub.add_parser("init-state", help="initialize a NEW empty project-services directory")
    p_init.add_argument("--state", required=True)
    p_init.set_defaults(func=cmd_init_state)
    p_token = sub.add_parser("token-create", help="provision a project credential; output secret once")
    p_token.add_argument("--state", required=True)
    p_token.add_argument("--project", required=True)
    p_token.add_argument("--principal", required=True)
    p_token.add_argument("--role", action="append", required=True)
    p_token.add_argument("--expires-at", type=float)
    p_token.set_defaults(func=cmd_token_create)

    p_revoke = sub.add_parser("token-revoke", help="revoke one exact credential key ID; restart required")
    p_revoke.add_argument("--state", required=True)
    p_revoke.add_argument("--key-id", required=True)
    p_revoke.set_defaults(func=cmd_token_revoke)

    p_server = sub.add_parser("server", help="run the cluster server")
    p_server.add_argument("--host", default="127.0.0.1")
    p_server.add_argument("--port", type=int, default=8765)
    p_server.add_argument("--state", default="./cluster-state")
    p_server.add_argument(
        "--trusted-tls-proxy", action="store_true", help="acknowledge TLS termination by a trusted proxy"
    )
    p_server.add_argument("--ssl-keyfile")
    p_server.add_argument("--ssl-certfile")
    p_server.add_argument("--allow-python-jobs", action="store_true")
    p_server.add_argument("--lease-ttl", type=float, default=60.0)
    p_server.add_argument("--log-level", choices=LOG_LEVELS, default="info", help="application diagnostics only")
    p_server.add_argument("--log-file", default=None, help="also write logs to this file")
    p_server.add_argument(
        "--cors-origin",
        action="append",
        help="allow this browser origin to call the API (repeatable; off by default)",
    )
    p_server.set_defaults(func=cmd_server)

    p_worker = sub.add_parser("worker", help="run a worker that polls the server")
    p_worker.add_argument("--server", help="server base URL (or $N4CLUSTER_SERVER)")
    p_worker.add_argument("--token-file", help="private credential file (or $NIRS4ALL_PROJECT_TOKEN)")
    p_worker.add_argument("--allow-path-root", action="append", help="allowed shared input root; deny all by default")
    p_worker.add_argument("--python-exe", help="explicitly provisioned runner Python interpreter")
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
    p_worker.add_argument("--log-level", choices=LOG_LEVELS, default="info", help="application diagnostics only")
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
    except httpx.HTTPError as exc:
        from .transport_diagnostics import http_failure_diagnostic

        print(f"[n4cluster] transport failure: {http_failure_diagnostic(exc)}", file=sys.stderr)
        return 3 if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in {401, 403} else 4
    except ClusterPermissionError as exc:
        missing = ", ".join(sorted(exc.missing_rights)) or "?"
        who = f" (as {exc.principal})" if exc.principal else ""
        print(f"[n4cluster] forbidden{who}: your credential lacks the right(s): {missing}", file=sys.stderr)
        return 3
    except ClusterAuthError:
        print("[n4cluster] unauthorized: provide a valid authentication credential", file=sys.stderr)
        return 3
    except ClusterConnectionError:
        print("[n4cluster] cannot reach server", file=sys.stderr)
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
