# Configuration

## Environment variables

`N4CLUSTER_SERVER`
: default coordinator URL for the client/worker (`http://127.0.0.1:8765`). Overridden by `--server`.

`N4CLUSTER_TOKEN`
: default bearer token for the server, client and worker. Overridden by `--token`.

Precedence everywhere is **flag > environment variable > built-in default**.

## Server settings (`ServerConfig`)

Set through `n4cluster server` flags (see {doc}`cli-reference`); the defaults below are the
trusted-LAN posture.

| Setting | Default | Meaning |
|---|---|---|
| `token` | `None` | bearer token; `None` ⇒ no auth (dev) |
| `allow_python_jobs` | `False` | permit `python_entrypoint` pipelines |
| `lease_ttl_s` | `60.0` | task lease TTL (renewed on heartbeat) |
| `worker_dead_after_s` | `45.0` | mark a silent worker dead after this |
| `max_artifact_mb` | `2048` | streaming cap for artifact uploads |
| `max_request_mb` | `16` | cap for JSON request bodies (uploads exempt) |
| `cors_origins` | `[]` | allowed browser origins (opt-in) |

A request whose JSON body exceeds `max_request_mb` is rejected with **413**; multipart
artifact uploads keep their own streaming `max_artifact_mb` limit.

## State directory layout

```
<state_dir>/
├── store.sqlite        # queue + metadata (WAL mode)
└── objects/aa/bb/<sha256>   # content-addressed blob store
```

## Logging

The server and worker use Python `logging`. Choose verbosity with `--log-level`
(`debug`/`info`/`warning`/…) and optionally tee to a file with `--log-file`. CLI command
output (`status`, `logs`, `jobs`, …) is printed directly and is independent of logging.
