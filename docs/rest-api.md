# REST & WebSocket API

All `/v1` endpoints are gated by **credential-bound rights** when the server is
configured with principals (`--principal NAME:TOKEN:ROLES` / `--auth-file`) or a
legacy `--token` (kept as a single all-rights admin). With neither configured the
server runs **open (dev mode)**. Every `/v1` request/response also carries the
version handshake headers (`X-N4C-Version`, `X-N4C-Api`, `X-N4C-Role`) — see
{doc}`versioning`.

## Authorization & rights

A caller's rights come from its bearer credential, **never** from the advisory
`X-N4C-Role` header. A missing/invalid credential returns **401**; a valid
credential lacking the required right returns **403**.

| Right | Gates |
|---|---|
| `submit` | `POST /v1/jobs`, `POST /v1/artifacts` (input upload) |
| `read` | every `GET /v1/jobs*`, `/v1/stats`, `/v1/workers`, `GET /v1/artifacts/{id}`, both WS streams (`?token=`) |
| `cancel` | `POST /v1/jobs/{id}/cancel` |
| `execute` | the whole worker API (`POST /v1/workers/*`, `POST /v1/tasks/*`) |
| `admin` | wildcard — grants every right |

Roles bundle rights: `submitter` = submit+read+cancel, `executor` = read+execute
(the worker agent), `viewer` = read, `admin` = all. `register` echoes the granted
rights in its `WorkerRegistered` response for executor self-diagnosis.

## Health & dashboard (no auth)

| Method | Path | Returns |
|---|---|---|
| GET | `/` | `{service, version, api_version, ok}` |
| GET | `/healthz` | `{ok: true}` (liveness probe) |
| GET | `/version` | `{service, version, api_version}` |
| GET | `/ui` | the live dashboard ({doc}`dashboard`) |

## Client API

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/jobs` | submit a job → `JobView` |
| GET | `/v1/jobs` | list jobs (`status`, `name`, `limit`, `created_before` cursor) |
| GET | `/v1/jobs/{id}` | job status + aggregate |
| GET | `/v1/jobs/{id}/tasks` | tasks of a job |
| GET | `/v1/jobs/{id}/events` | event history (`after_id`, `limit`) |
| GET | `/v1/jobs/{id}/artifacts` | artifacts of a job |
| POST | `/v1/jobs/{id}/cancel` | request cancellation |
| GET | `/v1/stats` | server-wide counters → `ClusterStats` |
| GET | `/v1/workers` | registered workers (+ version & divergence flag) |
| POST | `/v1/artifacts` | upload an input artifact (multipart) |
| GET | `/v1/artifacts/{id}` | download an artifact |
| WS | `/v1/jobs/{id}/events/stream` | live events for one job (`?token=`) |
| WS | `/v1/events/stream` | **global** live feed across all jobs/workers (`?token=`) |

## Worker API

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/workers/register` | register, declare labels/capabilities/versions |
| POST | `/v1/workers/{id}/heartbeat` | keep-alive; returns `cancel_task_ids` |
| POST | `/v1/workers/{id}/lease` | atomically claim the next eligible task |
| POST | `/v1/tasks/{id}/start` | mark a leased task running |
| POST | `/v1/tasks/{id}/events` | report progress |
| POST | `/v1/tasks/{id}/artifacts` | upload a task output (model/logs/workspace) |
| POST | `/v1/tasks/{id}/complete` | report success (`TaskResult` + fingerprint) |
| POST | `/v1/tasks/{id}/fail` | report failure (requeued if attempts remain) |

An incompatible protocol major is rejected with **HTTP 426**; an oversized JSON body with
**413**.
