# REST & WebSocket API

All `/v1` endpoints require the bearer token when the server is started with `--token`.
Every `/v1` request/response also carries the version handshake headers
(`X-N4C-Version`, `X-N4C-Api`, `X-N4C-Role`) — see {doc}`versioning`.

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
