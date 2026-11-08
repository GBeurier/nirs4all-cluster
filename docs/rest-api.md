# REST and WebSocket API v2

All /v1 routes use Authorization: Bearer project credentials. Shared verifier:
`packages/nirs4all-access/CONTRACT.md`. No query credentials, caller project paths
or project headers. 401 ->403 ->404 is the access/resource order; 401 includes
WWW-Authenticate: Bearer. Incompatible explicit X-N4C-Api yields 426 after auth.
Every HTTP/WS response advertises X-N4C-Api=2 and X-N4C-Version. Client role headers
are advisory only. Authenticated valid version drift is bounded per project.

| Operation | Right |
|---|---|
| GET project/jobs/tasks/events/artifacts/workers/stats, WS streams | cluster.read |
| POST jobs, input artifacts, consumers/register | cluster.submit |
| POST jobs/{id}/cancel | cluster.cancel |
| POST workers/register, worker lease/heartbeat, task lifecycle | cluster.execute |

Cluster admin is an owner-defined bundle of all five explicit rights; it never
crosses project boundaries. `/v1/project` returns immutable project_id,
principal_id, services, rights and reserved empty group_ids. Consumer registration
at POST /v1/consumers/register echoes the auth-bound identity. Registered workers
are compute providers in that same project.

Public GET /, /healthz, /version, /ui, /ui/ disclose only service/shell status.
/docs, /redoc and /openapi.json are disabled. No implicit unauthenticated startup.

Jobs: POST /v1/jobs, GET /v1/jobs (limit/status/name/created_before), GET
/v1/jobs/{id}, /tasks, /events (after_id/limit), /artifacts; POST /cancel.
Unknown/foreign job subresources return 404. The same project idempotency_key
replays an equal normalized payload; a different payload returns 409.
Server-attested scheduler/submission fields are excluded from this comparison.

POST /v1/artifacts uploads an input multipart file. All input refs (including
pipeline/dataset lists and entrypoint bundles) must exist in the project at submit;
leased payload.input_artifacts pins each artifact ID to its hash.
GET /v1/artifacts/{id} returns bytes with X-N4C-SHA256 and X-N4C-Size. Paths derive
from the hash in the selected project CAS, never a persisted arbitrary path.

POST /v1/workers/register returns worker_id, **worker_secret** (one-time session
secret), project_id, principal_id, rights, heartbeat_interval_s, lease_ttl_s.
Subsequent worker/task calls require both project Bearer and X-N4C-Worker-Secret.
Session secrets are stored hashed, have no n4p1 grammar and are repr-redacted.
POST /v1/workers/{id}/heartbeat body:
`{"active_attempts":[{"task_id":"task_id","attempt":1}]}`. Only matching,
unexpired active leases renew. Empty pairs leave orphan leases eligible for reaping.
POST /v1/workers/{id}/lease returns task payload or null. Slots derive from tasks.

POST /v1/tasks/{id}/start, /events, /artifacts, /complete, /fail all require query
**worker_id and attempt** in addition to the two auth headers. Events permit only
log/progress types. Upload roles: model/logs/workspace/output; best_model is reserved.
An upload retry with equal bytes replays the role binding; different bytes yield
409. All result.artifacts refs must match uploads by this current worker/task/
attempt/role. Delivery reports explicit exported or not_requested status; exported
model reports carry profile, owner version, format version, SHA256 and size.
Completion validates refs and state in the same project DB critical section.
Duplicate identical terminal reports yield 200; conflicting/illegal/stale reports
409. No terminal report mutates a later attempt. See security-and-scope.md for the
provider-attestation limit: this is transport verification, not scientific certification.

Job aggregate.completeness is pending, complete, incomplete or not_requested.
Partial matrix success retains existing succeeded semantics with incomplete status;
CLI --wait exits nonzero. Best-model selection reads only accepted task results,
including a deterministic task fallback if no rank metric is available.

WS /v1/jobs/{id}/events/stream and /v1/events/stream require Bearer **headers**.
The latter is global only within the credential project. Subscribe precedes paged
history replay; live IDs already replayed are suppressed. Authentication/rights/
unknown-job denials are real HTTP 401/403/404 before accepting the handshake.
Python websockets clients pass additional_headers={"Authorization": "Bearer " + token}.
The dashboard's old query credential feed is intentionally deferred.

Heartbeat responses also include `cancel_attempts` with immutable task_id/attempt
pairs. Agents act on these pairs; `cancel_task_ids` remains diagnostic only.
Cancellation commits the task sweep and job status in one transaction under the
project guard, and leases select only queued/running jobs. WebSocket receive and
publish have one structured stream lifetime; cleanup is shielded until subscriber
removal completes. Query credentials under **any key** recognized by the shared
leaf are rejected, and every denied request's query string is cleared before
transport logging. Legitimate pagination and worker lease queries remain supported.
