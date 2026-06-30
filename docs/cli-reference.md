# CLI reference

The `n4cluster` command wraps the {doc}`python-sdk`. Client subcommands read `--server`
(or `$N4CLUSTER_SERVER`, default `http://127.0.0.1:8765`) and `--token` (or
`$N4CLUSTER_TOKEN`). Precedence is **flag > environment > default**.

## `n4cluster server`

Run the coordinator (and the `/ui` dashboard).

`--host` (`127.0.0.1`)
: bind address.

`--port` (`8765`)
: listen port.

`--state` (`./cluster-state`)
: directory for `store.sqlite` and `objects/`.

`--token`
: legacy single bearer token (or `$N4CLUSTER_TOKEN`), treated as one **all-rights
  admin** principal. When neither `--token` nor any principal is set the server runs
  **open (dev mode)**.

`--principal NAME:TOKEN:ROLES` (repeatable)
: a credential-bound RBAC principal, e.g. `--principal alice:s3cr3t:submitter`. Roles
  are `submitter`, `executor`, `viewer`, `admin` (comma-separate to combine); they
  grant rights from `{submit, read, cancel, execute, admin}`. Any principal switches
  the server into enforced mode.

`--auth-file`
: JSON file of `[{"name","token","roles":[...]}]` principals (alternative to repeating
  `--principal`).

`--allow-python-jobs`
: permit `python_entrypoint` pipelines (arbitrary Python — trusted submitters only).

`--lease-ttl` (`60.0`)
: task lease TTL in seconds (renewed on each heartbeat).

`--cors-origin`
: allow a browser origin to call the API (repeatable / comma-separated; off by default).

`--log-level` (`info`) · `--log-file`
: logging verbosity, and an optional file to also write logs to.

## `n4cluster worker`

Run a polling worker (needs a provisioned `nirs4all` environment).

`--server` · `--token`
: coordinator URL and token.

`--state` (`./worker-state`)
: local task workspaces.

`--labels`
: comma-separated `k=v` capability labels (e.g. `site=lab,cuda=true`).

`--slots` (`1`)
: concurrent task capacity.

`--memory-gb`
: advertise available memory (for the soft memory floor).

`--gpus`
: force the declared GPU count (default: auto-detect via `nvidia-smi`; `0` hides GPUs).

`--allow-python`
: permit `python_entrypoint` pipelines on this worker.

`--name` · `--poll-interval` (`2.0`) · `--log-level` · `--log-file`
: worker name, lease poll interval, and logging.

## Client commands

`n4cluster submit JOB.yaml [--wait] [--timeout S] [--out DIR]`
: submit a YAML/JSON job; optionally block and download artifacts.

`n4cluster status JOB_ID`
: status, task counts, best metric and ranking.

`n4cluster jobs [--status S] [--name N] [--limit L]`
: list jobs, filtered by status and/or name substring.

`n4cluster logs JOB_ID [--limit N]`
: print recorded events for a job.

`n4cluster cancel JOB_ID`
: request cooperative cancellation.

`n4cluster artifacts JOB_ID [--out DIR]`
: list (and optionally download) a job's artifacts.

`n4cluster workers`
: list registered workers, their slots, labels and `nirs4all-cluster` version (with a
  `!version` marker when a worker diverges from the server).
