# Work log — nirs4all-cluster prototype

Living log of the implementation of `PROTOTYPE_DESIGN.md`. Newest entries at the bottom of each section.

## Goal

Implement the `nirs4all-cluster` prototype described in `PROTOTYPE_DESIGN.md`: a distributed job queue (FastAPI server + SQLite + local object store + polling workers) that runs `nirs4all.run()` jobs across workers, without modifying any other ecosystem library. Review with `codex`, test on `nirs4all-data`, then document the prototype→production path.

## Environment facts (verified 2026-06-04)

- `nirs4all` 0.9.1 installed editable in `/home/delete/nirs4all/nirs4all/.venv` (python 3.11). Import only resolves correctly from a cwd outside `/home/delete/nirs4all` (the ecosystem root shadows the package as a namespace dir).
- Public API: `nirs4all.run(pipeline, dataset, ...) -> RunResult`; `RunResult.export("x.n4a")`, `.best_rmse`, etc. Also `generate()` for synthetic data, `predict/explain/retrain/session/load_session`.
- `codex` CLI available: `codex-cli 0.136.0` at `/home/delete/.local/bin/codex`.
- Test data: `/home/delete/nirs4all/nirs4all-data/` with `regression/` (CORN, …) and `classification/` (FruitPuree, …) dataset folders.

## Timeline

### 2026-06-04 — Phase 0: recon
- Mapped ecosystem, located nirs4all venv + real public API, confirmed codex availability and test data.
- Launched parallel understanding sweep of: `run()` signature + `RunResult`, `DatasetConfigs`/dataset-folder format, `generate()`, `.n4a` export/import, Studio `JobManager`/FastAPI/WS, `nirs4all-datasets` catalog API.

### 2026-06-04 — Phase 0 findings (5-agent sweep, verified by running code)
- **`nirs4all.run(pipeline, dataset, *, name, session, verbose=1, save_artifacts=True, save_charts=True, plots_visible=False, random_state, refit=True, cache, project, report_naming, **runner_kwargs)`**. `workspace_path` is passed through `**runner_kwargs`. Pipeline accepts: list of sklearn steps, dict, **YAML/JSON path (str)**, or list (batch). Dataset accepts: **folder path (str)**, `(X, y)` tuple, dict, `SpectroDataset`, `DatasetConfigs`, or list.
- **`RunResult`**: `.best_rmse/.best_r2/.best_score/.best_accuracy` return **`float('nan')` (NOT None)** when unavailable → must sanitize NaN→None before JSON. Also `.num_predictions`, `.best` (dict with `model_name`/`metric`/`task_type`/`test_score`), `.export(path)->Path` (writes `.n4a` zip ~140-210 KB), `.summary()->str`, `.validate(raise_on_failure=False)->dict`.
- **`nirs4all.predict(model="x.n4a", data=X)` -> PredictResult** (`.y_pred`) — used for the local-vs-cluster parity check.
- **Dataset folders** under `nirs4all-data/`: `Xtrain/Xcal.csv`, `Ytrain/Ycal.csv`, `Xtest/Xval.csv`, `Ytest/Yval.csv`, `;`-delimited; pass the folder path directly. Verified fast smoke sets: `regression/GRAPEVINE_LeafTraits/PSI_spxyG70_30_byCultivar_MicroNIR_NeoSpectra` (~0.6s), `regression/AMYLOSE/Rice_Amylose_313_YbasedSplit` (~1.1s); `generate(n_samples=100, random_state=42)` (~0.4s).
- **Studio** (`nirs4all-studio/api/jobs/manager.py`): JobStatus pending/running/completed/failed/cancelled; WS events `job_started/job_progress/job_completed/job_failed/job_log` on channel `job:{job_id}`, progress 0-100. → informs the Phase-4 Studio-compat adapter and our event naming.
- **Catalog** (`nirs4all_datasets.load(name)->DatasetConfigs`, `resolve_config`, pooch cache `~/.cache/nirs4all-datasets`): basis for a future `catalog` dataset kind.

### Decisions taken
- Two venvs: `nirs4all/.venv` (+web stack) = worker/test env; `nirs4all-cluster/.venv` (web stack, no nirs4all) = proves server/client run without the library (validates decoupling). nirs4all NOT a hard dependency (lazy guarded import, like Studio).
- Runner sanitizes NaN metrics → null. Worker passes `workspace_path` (isolated per-task), `inner_n_jobs` popped from params (default 1), `save_charts=False` on workers.
- Pipeline `path`/`artifact`/`inline_json` pass straight to `run()`; `python_entrypoint` gated behind `--allow-python-jobs`.

### 2026-06-04 — Phases 0–3 implemented
Built the full package per the design layout:
- `schemas.py` (Pydantic contract: refs, JobRequest, TaskPayload, results, views, state enums).
- `server/`: `db.py` (SQLite WAL, single guarded connection, atomic leasing, lease reaping), `scheduler.py` (job/task state machines + `requirements_match`), `artifacts.py` (SHA-256 content-addressed store), `events.py` (persist + async fan-out broker), `app.py` (FastAPI client API + worker API + WS stream + reaper + finalize/aggregate).
- `worker/`: `materialize.py` (resolve refs → local paths, zip-slip-safe), `executor.py` (subprocess runner with capture + cancel), `agent.py` (register/heartbeat/lease/run/upload loop, thread-per-slot).
- `runners/nirs4all_run.py` (the ONLY nirs4all import; subprocess entrypoint; NaN→null sanitize; `.n4a` export).
- `client.py` (`ClusterClient` SDK), `cli.py` (`n4cluster server|worker|submit|status|logs|cancel|artifacts|workers`).
- `examples/` (pls.yaml + job.shared-path/matrix/uploaded-bundle), `tests/` (state machine, scheduler, artifacts, server API, integration).

Fixes found while building:
- Live WS broadcast was dropped from sync route handlers (no running loop in threadpool) → broker now records the server loop and uses `call_soon_threadsafe`.
- Leases were never renewed → a task longer than `lease_ttl_s` was wrongly reaped on a healthy worker. Now renewed on every heartbeat (lease lapses only when the worker stops heartbeating, per design).
- Reaper now moves cancelling-job tasks to `cancelled` instead of requeuing (a cancelled job can never relaunch).

Green gate: ruff clean; **48 tests pass** in the nirs4all venv (45 unit/API + 3 integration); **45 pass + 1 skip** in the no-nirs4all venv (integration auto-skips, proving server/client/worker import without nirs4all).

### 2026-06-04 — Validation harness on real nirs4all-data (8/8 PASS)
`scripts/validation.py` ran a real server + worker **subprocesses** on `nirs4all-data` and passed all design "Tests de validation":

| # | Test | Result |
|---|---|---|
| 1 | atomic job → `.n4a` (valid zip) | PASS |
| 2 | two jobs on two workers in parallel | PASS (2 distinct workers) |
| 3 | worker SIGKILL mid-task → retry | PASS (caught running, attempt=2, succeeded) |
| 4 | cancelled job not relaunched | PASS |
| 5 | `pipeline × dataset` matrix aggregation | PASS (3 CORN targets ranked) |
| 6 | metric parity vs local `nirs4all.run()` | PASS — **diff = 0.0** |
| 7 | no files outside state dirs | PASS |

Measurements (GRAPEVINE atomic job, PLS-5): cluster `best_rmse=0.37475072412543703`, local identical → **metric_diff = 0.0** (beats README go/no-go #3 of ≤1e-10). exec 0.38 s, total wall 3.5 s, submit latency 0.011 s, `.n4a` ≈ 49 KB. Matrix ranking moisture(0.055) < oil(0.112) < protein(0.236).

### 2026-06-04 — Review phase
- Launched `codex exec` (read-only) for a critical code+design review (the user asked for codex).
- Launched an independent multi-agent adversarial review workflow in parallel.

**Codex review** flagged 2 BLOCKER, 6 HIGH, 7 MEDIUM, 3 LOW, 1 NIT. **Multi-agent review** (4 dimensions, each finding verified by an independent skeptic) confirmed 12/19 findings. The two agreed strongly. Fixes applied (verified by new regression tests):

| Severity | Finding | Fix |
|---|---|---|
| BLOCKER | zip-slip bypass (`startswith` matched sibling dirs) | `is_relative_to` + reject absolute members & symlinks (`materialize._safe_extract`) |
| BLOCKER | `fail_task` didn't verify worker ownership → slot corruption | ownership check → stale reports rejected (409/ignored) |
| HIGH | dead-worker `slots_used=0` + heartbeat revival → oversubscription | **dropped the mutable counter**: in-flight derived live from the task table (`_in_flight_count`); `slots_used` is now a synced display cache; no zeroing on death. Regression test added. |
| HIGH | matrix `best_model` link went stale (early winner kept) | clear prior `best_model` links on finalize; client uses `aggregate.best_model_artifact_id`. Regression test added. |
| HIGH | worker ignored `/start` status (ran cancelled tasks) | agent aborts on non-200; `start_task` returns 409 (not 500) on illegal transition |
| HIGH/MED | idempotency TOCTOU + job/tasks in 2 commits | single-transaction `create_job_with_tasks`; submit catches `IntegrityError` and returns the existing job |
| HIGH/MED | artifact size enforced after write → leaked blob | streaming `max_bytes` in `put_stream` (aborts + unlinks); routes return 413. Regression test added. |
| MED | `rank_mode="max"` sorted `None` first | None-last sort key independent of direction |
| MED | empty `pipelines:[]`/`datasets:[]` accepted (zero-task job) | rejected at the schema boundary (422). Regression test added. |
| MED/LOW | `task_event` hand-parsed body → bad level 500s on read | route now validates `TaskEvent` (422 at boundary). Regression test added. |
| MED | client download filenames server-controlled (traversal) | `Path(name).name` sanitization + dedup downloads by artifact id |
| LOW | finalize race could 500 the loser | atomic `try_set_job_status` (idempotent) |
| LOW | `cancelling -> succeeded` not in design | removed from the transition table |
| LOW | live WS payload missing `ts` | added |
| LOW | dead code (`put_file`, `copy_to`) | removed |
| LOW | deterministic materialize failure burned retries | agent reports `retriable=False` for `NotImplementedError`/`FileNotFoundError`/`ValueError` |
| LOW | unused `HeartbeatAck`/`TaskEvent` schemas | now wired into the heartbeat/events routes |
| NIT | plain `==` token compare | `hmac.compare_digest` (header + WS) |

**Deliberately deferred** (consistent with the prototype's non-goals; documented in `PROTOTYPE_TO_PRODUCTION.md`): per-identity worker/client API scopes (multi-tenant non-goal), mTLS/TLS & WS-token-in-query, sandbox/quota enforcement, the `catalog` dataset kind (honest `NotImplementedError`), and `min_memory_gb` permissive-on-undeclared (deliberate).

### 2026-06-04 — Final green gate
- ruff + mypy clean. **54 tests pass** (nirs4all venv: 48 unit/API + 6 incl. integration); **51 pass + 1 skip** (no-nirs4all venv).
- Re-ran `scripts/validation.py` after the fixes: **8/8 PASS**, parity diff still **0.0**.
- Docs written: `README.md` (rewritten, keeps go/no-go framing), `PROTOTYPE_TO_PRODUCTION.md`, this `WORKLOG.md`. `.gitignore` added.

### 2026-06-04 — Audit: library version & availability handling
User audit question: *does the prototype take library versions/availability into account, and does it return results?*

Findings (grounded in code):
- **Results round-trip: yes.** Worker summarizes `RunResult` → `TaskResult` (regression `best_rmse/r2/mae/score`, classification `best_accuracy`, `num_predictions`), uploads best `.n4a` + logs, server aggregates → ranking + best metric + best-model artifact; each task records the producing `nirs4all_version`. Validated (parity diff 0.0). Full-workspace aggregation remains intentionally out of MVP scope.
- **Versions/availability: were *collected* but *not enforced*** — a real gap. The worker declared python/platform/nirs4all version, but `requirements.packages` (e.g. `nirs4all: ">=0.9,<0.10"`) was accepted and ignored; matching used only labels + memory. A job could be routed to a worker with the wrong/absent nirs4all (failing only at run time).

Fix applied (now implemented, not advisory):
- Worker advertises installed versions of a relevant package set (nirs4all, numpy, scipy, scikit-learn, pandas, polars, torch, tensorflow, jax) + python.
- `requirements_match` enforces `requirements.packages` via PEP 440 (`packaging.SpecifierSet`); `""` = presence-only; an undeclared package never satisfies an explicit requirement (unknown availability ⇒ unavailable).
- Specifiers validated at the schema boundary (422 on malformed).
- **Default availability**: the server auto-adds a presence requirement for `nirs4all` to every `nirs4all.run` job, so it only routes to workers that have the library (overridable with an explicit range).
- Added `packaging` to deps; new tests (`version_satisfies`, package/availability/python routing, malformed-spec 422, worker-without-nirs4all-doesn't-lease). Green gate: ruff+mypy clean, **59 tests** pass; validation **8/8**, parity diff still **0.0**.

### 2026-06-04 — GPU/CUDA routing (requested addition)
- Worker auto-detects GPUs via `nvidia-smi` (no torch/tf import — agent stays light & nirs4all-free): declares `capabilities.gpu_count`/`gpu_names`/`cuda`/`cuda_version` + an auto `cuda=true|false` label (user `--labels`/capabilities not overwritten). CLI `--gpus N` forces the count (0 hides GPUs).
- Scheduler routes on the `cuda` label **and** on a new `requirements.min_gpu_count` (fail-closed: undeclared GPU = 0, so a GPU job never lands on a CPU worker).
- **Verified on real hardware**: auto-detect found 2 GPUs (RTX 4090 + RTX 5090, CUDA 13.1, driver 591.86) via WSL `nvidia-smi`; live `n4cluster worker` registered with `cuda=true`, `gpu_count=2`.
- Tests added: `requirements_match` GPU count, API GPU routing (CPU worker doesn't lease a `min_gpu_count:1` job, 2-GPU worker does), `_detect_gpu` shape, `_declare_gpu` override, plus zip-slip regression tests. Green gate: ruff+mypy clean, **67 tests** pass; validation **8/8**, parity diff **0.0**.

### 2026-06-04 — Engine inspection & fine-grained distribution design
- Deep, grounded inspection of the `nirs4all` engine (6-agent read + own reads of the contract files), through the lens of fine-grained distributed execution (sub-trees / sweep points / folds).
- Finding: the engine already has ~80% of the abstractions a distributed graph executor needs — typed step unit (`OperatorController.execute`), `DataSelector` (materialization request), `(variant, fold, phase)` scopes, content-addressed artifacts + a **computation cache key** `(chain_path_hash, input_data_hash)`, `supports_step_cache` (cross-variant sub-tree reuse), trace/replay, deterministic select/refit — but wired in-process / single-host. Per-fold training is already a picklable `delayed(launch_training)` task; variant-level is already a `store=None` scatter-gather with parent store reconstruction.
- Missing = the **control plane**: explicit graph/task model, remote NodeTask transport, distributed data-view + shared content-addressed artifact provider. This is precisely `dag-ml`'s domain (the engine is "quasi pareil" to its `FIT_CV→SELECT→REFIT` over `(variant, fold)`).
- Wrote **`docs/DISTRIBUTED_EXECUTION_DESIGN.md`**: grounded capability inventory (file:line), the 3 precise extraction points (variant / `(variant,fold)` / sub-tree), the exact gaps, invariants not to reimplement (leakage/OOF/fingerprints), the data+artifact contract, two trajectories (dag-ml coordinator + Ray vs thin distributed orchestrator), and a de-risking spike.

### Status: prototype complete
All design Phase 0–3 deliverables implemented, reviewed (codex + multi-agent), fixed, and validated on real `nirs4all-data`. Phase 4 (Studio adapter) is scoped in `PROTOTYPE_TO_PRODUCTION.md` but not built (it depends on the go decision). The prototype meets go/no-go criterion #3 (metric parity) and #4 (data/security/recovery model written first); criteria #1 (≥2 demanders) and #2 (≥3× speedup on a real sweep) remain to be gathered — the harness to measure #2 exists (`job.matrix.yaml`).
