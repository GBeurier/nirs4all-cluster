# Fine-grained distributed execution of nirs4all — engine mapping & extraction plan

> **Status: design note (grounded), not a construction order.** This document
> mapping, *from the real source code of`nirs4all`*, what the engine already knows how to do
> for **fine-grained distributed execution** (subtrees / sweep points / folds
> distributed over several machines), identifies the **precise extraction points**, and lists
> **what exactly is missing**. It serves as decision input for the product trajectory
> (see`PROTOTYPE_TO_PRODUCTION.md`and`README.md`). >
> The line numbers are indicative (inspection at a given time; the code evolves) — the
> stable reference is *file + symbol*. Inspection:`nirs4all/nirs4all/`(full Python).

## 0. Cadrage

- The **speedup on full sweeps is trivial** (embarrassingly parallel): more hardware ⇒ more
  of variants in parallel. That's not the difficulty. - The value **and** the difficulty are **fine grain**: distribute **subtrees**,
  **sweep points** and **folds** (including refit-with-folds) rather than`pipeline.run()`whole. This is Level 2/3 that`PROTOTYPE_DESIGN.md`had marked “to be postponed”. - The natural coordinator of this correction (OOF / leakage / selection / deterministic refit,
  fingerprints, replay) is **`dag-ml`**: its`COMPILE → PLAN → FIT_CV → SELECT → REFIT →
  PREDICT`runtime ordered on`(variant, fold)`scopes is **almost identical** to the execution model
  of`nirs4all`documented below.`nirs4all`does not integrate`dag-ml`today (full Python),
  but the engine is already structured the same way.

**Verdict**: the engine already has **~80% of the necessary abstractions** (typed step unit,
data view selector,`(variant, fold, phase)`scopes, content-addressed artifacts, key
*calculation* cache, trace + replay, deterministic selection/refit). What is missing is the **plan of
distributed control** (explicit graph, remote task transport, data/artifact provider
shared) — not the engine.

## 1. Current execution model

Hierarchy (from big to thin):

```
Run  (orchestrator.execute : produit cartésien pipelines × datasets)
└─ Dataset
   └─ Variant  (config de pipeline expansée depuis 1 template)        ← parallélisé : loky
      └─ Step  (boucle séquentielle de l'executor ; branches récursives)
         └─ Fold (boucle interne au contrôleur modèle)               ← parallélisé : joblib
            └─ (HPO) trials Optuna                                    ← parallélisé : Optuna interne
```

Three levels of **single-host** parallelism already exist:

| Niveau | Backend | Config | Serialization boundary |
|---|---|---|---|
| Variant | `joblib`/`loky` (process) | `PipelineRunner(n_jobs=N)` | **oui** — `variant_data` dict in / `result` dict out |
| Fold | `joblib` (thread/process) | `model_config.train_params.n_jobs` | **oui** — `fold_args` tuple picklable |
| Trial HPO | Optuna interne | `finetune_params.n_trials` | interne Optuna |

Fichiers : `pipeline/runner.py:137-199` (n_jobs = variants/sweeps),
`pipeline/execution/orchestrator.py`, `pipeline/execution/executor.py`,
`controllers/models/base_model.py`.

## 2. Abstractions already present (reusable)

| Brique | State | Reference (file: symbol) |
|---|---|---|
| **Typed step unit** | `execute(step_info, dataset, context, runtime_context, source, mode, loaded_binaries, prediction_store) -> (context, StepOutput)`; dispatch by`matches()`+ registry sorted by`priority` | `controllers/controller.py` : `OperatorController.execute`, `controllers/registry.py` : `CONTROLLER_REGISTRY` |
| **Materialization request** | `DataSelector(partition, processing[], layout, fold_id, branch_path[], y, include_augmented, tag_filters)`= declares the exact data view | `pipeline/config/context.py` : `DataSelector` |
| **Inter-step state** | `ExecutionContext = DataSelector + PipelineState + StepMetadata` ; `RuntimeContext` = infra (store, artifact_provider/registry, trace_recorder, phase, random_state, step_cache) | `pipeline/config/context.py` : `ExecutionContext`, `RuntimeContext` |
| **Scope `(variant, fold, phase)`** | `ExecutionPhase{CV, REFIT}`+`fold_id`+`branch_path`; persisted:`chains(branch_path, fold_strategy='per_fold', cv_fold_count, fold_artifacts)`,`predictions(fold_id, partition, branch_id, refit_context)` | `context.py` : `ExecutionPhase` ; `pipeline/storage/store_schema.py` (tables `chains`, `predictions`) |
| **Standardized step output** | `StepOutput(artifacts=[(obj,name,fmt)], outputs, metadata)` ; `StepResult(updated_context, artifacts, predictions)` | `pipeline/execution/result.py` |
| **Artefacts content-addressed** | `ArtifactMeta.content_hash`(SHA-256);`artifacts/<hash[:2]>/<hash>.<ext>`sharded storage | `pipeline/execution/result.py` : `ArtifactMeta` ; `pipeline/storage/workspace_store.py` (`content_hash = sha256(...)`) |
| **Cache key of *calculation*** | index `artifacts(chain_path_hash, input_data_hash)` + `save_artifact_with_cache_key(chain_path_hash, input_data_hash, dataset_hash)` + registre `_by_chain_and_data:(chain_path_hash, input_data_hash)->artifact_id` ; chain path = `s1.MinMaxScaler>s3.SNV[br=0]>s4.PLS[br=0]` | `store_schema.py` (`idx_artifacts_cache_key`), `workspace_store.py` : `save_artifact_with_cache_key`, `artifacts/artifact_registry.py` : `_by_chain_and_data` |
| **Cross-variant subtree sharing** | `OperatorController.supports_step_cache()` (« cache this step's output for cross-variant reuse… only preprocessing transforms ») ; snapshots CoW (`SharedBlocks`) | `controllers/controller.py` : `supports_step_cache`, `pipeline/execution/step_cache.py` : `StepCache` |
| **Trace + replay** | `ExecutionTrace`/`ExecutionStep`(chain paths,`fold_artifact_ids`,`by_branch`/`by_chain`/`by_source`) →`trace.json`in the`.n4a`bundle; replay via chain path | `pipeline/trace/execution_trace.py`, `pipeline/bundle/` |
| **Determinism** | seed global`init_global_random_state`(numpy/torch/tf/sklearn); deterministic CV splits; replay without hazard (models already fitted) | (init at the start of the run) |
| **Folds = sample-IDs absolus** | `dataset.set_folds(list[(train_ids, val_ids)])`(absolute IDs, robust to filtering); remap IDs→positions at runtime | `controllers/splitters/split.py` : `set_folds`, `controllers/models/base_model.py` : `_remap_folds_to_positions` |
| **Deterministic selection / refit** | `parse_refit_param`,`extract_winning_config`(rank by`best_val`= RMSECV of fold “avg”), multi-criteria, **per-model**, refit = replace splitter by`_FullTrainFoldSplitter`+ inject`best_params`→`fold_id="final"` | `pipeline/execution/refit/{config_extractor,executor,model_selector}.py` |
| **Sweeps / HPO** | generators`_or_/_range_/_cartesian_/_grid_/_sample_…`→`expand_spec_with_choices(seed)`(replayable via`generator_choices`); Optuna by`(variant, fold)`(`approach=single|individual`) | `pipeline/config/_generator/`, `pipeline/config/pipeline_config.py`, `optimization/optuna.py` |

## 3. Precise extraction points (3 grains)

For each grain: *where* in the code, the **work packet** (inputs), the **result** (outputs),
serializability, and *what to lift*.

### Grain A — Variant (already a process-safe scatter-gather)

- **Where**:`pipeline/execution/orchestrator.py`— selection of parallel mode and preparation of
  packets (≈`:310-399`), execution worker`_execute_single_variant`(≈`:2296-2433`),
  **reconstruction of the blind by the parent** (≈`:401-530`). - **Work packet** (`variant_data`dict):`steps`(expanded config),`config_name`,`gen_choices`,`dataset`(deep-copy),`context`,`runtime_context`**with `store=None, step_cache=None,
  explainer=None`** (rendu picklable), `run_number`.
- **Result** (`result`dict):`predictions`(in memory),`execution_trace`,`artifact_records`(`artifact_id, path, content_hash, operator_class, …`),`chain_data_list`,`dataset`(state
  deep-copied),`duration_ms`,`failed/failure_reason`. - **Serializable?** Yes — this is already the border: loky pickle`variant_data`, the worker does not write
  not the store, the parent **replays** the writes (`begin_pipeline`→`register_existing_artifact`→`save_chain`→`flush_predictions`→`complete_pipeline`). - **To be upgraded for distributed distribution**: replace`loky`with network transport; serve the artifacts
  by`content_hash`from a shared content store (instead of the common local disk). **This is the grain closest to being distributable as is.**

### Grain B —`(variant, fold)`(internal loop to wind up)

- **Where**:`controllers/models/base_model.py`— folds loop (≈`:867-910`):
  ```python
  for fold_idx, (train_indices, val_indices) in enumerate(folds):
      fold_args.append((dataset, model_config, context, runtime_context, prediction_store,
                        X_train[train_indices], y_train[train_indices],
                        X_train[val_indices],  y_train[val_indices], X_test, …,
                        train_indices, val_indices, fold_idx, best_params_fold, …))
  results = (Parallel(n_jobs)(delayed(self.launch_training)(*a) for a in fold_args)
             if n_jobs > 1 else [self.launch_training(*a) for a in fold_args])
  ```
- **Work packet** (`fold_args`tuple): already picklable — arrays X/y of the fold,`model_config`,
  indices,`fold_idx`,`best_params`.`launch_training`(≈`:1077`). - **Result**:`(model, model_id, val_score, model_name, prediction_data)`; OOF predictions`partition="train"`keys`(sample_id, fold_id, model_name, chain)`; overall weight
  deterministic (`EnsembleUtils._scores_to_weights`). - **Serializable?** Yes (“no refactoring of fold logic needed” — inspection report). - **To lift**: the folds loop is **in the controller**; to distribute between machines it
  it must be traced back to an orchestrator who dispatches`NodeTask``(variant, fold)`and collects
  the`prediction_data`+ artifacts. Upstream preprocessing must be available on the worker side (via the
  content store / cache key, cf. §6).

### Grain C — Subtree / step preprocessing (cross-variant sharing)

- **Where**:`controllers/controller.py:execute`+`pipeline/execution/executor.py`cache (lookup`step_cache.get(step_hash, pre_step_data_hash, selector)`before execution;`put`after) +`artifacts/artifact_registry.py`(`(chain_path_hash, input_data_hash)`key). - **Work packet**:`DataSelector`(view) +`step_info`(operator + params) + fitted upstream artifacts
  (`loaded_binaries`in predict mode). - **Result**: content-addressed fitted artifact + transformed dataset (delta of features) + context
  updated (new`processing`chain). - **Serializable?** Partially — the artifact and context are; the`replace_features`mutation
  is in-process (you must return a delta or materialize the remote view). - **To be lifted**: this is the “subtree shared between 50 variants” grain. The **calculation key exists
  already** ; missing a **shared cache store** (serve the artifact by`(chain_path_hash,
  input_data_hash)`to any machine) instead of the local CoW memory cache.

## 4. What exactly is missing (the control plane)

1. **No explicit graph/task.** Steps = sequential imperative loop
   (`executor._execute_steps`); branches = recursive calls`BranchController`. To address/
   distribute subtrees it is necessary **reify the implicit graph** (nodes + dependency edges,`requires_oof`included). 2. **Distributed grain = variant + fold, single-host.** No remote transport; everything is by reference
   memory or pickle loky on **one** machine. The folds loop is internal to the controller. 3. **Distributed data provider missing.** The`SpectroDataset`is alive, mutated in place
   (`replace_features`), passed by reference; CoW snapshots are local. A service is missing
   which **materializes a`DataSelector`(+ upstream artifacts) on a remote machine**. 4. **Shared content store missing.**`content_hash`and`(chain_path_hash, input_data_hash)`key
   are persisted in SQLite, but the cache itself is in-process; no shared artifact store
   between machines. 5. **Single-writer SQLite store.** The current pattern already circumvents this (workers`store=None`+ parent
   rebuilt); a multi-server distributed system would require Postgres/object (cf.`PROTOTYPE_TO_PRODUCTION.md §Fiabilité`).

## 5. Invariants to preserve (do NOT reimplement on the distributor side)

Same boundary rule as the prototype (the cluster never reimplements`nirs4all`):

- **Anti-leakage security**:`controllers/splitters/split.py`—`group_by`, repetition,`include_augmented=False`(increase **after** split),`_check_group_leakage`. It is the heart of
  correction. - **OOF semantics / selection / refit**: OOF aggregation by`sample_id`, order-insensitive selection
  (`mean(val_score)`),`_FullTrainFoldSplitter`+`best_params`injection at refit. Must stay
  authoritarian (role claimed by`dag-ml`). - **Fingerprints & determinism**:`content_hash`,`chain_path_hash`, trace, global seed. Parity
  node-level depends on it; a distributor **propagates** them, it does not recalculate them differently.

## 6. Data & artifacts contract for distribution

The new key part = **serve views and artifacts by identity, without moving the datasets**:

- **Data view**:`DataSelector`is already the materialization request. A *distributed provider*
  resolves`(dataset_ref, DataSelector)`→ local matrix, on the machine where the data lives. Datasets
  do not move; we ship the **spec** (sample indices + representation), not the matrix. - **Upstream artifacts (sub-trees)**: addressed by`content_hash`**and** by calculation key`(chain_path_hash, input_data_hash)`. A *shared content store* serves any machine ⇒
  reuse of shared preprocessing between variants (calculated once) and reproducibility. - **Transfer vs recompute arbitration**: on pre-processed NIR, recompute a subtree can be
  cheaper than transferring the matrix. The calculation key allows you to choose (present in local cache? remote? recompute?). To measure (see spike).

## 7. Deux trajectoires

### (a)`dag-ml`as coordinator (recommended in the long term)`dag-ml`already has the`FIT_CV → SELECT → REFIT`plan on`(variant, fold)`scopes, the OFF by`sample_id`, fingerprints and replay — exactly the missing “brain”. The bridge at
construire : exposer le moteur `nirs4all` comme **host controller** de `dag-ml` (un `NodeTask` →
`OperatorController.execute`; a`NodeResult`←`StepOutput`+ content-addressed artifact). The engine
being “almost the same”, it is a work of **contract adaptation**, not rewriting. Substrate
recommended execution: Ray (distributed object store + GPU actors + locality).

### (b) Thin distributed orchestrator (incremental path)
Reuse existing borders without`dag-ml`: reassemble the folds loop (§3-B) in a
orchestrator who ships`variant_data`/`fold_args`to workers and collects`result`/`prediction_data`. Quicker to prototype, but you have to **reimplement** part of the coordination (selection/refit
are already in`nirs4all`, therefore reusable; the explicit graph + the distributed provider remain
do). Risk: recreate a mini-`dag-ml`.

In both cases, the **prototype cluster of this repository** provides the plumbing: lease/heartbeat/retry,
object-store content-addressed, routing by capabilities (labels/versions/**GPU**). Direct mapping: “ship`variant_data`/`fold_args`” = our`NodeTask`, “serve artifacts by hash/cache-key” = our
object-store, “GPU placement” = our GPU routing.

## 8. Spike proposed (de-risk, measures the true unknown)

But : prouver la **parité node-level** et mesurer le **coût données** sous décomposition fine, sur le
moteur tel quel.

1. Bring up the folds loop (`base_model.py:867-910`) behind an interface`run_fold(variant_cfg, fold_idx, data_view_ref) -> (prediction_data, artifact_ids)`. 2. Run a **`(variant, fold)`campaign** out of process (first 2 local processes, then 2
   machines) with a shared content store serving artifacts via`content_hash`/cache-key. 3. Measure: (a) **parity** — predictions/scores **fingerprint-identical** vs single-machine; (b)
   **materialization/transfer cost** vs recompute; (c) real gain from **subtree sharing**
   preprocessing between variants.

Relevant go/no-go criterion = **parity (≤ 1e-10 / fingerprint-identical) + acceptable transfer cost**,
not the speedup (trivial).

## 9. Appendix — index of code references

| Sujet | Fichier | Symbole / zone |
|---|---|---|
| Step contract | `controllers/controller.py` | `OperatorController.execute`, `supports_step_cache`, `matches` |
| Registre | `controllers/registry.py` | `CONTROLLER_REGISTRY`, `register_controller` |
| Contexte / vue | `pipeline/config/context.py` | `DataSelector`, `ExecutionContext`, `RuntimeContext`, `ExecutionPhase` |
| Sorties | `pipeline/execution/result.py` | `StepOutput`, `StepResult`, `ArtifactMeta.content_hash` |
| Execution loop + step cache | `pipeline/execution/executor.py` | `_execute_steps`, `_execute_single_step`, lookup/put `step_cache` |
| Parallel variants + blind reconstruction | `pipeline/execution/orchestrator.py` | `_execute_single_variant`, reconstruction (`begin_pipeline`→`save_chain`→`flush_predictions`) |
| Folds | `controllers/models/base_model.py` | boucle `for fold_idx …`, `launch_training`, `_remap_folds_to_positions` |
| Splitters / anti-leakage | `controllers/splitters/split.py` | `CrossValidatorController`, `set_folds`, `_check_group_leakage` |
| Selection / refit | `pipeline/execution/refit/` | `config_extractor.py`, `executor.py` (`_FullTrainFoldSplitter`), `model_selector.py` |
| Sweeps/generators | `pipeline/config/` | `pipeline_config.py`, `_generator/`, `optimization/optuna.py` |
| Store (SQLite + Parquet) | `pipeline/storage/` | `store_schema.py` (tables + `idx_artifacts_cache_key`), `workspace_store.py`, `artifacts/artifact_registry.py` |
| Trace / replay / bundle | `pipeline/trace/`, `pipeline/bundle/` | `execution_trace.py`, `.n4a` (`manifest`, `trace.json`, `artifacts/`) |
