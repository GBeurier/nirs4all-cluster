# Exécution distribuée fine de nirs4all — cartographie du moteur & plan d'extraction

> **Statut : note de conception (grounded), pas un ordre de construction.** Ce document
> cartographie, *à partir du code source réel de `nirs4all`*, ce que le moteur sait déjà faire
> en vue d'une **exécution distribuée à grain fin** (sous-arbres / points de sweep / folds
> répartis sur plusieurs machines), identifie les **points d'extraction précis**, et liste
> **ce qui manque exactement**. Il sert d'entrée de décision pour la trajectoire produit
> (cf. `PROTOTYPE_TO_PRODUCTION.md` et `README.md`).
>
> Les numéros de ligne sont indicatifs (inspection à un instant T ; le code évolue) — la
> référence stable est *fichier + symbole*. Inspection : `nirs4all/nirs4all/` (full Python).

## 0. Cadrage

- Le **speedup sur sweeps pleins est trivial** (embarrassingly parallel) : plus de hardware ⇒ plus
  de variants en parallèle. Ce n'est pas la difficulté.
- La valeur **et** la difficulté sont au **grain fin** : distribuer des **sous-arbres**, des
  **points de sweep** et des **folds** (refit-with-folds compris) plutôt que des `pipeline.run()`
  entiers. C'est le Level 2/3 que `PROTOTYPE_DESIGN.md` avait marqué « à différer ».
- Le coordinateur naturel de cette correction (OOF / leakage / sélection / refit déterministes,
  fingerprints, replay) est **`dag-ml`** : son runtime `COMPILE → PLAN → FIT_CV → SELECT → REFIT →
  PREDICT` ordonné sur des scopes `(variant, fold)` est **quasi identique** au modèle d'exécution
  de `nirs4all` documenté ci-dessous. `nirs4all` n'intègre pas `dag-ml` aujourd'hui (full Python),
  mais le moteur est déjà structuré de la même façon.

**Verdict** : le moteur possède déjà **~80 % des abstractions** nécessaires (unité de step typée,
sélecteur de vue de données, scopes `(variant, fold, phase)`, artefacts content-addressed, clé de
cache de *calcul*, trace + replay, sélection/refit déterministes). Ce qui manque est le **plan de
contrôle distribué** (graphe explicite, transport de tâches distant, provider de données/artefacts
partagé) — pas le moteur.

## 1. Modèle d'exécution actuel

Hiérarchie (du gros au fin) :

```
Run  (orchestrator.execute : produit cartésien pipelines × datasets)
└─ Dataset
   └─ Variant  (config de pipeline expansée depuis 1 template)        ← parallélisé : loky
      └─ Step  (boucle séquentielle de l'executor ; branches récursives)
         └─ Fold (boucle interne au contrôleur modèle)               ← parallélisé : joblib
            └─ (HPO) trials Optuna                                    ← parallélisé : Optuna interne
```

Trois niveaux de parallélisme **mono-hôte** existent déjà :

| Niveau | Backend | Config | Frontière de sérialisation |
|---|---|---|---|
| Variant | `joblib`/`loky` (process) | `PipelineRunner(n_jobs=N)` | **oui** — `variant_data` dict in / `result` dict out |
| Fold | `joblib` (thread/process) | `model_config.train_params.n_jobs` | **oui** — `fold_args` tuple picklable |
| Trial HPO | Optuna interne | `finetune_params.n_trials` | interne Optuna |

Fichiers : `pipeline/runner.py:137-199` (n_jobs = variants/sweeps),
`pipeline/execution/orchestrator.py`, `pipeline/execution/executor.py`,
`controllers/models/base_model.py`.

## 2. Abstractions déjà présentes (réutilisables)

| Brique | État | Référence (fichier : symbole) |
|---|---|---|
| **Unité de step typée** | `execute(step_info, dataset, context, runtime_context, source, mode, loaded_binaries, prediction_store) -> (context, StepOutput)` ; dispatch par `matches()` + registry trié par `priority` | `controllers/controller.py` : `OperatorController.execute`, `controllers/registry.py` : `CONTROLLER_REGISTRY` |
| **Materialization request** | `DataSelector(partition, processing[], layout, fold_id, branch_path[], y, include_augmented, tag_filters)` = déclare la vue exacte de données | `pipeline/config/context.py` : `DataSelector` |
| **État inter-step** | `ExecutionContext = DataSelector + PipelineState + StepMetadata` ; `RuntimeContext` = infra (store, artifact_provider/registry, trace_recorder, phase, random_state, step_cache) | `pipeline/config/context.py` : `ExecutionContext`, `RuntimeContext` |
| **Scope `(variant, fold, phase)`** | `ExecutionPhase{CV, REFIT}` + `fold_id` + `branch_path` ; persistés : `chains(branch_path, fold_strategy='per_fold', cv_fold_count, fold_artifacts)`, `predictions(fold_id, partition, branch_id, refit_context)` | `context.py` : `ExecutionPhase` ; `pipeline/storage/store_schema.py` (tables `chains`, `predictions`) |
| **Sortie de step standardisée** | `StepOutput(artifacts=[(obj,name,fmt)], outputs, metadata)` ; `StepResult(updated_context, artifacts, predictions)` | `pipeline/execution/result.py` |
| **Artefacts content-addressed** | `ArtifactMeta.content_hash` (SHA-256) ; stockage shardé `artifacts/<hash[:2]>/<hash>.<ext>` | `pipeline/execution/result.py` : `ArtifactMeta` ; `pipeline/storage/workspace_store.py` (`content_hash = sha256(...)`) |
| **Clé de cache de *calcul*** | index `artifacts(chain_path_hash, input_data_hash)` + `save_artifact_with_cache_key(chain_path_hash, input_data_hash, dataset_hash)` + registre `_by_chain_and_data:(chain_path_hash, input_data_hash)->artifact_id` ; chain path = `s1.MinMaxScaler>s3.SNV[br=0]>s4.PLS[br=0]` | `store_schema.py` (`idx_artifacts_cache_key`), `workspace_store.py` : `save_artifact_with_cache_key`, `artifacts/artifact_registry.py` : `_by_chain_and_data` |
| **Partage de sous-arbre cross-variant** | `OperatorController.supports_step_cache()` (« cache this step's output for cross-variant reuse… only preprocessing transforms ») ; snapshots CoW (`SharedBlocks`) | `controllers/controller.py` : `supports_step_cache`, `pipeline/execution/step_cache.py` : `StepCache` |
| **Trace + replay** | `ExecutionTrace`/`ExecutionStep` (chain paths, `fold_artifact_ids`, `by_branch`/`by_chain`/`by_source`) → `trace.json` dans le bundle `.n4a` ; replay par chain path | `pipeline/trace/execution_trace.py`, `pipeline/bundle/` |
| **Déterminisme** | seed global `init_global_random_state` (numpy/torch/tf/sklearn) ; splits CV déterministes ; replay sans aléa (modèles déjà fittés) | (init au début du run) |
| **Folds = sample-IDs absolus** | `dataset.set_folds(list[(train_ids, val_ids)])` (IDs absolus, robustes au filtrage) ; remap IDs→positions à l'exécution | `controllers/splitters/split.py` : `set_folds`, `controllers/models/base_model.py` : `_remap_folds_to_positions` |
| **Sélection / refit déterministes** | `parse_refit_param`, `extract_winning_config` (rank par `best_val` = RMSECV du fold « avg »), multi-critère, **per-model**, refit = remplace splitter par `_FullTrainFoldSplitter` + injecte `best_params` → `fold_id="final"` | `pipeline/execution/refit/{config_extractor,executor,model_selector}.py` |
| **Sweeps / HPO** | générateurs `_or_/_range_/_cartesian_/_grid_/_sample_…` → `expand_spec_with_choices(seed)` (rejouable via `generator_choices`) ; Optuna par `(variant, fold)` (`approach=single|individual`) | `pipeline/config/_generator/`, `pipeline/config/pipeline_config.py`, `optimization/optuna.py` |

## 3. Points d'extraction précis (3 grains)

Pour chaque grain : *où* dans le code, le **work packet** (entrées), le **résultat** (sorties),
la sérialisabilité, et *ce qu'il faut lifter*.

### Grain A — Variant (déjà un scatter-gather process-safe)

- **Où** : `pipeline/execution/orchestrator.py` — sélection du mode parallèle et préparation des
  paquets (≈`:310-399`), exécution worker `_execute_single_variant` (≈`:2296-2433`),
  **reconstruction du store par le parent** (≈`:401-530`).
- **Work packet** (`variant_data` dict) : `steps` (config expansée), `config_name`, `gen_choices`,
  `dataset` (deep-copy), `context`, `runtime_context` **avec `store=None, step_cache=None,
  explainer=None`** (rendu picklable), `run_number`.
- **Résultat** (`result` dict) : `predictions` (en mémoire), `execution_trace`, `artifact_records`
  (`artifact_id, path, content_hash, operator_class, …`), `chain_data_list`, `dataset` (état
  deep-copié), `duration_ms`, `failed/failure_reason`.
- **Sérialisable ?** Oui — c'est déjà la frontière : loky pickle `variant_data`, le worker n'écrit
  pas le store, le parent **rejoue** les écritures (`begin_pipeline` → `register_existing_artifact`
  → `save_chain` → `flush_predictions` → `complete_pipeline`).
- **À lifter pour le distribué** : remplacer `loky` par un transport réseau ; servir les artefacts
  par `content_hash` depuis un store de contenu partagé (au lieu du disque local commun).
  **C'est le grain le plus proche d'être distribuable tel quel.**

### Grain B — `(variant, fold)` (boucle interne à remonter)

- **Où** : `controllers/models/base_model.py` — boucle de folds (≈`:867-910`) :
  ```python
  for fold_idx, (train_indices, val_indices) in enumerate(folds):
      fold_args.append((dataset, model_config, context, runtime_context, prediction_store,
                        X_train[train_indices], y_train[train_indices],
                        X_train[val_indices],  y_train[val_indices], X_test, …,
                        train_indices, val_indices, fold_idx, best_params_fold, …))
  results = (Parallel(n_jobs)(delayed(self.launch_training)(*a) for a in fold_args)
             if n_jobs > 1 else [self.launch_training(*a) for a in fold_args])
  ```
- **Work packet** (`fold_args` tuple) : déjà picklable — arrays X/y du fold, `model_config`,
  indices, `fold_idx`, `best_params`. `launch_training` (≈`:1077`).
- **Résultat** : `(model, model_id, val_score, model_name, prediction_data)` ; prédictions OOF
  `partition="train"` clés `(sample_id, fold_id, model_name, chain)` ; poids d'ensemble
  déterministes (`EnsembleUtils._scores_to_weights`).
- **Sérialisable ?** Oui (« no refactoring of fold logic needed » — rapport d'inspection).
- **À lifter** : la boucle de folds est **dans le contrôleur** ; pour répartir entre machines il
  faut la **remonter à un orchestrateur** qui dispatche des `NodeTask` `(variant, fold)` et collecte
  les `prediction_data` + artefacts. Le préprocessing amont doit être disponible côté worker (via le
  store de contenu / la clé de cache, cf. §6).

### Grain C — Sous-arbre / step préprocessing (partage cross-variant)

- **Où** : `controllers/controller.py:execute` + cache `pipeline/execution/executor.py` (lookup
  `step_cache.get(step_hash, pre_step_data_hash, selector)` avant exécution ; `put` après) +
  `artifacts/artifact_registry.py` (clé `(chain_path_hash, input_data_hash)`).
- **Work packet** : `DataSelector` (vue) + `step_info` (operator + params) + artefacts amont fittés
  (`loaded_binaries` en mode predict).
- **Résultat** : artefact fitté content-addressed + dataset transformé (delta de features) + contexte
  mis à jour (nouvelle `processing` chain).
- **Sérialisable ?** Partiellement — l'artefact et le contexte le sont ; la mutation `replace_features`
  est in-process (il faut renvoyer un delta ou matérialiser la vue à distance).
- **À lifter** : c'est le grain « sous-arbre partagé entre 50 variants ». La **clé de calcul existe
  déjà** ; il manque un **store de cache partagé** (servir l'artefact par `(chain_path_hash,
  input_data_hash)` à n'importe quelle machine) au lieu du cache mémoire CoW local.

## 4. Ce qui manque exactement (le plan de contrôle)

1. **Pas de graphe/task explicite.** Steps = boucle impérative séquentielle
   (`executor._execute_steps`) ; branches = appels récursifs `BranchController`. Pour adresser/
   distribuer des sous-arbres il faut **réifier le graphe implicite** (nœuds + arêtes de dépendance,
   `requires_oof` inclus).
2. **Grain distribué = variant + fold, mono-hôte.** Aucun transport distant ; tout est par référence
   mémoire ou pickle loky sur **une** machine. La boucle de folds est interne au contrôleur.
3. **Provider de données distribué absent.** Le `SpectroDataset` est vivant, muté en place
   (`replace_features`), passé par référence ; les snapshots CoW sont locaux. Il manque un service
   qui **matérialise une `DataSelector` (+ artefacts amont) sur une machine distante**.
4. **Store de contenu partagé absent.** `content_hash` et la clé `(chain_path_hash, input_data_hash)`
   sont persistés en SQLite, mais le cache lui-même est in-process ; pas de store d'artefacts partagé
   entre machines.
5. **Store SQLite mono-écrivain.** Le pattern actuel contourne déjà ça (workers `store=None` + parent
   reconstruit) ; un système distribué multi-serveurs exigerait Postgres/objet (cf.
   `PROTOTYPE_TO_PRODUCTION.md §Fiabilité`).

## 5. Invariants à préserver (ne PAS réimplémenter côté distributeur)

Même règle de frontière que le prototype (le cluster ne réimplémente jamais `nirs4all`) :

- **Sécurité anti-leakage** : `controllers/splitters/split.py` — `group_by`, répétition,
  `include_augmented=False` (augmentation **après** split), `_check_group_leakage`. C'est le cœur de
  correction.
- **Sémantique OOF / sélection / refit** : agrégation OOF par `sample_id`, sélection order-insensitive
  (`mean(val_score)`), `_FullTrainFoldSplitter` + injection `best_params` au refit. Doit rester
  autoritaire (rôle revendiqué par `dag-ml`).
- **Fingerprints & déterminisme** : `content_hash`, `chain_path_hash`, trace, seed global. La parité
  node-level en dépend ; un distributeur les **propage**, il ne les recalcule pas différemment.

## 6. Contrat données & artefacts pour la distribution

La pièce neuve clé = **servir vues et artefacts par identité, sans bouger les datasets** :

- **Vue de données** : `DataSelector` est déjà la requête de matérialisation. Un *provider distribué*
  résout `(dataset_ref, DataSelector)` → matrice locale, sur la machine où vit la donnée. Les datasets
  ne bougent pas ; on ship la **spec** (indices de samples + représentation), pas la matrice.
- **Artefacts amont (sous-arbres)** : adressés par `content_hash` **et** par clé de calcul
  `(chain_path_hash, input_data_hash)`. Un *store de contenu partagé* les sert à toute machine ⇒
  réutilisation du préprocessing partagé entre variants (calculé une fois) et reproductibilité.
- **Arbitrage transfert vs recompute** : sur du NIR pré-traité, recomputer un sous-arbre peut être
  moins cher que transférer la matrice. La clé de calcul permet de choisir (présent en cache local ?
  distant ? recompute ?). À mesurer (cf. spike).

## 7. Deux trajectoires

### (a) `dag-ml` comme coordinateur (recommandé à terme)
`dag-ml` possède déjà le plan `FIT_CV → SELECT → REFIT` sur scopes `(variant, fold)`, l'OOF par
`sample_id`, les fingerprints et le replay — exactement le « cerveau » manquant. Le pont à
construire : exposer le moteur `nirs4all` comme **host controller** de `dag-ml` (un `NodeTask` →
`OperatorController.execute` ; un `NodeResult` ← `StepOutput` + artefact content-addressed). Le moteur
étant « quasi pareil », c'est un travail d'**adaptation de contrats**, pas de réécriture. Substrat
d'exécution conseillé : Ray (object store distribué + acteurs GPU + localité).

### (b) Orchestrateur distribué mince (chemin incrémental)
Réutiliser les frontières existantes sans `dag-ml` : remonter la boucle de folds (§3-B) dans un
orchestrateur qui ship `variant_data`/`fold_args` vers des workers et collecte `result`/`prediction_data`.
Plus rapide à prototyper, mais il faut **réimplémenter** une partie de la coordination (sélection/refit
sont déjà dans `nirs4all`, donc réutilisables ; le graphe explicite + le provider distribué restent à
faire). Risque : recréer un mini-`dag-ml`.

Dans les deux cas, le **prototype cluster de ce dépôt** fournit la plomberie : lease/heartbeat/retry,
object-store content-addressed, routage par capacités (labels/versions/**GPU**). Mapping direct :
« ship `variant_data`/`fold_args` » = nos `NodeTask`, « servir artefacts par hash/cache-key » = notre
object-store, « placement GPU » = notre routage GPU.

## 8. Spike proposé (de-risk, mesure la vraie inconnue)

But : prouver la **parité node-level** et mesurer le **coût données** sous décomposition fine, sur le
moteur tel quel.

1. Remonter la boucle de folds (`base_model.py:867-910`) derrière une interface
   `run_fold(variant_cfg, fold_idx, data_view_ref) -> (prediction_data, artifact_ids)`.
2. Exécuter une **campagne `(variant, fold)`** hors-process (d'abord 2 process locaux, puis 2
   machines) avec un store de contenu partagé servant les artefacts par `content_hash`/cache-key.
3. Mesurer : (a) **parité** — prédictions/scores **fingerprint-identiques** vs mono-machine ; (b)
   **coût matérialisation/transfert** vs recompute ; (c) gain réel du **partage de sous-arbre**
   préprocessing entre variants.

Critère go/no-go pertinent = **parité (≤ 1e-10 / fingerprint-identique) + coût transfert acceptable**,
pas le speedup (trivial).

## 9. Annexe — index des références code

| Sujet | Fichier | Symbole / zone |
|---|---|---|
| Contrat de step | `controllers/controller.py` | `OperatorController.execute`, `supports_step_cache`, `matches` |
| Registre | `controllers/registry.py` | `CONTROLLER_REGISTRY`, `register_controller` |
| Contexte / vue | `pipeline/config/context.py` | `DataSelector`, `ExecutionContext`, `RuntimeContext`, `ExecutionPhase` |
| Sorties | `pipeline/execution/result.py` | `StepOutput`, `StepResult`, `ArtifactMeta.content_hash` |
| Boucle d'exécution + step cache | `pipeline/execution/executor.py` | `_execute_steps`, `_execute_single_step`, lookup/put `step_cache` |
| Variants parallèles + reconstruction store | `pipeline/execution/orchestrator.py` | `_execute_single_variant`, reconstruction (`begin_pipeline`→`save_chain`→`flush_predictions`) |
| Folds | `controllers/models/base_model.py` | boucle `for fold_idx …`, `launch_training`, `_remap_folds_to_positions` |
| Splitters / anti-leakage | `controllers/splitters/split.py` | `CrossValidatorController`, `set_folds`, `_check_group_leakage` |
| Sélection / refit | `pipeline/execution/refit/` | `config_extractor.py`, `executor.py` (`_FullTrainFoldSplitter`), `model_selector.py` |
| Sweeps / générateurs | `pipeline/config/` | `pipeline_config.py`, `_generator/`, `optimization/optuna.py` |
| Store (SQLite + Parquet) | `pipeline/storage/` | `store_schema.py` (tables + `idx_artifacts_cache_key`), `workspace_store.py`, `artifacts/artifact_registry.py` |
| Trace / replay / bundle | `pipeline/trace/`, `pipeline/bundle/` | `execution_trace.py`, `.n4a` (`manifest`, `trace.json`, `artifacts/`) |
