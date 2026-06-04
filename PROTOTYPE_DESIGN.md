# Prototype design - nirs4all-cluster

## Objectif

Construire un prototype Python isole, dans ce depot, qui permet a des clients
`nirs4all` de soumettre des jobs a un serveur, puis de les executer sur plusieurs
workers. Le prototype doit valider le besoin "queue distribuee de jobs
nirs4all" sans modifier `nirs4all`, `nirs4all-studio`, `nirs4all-io`,
`nirs4all-methods` ou les autres bibliotheques de l'ecosysteme.

Le prototype n'est pas une plateforme definitive. Il sert a mesurer :

- si l'unite de travail distribuee est bien choisie ;
- si les resultats restent compatibles avec une execution locale ;
- si le transfert de donnees et d'artefacts est acceptable ;
- quelles garanties reseau, reprise et securite deviennent indispensables.

## Contexte observe dans l'ecosysteme

- `nirs4all.run()` est l'entree publique stable pour lancer un pipeline sur un
  dataset. Elle accepte un pipeline, une liste de pipelines, un dataset ou une
  liste de datasets, puis execute le produit cartesien.
- `PipelineRunner` expose deja `n_jobs` pour paralleliser localement des variants
  avec `joblib/loky`. En mode parallele, les workers locaux n'ecrivent pas
  directement dans le `WorkspaceStore`; le parent reconstruit ensuite l'etat.
  C'est un signal important : en cluster, chaque worker doit produire un resultat
  isole, puis le serveur doit agreger.
- Le workspace `nirs4all` est un dossier contenant `store.sqlite`, des arrays et
  des artefacts. Il ne faut pas faire ecrire plusieurs machines dans le meme
  workspace SQLite.
- `nirs4all-studio` possede deja un `JobManager` en memoire, des routes FastAPI
  et des WebSockets de progression. C'est utile pour l'UX, mais ce n'est pas une
  queue durable multi-machine.
- `nirs4all-datasets` et `nirs4all-io` donnent une direction pour les datasets :
  references versionnees, checksums, cache local, materialisation tardive.

## Use cases a couvrir

### MVP

1. Soumettre un job `nirs4all.run()` depuis une CLI ou une petite SDK Python.
2. Demarrer un serveur local ou LAN.
3. Connecter plusieurs workers Python preconfigures.
4. Assigner les jobs aux workers selon disponibilite et capacites simples.
5. Suivre statut, logs, progression approximative et resultats.
6. Telecharger les artefacts de sortie : resume JSON, logs, modele `.n4a`,
   workspace de task optionnel.

### Cas a anticiper

- Lancement depuis Studio : le backend Studio soumettrait au cluster au lieu
  d'utiliser son `JobManager` local.
- Batch `pipelines x datasets` : decomposition en plusieurs tasks
  independantes.
- Grid search / HPO : decomposition en variants explicites, puis aggregation.
- Workers heterogenes : CPU, GPU, RAM, backend `torch/tensorflow/jax`, versions.
- Arena interne : batch nocturne sur datasets et scenarios cures.
- Calcul federe : dataset restant sur un worker/site donne, seul le resultat
  remonte.
- Reprise apres crash worker ou serveur.

## Non-objectifs du prototype

- Pas de modification des autres bibliotheques.
- Pas de multi-tenancy ouvert a des tiers.
- Pas de sandbox securise pour code Python arbitraire.
- Pas de scheduler avance type Kubernetes, Ray ou Dask.
- Pas d'ecriture concurrente dans un workspace `nirs4all` partage.
- Pas de garantie de parite parfaite sur les jobs decomposes tant que les
  mesures de non-regression ne sont pas ecrites.

## Architecture proposee

```
submitter Python/CLI/Studio
        |
        | REST + WebSocket/SSE
        v
cluster server
  - API FastAPI
  - queue SQLite
  - scheduler simple
  - object store local
  - events/logs
        ^
        | long-polling HTTP + heartbeat
        |
workers nirs4all
  - environnement Python preinstalle
  - sandbox de task par dossier
  - nirs4all.run(..., workspace_path=task_workspace)
  - upload des resultats
```

Choix reseau : les workers pollent le serveur au lieu de recevoir des pushes.
C'est plus simple pour un LAN, des machines derriere NAT, et un prototype. Le
serveur garde une API publique stable pour les clients et une API worker separee.

## Composants

### Serveur

Responsabilites :

- recevoir les submissions ;
- valider et persister les jobs ;
- materialiser les artefacts d'entree ;
- decomposer un job logique en tasks executables ;
- enregistrer les workers et leurs capacites ;
- attribuer des leases de tasks ;
- suivre heartbeats, retries, timeouts et annulations ;
- stocker les events, logs et resultats ;
- exposer REST + WebSocket/SSE aux clients.

Implementation MVP :

- FastAPI + Uvicorn ;
- SQLite via `sqlite3` standard library ;
- stockage d'artefacts sur disque par SHA-256 ;
- scheduler FIFO avec priorite optionnelle ;
- un process serveur unique.

### Worker

Responsabilites :

- s'enregistrer avec ses capacites ;
- demander une task disponible ;
- telecharger ou resoudre les inputs ;
- creer un workspace de task isole ;
- executer `nirs4all.run()` avec `workspace_path` dedie ;
- capturer stdout/stderr/logs ;
- exporter le meilleur modele si demande ;
- uploader resultats et artefacts ;
- envoyer heartbeat et progress events.

Le worker ne recoit pas de dependances a installer dynamiquement. Son
environnement Python est provisionne avant demarrage. Les capacites declarees
servent au routage.

### Client Python

Surface cible :

```python
from nirs4all_cluster import ClusterClient

client = ClusterClient("http://server:8765")
job = client.submit_run(
    pipeline={"kind": "path", "path": "/shared/pipelines/pls.yaml"},
    dataset={"kind": "shared_path", "path": "/shared/data/corn"},
    params={"verbose": 1, "random_state": 42, "refit": True},
)
client.wait(job.id)
result = client.get_result(job.id)
```

Le client est volontairement mince : il parle au serveur, mais ne reimplemente
pas `nirs4all`.

### CLI

Commandes proposees :

```bash
n4cluster server --host 0.0.0.0 --port 8765 --state ./cluster-state
n4cluster worker --server http://host:8765 --labels site=lab,cuda=false
n4cluster submit job.yaml
n4cluster status <job_id>
n4cluster logs <job_id>
n4cluster cancel <job_id>
n4cluster artifacts <job_id> --out ./results
```

## Modele de donnees

### Entites

- `Job` : demande logique soumise par un client.
- `Task` : unite executable louee a un worker.
- `Worker` : agent connecte, avec heartbeats et capacites.
- `Lease` : attribution temporaire d'une task a un worker.
- `Artifact` : blob adresse par hash, entree ou sortie.
- `Event` : changement d'etat, log structure, progression.

### Etats job

```
queued -> running -> succeeded
queued -> cancelled
running -> cancelling -> cancelled
running -> failed
failed -> queued    # retry manuel optionnel
```

### Etats task

```
queued -> leased -> running -> succeeded
queued -> leased -> lost -> queued
running -> lost -> queued|failed
running -> failed -> queued|failed
running -> cancelled
```

Une lease expire si le worker ne heartbeat plus. La task redevient `queued` tant
que `attempt < max_attempts`.

### Tables SQLite

- `jobs(id, type, status, priority, created_at, updated_at, owner, request_json,
  result_json, error, idempotency_key)`
- `tasks(id, job_id, status, attempt, max_attempts, worker_id, lease_expires_at,
  requirements_json, payload_json, result_json, error)`
- `workers(id, status, last_seen_at, labels_json, capabilities_json, slots_total,
  slots_used, version_json)`
- `artifacts(id, sha256, kind, path, size_bytes, created_at, metadata_json)`
- `events(id, job_id, task_id, worker_id, ts, level, type, message, data_json)`

## Granularite d'execution

### Niveau 0 - job atomique

Le serveur cree une task unique qui appelle `nirs4all.run()` sur un worker.
C'est le plus rapide a implementer et utile pour distribuer plusieurs jobs
independants.

Limite : un gros grid search reste monolithique sur un worker.

### Niveau 1 - matrice pipelines x datasets

Si la submission contient plusieurs pipelines ou datasets explicites, le serveur
cree une task par combinaison. Chaque task execute un `nirs4all.run()` simple,
avec son propre workspace. Le serveur agrege ensuite les metriques.

Cette decomposition est naturelle parce que `nirs4all.run()` fait deja le
produit cartesien en local.

### Niveau 2 - variants explicites

Pour les gros sweeps, le client ou le serveur fournit une liste de pipelines
deja concretises. Chaque variant devient une task, typiquement avec
`refit=False`. Le serveur selectionne les meilleurs resultats et lance une task
finale de refit/export du meilleur pipeline.

Ce niveau doit etre teste contre une execution locale monolithique. Il ne faut
pas promettre la parite tant que l'aggregation ne reproduit pas les semantics
exactes de `nirs4all`.

### Niveau 3 - folds distribues

A differer. Distribuer les folds touche aux garanties anti-leakage, a la
reconstruction du store et a la selection/refit. A envisager seulement apres le
couplage avec une couche d'orchestration plus formelle ou apres un spike dedie.

## Specification de job

Format YAML/JSON cible :

```yaml
type: nirs4all.run
name: pls-corn
pipeline:
  kind: path
  path: /shared/pipelines/pls.yaml
dataset:
  kind: shared_path
  path: /shared/datasets/corn
params:
  verbose: 1
  random_state: 42
  refit: true
  save_artifacts: true
requirements:
  labels:
    cuda: "false"
  min_memory_gb: 8
  packages:
    nirs4all: ">=0.9,<0.10"
outputs:
  export_best_model: true
  keep_task_workspace: false
retry:
  max_attempts: 2
```

## References d'entree

### Pipeline

Kinds supportes par ordre de preference :

1. `path` : fichier YAML/JSON accessible par le worker.
2. `artifact` : fichier uploade au serveur puis telecharge par le worker.
3. `inline_json` : pipeline serialisable JSON.
4. `python_entrypoint` : module Python dans un bundle avec
   `build_pipeline()`. A reserver aux environnements de confiance.

Le point 4 est utile pour un proto car beaucoup de pipelines Python contiennent
des objets sklearn non serialisables en JSON propre. Il est aussi dangereux :
pas de multi-tenant avec ce mode sans sandbox.

### Dataset

Kinds supportes :

1. `shared_path` : chemin disponible sur tous les workers.
2. `artifact` : zip uploade, decompresse dans le sandbox de task.
3. `catalog` : identifiant `nirs4all-datasets` / DOI versionne, resolu par le
   worker avec cache local.
4. `worker_local` : dataset present seulement sur des workers labels, utile pour
   un futur mode federe.

Pour le MVP, `shared_path` est le plus simple et le plus realiste en cluster de
labo. `artifact` sert aux petits datasets et aux demos.

## Execution worker

Pseudo-code :

```python
task = lease_task()
workdir = state / "tasks" / task.id
workspace = workdir / "workspace"
inputs = materialize_inputs(task, workdir / "inputs")

pipeline = load_pipeline(inputs.pipeline)
dataset = load_dataset_spec(inputs.dataset)
run_params = dict(task.params)
inner_n_jobs = run_params.pop("inner_n_jobs", 1)

result = nirs4all.run(
    pipeline=pipeline,
    dataset=dataset,
    workspace_path=workspace,
    n_jobs=inner_n_jobs,
    **run_params,
)

summary = summarize_run_result(result)
if task.outputs.export_best_model:
    result.export(workdir / "outputs" / "best_model.n4a")

upload_outputs(summary, logs, optional_workspace, model)
complete_task()
```

Par defaut, `inner_n_jobs=1` pour eviter de surconsommer une machine en
combinant parallelisme local et parallelisme cluster. Un worker peut annoncer
plusieurs slots si la machine le permet.

## API REST

### Client API

- `POST /v1/jobs` : soumettre un job.
- `GET /v1/jobs` : lister les jobs.
- `GET /v1/jobs/{job_id}` : statut et resume.
- `POST /v1/jobs/{job_id}/cancel` : demander annulation.
- `GET /v1/jobs/{job_id}/tasks` : details tasks.
- `GET /v1/jobs/{job_id}/events` : events pagines.
- `GET /v1/jobs/{job_id}/artifacts` : sorties disponibles.
- `GET /v1/artifacts/{artifact_id}` : telecharger un artefact.
- `WS /v1/jobs/{job_id}/events/stream` : progression temps reel.

### Worker API

- `POST /v1/workers/register`
- `POST /v1/workers/{worker_id}/heartbeat`
- `POST /v1/workers/{worker_id}/lease`
- `POST /v1/tasks/{task_id}/start`
- `POST /v1/tasks/{task_id}/events`
- `POST /v1/tasks/{task_id}/complete`
- `POST /v1/tasks/{task_id}/fail`
- `POST /v1/tasks/{task_id}/artifacts`

## Scheduling

MVP :

- FIFO par priorite ;
- filtrage par labels (`cuda=true`, `site=lab-a`, `python=3.11`) ;
- slots par worker ;
- lease timeout ;
- retry borne ;
- cancellation cooperative.

Plus tard :

- estimation duree/RAM ;
- data locality ;
- quotas par utilisateur/projet ;
- fairness ;
- routage GPU ;
- preemption.

## Resultats et aggregation

Chaque task retourne au minimum :

```json
{
  "status": "succeeded",
  "nirs4all_version": "0.9.1",
  "duration_seconds": 123.4,
  "metrics": {
    "best_score": 0.91,
    "best_rmse": 0.12,
    "best_r2": 0.91,
    "best_accuracy": null
  },
  "counts": {
    "num_predictions": 12
  },
  "artifacts": {
    "model": "artifact_id",
    "logs": "artifact_id",
    "workspace": null
  }
}
```

Pour un job compose de plusieurs tasks, le serveur calcule :

- nombre de tasks reussies/echouees ;
- meilleur resultat selon la metrique demandee ;
- table de ranking ;
- artefact du meilleur modele ;
- erreurs par task.

L'aggregation du workspace complet `nirs4all` n'est pas MVP. Elle est possible
plus tard via import/export controle du `WorkspaceStore`, mais il faut eviter
de bricoler du SQLite cross-machine.

## Securite

MVP acceptable uniquement pour un LAN de confiance :

- token statique serveur/worker/client ;
- CORS ferme par defaut ;
- pas d'execution de jobs anonymes ;
- logs sans secrets ;
- nettoyage de workdir apres retention ;
- refus par defaut du mode `python_entrypoint` si `--allow-python-jobs` n'est
  pas active.

Avant tout usage multi-utilisateur :

- TLS ou mTLS ;
- identites clients/workers ;
- rotation des tokens ;
- sandbox container par task ;
- quotas CPU/RAM/disk ;
- politique no-network optionnelle ;
- allowlist de chemins partages ;
- chiffrement ou retention stricte des artefacts sensibles.

## Layout de code propose

```text
nirs4all-cluster/
  pyproject.toml
  PROTOTYPE_DESIGN.md
  nirs4all_cluster/
    __init__.py
    cli.py
    schemas.py
    client.py
    server/
      app.py
      db.py
      scheduler.py
      artifacts.py
      events.py
    worker/
      agent.py
      executor.py
      materialize.py
    runners/
      nirs4all_run.py
  tests/
    test_scheduler.py
    test_state_machine.py
    test_artifacts.py
    test_worker_smoke.py
  examples/
    job.shared-path.yaml
    job.uploaded-bundle.yaml
```

## Plan d'implementation prototype

### Phase 0 - squelette

- `pyproject.toml` minimal.
- CLI `server`, `worker`, `submit`, `status`.
- Schemas Pydantic.
- SQLite migrations simples.
- Tests unitaires des transitions d'etat.

### Phase 1 - queue distribuee minimale

- Serveur FastAPI.
- Register/heartbeat workers.
- Lease FIFO.
- Execution d'une task factice `echo`.
- Events/logs.
- Retry sur lease expiree.

### Phase 2 - runner nirs4all atomique

- Materialisation `shared_path` et `artifact`.
- Execution `nirs4all.run()` dans workspace de task.
- Resume JSON.
- Export `.n4a`.
- Upload/download artefacts.
- Smoke test avec un mini dataset.

### Phase 3 - decomposition simple

- Job `matrix` : pipelines explicites x datasets explicites.
- Aggregation ranking.
- Selection meilleur artefact.
- Comparaison avec execution locale sur un workload petit.

### Phase 4 - integration Studio/API future

- Adapter REST qui reproduit les concepts `JobManager` de Studio.
- WebSocket compatible progression job.
- Documentation pour remplacer l'execution locale Studio par cluster en opt-in.

## Tests de validation

Tests obligatoires avant de considerer le proto utile :

- un worker execute un job atomique et remonte un modele `.n4a` ;
- deux workers executent deux jobs en parallele ;
- un worker tue pendant une task provoque un retry ;
- un job annule n'est pas relance ;
- un job `pipeline x dataset` agrege les resultats ;
- le meme job atomique donne des metriques equivalentes a `nirs4all.run()`
  local ;
- aucun fichier hors du `state_dir` serveur/worker n'est cree sauf chemins
  explicitement declares.

Mesures a collecter :

- temps d'attente queue ;
- temps de transfert inputs/outputs ;
- temps d'execution worker ;
- overhead serveur ;
- taille artefacts ;
- taux de retry ;
- difference metrique vs local.

## Decisions pragmatiques

- Commencer par HTTP polling et SQLite, pas Redis/RabbitMQ.
- Ne pas partager les workspaces `nirs4all` entre workers.
- Utiliser `nirs4all.run()` comme boite noire au depart.
- Ne pas distribuer les folds dans le premier prototype.
- Accepter `python_entrypoint` seulement en mode confiance explicite.
- Mesurer la parite avant de decomposer automatiquement les variants.

## Questions ouvertes

- Quelle representation canonique de pipeline doit devenir le contrat reseau :
  YAML `nirs4all`, JSON Studio, bundle Python, ou plusieurs formats supportes ?
- Faut-il garder les workspaces workers complets ou seulement les resumes et
  `.n4a` ?
- Comment importer proprement plusieurs resultats dans un workspace Studio sans
  toucher `nirs4all` ?
- Quelle granularite donne le meilleur compromis : run complet, variant, fold ?
- Quel minimum de securite est requis pour les premiers utilisateurs reels ?
- Quelle politique de cache dataset adopter sur les workers ?

## Recommandation

Pour un prototype rapide dans ce dossier, implementer d'abord :

1. serveur FastAPI + SQLite + object store local ;
2. worker polling + sandbox de task ;
3. job atomique `nirs4all.run()` ;
4. artefacts `.n4a` + resume JSON ;
5. decomposition explicite `pipelines x datasets`.

Cette trajectoire valide le modele client/serveur/workers sans forcer de
changements dans les autres bibliotheques. Si les mesures montrent un vrai gain
et une parite acceptable, la suite logique est de decider si le backend doit
rester une queue native ou si l'effort doit migrer vers un backend Dask/Ray plus
standard.
