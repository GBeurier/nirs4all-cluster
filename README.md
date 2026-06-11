# nirs4all-cluster

> **Statut : public alpha / prototype de validation.** Ce dépôt est public pour que
> l'architecture, les tests et les mesures soient inspectables. Il reste un prototype :
> son but est de mesurer si une file de jobs distribuée pour `nirs4all.run()` est
> justifiée, pas de promettre une plateforme cluster prête à exploiter. Voir
> [`PROTOTYPE_DESIGN.md`](PROTOTYPE_DESIGN.md) pour la conception et
> [`PROTOTYPE_TO_PRODUCTION.md`](PROTOTYPE_TO_PRODUCTION.md) pour les conditions
> de passage éventuel en produit.

Exécution **distribuée** de pipelines `nirs4all` (client / serveur / workers) : un coordinateur
reçoit des jobs et dispatche le travail à des workers qui pollent le serveur. Le prototype
**ne modifie aucune autre bibliothèque** de l'écosystème : `nirs4all` est importé uniquement par le
sous-processus runner, et le serveur/le client fonctionnent sans lui.

## Ce que le prototype fait

- Soumission d'un job `nirs4all.run()` via SDK Python ou CLI.
- Serveur FastAPI + file SQLite + object store local adressé par SHA-256.
- Workers en polling (long-polling HTTP + heartbeat), sandbox de task par dossier.
- Job atomique (Level 0) et décomposition `pipelines × datasets` (Level 1) avec agrégation/ranking.
- Téléchargement des artefacts : résumé JSON, logs, meilleur modèle `.n4a`.
- Reprise après crash worker (lease + retry), annulation coopérative, idempotence.
- Routage par capacités : labels, mémoire, **versions de paquets (PEP 440)**, **GPU/CUDA** (auto-détecté,
  `requirements.min_gpu_count` ou label `cuda=true`). Un job `nirs4all.run` exige `nirs4all` par défaut.

## Installation

```bash
# Environnement worker = un environnement nirs4all existant + ce paquet :
uv pip install -e .            # serveur + client + transport worker
# (les workers fournissent nirs4all eux-mêmes ; il n'est PAS une dépendance dure)
```

Python ≥ 3.11. Le serveur et le client n'ont besoin que de FastAPI/uvicorn/httpx/pydantic ; seul le
worker a besoin d'un environnement `nirs4all` provisionné.

## Quickstart (LAN de confiance)

```bash
# 1) serveur
n4cluster server --host 0.0.0.0 --port 8765 --state ./cluster-state

# 2) un ou plusieurs workers (sur des machines qui voient nirs4all et le dataset)
#    Le worker auto-détecte les GPU (nvidia-smi) et déclare le label cuda + gpu_count ;
#    forcer avec --gpus N (0 pour masquer les GPU).
n4cluster worker --server http://HOST:8765 --labels site=lab --slots 1

# 3) soumettre un job et attendre le résultat
n4cluster submit examples/job.shared-path.yaml --wait --out ./results
n4cluster status   <job_id>
n4cluster logs     <job_id>
n4cluster cancel   <job_id>
n4cluster artifacts <job_id> --out ./results
```

SDK Python :

```python
from nirs4all_cluster import ClusterClient

client = ClusterClient("http://host:8765", token=None)
job = client.submit_run(
    pipeline="/shared/pipelines/pls.yaml",                 # kind=path
    dataset="/shared/datasets/corn",                       # kind=shared_path
    params={"random_state": 42, "refit": True},
)
job = client.wait(job.id)
print(job.aggregate.best_metric, job.aggregate.ranking)
client.download_best_model(job.id, "best_model.n4a")
```

## Architecture

```
submitter (SDK/CLI/Studio) ──REST + WS──► serveur (FastAPI + SQLite + object store + scheduler + events)
                                              ▲
                          long-polling HTTP + heartbeat
                                              │
                                          workers ──► subprocess runner ──► nirs4all.run(workspace=task_ws)
```

- **`nirs4all_cluster/server/`** — `app.py` (API), `db.py` (file SQLite, leasing atomique, reaper),
  `scheduler.py` (machines à états + matching), `artifacts.py` (store SHA-256), `events.py` (broker).
- **`nirs4all_cluster/worker/`** — `agent.py` (boucle de polling), `materialize.py` (résolution des
  références → chemins locaux), `executor.py` (sous-processus + capture + annulation).
- **`nirs4all_cluster/runners/nirs4all_run.py`** — **seul** module qui importe `nirs4all`.
- **`client.py`** (SDK), **`cli.py`** (`n4cluster`), **`schemas.py`** (contrat Pydantic).

## Tests et validation

```bash
pytest -q                                   # 45 tests unit/API sans nirs4all + 3 d'intégration
python scripts/validation.py                # harnais bout-en-bout sur nirs4all-data (8/8)
```

Résultats mesurés sur `nirs4all-data` (voir [`WORKLOG.md`](WORKLOG.md)) : job atomique → `.n4a`,
2 workers en parallèle, **kill worker → retry**, annulation non relancée, agrégation `pipeline ×
dataset`, et **parité métrique exacte vs `nirs4all.run()` local (diff = 0.0)** — au-delà du critère
go/no-go ≤ 1e-10.

## Critères go/no-go pour passer en produit

Le go reste conditionnel à **toutes** ces conditions :

1. ≥ 2 labos / partenaires demandent explicitement l'exécution distribuée. *(non mesurable ici)*
2. Speedup ≥ 3× sur un workload réel (grid search AOM / HPO sur ≥ 32 datasets). *(à mesurer)*
3. Résultats metric-identiques (≤ 1e-10) au mono-machine. → **atteint : diff = 0.0** sur le job atomique.
4. Modèle data + sécurité + reprise écrit **avant** le code. → fait dans `PROTOTYPE_DESIGN.md`.
5. Sujets de cadrage traités dès le départ (mTLS, secrets, sandboxing tiers, IP/RGPD datasets,
   environnements lourds TF/Torch/JAX, coût des transferts, idempotence/reprise, quotas/fairness,
   scheduling hétérogène). → recensés dans `PROTOTYPE_TO_PRODUCTION.md`.

Sans ces conditions : **no-go produit** — et l'option par défaut reste un backend Dask opt-in dans
`nirs4all`, pas une plateforme maison. Le dépôt reste public comme banc de mesure et référence de
conception, pas comme engagement de roadmap produit.

## Non-objectifs (rappel)

Pas de modification des autres libs, pas de multi-tenant ouvert, pas de sandbox pour code Python
arbitraire, pas de scheduler type K8s/Ray/Dask, pas d'écriture concurrente dans un workspace
`nirs4all` partagé, pas de distribution des folds. Voir `PROTOTYPE_DESIGN.md` § Non-objectifs.

## Références

`PROTOTYPE_DESIGN.md`, `PROTOTYPE_TO_PRODUCTION.md`, `WORKLOG.md`, et
`nirs4all-ecosystem/NIRS4ALL-ECOSYSTEM_VISION.md` (annexe *Perspective : exécution distribuée*, risque R13).
