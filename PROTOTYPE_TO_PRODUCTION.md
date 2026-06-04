# Du prototype à la production — nirs4all-cluster

Ce document fait le bilan de ce que le prototype a **validé**, puis décrit honnêtement ce qui
manque pour un usage réel. Il complète `PROTOTYPE_DESIGN.md` (la conception) et `WORKLOG.md` (le
journal). Il s'adresse à la décision : *faut-il faire de ce prototype un produit, et si oui,
comment ?*

> **Position de départ (inchangée).** L'option par défaut recommandée par l'écosystème reste un
> **backend d'exécution opt-in dans `nirs4all`** (p. ex. `nirs4all[dask]`), **pas** un dépôt cluster
> complet. Le prototype existe pour produire des mesures et dé-risquer la décision, pas pour la
> préempter. Voir les critères go/no-go du `README.md`.

## 1. Ce que le prototype a démontré

Mesuré sur `nirs4all-data` via `scripts/validation.py` (8/8) et la suite d'intégration (`pytest`) :

| Question de la conception | Réponse mesurée |
|---|---|
| L'unité de travail distribuée est-elle bien choisie ? | Oui pour Level 0 (job atomique) et Level 1 (`pipelines × datasets`). Chaque task = un `nirs4all.run()` isolé avec son propre workspace. |
| Les résultats restent-ils compatibles avec l'exécution locale ? | **Oui, métrique-identique** : `best_rmse` cluster == local, **diff = 0.0** (≪ critère ≤ 1e-10). |
| Le transfert données/artefacts est-il acceptable ? | Oui pour `shared_path` (aucun transfert) et petits `.n4a` (~50 KB). Le transfert de gros datasets via `artifact` reste à mesurer. |
| Quelles garanties réseau/reprise/sécurité deviennent indispensables ? | Reprise (lease+retry) et annulation : validées. Sécurité : seul un token statique — insuffisant hors LAN de confiance (voir §4). |

**Architecture validée** : serveur unique FastAPI + SQLite (WAL) + object store SHA-256 ; workers en
polling (long-poll + heartbeat, leases renouvelées au heartbeat) ; isolation par sous-processus runner
(crash containment + annulation réelle) ; `nirs4all` importé **uniquement** par le runner.

**Limites assumées du prototype** (conformes aux non-objectifs) : un seul process serveur ; pas de
multi-tenant ; pas de sandbox pour code Python arbitraire ; pas de distribution des folds (Level 3) ;
pas d'écriture concurrente dans un workspace `nirs4all` partagé.

## 2. La bifurcation : file native vs backend Dask/Ray

Avant d'industrialiser **quoi que ce soit**, trancher :

- **Backend `nirs4all[dask]` opt-in (recommandé par défaut).** Réutilise un ordonnanceur éprouvé
  (Dask/Ray), zéro nouveau service à exploiter, intégration directe dans `nirs4all.run(..., backend=...)`.
  Le prototype montre que la décomposition `pipelines × datasets` est triviale et métrique-identique :
  c'est exactement ce qu'un backend Dask ferait, sans serveur/queue maison.
- **File native (ce dépôt).** Justifiée **seulement** si des besoins que Dask/Ray ne couvrent pas bien
  émergent : workers hétérogènes derrière NAT en long-polling, calcul fédéré (dataset qui ne bouge pas),
  files durables multi-jours, intégration Studio multi-tenant avec quotas. Tant que ces besoins ne sont
  pas **financés et demandés**, le coût d'exploitation d'un service distribué maison n'est pas justifié.

Le prototype est conçu pour que cette bascule soit possible dans les deux sens : `nirs4all.run()` est
utilisé en boîte noire, donc le même contrat (`pipeline`, `dataset`, `params`) alimenterait un backend
Dask sans réécriture côté client.

> **Au-delà du grain « run entier ».** La vraie valeur (et difficulté) est la distribution **fine** :
> sous-arbres, points de sweep et folds répartis (refit-with-folds compris), pas des `run()` entiers.
> L'inspection du moteur `nirs4all` (unité de step typée, `DataSelector` = vue de données, scopes
> `(variant, fold, phase)`, artefacts content-addressed + clé de cache de calcul, trace/replay,
> sélection/refit déterministes) montre que le moteur a déjà ~80 % des abstractions nécessaires, mais
> câblées in-process/mono-hôte. Cartographie complète des **points d'extraction** et de ce qui manque
> (plan de contrôle distribué) : **[`docs/DISTRIBUTED_EXECUTION_DESIGN.md`](docs/DISTRIBUTED_EXECUTION_DESIGN.md)**.

## 3. Mesures encore à produire avant un go

Le go reste conditionné (README §go/no-go). Restent à mesurer :

1. **Speedup ≥ 3×** sur un workload réel (grid search AOM / HPO sur ≥ 32 datasets) — le prototype le
   permet (`job.matrix.yaml`), il faut juste lancer un vrai sweep et chronométrer vs mono-machine.
2. **Coût des transferts** pour de gros datasets en `artifact` (zip upload/extract) et de gros `.n4a`
   (modèles deep). Le prototype mesure déjà `submit_latency`, `exec`, `overhead`, `n4a_size` ; étendre.
3. **Parité Level 2 (variants explicites)** : la conception interdit de promettre la parité tant que
   l'agrégation ne reproduit pas la sémantique de sélection/refit de `nirs4all`. À écrire et mesurer
   avant toute décomposition automatique des sweeps.
4. **≥ 2 partenaires** demandeurs (non technique).

## 4. Écarts de production par domaine

Format : *prototype actuel → exigence production*.

### Fiabilité & passage à l'échelle
- Un process serveur + SQLite (un seul écrivain) → **Postgres** (ou Redis/broker dédié) pour autoriser
  plusieurs process serveur et un vrai débit ; conserver SQLite seulement pour le mono-nœud de labo.
- Object store sur disque local → **S3/MinIO** (ou stockage réseau) avec cycle de vie/rétention.
- Reaper in-process → superviseur résilient + métriques de lease perdues/retries.
- *Déjà fait* : renouvellement des leases au heartbeat, retry borné, idempotence, sanitization NaN→null.

### Sécurité (le plus gros écart)
- Token statique partagé → **mTLS ou OIDC**, identités distinctes client/worker, **rotation** des tokens.
- Pas d'isolation entre soumetteurs → **sandbox conteneur par task**, quotas CPU/RAM/disk, politique
  no-network optionnelle, **allowlist** de chemins partagés (les `shared_path`/`path` sont aujourd'hui
  pris tels quels).
- `python_entrypoint` (code arbitraire) → **jamais** en multi-tenant ; aujourd'hui correctement
  derrière `--allow-python-jobs`, mais à interdire dès qu'un tiers peut soumettre.
- Artefacts → chiffrement/rétention stricte pour données sensibles (IP/RGPD datasets).
- Limite de taille d'artefact : présente, mais à durcir (refus en streaming, pas seulement a posteriori).

### Ordonnancement
- FIFO + priorité + labels + slots → estimation durée/RAM, **data locality**, routage **GPU**,
  **quotas/fairness** par utilisateur/projet, préemption.

### Données
- `shared_path` / `artifact` → câbler la **kind `catalog`** (DOI `nirs4all-datasets` :
  `nirs4all_datasets.load()` + `resolve_config()` + cache vérifié par checksum). Aujourd'hui `catalog`
  lève un `NotImplementedError` explicite (déféré, honnête).
- Politique de cache dataset sur les workers (réutilisation entre tasks).
- `worker_local` → mode **fédéré** (dataset qui reste sur le site, seul le résultat remonte).

### Résultats & agrégation
- Résumé par task + ranking + meilleur `.n4a` → import propre de plusieurs résultats dans un workspace
  Studio **sans toucher `nirs4all`** (via export/import contrôlé du `WorkspaceStore`, pas de SQLite
  cross-machine bricolé).
- Level 3 (folds distribués) : **différé** — touche à l'anti-leakage, à la reconstruction du store et
  au refit ; à n'envisager qu'après un spike dédié ou un couplage avec `dag-ml`.

### Intégration Studio (Phase 4)
- Le serveur expose déjà des events typés et un flux WS. Pour un backend cluster **opt-in** dans Studio,
  fournir un adaptateur qui mappe les états job (`queued/running/succeeded/failed/cancelling/cancelled`)
  et renomme les events vers le vocabulaire WS de Studio (`job_started/job_progress/job_completed/
  job_failed/job_log`, canal `job:{id}`). Studio remplacerait son `JobManager` local par le cluster en
  opt-in, sans réimplémenter la logique NIRS/ML.

### Versions & disponibilité des bibliothèques (implémenté)
- Le worker **déclare** au registre son interpréteur et les versions installées d'un jeu de paquets
  pertinents (`nirs4all`, `numpy`, `scipy`, `scikit-learn`, `pandas`, `polars`, `torch`, `tensorflow`,
  `jax`). Le scheduler **applique** `requirements.packages` (spécificateurs PEP 440 via `packaging`,
  ex. `{"nirs4all": ">=0.9,<0.10"}`, `{"python": ">=3.11"}`, ou `""` = présence requise). Un paquet
  non déclaré ne satisfait jamais une exigence explicite (disponibilité inconnue = indisponible).
- **Disponibilité par défaut** : un job `nirs4all.run` exige implicitement la présence de `nirs4all`,
  donc il n'est jamais routé vers un worker qui ne l'a pas (le client peut surcharger avec une plage).
  Chaque résultat de task enregistre aussi la `nirs4all_version` qui l'a produit (traçabilité).
- **GPU/CUDA (implémenté)** : le worker auto-détecte les GPU via `nvidia-smi` et déclare
  `capabilities.gpu_count`/`gpu_names`/`cuda_version` + un label `cuda=true|false` (auto). Le scheduler
  route sur le label `cuda` **et** sur `requirements.min_gpu_count` (fail-closed : GPU non déclaré = 0).
  Override CLI : `--gpus N`. Validé sur 2 GPU réels (RTX 4090 + 5090, CUDA 13.1) via WSL.
- Reste à produire : images conteneur versionnées par capacité (CPU/GPU, TF/Torch/JAX) côté
  provisioning worker, et déclaration mémoire GPU par device.

### Observabilité & exploitation
- Table `events` + WS → métriques Prometheus, logs structurés, tracing, tableaux de bord de queue.
- Packaging worker : aujourd'hui le worker hérite d'un env `nirs4all` provisionné ; en production,
  images conteneur versionnées par capacité (CPU/GPU, TF/Torch/JAX).

## 5. Trajectoire recommandée

1. **Maintenant** : garder le prototype comme banc de mesure. Lancer le sweep AOM/HPO réel pour le
   critère speedup ≥ 3× ; mesurer le coût transfert sur gros datasets.
2. **Si parité Level 2 + speedup confirmés et ≥ 2 demandeurs** : prototyper le **backend Dask opt-in**
   dans `nirs4all` et le comparer à cette file native sur le même workload. Décider du backend par les
   chiffres, pas par l'architecture.
3. **Seulement si un besoin non couvert par Dask est financé** (fédéré, NAT/long-poll, Studio
   multi-tenant) : durcir ce dépôt selon §4 (sécurité d'abord : mTLS + identités + sandbox), migrer
   SQLite→Postgres et disque→objet réseau, puis l'adaptateur Studio.

## 6. Questions ouvertes — réponses informées par le prototype

- *Contrat réseau du pipeline ?* → Le prototype valide **YAML/JSON `nirs4all`** (`path`/`artifact`/
  `inline_json`) comme contrat principal, JSON-sérialisable ; `python_entrypoint` réservé à la confiance.
- *Garder les workspaces workers complets ?* → Non par défaut : résumé + `.n4a` suffisent (option
  `keep_task_workspace` pour debug). L'agrégation de workspaces complets reste hors MVP.
- *Granularité optimale ?* → Job atomique et `pipeline × dataset` donnent déjà parité + parallélisme ;
  le variant (Level 2) demande une mesure de parité dédiée ; le fold (Level 3) est différé.
- *Sécurité minimale pour de vrais utilisateurs ?* → Au minimum mTLS + identités + sandbox par task
  **avant** tout usage hors LAN de confiance.

## 7. Revue

Le code et cette feuille de route ont été relus par `codex` (revue read-only) et par une revue
multi-agents adversariale (4 dimensions : concurrence/états, sécurité, contrat d'API, adhérence au
design). Les conclusions et corrections appliquées sont consignées dans `WORKLOG.md` (§ Review).
