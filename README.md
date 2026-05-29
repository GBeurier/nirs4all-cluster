# nirs4all-cluster

> **Statut : perspective, hors roadmap.** Ce dépôt est un placeholder réservant le nom. La vision
> de l'écosystème recommande explicitement de **ne pas** créer de dépôt cluster dédié prématurément
> (anti-pattern « plateforme ML »). Aucun code d'exécution distribuée ne doit y atterrir tant que les
> critères go/no-go ci-dessous ne sont pas tous satisfaits.

Exploration de l'**exécution distribuée** de pipelines `nirs4all` (client / serveur / workers) :
un coordinateur reçoit des jobs et dispatche le travail à des workers distants.

## Quatre usages possibles

1. **Cluster de labo** — mutualisation interne sur 5-10 machines d'un groupe de recherche.
2. **Arena en exécution interne distribuée** — compute interne curé, scenarios méthode × dataset
   assez nombreux pour bénéficier d'un dispatch multi-machine.
3. **Studio multi-tenant** — backend partagé (ajoute auth, isolation des workspaces).
4. **Calcul fédéré** — les datasets restent sur la machine d'origine, seul le résultat agrégé remonte.

## Approche recommandée (avant tout dépôt)

**Pas de nouveau dépôt en 0-12 mois.** Prototyper d'abord un **backend Dask opt-in dans `nirs4all`**
(par ex. `nirs4all[dask]`), jamais ici. Les options plus lourdes (worker natif, host controllers
`dag-ml` RPC, dépôt cluster complet) sont conditionnées à la validation du prototype Dask et à
l'émergence d'un cas d'usage tiers financé. **Ne jamais commencer par un dépôt cluster complet.**

## Critères go/no-go pour ouvrir réellement ce chantier

Le go est conditionnel à **toutes** ces conditions :

1. ≥ 2 labos / partenaires demandent explicitement l'exécution distribuée.
2. Speedup ≥ 3× mesuré sur un workload réel (grid search AOM / HPO sur ≥ 32 datasets).
3. Résultats *bit-identiques ou metric-identiques* (≤ 1e-10) à l'exécution mono-machine.
4. Modèle data + sécurité + reprise écrit **avant** le code.
5. Sujets de cadrage traités dès le départ : mTLS, secrets, sandboxing tiers, IP/RGPD datasets,
   compat environnements Python lourds (TF/Torch/JAX), coût des transferts, idempotence/reprise,
   quotas/fairness, scheduling hétérogène (GPU/CPU).

Sans ces conditions : **no-go**.

## Références

Voir `nirs4all-ecosystem/NIRS4ALL-ECOSYSTEM_VISION.md`, annexe *Perspective : exécution distribuée*
(et risque R13).
