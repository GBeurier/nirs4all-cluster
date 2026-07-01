# Python SDK

`ClusterClient` is the public SDK. It speaks the REST API and nothing more — it never
imports `nirs4all`.

```{code-block} python
from nirs4all_cluster import ClusterClient, build_nirs4all_run_request

with ClusterClient("http://host:8765", token=None) as client:
    job = client.submit_nirs4all_run(
        pipelines=["/shared/pls.yaml", "/shared/rf.yaml"],
        datasets=["/shared/corn", "/shared/wheat"],
        n_jobs=1,
        rank_metric="best_rmse", rank_mode="min",
    )
    job = client.wait(job.id)
    for row in job.aggregate.ranking:
        print(row["pipeline"], row["dataset"], row["metrics"])
    client.download_best_model(job.id, "best.n4a")

    # ops helpers
    print(client.stats())
    for j in client.list_jobs(status="running"):
        print(j.id, j.status)

    # Same request builder, useful for core / CLI callers that need to inspect or
    # persist the exact contract before submission.
    req = build_nirs4all_run_request(
        pipeline="/shared/pls.yaml",
        dataset="/shared/corn",
        params={"random_state": 42, "refit": True},
    )
    assert req.parity.scope == "atomic"
```

`submit_run()` remains as a compatibility alias. The core-facing adapter accepts local
`nirs4all.run` vocabulary where it is meaningful: `n_jobs` is translated to the
worker-local `inner_n_jobs`, while `workspace_path` is omitted because every distributed
task gets an isolated workspace. The stored `DistributedRunParity` contract says the beta
expects metric parity for whole-run tasks (atomic or explicit `pipelines x datasets`) and
does **not** claim fine-grained DAG, variant, fold, subtree, or workspace parity.

If the server runs an incompatible protocol major the client raises
`nirs4all_cluster.versioning.ClusterVersionError`; a compatible-but-different package
version is logged once (see {doc}`versioning`).

## API

```{eval-rst}
.. autoclass:: nirs4all_cluster.ClusterClient
   :members:
   :special-members: __init__, __enter__, __exit__
```
