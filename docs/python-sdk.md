# Python SDK

`ClusterClient` is the entire public SDK. It speaks the REST API and nothing more — it
never imports `nirs4all`.

```{code-block} python
from nirs4all_cluster import ClusterClient

with ClusterClient("http://host:8765", token=None) as client:
    job = client.submit_run(
        pipelines=["/shared/pls.yaml", "/shared/rf.yaml"],
        datasets=["/shared/corn", "/shared/wheat"],
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
```

If the server runs an incompatible protocol major the client raises
`nirs4all_cluster.versioning.ClusterVersionError`; a compatible-but-different package
version is logged once (see {doc}`versioning`).

## API

```{eval-rst}
.. autoclass:: nirs4all_cluster.ClusterClient
   :members:
   :special-members: __init__, __enter__, __exit__
```
