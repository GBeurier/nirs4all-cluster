# Quickstart

This walkthrough assumes a **trusted LAN**. For a real deployment read
{doc}`security-and-scope` first.

## 1. Start the coordinator

```{code-block} bash
n4cluster server --host 0.0.0.0 --port 8765 --state ./cluster-state
# optionally:  --allow-python-jobs  --log-file server.log
```

The server prints its URL and the dashboard address (`http://HOST:8765/ui`).

## 2. Start one or more workers

Run on machines that can see `nirs4all` and the datasets. The worker auto-detects GPUs
(`nvidia-smi`) and advertises a `cuda` label.

```{code-block} bash
n4cluster worker --server http://HOST:8765 --labels site=lab --slots 1
# force GPU count with --gpus N (0 hides GPUs); add --log-file as needed
```

## 3. Submit a job and wait

```{code-block} bash
n4cluster submit examples/job.shared-path.yaml --wait --out ./results
n4cluster status   <job_id>
n4cluster jobs      --status running
n4cluster logs     <job_id>
n4cluster cancel   <job_id>
n4cluster artifacts <job_id> --out ./results
```

## Python SDK

```{code-block} python
from nirs4all_cluster import ClusterClient

with ClusterClient("http://host:8765", token=None) as client:
    job = client.submit_run(
        pipeline="/shared/pipelines/pls.yaml",   # kind=path
        dataset="/shared/datasets/corn",         # kind=shared_path
        params={"random_state": 42},
    )
    job = client.wait(job.id)
    print(job.aggregate.best_metric, job.aggregate.ranking)
    client.download_best_model(job.id, "best_model.n4a")
```

A job that provides lists (`pipelines` / `datasets`) decomposes into one task per
combination — see {doc}`concepts/job-decomposition`.
