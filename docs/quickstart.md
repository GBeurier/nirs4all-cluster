# Local quickstart

Use provisioned Python environments; no dependency installation happens on workers.
Initialize and provision a fresh state directory as described in cli-reference.md.
Existing unmarked state is refused untouched.

```bash
n4cluster init-state --state ./cluster-state
umask 077
n4cluster token-create --state ./cluster-state --project lab --principal consumer --role submitter > consumer.token
n4cluster token-create --state ./cluster-state --project lab --principal provider --role executor > provider.token
n4cluster server --state ./cluster-state
n4cluster worker --server http://127.0.0.1:8765 --token-file provider.token --state ./provider-state --allow-path-root /shared
n4cluster run --pipeline /shared/pls.yaml --dataset /shared/corn --token-file consumer.token --wait --out ./results
```

Provide separate shells for server/worker/consumer. Credentials select the project
and permissions. Choose HTTPS for non-loopback clients. The server never consumes
a secret from NIRS4ALL_PROJECT_TOKEN or N4CLUSTER_TOKEN.

Requested export must produce a genuine owner archive or the job fails explicitly.
--no-export-best-model permits an explicit no-export success. Canonical Core T3/T4
export is pending; the current supported runner profile is nirs4all-python-n4a-1.0.
The unchanged /ui live feed needs a future joint frontend migration.
