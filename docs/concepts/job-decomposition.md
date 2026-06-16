# Jobs, tasks and decomposition

A **job** is what a client submits. A **task** is one atomic `nirs4all.run()` with its own
isolated workspace. The server decomposes a job into tasks and aggregates their results.

## Level 0 — atomic job

One `pipeline` + one `dataset` → exactly one task. Its metrics are the job's metrics. The
prototype proved this path is **metric-identical** to a local `nirs4all.run()`.

## Level 1 — `pipelines × datasets`

Provide a list for either side (or both) and the job decomposes into the **cartesian
product**, one task per `(pipeline, dataset)` pair:

```{mermaid}
flowchart TD
    J["job: pipelines=[P1,P2] × datasets=[A,B,C]"] --> T1[P1×A] & T2[P1×B] & T3[P1×C]
    J --> T4[P2×A] & T5[P2×B] & T6[P2×C]
    T1 & T2 & T3 & T4 & T5 & T6 --> AGG["aggregate: rank by rank_metric → best model"]
```

Each task is leased independently, so the work parallelises across all available worker
slots. When tasks finish, the server builds a **ranking** (sorted by `rank_metric` in
`rank_mode` direction) and links the single best model artifact. A later task that beats an
earlier one replaces the job-level `best_model` link atomically.

```{note}
Explicit-variant parity (Level 2) and fold distribution (Level 3) are **non-goals** of this
beta — see {doc}`../security-and-scope` and `design/prototype-to-production`.
```
