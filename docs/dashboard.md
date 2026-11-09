# Web dashboard

The server ships a small **ops dashboard** at `/ui` — a single self-contained page (vanilla
JS, no build step, no CDN) that talks to the same REST + WebSocket API.

```{code-block} text
http://HOST:8765/ui
```

It shows:

- a **stats header** (`GET /v1/stats`) — jobs by status, tasks in flight, live worker count;
- a **jobs table** filterable by status/name, with per-job task counts, best metric and a
  **cancel** button; expand a row for its ranking and recent events;
- a **workers table** with slots, labels and each worker's `nirs4all-cluster` version
  (flagged when it diverges from the server);
- a **live event feed** over the global stream `GET /v1/events/stream`.

If the server has a token, paste it into the dashboard's token field (stored in the
browser's `localStorage`); it is sent as `Authorization: Bearer …` on requests and as
an Authorization Bearer header for API v2. The static dashboard still sends
query WebSocket credentials; its live feed is unavailable until a joint frontend
migration. Query credentials are rejected and redacted before uvicorn logs them.
There is no open/dev server mode.

```{note}
This is a deliberately minimal **operations** view for the queue itself (in the spirit of
Flower / RQ-dashboard) — distinct from `nirs4all-studio`, which is the full data-science
application. For cross-origin browser access to the API, enable CORS with `--cors-origin`.
```
