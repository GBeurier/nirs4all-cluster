"""nirs4all-cluster — a small distributed job queue for ``nirs4all.run()`` (beta).

A small FastAPI server backed by SQLite and a content-addressed object store,
plus polling workers that execute ``nirs4all.run()`` in isolated task
workspaces. See ``PROTOTYPE_DESIGN.md`` for the full design and non-goals.

The package is intentionally thin: it orchestrates jobs and moves artifacts,
but never reimplements ``nirs4all`` logic (the worker imports it lazily).
"""

__version__ = "0.1.0"

from .client import ClusterClient

__all__ = ["ClusterClient", "__version__"]
