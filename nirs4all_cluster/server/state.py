"""Explicit bootstrap and pre-SQLite legacy-state refusal."""

import json
import os
from pathlib import Path

from nirs4all_access import CredentialRecord, load_credential_store, write_credential_store

MARKER = "cluster-state-v2.json"
FORMAT = {"format": "nirs4all-cluster/project-services/v2"}
GUIDANCE = "Refusing unmarked or incompatible state without changes. Use a NEW empty directory with n4cluster init-state; preserve/export old state separately."


def validate_state(state: str | Path) -> Path:
    path = Path(state)
    if not path.is_dir() or not (path / MARKER).is_file():
        raise ValueError(GUIDANCE)
    try:
        valid = json.loads((path / MARKER).read_text()) == FORMAT
    except (ValueError, OSError):
        valid = False
    if not valid:
        raise ValueError(GUIDANCE)
    if path.is_symlink() or (path / "projects").is_symlink() or (path / "auth").is_symlink():
        raise ValueError("state directories must not be symlinks")
    return path


def bootstrap_state(state: str | Path, records: tuple[CredentialRecord, ...]) -> None:
    path = Path(state)
    if path.exists() and (not path.is_dir() or any(path.iterdir())):
        raise ValueError(GUIDANCE)
    # Validate before creating anything.
    from nirs4all_access import dump_credential_store

    dump_credential_store(records)
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    (path / "auth").mkdir(mode=0o700)
    (path / "projects").mkdir(mode=0o700)
    write_credential_store(path / "auth" / "credentials.json", records)
    fd = os.open(path / MARKER, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as fh:
        json.dump(FORMAT, fh)


def state_credentials(state: str | Path):
    path = validate_state(state)
    return load_credential_store(path / "auth" / "credentials.json")
