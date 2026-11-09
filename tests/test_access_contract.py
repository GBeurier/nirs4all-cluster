"""Additional constructor/import checks over the accepted installed shared leaf."""

import subprocess
import sys
from pathlib import Path

import pytest
from nirs4all_access import CredentialRecord, CredentialStoreError, ProjectPrincipal


@pytest.mark.parametrize(
    "patch",
    [
        {"group_ids": ("future-group",)},
        {"services": frozenset()},
        {"rights": frozenset({"cluster.*"})},
        {"project_id": "nul"},
    ],
)
def test_constructor_validation_cannot_be_bypassed(patch):
    values = {
        "project_id": "project-a",
        "principal_id": "consumer",
        "services": frozenset({"cluster"}),
        "rights": frozenset({"cluster.read"}),
    }
    values.update(patch)
    with pytest.raises(CredentialStoreError):
        ProjectPrincipal(**values)


@pytest.mark.parametrize("expiry", [True, float("nan"), float("inf"), 10**500])
def test_expiry_constructor_validation(expiry):
    principal = ProjectPrincipal("project-a", "consumer", frozenset({"cluster"}), frozenset({"cluster.read"}))
    with pytest.raises(CredentialStoreError):
        CredentialRecord("0" * 16, "sha256:" + "0" * 64, principal, expires_at=expiry)


def test_leaf_has_no_nonstdlib_imports():
    import nirs4all_access

    package = Path(nirs4all_access.__file__).parents[1]
    script = """import sys
sys.path.insert(0,sys.argv[1])
before=set(sys.modules)
import nirs4all_access
assert not any(name.split('.')[0] not in sys.stdlib_module_names | {'nirs4all_access','sitecustomize','__main__'} for name in set(sys.modules)-before)
"""
    result = subprocess.run(
        [sys.executable, "-I", "-B", "-c", script, str(package)], capture_output=True, text=True, timeout=5
    )
    assert result.returncode == 0, result.stderr
