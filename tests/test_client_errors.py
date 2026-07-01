"""Unit tests for the typed, rights-respecting client error layer.

Pure — no server, no nirs4all. Drives ``raise_for_response`` with stub responses
so the HTTP-status → exception mapping and the 403 rights-parsing are pinned down
independently of the network.
"""

import pytest

from nirs4all_cluster.client_errors import (
    ClusterAuthError,
    ClusterConflictError,
    ClusterError,
    ClusterNotFoundError,
    ClusterPayloadTooLargeError,
    ClusterPermissionError,
    ClusterRequestError,
    ClusterServerError,
    ClusterValidationError,
    ClusterVersionError,
    parse_missing_rights,
    raise_for_response,
)


class _StubRequest:
    def __init__(self, method="GET", url="http://test/v1/jobs"):
        self.method = method
        self.url = url


class _StubResponse:
    """Minimal duck-type of httpx.Response for the error translator."""

    def __init__(self, status_code, json_body=None, text="", *, method="GET", url="http://test/x"):
        self.status_code = status_code
        self._json = json_body
        self.text = text
        self.request = _StubRequest(method, url)

    def read(self):
        return b""

    def json(self):
        if self._json is None:
            raise ValueError("no json")
        return self._json


def _detail(msg):
    return {"detail": msg}


# --------------------------------------------------------------------------- #
# parse_missing_rights
# --------------------------------------------------------------------------- #


def test_parse_missing_rights_single():
    principal, rights = parse_missing_rights("principal 'alice' lacks required right(s): submit")
    assert principal == "alice"
    assert rights == frozenset({"submit"})


def test_parse_missing_rights_multiple():
    principal, rights = parse_missing_rights(
        "principal 'worker1' lacks required right(s): submit, cancel"
    )
    assert principal == "worker1"
    assert rights == frozenset({"submit", "cancel"})


def test_parse_missing_rights_non_matching():
    assert parse_missing_rights("invalid or missing token") == (None, frozenset())
    assert parse_missing_rights(None) == (None, frozenset())
    assert parse_missing_rights("") == (None, frozenset())


# --------------------------------------------------------------------------- #
# raise_for_response — status → type mapping
# --------------------------------------------------------------------------- #


def test_2xx_and_3xx_do_not_raise():
    assert raise_for_response(_StubResponse(200, {"ok": True})) is None
    assert raise_for_response(_StubResponse(204)) is None
    assert raise_for_response(_StubResponse(302)) is None


def test_401_maps_to_auth_error():
    with pytest.raises(ClusterAuthError) as exc:
        raise_for_response(_StubResponse(401, _detail("invalid or missing token")))
    assert exc.value.status == 401
    assert isinstance(exc.value, ClusterError)


def test_403_maps_to_permission_error_with_rights():
    body = _detail("principal 'dash' lacks required right(s): submit")
    with pytest.raises(ClusterPermissionError) as exc:
        raise_for_response(_StubResponse(403, body, method="POST", url="http://test/v1/jobs"))
    err = exc.value
    assert err.status == 403
    assert err.principal == "dash"
    assert err.missing_rights == frozenset({"submit"})
    assert err.method == "POST"
    assert "submit" in str(err)


@pytest.mark.parametrize(
    "status,expected",
    [
        (404, ClusterNotFoundError),
        (409, ClusterConflictError),
        (413, ClusterPayloadTooLargeError),
        (422, ClusterValidationError),
        (400, ClusterRequestError),
        (429, ClusterRequestError),
        (500, ClusterServerError),
        (503, ClusterServerError),
    ],
)
def test_status_maps_to_expected_type(status, expected):
    with pytest.raises(expected) as exc:
        raise_for_response(_StubResponse(status, _detail(f"boom {status}")))
    assert exc.value.status == status
    assert isinstance(exc.value, ClusterError)


def test_426_maps_to_version_error():
    with pytest.raises(ClusterVersionError):
        raise_for_response(_StubResponse(426, _detail("incompatible protocol")))


def test_422_list_detail_is_stringified():
    body = {"detail": [{"loc": ["body", "x"], "msg": "field required"}]}
    with pytest.raises(ClusterValidationError) as exc:
        raise_for_response(_StubResponse(422, body))
    assert "field required" in (exc.value.detail or "")


def test_non_json_body_falls_back_to_text():
    with pytest.raises(ClusterServerError) as exc:
        raise_for_response(_StubResponse(500, json_body=None, text="Internal Server Error"))
    assert "Internal Server Error" in (exc.value.detail or "")


def test_cluster_error_str_includes_status():
    err = ClusterNotFoundError("job not found", status=404)
    assert str(err) == "[404] job not found"
    # A connection error has no status.
    from nirs4all_cluster.client_errors import ClusterConnectionError

    assert str(ClusterConnectionError("connection refused")) == "connection refused"
