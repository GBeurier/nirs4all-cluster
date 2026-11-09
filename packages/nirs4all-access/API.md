# Closed API 0.1.0

All names are exported from `nirs4all_access`:

```python
ProjectPrincipal(project_id: str, principal_id: str, services: frozenset[str], rights: frozenset[str], group_ids: tuple[str, ...] = ())
CredentialRecord(key_id: str, token_hash: str, principal: ProjectPrincipal, expires_at: float | None = None, revoked: bool = False)
AccessError(status, detail, *, code, missing_rights=frozenset())
CredentialStoreError(ValueError)
ProjectTokenVerifier(records: Iterable[CredentialRecord])
ProjectTokenVerifier.verify(token: str | None, *, service: str, required_rights: Iterable[str] = (), now: float | None = None) -> ProjectPrincipal
parse_bearer(authorization: str | None) -> str | None
generate_token() -> str
hash_token(token: str) -> str
issue_credential(principal, *, expires_at=None) -> tuple[str, CredentialRecord]
parse_credential_store(data: bytes) -> tuple[CredentialRecord, ...]
dump_credential_store(records) -> bytes
load_credential_store(path) -> tuple[CredentialRecord, ...]
write_credential_store(path, records) -> None
require_secure_transport(host, *, tls: bool, trusted_proxy: bool = False) -> None
```

See CONTRACT.md for validation, record schema and HTTP status semantics. Hashes
are repr-redacted; no error message includes the presented token or digest.
