import datetime
import pathlib

from typing import TYPE_CHECKING, Iterable, List, Optional, Any

if TYPE_CHECKING:
    class SigningCtx:
        def __init__(self) -> None: ...
        def set_issuer(self, issuer: str) -> None: ...
        def set_audience(self, audience: str) -> None: ...
        def set_expiry(self, expiry: int) -> None: ...
        def set_not_before(self, not_before: int) -> None: ...

    class ValidationCtx:
        def __init__(self) -> None: ...
        def allow(
            self,
            claim: str,
            values: List[str] | Iterable[str],
        ) -> None: ...
        def deny(
            self,
            claim: str,
            values: List[str] | Iterable[str],
        ) -> None: ...
        def require(self, claim: str) -> None: ...
        def reject(self, claim: str) -> None: ...
        def ignore(self, claim: str) -> None: ...
        def require_expiry(self) -> None: ...
        def ignore_expiry(self) -> None: ...

    class JWKSet:
        @staticmethod
        def from_hs256_key(key: bytes) -> "JWKSet": ...
        def __init__(self) -> None: ...
        def generate(self, *, kid: Optional[str], kty: str) -> None: ...
        def add(self, **kwargs: Any) -> None: ...
        def load(self, keys: str) -> int: ...
        def load_json(self, keys: str) -> int: ...
        def export_pem(self, *, private_keys: bool=True) -> bytes: ...
        def export_json(self, *, private_keys: bool=True) -> bytes: ...
        def can_sign(self) -> bool: ...
        def can_validate(self) -> bool: ...
        def has_public_keys(self) -> bool: ...
        def has_private_keys(self) -> bool: ...
        def has_symmetric_keys(self) -> bool: ...
        def sign(
            self, claims: dict[str, Any], *, ctx: Optional[SigningCtx] = None
        ) -> str: ...
        def validate(
            self, token: str, *, ctx: Optional[ValidationCtx] = None
        ) -> dict[str, Any]: ...
        @property
        def default_signing_context(self) -> SigningCtx: ...
        @property
        def default_validation_context(self) -> ValidationCtx: ...

    class JWKSetCache:
        def __init__(self, expiry_seconds: int) -> None: ...
        # Returns a tuple of (is_fresh, registry)
        def get(self, key: str) -> tuple[bool, Optional[JWKSet]]: ...
        def set(self, key: str, registry: JWKSet) -> None: ...

    def generate_gel_token(
        registry: JWKSet,
        *,
        instances: Optional[List[str] | Iterable[str]] = None,
        roles: Optional[List[str] | Iterable[str]] = None,
        databases: Optional[List[str] | Iterable[str]] = None,
        **kwargs: Any,
    ) -> str: ...

    def validate_gel_token(
        registry: JWKSet,
        token: str,
        user: str,
        dbname: str,
        instance_name: str,
    ) -> str | None: ...
else:
    from edb.server._rust_native._jwt import (
        JWKSet, JWKSetCache, generate_gel_token, validate_gel_token, SigningCtx, ValidationCtx  # noqa
    )


def load_secret_key(key_file: pathlib.Path) -> JWKSet:
    try:
        with open(key_file, 'rb') as kf:
            jws_key = JWKSet()
            jws_key.load(kf.read().decode('ascii'))
    except Exception as e:
        raise SecretKeyReadError(f"cannot load JWS key {key_file}: {e}") from e
    if not jws_key.can_validate():
        raise SecretKeyReadError(
            f"the cluster JWS key file {key_file} does not "
            f"contain a valid key for token validation (RSA, EC or "
            f"HMAC-SHA256)")

    # TODO: We should also add a default issuer and add that to the allow-list.

    # Default to one day expiry for tokens -- we will probably tighten this up
    jws_key.default_signing_context.set_expiry(86400)
    # 60 second leeway for not before
    jws_key.default_signing_context.set_not_before(60)

    return jws_key


def generate_jwk(keys_file: pathlib.Path) -> None:
    key = JWKSet()
    # kid is yyyymmdd
    kid = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")
    key.generate(kid=kid, kty='ES256')
    if keys_file.name.endswith(".pem"):
        with keys_file.open("wb") as f:
            f.write(key.export_pem())
    elif keys_file.name.endswith(".json"):
        with keys_file.open("wb") as f:
            f.write(key.export_json())
    else:
        raise ValueError(f"Unsupported key file extension {keys_file.suffix}. "
                         "Use .pem or .json extension when generating a key.")

    keys_file.chmod(0o600)


class SecretKeyReadError(Exception):
    pass
