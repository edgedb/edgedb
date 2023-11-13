#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Authentication code that is shared between HTTP and binary protocols"""

from edgedb import scram

import base64
import hashlib
import json
import logging

from jwcrypto import jwt

from edb import errors
from edb.common import debug

from edb.server.protocol cimport args_ser


cdef object logger = logging.getLogger('edb.server')


cdef extract_token_from_auth_data(auth_data: bytes):
    header_value = auth_data.decode("ascii")
    scheme, _, payload = header_value.partition(" ")
    return scheme.lower(), payload.strip()


cdef auth_jwt(tenant, prefixed_token: str, user: str, dbname: str):
    if not prefixed_token:
        raise errors.AuthenticationError(
            'authentication failed: no authorization data provided')

    token_version = 0
    for prefix in ["nbwt1_", "nbwt_", "edbt1_", "edbt_"]:
        encoded_token = prefixed_token.removeprefix(prefix)
        if encoded_token != prefixed_token:
            if prefix == "nbwt1_" or prefix == "edbt1_":
                token_version = 1
            break
    else:
        raise errors.AuthenticationError(
            'authentication failed: malformed JWT')

    role = tenant.get_roles().get(user)
    if role is None:
        raise errors.AuthenticationError('authentication failed')

    skey = tenant.server.get_jws_key()

    try:
        token = jwt.JWT(
            key=skey,
            algs=["RS256", "ES256"],
            jwt=encoded_token,
        )
    except jwt.JWException as e:
        logger.debug('authentication failure', exc_info=True)
        raise errors.AuthenticationError(
            f'authentication failed: {e.args[0]}'
        ) from None
    except Exception as e:
        logger.debug('authentication failure', exc_info=True)
        raise errors.AuthenticationError(
            f'authentication failed: cannot decode JWT'
        ) from None

    try:
        claims = json.loads(token.claims)
    except Exception as e:
        raise errors.AuthenticationError(
            f'authentication failed: malformed claims section in JWT'
        ) from None

    _check_jwt_authz(
        tenant, claims, token_version, user, dbname)


cdef _check_jwt_authz(tenant, claims, token_version, user: str, dbname: str):
    # Check general key validity (e.g. whether it's a revoked key)
    tenant.check_jwt(claims)

    token_instances = None
    token_roles = None
    token_databases = None

    if token_version == 1:
        token_roles = _get_jwt_edb_scope(claims, "edb.r")
        token_instances = _get_jwt_edb_scope(claims, "edb.i")
        token_databases = _get_jwt_edb_scope(claims, "edb.d")
    else:
        namespace = "edgedb.server"
        if not claims.get(f"{namespace}.any_role"):
            token_roles = claims.get(f"{namespace}.roles")
            if not isinstance(token_roles, list):
                raise errors.AuthenticationError(
                    f'authentication failed: malformed claims section in'
                    f' JWT: expected a list in "{namespace}.roles"'
                )

    if (
        token_instances is not None
        and tenant.get_instance_name() not in token_instances
    ):
        raise errors.AuthenticationError(
            'authentication failed: secret key does not authorize '
            f'access to this instance')

    if (
        token_databases is not None
        and dbname not in token_databases
    ):
        raise errors.AuthenticationError(
            'authentication failed: secret key does not authorize '
            f'access to database "{dbname}"')

    if token_roles is not None and user not in token_roles:
        raise errors.AuthenticationError(
            'authentication failed: secret key does not authorize '
            f'access in role "{user}"')


cdef _get_jwt_edb_scope(claims, claim):
    if not claims.get(f"{claim}.all"):
        scope = claims.get(claim, [])
        if not isinstance(scope, list):
            raise errors.AuthenticationError(
                f'authentication failed: malformed claims section in'
                f' JWT: expected a list in "{claim}"'
            )
        return frozenset(scope)
    else:
        return None


cdef scram_get_verifier(tenant, user: str):
    roles = tenant.get_roles()

    rolerec = roles.get(user)
    if rolerec is not None:
        verifier_string = rolerec['password']
        if verifier_string is not None:
            try:
                verifier = scram.parse_verifier(verifier_string)
            except ValueError:
                raise errors.AuthenticationError(
                    f'invalid SCRAM verifier for user {user!r}') from None
            is_mock = False
            return verifier, is_mock

    # To avoid revealing the validity of the submitted user name,
    # generate a mock verifier using a salt derived from the
    # received user name and the cluster mock auth nonce.
    # The same approach is taken by Postgres.
    nonce = tenant.get_instance_data('mock_auth_nonce')
    salt = hashlib.sha256(nonce.encode() + user.encode()).digest()

    verifier = scram.SCRAMVerifier(
        mechanism='SCRAM-SHA-256',
        iterations=scram.DEFAULT_ITERATIONS,
        salt=salt[:scram.DEFAULT_SALT_LENGTH],
        stored_key=b'',
        server_key=b'',
    )
    is_mock = True
    return verifier, is_mock


def scram_verify_password(password: str, verifier: object) -> bool:
    """Check the given password against a verifier.

    Returns True if the password is OK, False otherwise.
    """

    # adapted from edgedb-python's scram.verify_password but made to
    # take a verifier object instead of a string

    bpassword = scram.saslprep(password).encode('utf-8')
    salted_password = scram.get_salted_password(
        bpassword, verifier.salt, verifier.iterations)
    computed_key = scram.get_server_key(salted_password)
    return verifier.server_key == computed_key


cdef parse_basic_auth(auth_payload: str):
    try:
        decoded = base64.b64decode(auth_payload).decode('utf-8')
    except ValueError:
        raise errors.AuthenticationError(
            'authentication failed: malformed authentication') from None
    username, colon, password = decoded.partition(':')
    if colon != ':':
        raise errors.AuthenticationError(
            'authentication failed: malformed authentication')
    return username, password


cdef extract_http_user(scheme, auth_payload, params):
    """Extract the username from an HTTP request.

    Raises an AuthenticationError if something is too malformed.

    Returns the username, along with the password, if appropriate.
    (To avoid needing to parse the packet twice.)
    """

    if scheme == 'basic':
        return parse_basic_auth(auth_payload)
    else:
        # Respect X-EdgeDB-User if present, but otherwise default to 'edgedb'
        if params and b'user' in params:
            username = params[b'user'].decode('ascii')
        else:
            username = 'edgedb'
        return username, None


cdef auth_basic(tenant, username: str, password: str):
    verifier, mock_auth = scram_get_verifier(tenant, username)
    if not scram_verify_password(password, verifier) or mock_auth:
        raise errors.AuthenticationError('authentication failed')
