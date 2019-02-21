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

"""Helpers for SCRAM authentication."""

import base64
import hashlib
import hmac
import os
import typing

from edb.common.vendor.saslprep import saslprep


RAW_NONCE_LENGTH = 18

# Per recommendations in RFC 7677.
DEFAULT_SALT_LENGTH = 16
DEFAULT_ITERATIONS = 4096


def generate_salt(length: int = DEFAULT_SALT_LENGTH) -> bytes:
    return os.urandom(length)


def generate_nonce(length: int = RAW_NONCE_LENGTH) -> bytes:
    return os.urandom(length)


def build_verifier(password: str, *, salt: typing.Optional[bytes] = None,
                   iterations: int = DEFAULT_ITERATIONS) -> str:
    """Build the SCRAM verifier for the given password.

    Returns a string in the following format:

        "<MECHANISM>$<iterations>:<salt>$<StoredKey>:<ServerKey>"

    The salt and keys are base64-encoded values.
    """
    password = saslprep(password).encode('utf-8')

    if salt is None:
        salt = generate_salt()

    salted_password = get_salted_password(password, salt, iterations)
    client_key = get_client_key(salted_password)
    stored_key = H(client_key)
    server_key = get_server_key(salted_password)

    return (f'SCRAM-SHA-256${iterations}:{B64(salt)}$'
            f'{B64(stored_key)}:{B64(server_key)}')


class SCRAMVerifier(typing.NamedTuple):

    mechanism: str
    iterations: int
    salt: bytes
    stored_key: bytes
    server_key: bytes


def parse_verifier(verifier: str) -> SCRAMVerifier:

    parts = verifier.split('$')
    if len(parts) != 3:
        raise ValueError('invalid SCRAM verifier')

    mechanism = parts[0]
    if mechanism != 'SCRAM-SHA-256':
        raise ValueError('invalid SCRAM verifier')

    iterations, _, salt = parts[1].partition(':')
    stored_key, _, server_key = parts[2].partition(':')
    if not salt or not server_key:
        raise ValueError('invalid SCRAM verifier')

    try:
        iterations = int(iterations)
    except ValueError:
        raise ValueError('invalid SCRAM verifier') from None

    return SCRAMVerifier(
        mechanism=mechanism,
        iterations=iterations,
        salt=base64.b64decode(salt),
        stored_key=base64.b64decode(stored_key),
        server_key=base64.b64decode(server_key),
    )


def verify_password(password: bytes, verifier: str) -> bool:
    """Check the given password against a verifier.

    Returns True if the password is OK, False otherwise.
    """

    password = saslprep(password).encode('utf-8')
    v = parse_verifier(verifier)
    salted_password = get_salted_password(password, v.salt, v.iterations)
    computed_key = get_server_key(salted_password)
    return v.server_key == computed_key


def B64(val: bytes) -> str:
    """Return base64-encoded string representation of input binary data."""
    return base64.b64encode(val).decode()


def HMAC(key: bytes, msg: bytes) -> bytes:
    return hmac.new(key, msg, digestmod=hashlib.sha256).digest()


def XOR(a: bytes, b: bytes) -> bytes:
    xint = int.from_bytes(a, 'big') ^ int.from_bytes(b, 'big')
    return xint.to_bytes(len(a), 'big')


def H(s: bytes) -> bytes:
    return hashlib.sha256(s).digest()


def get_salted_password(password: bytes, salt: bytes,
                        iterations: int) -> bytes:
    # U1 := HMAC(str, salt + INT(1))
    H_i = U_i = HMAC(password, salt + b'\x00\x00\x00\x01')

    for _ in range(iterations - 1):
        U_i = HMAC(password, U_i)
        H_i = XOR(H_i, U_i)

    return H_i


def get_client_key(salted_password: bytes) -> bytes:
    return HMAC(salted_password, b'Client Key')


def get_server_key(salted_password: bytes) -> bytes:
    return HMAC(salted_password, b'Server Key')
