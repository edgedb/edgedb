#
# This source file is part of the EdgeDB open source project.
#
# Copyright EdgeDB Inc. and the EdgeDB authors.
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

from __future__ import annotations
from typing import *

import pathlib

from datetime import datetime, timezone

from jwcrypto import jwk, jwt

from . import uuidgen


class SecretKeyReadError(Exception):
    pass


def generate_secret_key(
    skey: jwk.JWK,
    *,
    instances: Optional[list[str] | AbstractSet[str]] = None,
    roles: Optional[list[str] | AbstractSet[str]] = None,
    databases: Optional[list[str] | AbstractSet[str]] = None,
    subject: Optional[str] = None,
    key_id: Optional[str] = None,
) -> str:
    claims = {
        "iat": int(datetime.now(timezone.utc).timestamp()),
        "iss": "edgedb-server",
    }

    if instances is None:
        claims["edb.i.all"] = True
    else:
        claims["edb.i"] = list(instances)

    if roles is None:
        claims["edb.r.all"] = True
    else:
        claims["edb.r"] = list(roles)

    if databases is None:
        claims["edb.d.all"] = True
    else:
        claims["edb.d"] = list(databases)

    if subject is not None:
        claims["sub"] = subject

    if key_id is None:
        key_id = str(uuidgen.uuid4())

    claims["jti"] = key_id

    token = jwt.JWT(
        header={"alg": "ES256" if skey["kty"] == "EC" else "RS256"},
        claims=claims,
    )
    token.make_signed_token(skey)
    return "edbt1_" + token.serialize()


def load_secret_key(key_file: pathlib.Path) -> jwk.JWK:
    try:
        with open(key_file, 'rb') as kf:
            jws_key = jwk.JWK.from_pem(kf.read())
    except Exception as e:
        raise SecretKeyReadError(f"cannot load JWS key: {e}") from e

    if (
        not jws_key.has_public
        or jws_key['kty'] not in {"RSA", "EC"}
    ):
        raise SecretKeyReadError(
            f"the cluster JWS key file does not "
            f"contain a valid RSA or EC public key")

    return jws_key
