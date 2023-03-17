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

from datetime import datetime, timezone

from jwcrypto import jwk, jwt


def generate_secret_key(
    skey: jwk.JWK,
    *,
    instances: Optional[list[str] | AbstractSet[str]] = None,
    roles: Optional[list[str] | AbstractSet[str]] = None,
    databases: Optional[list[str] | AbstractSet[str]] = None,
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

    token = jwt.JWT(
        header={"alg": "ES256" if skey["kty"] == "EC" else "RS256"},
        claims=claims,
    )
    token.make_signed_token(skey)
    return "edbt1_" + token.serialize()
