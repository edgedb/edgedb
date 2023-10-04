#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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

import argon2
import json

from typing import Any
from edb.errors import ConstraintViolationError
from edb.server.protocol import execute

from . import errors, util, data

ph = argon2.PasswordHasher()


class Client:
    def __init__(self, db: Any, provider_id: str):
        self.db = db
        self.db_config = db.db_config
        provider_type = self._get_provider_config(provider_id)
        match provider_type:
            case "password":
                self.provider = PasswordProvider()
            case _:
                raise errors.InvalidData(f"Invalid provider: {provider_type}")

    async def register(self, *args, **kwargs):
        return await self.provider.register(self.db, *args, **kwargs)

    async def authenticate(self, *args, **kwargs):
        return await self.provider.authenticate(self.db, *args, **kwargs)

    async def logout(self, *args, **kwargs):
        return await self.provider.logout(*args, **kwargs)

    def _get_provider_config(self, provider_id: str) -> str:
        provider_client_config = util.get_config(
            self.db_config, "ext::auth::AuthConfig::providers", frozenset
        )
        provider_name: str | None = None
        for cfg in provider_client_config:
            if cfg.provider_id == provider_id:
                provider_name = cfg.provider_name
        match provider_name:
            case "password":
                return "password"
            case _:
                raise errors.InvalidData(
                    f"Invalid provider configuration: {provider_id}\n"
                    f"providers={provider_client_config!r}"
                )


class PasswordProvider:
    async def register(self, db: Any, input: dict[str, Any]):
        match (input.get("email"), input.get("password")):
            case (str(e), str(p)):
                email = e
                password = p
            case _:
                raise errors.InvalidData(
                    "Missing 'email' or 'password' in data"
                )

        try:
            r = await execute.parse_execute_json(
                db=db,
                query="""\
    with
      email := <optional str>$email,
      password_hash := <str>$password_hash,
      identity := (insert ext::auth::LocalIdentity {
        issuer := "local",
        subject := "",
      }),
      password := (insert ext::auth::EmailPasswordFactor {
        password_hash := password_hash,
        email := email,
        identity := identity,
      }),

    select identity { * };""",
                variables={
                    "email": email,
                    "password_hash": ph.hash(password),
                },
            )
        except Exception as e:
            exc = await execute.interpret_error(e, db)
            if isinstance(exc, ConstraintViolationError):
                raise errors.UserAlreadyRegistered()
            else:
                raise exc

        result_json = json.loads(r.decode())
        assert len(result_json) == 1

        return data.LocalIdentity(**result_json[0])

    async def authenticate(self, db: Any, input: dict[str, Any]):
        if 'email' not in input or 'password' not in input:
            raise errors.InvalidData("Missing 'email' or 'password' in data")

        password = input["password"]
        email = input["email"]
        r = await execute.parse_execute_json(
            db=db,
            query="""\
with
  email := <str>$email,
select ext::auth::EmailPasswordFactor { password_hash, identity: { * } }
filter .email = email;""",
            variables={
                "email": email,
            },
        )

        password_credential_dicts = json.loads(r.decode())
        if len(password_credential_dicts) != 1:
            raise errors.NoIdentityFound()
        password_credential_dict = password_credential_dicts[0]

        password_hash = password_credential_dict["password_hash"]
        try:
            ph.verify(password_hash, password)
        except argon2.exceptions.VerifyMismatchError:
            raise errors.NoIdentityFound()

        local_identity = data.LocalIdentity(
            **password_credential_dict["identity"]
        )

        if ph.check_needs_rehash(password_hash):
            new_hash = ph.hash(password)
            await execute.parse_execute_json(
                db=db,
                query="""\
with
  email := <str>$email,
  new_hash := <str>$new_hash,

update ext::auth::EmailPasswordFactor
filter .email = email
set { password_hash := new_hash };""",
                variables={
                    "email": email,
                    "new_hash": new_hash,
                },
            )

        return local_identity
