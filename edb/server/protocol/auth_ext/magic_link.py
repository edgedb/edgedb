#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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


import logging
import aiosmtplib
import json

from typing import Any, cast

from edb import errors as edb_errors
from edb.common import debug
from edb.server.protocol import execute

from . import config, data, errors, util, local, email as auth_emails


logger = logging.getLogger('edb.server')


class Client(local.Client):
    def __init__(self, db: Any, tenant: Any, test_mode: bool, issuer: str):
        super().__init__(db)
        self.tenant = tenant
        self.test_mode = test_mode
        self.issuer = issuer
        self.provider = self._get_provider()

    def _get_provider(self) -> config.MagicLinkProviderConfig:
        provider_name = "builtin::local_magic_link"
        provider_client_config = cast(
            list[config.ProviderConfig],
            util.get_config(
                self.db, "ext::auth::AuthConfig::providers", frozenset
            ),
        )
        for cfg in provider_client_config:
            if cfg.name == provider_name:
                cfg = cast(config.MagicLinkProviderConfig, cfg)
                return config.MagicLinkProviderConfig(
                    name=cfg.name,
                    token_time_to_live=cfg.token_time_to_live,
                )

        raise errors.MissingConfiguration(
            provider_name, f"Provider is not configured"
        )

    async def register(self, email: str) -> data.EmailFactor:
        try:
            result = await execute.parse_execute_json(
                self.db,
                """
with
    email := <str>$email,
    identity := (insert ext::auth::LocalIdentity {
        issuer := "local",
        subject := "",
    }),
    email_factor := (insert ext::auth::MagicLinkFactor {
        email := email,
        identity := identity,
    })
select email_factor { ** };""",
                variables={
                    "email": email,
                },
                cached_globally=True,
                query_tag='gel/auth',
            )

        except Exception as e:
            exc = await execute.interpret_error(e, self.db)
            if isinstance(exc, edb_errors.ConstraintViolationError):
                raise errors.UserAlreadyRegistered()
            else:
                raise exc

        result_json = json.loads(result.decode())
        assert len(result_json) == 1
        factor_dict = result_json[0]
        return data.EmailFactor(**factor_dict)

    def make_magic_link_token(
        self,
        *,
        identity_id: str,
        callback_url: str,
        challenge: str,
    ) -> str:
        initial_key_material = self._get_signing_key()
        signing_key = util.derive_key(initial_key_material, "magic_link")
        return util.make_token(
            signing_key=signing_key,
            issuer=self.issuer,
            subject=identity_id,
            additional_claims={
                "challenge": challenge,
                "callback_url": callback_url,
            },
            include_issued_at=True,
            expires_in=self.provider.token_time_to_live.to_timedelta(),
        )

    async def send_magic_link(
        self,
        *,
        email: str,
        link_url: str,
        token: str,
        redirect_on_failure: str,
    ) -> None:
        link = util.join_url_params(
            link_url,
            {
                "token": token,
                "redirect_on_failure": redirect_on_failure,
            },
        )
        try:
            await auth_emails.send_magic_link_email(
                db=self.db,
                tenant=self.tenant,
                to_addr=email,
                link=link,
                test_mode=self.test_mode,
            )
        except aiosmtplib.SMTPException as ex:
            if not debug.flags.server:
                logger.warning(
                    "Failed to send magic link via SMTP", exc_info=True
                )
            raise edb_errors.InternalServerError(
                "Failed to send magic link email, please try again later."
            ) from ex
