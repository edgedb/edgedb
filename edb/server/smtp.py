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

from __future__ import annotations

import dataclasses
import email.message
import asyncio
import logging
import os
import hashlib
import pickle
import aiosmtplib

from typing import Optional

from edb.common import retryloop
from edb.ir import statypes
from edb import errors
from . import dbview


_semaphore: asyncio.BoundedSemaphore | None = None

logger = logging.getLogger('edb.server.smtp')


@dataclasses.dataclass
class SMTPProviderConfig:
    name: str
    sender: Optional[str]
    host: Optional[str]
    port: Optional[int]
    username: Optional[str]
    password: Optional[str]
    security: str
    validate_certs: bool
    timeout_per_email: statypes.Duration
    timeout_per_attempt: statypes.Duration


class SMTP:
    def __init__(self, db: dbview.Database):
        current_provider = get_current_email_provider(db)
        self.sender = current_provider.sender or "noreply@example.com"
        default_port = (
            465
            if current_provider.security == "TLS"
            else 587 if current_provider.security == "STARTTLS" else 25
        )
        use_tls: bool
        start_tls: bool | None
        match current_provider.security:
            case "PlainText":
                use_tls = False
                start_tls = False

            case "TLS":
                use_tls = True
                start_tls = False

            case "STARTTLS":
                use_tls = False
                start_tls = True

            case "STARTTLSOrPlainText":
                use_tls = False
                start_tls = None

            case _:
                raise NotImplementedError

        host = current_provider.host or "localhost"
        port = current_provider.port or default_port
        username = current_provider.username
        password = current_provider.password
        validate_certs = current_provider.validate_certs
        timeout_per_attempt = current_provider.timeout_per_attempt

        req_timeout = timeout_per_attempt.to_microseconds() / 1_000_000.0
        self.timeout_per_email = (
            current_provider.timeout_per_email.to_microseconds() / 1_000_000.0
        )
        self.client = aiosmtplib.SMTP(
            hostname=host,
            port=port,
            username=username,
            password=password,
            timeout=req_timeout,
            use_tls=use_tls,
            start_tls=start_tls,
            validate_certs=validate_certs,
        )

    async def send(
        self,
        message: email.message.Message,
        *,
        test_mode: bool = False,
    ) -> None:
        global _semaphore
        if _semaphore is None:
            _semaphore = asyncio.BoundedSemaphore(
                int(
                    os.environ.get(
                        "EDGEDB_SERVER_AUTH_SMTP_CONCURRENCY",
                        os.environ.get("EDGEDB_SERVER_SMTP_CONCURRENCY", 5),
                    )
                )
            )

        # n.b. When constructing EmailMessage objects, we don't set the "From"
        # header since that is configured in the SmtpProviderConfig. However,
        # the EmailMessage will have the correct "To" header.
        message["From"] = self.sender
        rloop = retryloop.RetryLoop(
            timeout=self.timeout_per_email,
            backoff=retryloop.exp_backoff(),
            ignore=(
                aiosmtplib.SMTPConnectError,
                aiosmtplib.SMTPHeloError,
                aiosmtplib.SMTPServerDisconnected,
                aiosmtplib.SMTPConnectTimeoutError,
                aiosmtplib.SMTPConnectResponseError,
            ),
        )
        async for iteration in rloop:
            async with iteration:
                async with _semaphore:
                    # Currently we are not reusing SMTP connections, but
                    # ideally we should replace this with a pool of
                    # connections, and drop idle connections after configured
                    # time.
                    if test_mode:
                        self._send_test_mode_email(message)
                    else:
                        logger.info(
                            "Sending SMTP message to "
                            f"{self.client.hostname}:{self.client.port}"
                        )

                        async with self.client:
                            errors, response = await self.client.send_message(
                                message
                            )
                        if errors:
                            logger.error(
                                f"SMTP server returned errors: {errors}"
                            )
                        else:
                            logger.info(
                                f"SMTP message sent successfully: {response}"
                            )

    def _send_test_mode_email(self, message: email.message.Message):
        sender = message["From"]
        recipients = message["To"]
        recipients_list: list[str]
        if isinstance(recipients, str):
            recipients_list = [recipients]
        elif recipients is None:
            recipients_list = []
        else:
            recipients_list = list(recipients)

        hash_input = f"{sender}{','.join(recipients_list)}"
        file_name_hash = hashlib.sha256(hash_input.encode()).hexdigest()
        file_name = f"/tmp/edb-test-email-{file_name_hash}.pickle"
        test_file = os.environ.get(
            "EDGEDB_TEST_EMAIL_FILE",
            file_name,
        )
        if os.path.exists(test_file):
            os.unlink(test_file)
        with open(test_file, "wb") as f:
            logger.info(f"Dumping SMTP message to {test_file}")
            args = dict(
                message=message,
                sender=sender,
                recipients=recipients,
                hostname=self.client.hostname,
                port=self.client.port,
                username=self.client._login_username,
                password=self.client._login_password,
                timeout=self.client.timeout,
                use_tls=self.client.use_tls,
                start_tls=self.client._start_tls_on_connect,
                validate_certs=self.client.validate_certs,
            )
            pickle.dump(args, f)


def get_current_email_provider(
    db: dbview.Database,
) -> SMTPProviderConfig:
    current_provider_name = db.lookup_config("current_email_provider_name")
    if current_provider_name is None:
        raise errors.ConfigurationError("No email provider configured")

    found = None
    objs = (
        list(db.lookup_config("email_providers"))
        + db.tenant._sidechannel_email_configs
    )
    for obj in objs:
        if obj.name == current_provider_name:
            values = {}
            for field in dataclasses.fields(SMTPProviderConfig):
                key = field.name
                values[key] = getattr(obj, key)
            found = SMTPProviderConfig(**values)
            break

    if found is None:
        raise errors.ConfigurationError(
            f"No email provider named {current_provider_name!r}"
        )
    return found
