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

from typing import Any, Optional, Union, Sequence

import asyncio
import email
import os
import pickle

import aiosmtplib

from edb.common import retryloop
from edb.ir import statypes

from . import util


_semaphore: asyncio.BoundedSemaphore | None = None


async def send_email(
    db: Any,
    message: Union[
        email.message.EmailMessage,
        email.message.Message,
        str,
        bytes,
    ],
    sender: Optional[str] = None,
    recipients: Optional[Union[str, Sequence[str]]] = None,
    test_mode: bool = False,
) -> None:
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.BoundedSemaphore(
            int(os.environ.get("EDGEDB_SERVER_AUTH_SMTP_CONCURRENCY", 5))
        )

    host = util.maybe_get_config(
        db,
        "ext::auth::SMTPConfig::host",
    ) or "localhost"
    port = util.maybe_get_config(
        db,
        "ext::auth::SMTPConfig::port",
        expected_type=int,
    )
    username = util.maybe_get_config(
        db,
        "ext::auth::SMTPConfig::username",
    )
    password = util.maybe_get_config(
        db,
        "ext::auth::SMTPConfig::password",
    )
    timeout_per_attempt = util.get_config(
        db,
        "ext::auth::SMTPConfig::timeout_per_attempt",
        expected_type=statypes.Duration,
    )
    req_timeout = timeout_per_attempt.to_microseconds() / 1_000_000.0
    timeout_per_email = util.get_config(
        db,
        "ext::auth::SMTPConfig::timeout_per_email",
        expected_type=statypes.Duration,
    )
    validate_certs = util.get_config(
        db,
        "ext::auth::SMTPConfig::validate_certs",
        expected_type=bool,
    )
    security = util.get_config(
        db,
        "ext::auth::SMTPConfig::security",
    )
    start_tls: bool | None
    match security:
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

    rloop = retryloop.RetryLoop(
        timeout=timeout_per_email.to_microseconds() / 1_000_000.0,
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
                # Currently we are not reusing SMTP connections, but ideally we
                # should replace this with a pool of connections, and drop idle
                # connections after configured time.
                args = dict(
                    message=message,
                    sender=sender,
                    recipients=recipients,
                    hostname=host,
                    port=port,
                    username=username,
                    password=password,
                    timeout=req_timeout,
                    use_tls=use_tls,
                    start_tls=start_tls,
                    validate_certs=validate_certs,
                )
                if test_mode:
                    test_file = os.environ.get(
                        "EDGEDB_TEST_EMAIL_FILE", "/tmp/edb-test-email.pickle"
                    )
                    if os.path.exists(test_file):
                        os.unlink(test_file)
                    with open(test_file, "wb") as f:
                        pickle.dump(args, f)
                else:
                    await aiosmtplib.send(**args)  # type: ignore
