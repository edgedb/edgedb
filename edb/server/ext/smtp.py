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

from typing import *

import asyncio
import email

import aiosmtplib

from edb import errors
from edb.common import retryloop
from edb.ir import statypes

from . import util


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
) -> None:
    if db.db_config is None:
        await db.introspection()

    if "smtp" not in db.extensions:
        raise errors.UnsupportedFeatureError(
            "The smtp extension is not enabled."
        )

    semaphore = db.extension_states.get("smtp_semaphore")
    # Initialize or adjust the size of the semaphore for concurrency control.
    # This is a carefully-written loop to deal with concurrent updates.
    while True:
        concurrency = util.get_config(
            db.db_config,  # type: ignore
            "ext::smtp::ServerConfig::concurrency",
            expected_type=int,
        )
        if semaphore is None:
            semaphore = asyncio.Semaphore(concurrency)
            db.extension_states["smtp_semaphore_size"] = concurrency
            db.extension_states["smtp_semaphore"] = semaphore
            break

        size = db.extension_states["smtp_semaphore_size"]
        if concurrency > size:
            for _ in range(concurrency - size):
                semaphore.release()
            db.extension_states["smtp_semaphore_size"] = concurrency
            break

        while concurrency < size and not semaphore.locked():
            # Fast-reducing path, no concurrent update here
            size -= 1
            db.extension_states["smtp_semaphore_size"] = size
            await semaphore.acquire()

        if concurrency == size:
            break

        # At last, this will block. We consume only 1 token every time here,
        # so that we can restart with fresh values in the next iteration.
        db.extension_states["smtp_semaphore_size"] = size - 1
        await semaphore.acquire()

    host = util.maybe_get_config(
        db.db_config,  # type: ignore
        "ext::smtp::ServerConfig::host",
    )
    port = util.maybe_get_config(
        db.db_config,  # type: ignore
        "ext::smtp::ServerConfig::port",
        expected_type=int,
    )
    username = util.maybe_get_config(
        db.db_config,  # type: ignore
        "ext::smtp::ServerConfig::username",
    )
    password = util.maybe_get_config(
        db.db_config,  # type: ignore
        "ext::smtp::ServerConfig::password",
    )
    timeout_per_attempt = util.get_config(
        db.db_config,  # type: ignore
        "ext::smtp::ServerConfig::timeout_per_attempt",
        expected_type=statypes.Duration,
    )
    req_timeout = timeout_per_attempt.to_microseconds() / 1_000_000.0
    timeout_per_email = util.get_config(
        db.db_config,  # type: ignore
        "ext::smtp::ServerConfig::timeout_per_email",
        expected_type=statypes.Duration,
    )
    validate_certs = util.get_config(
        db.db_config,  # type: ignore
        "ext::smtp::ServerConfig::validate_certs",
        expected_type=bool,
    )
    security = util.get_config(
        db.db_config,  # type: ignore
        "ext::smtp::ServerConfig::security",
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

        case "PreferSTARTTLS":
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
            async with semaphore:
                # Currently we are not reusing SMTP connections, but ideally we
                # should replace this with a pool of connections, and drop idle
                # connections after configured time.
                await aiosmtplib.send(
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
