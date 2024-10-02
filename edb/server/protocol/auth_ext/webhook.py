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
import typing
import abc
import datetime
import json
import hmac
import hashlib

from edb.server.protocol import execute

if typing.TYPE_CHECKING:
    from edb.server import tenant as edbtenant


@dataclasses.dataclass
class Event(abc.ABC):
    event_type: str
    event_id: str
    timestamp: datetime.datetime

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"event_id={self.event_id!r}, "
            f"timestamp={self.timestamp!r})"
        )


@dataclasses.dataclass
class HasIdentity(abc.ABC):
    identity_id: str


@dataclasses.dataclass
class HasEmailFactor(abc.ABC):
    email_factor_id: str


@dataclasses.dataclass
class IdentityCreated(Event, HasIdentity):
    event_type: typing.Literal["IdentityCreated"] = dataclasses.field(
        default="IdentityCreated",
        init=False,
    )


@dataclasses.dataclass
class IdentityAuthenticated(Event, HasIdentity):
    event_type: typing.Literal["IdentityAuthenticated"] = dataclasses.field(
        default="IdentityAuthenticated",
        init=False,
    )


@dataclasses.dataclass
class EmailFactorCreated(Event, HasIdentity, HasEmailFactor):
    event_type: typing.Literal["EmailFactorCreated"] = dataclasses.field(
        default="EmailFactorCreated",
        init=False,
    )


@dataclasses.dataclass
class EmailVerificationRequested(Event, HasIdentity, HasEmailFactor):
    event_type: typing.Literal["EmailVerificationRequested"] = (
        dataclasses.field(
            default="EmailVerificationRequested",
            init=False,
        )
    )
    verification_token: str


@dataclasses.dataclass
class EmailVerified(Event, HasIdentity, HasEmailFactor):
    event_type: typing.Literal["EmailVerified"] = dataclasses.field(
        default="EmailVerified",
        init=False,
    )


@dataclasses.dataclass
class PasswordResetRequested(Event, HasIdentity, HasEmailFactor):
    event_type: typing.Literal["PasswordResetRequested"] = dataclasses.field(
        default="PasswordResetRequested",
        init=False,
    )
    reset_token: str


@dataclasses.dataclass
class MagicLinkRequested(Event, HasIdentity, HasEmailFactor):
    event_type: typing.Literal["MagicLinkRequested"] = dataclasses.field(
        default="MagicLinkRequested",
        init=False,
    )
    magic_link_token: str


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj: typing.Any) -> typing.Any:
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super().default(obj)


async def send(
    db: edbtenant.dbview.Database,
    url: str,
    secret: typing.Optional[str],
    event: Event,
) -> str:
    payload = json.dumps(
        dataclasses.asdict(event), cls=DateTimeEncoder
    ).encode()
    headers = [("Content-Type", "application/json")]
    if secret:
        signature = hmac.new(
            secret.encode(), payload, hashlib.sha256
        ).hexdigest()
        headers.append(("x-ext-auth-signature-sha256", signature))

    result_json = await execute.parse_execute_json(
        db,
        """
with
    nh as module std::net::http,
    net as module std::net,

    url := <str>$url,
    payload := <bytes>$payload,
    headers := <array<tuple<str, str>>>$headers,
insert nh::ScheduledRequest {
    created_at := datetime_of_statement(),
    state := net::RequestState.Pending,

    url := url,
    method := nh::Method.POST,
    headers := headers,
    body := payload,
}""",
        variables={
            "url": url,
            "payload": payload,
            "headers": headers,
        },
    )
    result = json.loads(result_json)

    match result[0]["id"]:
        case str(id):
            return id
        case _:
            raise ValueError(
                "Expected single result with 'id' string property, got "
                f"{result[0]!r}"
            )
