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
            f"timestamp={self.timestamp!r}, "
            f"event_id={self.event_id!r}"
            ")"
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

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"timestamp={self.timestamp}, "
            f"event_id={self.event_id}, "
            f"identity_id={self.identity_id}"
            ")"
        )


@dataclasses.dataclass
class IdentityAuthenticated(Event, HasIdentity):
    event_type: typing.Literal["IdentityAuthenticated"] = dataclasses.field(
        default="IdentityAuthenticated",
        init=False,
    )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"timestamp={self.timestamp}, "
            f"event_id={self.event_id}, "
            f"identity_id={self.identity_id}"
            ")"
        )


@dataclasses.dataclass
class EmailFactorCreated(Event, HasIdentity, HasEmailFactor):
    event_type: typing.Literal["EmailFactorCreated"] = dataclasses.field(
        default="EmailFactorCreated",
        init=False,
    )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"timestamp={self.timestamp}, "
            f"event_id={self.event_id}, "
            f"identity_id={self.identity_id}, "
            f"email_factor_id={self.email_factor_id}"
            ")"
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

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"timestamp={self.timestamp}, "
            f"event_id={self.event_id}, "
            f"identity_id={self.identity_id}, "
            f"email_factor_id={self.email_factor_id}"
            ")"
        )


@dataclasses.dataclass
class EmailVerified(Event, HasIdentity, HasEmailFactor):
    event_type: typing.Literal["EmailVerified"] = dataclasses.field(
        default="EmailVerified",
        init=False,
    )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"timestamp={self.timestamp}, "
            f"event_id={self.event_id}, "
            f"identity_id={self.identity_id}, "
            f"email_factor_id={self.email_factor_id}"
            ")"
        )


@dataclasses.dataclass
class PasswordResetRequested(Event, HasIdentity, HasEmailFactor):
    event_type: typing.Literal["PasswordResetRequested"] = dataclasses.field(
        default="PasswordResetRequested",
        init=False,
    )
    reset_token: str

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"timestamp={self.timestamp}, "
            f"event_id={self.event_id}, "
            f"identity_id={self.identity_id}, "
            f"email_factor_id={self.email_factor_id}"
            ")"
        )


@dataclasses.dataclass
class MagicLinkRequested(Event, HasIdentity, HasEmailFactor):
    event_type: typing.Literal["MagicLinkRequested"] = dataclasses.field(
        default="MagicLinkRequested",
        init=False,
    )
    magic_link_token: str
    magic_link_url: str

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"timestamp={self.timestamp}, "
            f"event_id={self.event_id}, "
            f"identity_id={self.identity_id}, "
            f"email_factor_id={self.email_factor_id}"
            ")"
        )


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
    body = json.dumps(
        dataclasses.asdict(event), cls=DateTimeEncoder
    ).encode()
    headers = [("Content-Type", "application/json")]
    if secret:
        signature = hmac.new(
            secret.encode(), body, hashlib.sha256
        ).hexdigest()
        headers.append(("x-ext-auth-signature-sha256", signature))

    result_json = await execute.parse_execute_json(
        db,
        """
with
    nh as module std::net::http,
    net as module std::net,

    # n.b. workaround for bug in parse_execute_json
    url := <required str>$url,
    headers := <array<tuple<str, str>>>$headers,
    body := <bytes>$body,

    REQUEST := (
        nh::schedule_request(
            url,
            method := nh::Method.POST,
            headers := headers,
            body := body,
        )
    ),
select REQUEST;
""",
        variables={
            "url": url,
            "body": body,
            "headers": headers,
        },
        cached_globally=True,
        query_tag='gel/auth',
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
