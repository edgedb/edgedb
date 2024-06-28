#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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
from typing import NamedTuple, Optional, TYPE_CHECKING
import base64
import collections
import hashlib
import http
import os
import time

from edgedb import scram

from edb.common import debug
from edb.common import markup
from edb.common import secretkey

if TYPE_CHECKING:
    from edb.server import tenant as edbtenant
    from edb.server.protocol import protocol


SESSION_TIMEOUT: float = 30
SESSION_HIGH_WATER_MARK: float = SESSION_TIMEOUT * 10


class Session(NamedTuple):
    time: float
    client_nonce: str
    server_nonce: str
    client_first_bare: bytes
    cb_flag: bool
    server_first: bytes
    verifier: scram.SCRAMVerifier
    mock_auth: bool
    username: str


sessions: collections.OrderedDict[str, Session] = collections.OrderedDict()


def handle_request(
    scheme: str,
    auth_str: str,
    response: protocol.HttpResponse,
    tenant: edbtenant.Tenant,
) -> None:
    server = tenant.server
    if scheme != "SCRAM-SHA-256":
        response.body = (
            b"Client selected an invalid SASL authentication mechanism"
        )
        response.status = http.HTTPStatus.UNAUTHORIZED
        response.custom_headers["WWW-Authenticate"] = "SCRAM-SHA-256"
        return

    data = None
    sid = None
    try:
        for kv_str in auth_str.split():
            key, _, value = kv_str.rstrip(",").partition("=")
            if key == "data":
                data = base64.b64decode(value.strip('"')).strip()
            elif key == "sid":
                sid = value.strip('"')
        if data is None:
            raise ValueError("Malformed SCRAM message: data is missing")
    except Exception as ex:
        if debug.flags.server:
            markup.dump(ex)
        response.body = str(ex).encode("ascii")
        response.status = http.HTTPStatus.BAD_REQUEST
        response.close_connection = True
        return

    if not server.get_jws_key().has_private:  # type: ignore[union-attr]
        response.body = b"Server doesn't support HTTP SCRAM authentication"
        response.status = http.HTTPStatus.FORBIDDEN
        response.close_connection = True
        return

    if sid is None:
        try:
            bare_offset: int
            cb_flag: bool
            authzid: Optional[bytes]
            username_bytes: bytes
            client_nonce: str
            (
                bare_offset,
                cb_flag,
                authzid,
                username_bytes,
                client_nonce,
            ) = scram.parse_client_first_message(data)
        except ValueError as ex:
            if debug.flags.server:
                markup.dump(ex)
            response.body = f"Bad client first message: {ex!s}".encode("ascii")
            response.status = http.HTTPStatus.BAD_REQUEST
            response.close_connection = True
            return

        username = username_bytes.decode("utf-8")
        client_first_bare = data[bare_offset:]

        if isinstance(cb_flag, str):
            response.body = (
                b"Malformed SCRAM message: "
                b"The client selected SCRAM-SHA-256 without "
                b"channel binding, but the SCRAM message "
                b"includes channel binding data."
            )
            response.status = http.HTTPStatus.BAD_REQUEST
            response.close_connection = True
            return

        if authzid:
            response.body = (
                b"Client uses SASL authorization identity, "
                b"which is not supported"
            )
            response.status = http.HTTPStatus.BAD_REQUEST
            response.close_connection = True
            return

        try:
            verifier, mock_auth = get_scram_verifier(username, tenant)
        except ValueError as ex:
            if debug.flags.server:
                markup.dump(ex)
            response.body = b"Authentication failed"
            response.status = http.HTTPStatus.UNAUTHORIZED
            response.custom_headers["WWW-Authenticate"] = "SCRAM-SHA-256"
            return

        server_nonce: str = scram.generate_nonce()
        server_first: bytes = scram.build_server_first_message(
            server_nonce, client_nonce, verifier.salt, verifier.iterations
        ).encode("utf-8")

        if len(sessions) > SESSION_HIGH_WATER_MARK:
            while sessions:
                key, session = sessions.popitem(last=False)
                if session.time + SESSION_TIMEOUT > time.monotonic():
                    sessions[key] = session
                    sessions.move_to_end(key, last=False)
                    break

        sid = (
            base64.urlsafe_b64encode(os.urandom(16))
            .decode("ascii")
            .rstrip("=")
        )
        assert sid not in sessions
        sessions[sid] = Session(
            time.monotonic(),
            client_nonce,
            server_nonce,
            client_first_bare,
            cb_flag,
            server_first,
            verifier,
            mock_auth,
            username,
        )

        server_first_str = base64.b64encode(server_first).decode("ascii")
        response.status = http.HTTPStatus.UNAUTHORIZED
        response.custom_headers[
            "WWW-Authenticate"
        ] = f"SCRAM-SHA-256 sid={sid}, data={server_first_str}"

    else:
        session = sessions.pop(sid)
        if session is None:
            response.body = b"Bad session ID"
            response.status = http.HTTPStatus.UNAUTHORIZED
            response.custom_headers["WWW-Authenticate"] = "SCRAM-SHA-256"
            return

        (
            ts,
            client_nonce,
            server_nonce,
            client_first_bare,
            cb_flag,
            server_first,
            verifier,
            mock_auth,
            username,
        ) = session
        if ts + SESSION_TIMEOUT < time.monotonic():
            response.body = b"Session timed out"
            response.status = http.HTTPStatus.UNAUTHORIZED
            response.custom_headers["WWW-Authenticate"] = "SCRAM-SHA-256"
            return

        try:
            (
                cb_data,
                client_proof,
                proof_len,
            ) = scram.parse_client_final_message(
                data, client_nonce, server_nonce
            )
        except ValueError as ex:
            if debug.flags.server:
                markup.dump(ex)
            response.body = f"Bad client final message: {ex!s}".encode("ascii")
            response.status = http.HTTPStatus.BAD_REQUEST
            response.close_connection = True
            return

        client_final_without_proof = data[:-proof_len]

        cb_data_ok = (cb_flag is False and cb_data == b"biws") or (
            cb_flag is True and cb_data == b"eSws"
        )
        if not cb_data_ok:
            response.body = (
                b"Malformed SCRAM message: "
                b"Unexpected SCRAM channel-binding attribute "
                b"in client-final-message."
            )
            response.status = http.HTTPStatus.BAD_REQUEST
            response.close_connection = True
            return

        if (
            not scram.verify_client_proof(
                client_first_bare,
                server_first,
                client_final_without_proof,
                verifier.stored_key,
                client_proof,
            )
            or mock_auth
        ):
            response.body = b"Authentication failed"
            response.status = http.HTTPStatus.UNAUTHORIZED
            response.custom_headers["WWW-Authenticate"] = "SCRAM-SHA-256"
            return

        server_final = base64.b64encode(
            scram.build_server_final_message(
                client_first_bare,
                server_first,
                client_final_without_proof,
                verifier.server_key,
            ).encode("utf-8")
        ).decode("ascii")

        try:
            response.body = secretkey.generate_secret_key(
                server.get_jws_key(),
                roles=[username],
            ).encode("ascii")
        except ValueError as ex:
            if debug.flags.server:
                markup.dump(ex)
            response.body = b"Authentication failed"
            response.status = http.HTTPStatus.UNAUTHORIZED
            response.custom_headers["WWW-Authenticate"] = "SCRAM-SHA-256"
            return

        response.custom_headers[
            "Authentication-Info"
        ] = f"sid={sid}, data={server_final}"


def get_scram_verifier(
    user: str,
    tenant: edbtenant.Tenant,
) -> tuple[scram.SCRAMVerifier, bool]:
    roles = tenant.get_roles()

    rolerec = roles.get(user)
    if rolerec is not None:
        verifier_string = rolerec["password"]
        if verifier_string is not None:
            verifier = scram.parse_verifier(verifier_string)
            is_mock = False
            return verifier, is_mock

    # To avoid revealing the validity of the submitted user name,
    # generate a mock verifier using a salt derived from the
    # received user name and the cluster mock auth nonce.
    # The same approach is taken by Postgres.
    nonce = tenant.get_instance_data("mock_auth_nonce")
    salt = hashlib.sha256(nonce.encode() + user.encode()).digest()

    verifier = scram.SCRAMVerifier(
        mechanism="SCRAM-SHA-256",
        iterations=scram.DEFAULT_ITERATIONS,
        salt=salt[: scram.DEFAULT_SALT_LENGTH],
        stored_key=b"",
        server_key=b"",
    )
    is_mock = True
    return verifier, is_mock
