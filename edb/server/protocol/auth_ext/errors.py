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

from edb.server.ext.errors import ExtError


class AuthExtError(ExtError):
    """Base class for all exceptions raised by the auth extension."""

    pass


class NotFound(AuthExtError):
    """Required resource could not be found."""

    def __init__(self, description: str):
        self.description = description

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"description={self.description!r}"
            ")"
        )

    def __str__(self) -> str:
        return self.description


class InvalidData(AuthExtError):
    """Data received from the client is invalid."""

    def __init__(self, description: str):
        self.description = description

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"description={self.description!r}"
            ")"
        )

    def __str__(self) -> str:
        return self.description


class MisconfiguredProvider(AuthExtError):
    """Data received from the auth provider is invalid."""

    def __init__(self, description: str):
        self.description = description

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"description={self.description!r}"
            ")"
        )

    def __str__(self) -> str:
        return self.description


class NoIdentityFound(AuthExtError):
    """Could not find a matching identity."""

    def __init__(
        self,
        description: str = (
            "Could not find an Identity matching the provided credentials"
        ),
    ):
        self.description = description

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"description={self.description!r}"
            ")"
        )

    def __str__(self) -> str:
        return self.description


class UserAlreadyRegistered(AuthExtError):
    """Attempt to register an already registered handle."""

    def __init__(
        self,
        description: str = ("This user has already been registered"),
    ):
        self.description = description

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"description={self.description!r}"
            ")"
        )

    def __str__(self) -> str:
        return self.description


class OAuthProviderFailure(AuthExtError):
    """OAuth Provider returned a non-success for some part of the flow"""

    def __init__(
        self,
        description: str,
    ):
        self.description = description

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"description={self.description!r}"
            ")"
        )

    def __str__(self) -> str:
        return self.description
