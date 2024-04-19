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

import typing
import uuid

import immutables

from edb import edgeql
from edb.server import defines, config
from edb.server.compiler import sertypes, enums

class CompilationRequest:
    source: edgeql.Source
    protocol_version: defines.ProtocolVersion
    output_format: enums.OutputFormat
    json_parameters: bool
    expect_one: bool
    implicit_limit: int
    inline_typeids: bool
    inline_typenames: bool
    inline_objectids: bool

    modaliases: immutables.Map[str | None, str] | None
    session_config: immutables.Map[str, config.SettingValue] | None

    def __init__(
        self,
        compilation_config_serializer: sertypes.CompilationConfigSerializer,
    ):
        ...

    def update(
        self,
        source: edgeql.Source,
        protocol_version: defines.ProtocolVersion,
        *,
        output_format: enums.OutputFormat = enums.OutputFormat.BINARY,
        input_format: enums.InputFormat = enums.InputFormat.BINARY,
        expect_one: bool = False,
        implicit_limit: int = 0,
        inline_typeids: bool = False,
        inline_typenames: bool = False,
        inline_objectids: bool = True,
    ) -> CompilationRequest:
        ...

    def set_modaliases(
        self, value: typing.Mapping[str | None, str] | None
    ) -> CompilationRequest:
        ...

    def set_session_config(
        self, value: typing.Mapping[str, config.SettingValue] | None
    ) -> CompilationRequest:
        ...

    def set_database_config(
            self, value: typing.Mapping[str, config.SettingValue] | None
    ) -> CompilationRequest:
        ...

    def set_system_config(
        self, value: typing.Mapping[str, config.SettingValue] | None
    ) -> CompilationRequest:
        ...

    def set_schema_version(self, version: uuid.UUID) -> CompilationRequest:
        ...

    def serialize(self) -> bytes:
        ...

    def deserialize(self, data: bytes, query_text: str) -> CompilationRequest:
        ...

    def get_cache_key(self) -> uuid.UUID:
        ...
