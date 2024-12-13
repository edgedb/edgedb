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

class SQLParamsSource:
    types_in_out: list[tuple[list[str], list[tuple[str, str]]]]

    def cache_key(self) -> bytes:
        ...

    def serialize(self) -> bytes:
        ...

    @staticmethod
    def deserialize(data: bytes) -> SQLParamsSource:
        ...

    def text(self) -> str:
        ...

class CompilationRequest:
    source: edgeql.Source
    protocol_version: defines.ProtocolVersion
    input_language: enums.InputLanguage
    output_format: enums.OutputFormat
    input_format: enums.InputFormat
    expect_one: bool
    implicit_limit: int
    inline_typeids: bool
    inline_typenames: bool
    inline_objectids: bool
    role_name: str
    branch_name: str

    modaliases: immutables.Map[str | None, str] | None
    session_config: immutables.Map[str, config.SettingValue] | None

    def __init__(
        self,
        *,
        source: edgeql.Source,
        protocol_version: defines.ProtocolVersion,
        schema_version: uuid.UUID,
        compilation_config_serializer: sertypes.CompilationConfigSerializer,
        input_language: enums.InputLanguage = enums.InputLanguage.EDGEQL,
        output_format: enums.OutputFormat = enums.OutputFormat.BINARY,
        input_format: enums.InputFormat = enums.InputFormat.BINARY,
        expect_one: bool = False,
        implicit_limit: int = 0,
        inline_typeids: bool = False,
        inline_typenames: bool = False,
        inline_objectids: bool = True,
        modaliases: typing.Mapping[str | None, str] | None = None,
        session_config: typing.Mapping[str, config.SettingValue] | None = None,
        database_config: typing.Mapping[str, config.SettingValue] | None = None,
        system_config: typing.Mapping[str, config.SettingValue] | None = None,
        role_name: str = defines.EDGEDB_SUPERUSER,
        branch_name: str = defines.EDGEDB_SUPERUSER_DB,
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

    @classmethod
    def deserialize(
        cls,
        data: bytes,
        query_text: str,
        compilation_config_serializer: sertypes.CompilationConfigSerializer,
    ) -> CompilationRequest:
        ...

    def get_cache_key(self) -> uuid.UUID:
        ...
