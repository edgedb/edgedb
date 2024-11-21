# Copyright (C) 2020-present MagicStack Inc. and the EdgeDB authors.
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


from __future__ import annotations
from typing import Any, Optional, Mapping, NamedTuple

import enum
import functools
import locale

from edb import buildmeta


BackendVersion = buildmeta.BackendVersion


class BackendCapabilities(enum.IntFlag):

    NONE = 0
    #: Whether CREATE ROLE .. SUPERUSER is allowed
    SUPERUSER_ACCESS = 1 << 0
    #: Whether reading PostgreSQL configuration files
    #: via pg_file_settings is allowed
    CONFIGFILE_ACCESS = 1 << 1
    #: Whether the PostgreSQL server supports the C.UTF-8 locale
    C_UTF8_LOCALE = 1 << 2
    #: Whether CREATE ROLE is allowed
    CREATE_ROLE = 1 << 3
    #: Whether CREATE DATABASE is allowed
    CREATE_DATABASE = 1 << 4
    #: Whether extension "edb_stat_statements" is available
    STAT_STATEMENTS = 1 << 5


ALL_BACKEND_CAPABILITIES = (
    BackendCapabilities.SUPERUSER_ACCESS
    | BackendCapabilities.CONFIGFILE_ACCESS
    | BackendCapabilities.C_UTF8_LOCALE
    | BackendCapabilities.CREATE_ROLE
    | BackendCapabilities.CREATE_DATABASE
    | BackendCapabilities.STAT_STATEMENTS
)


class BackendInstanceParams(NamedTuple):

    capabilities: BackendCapabilities
    version: BackendVersion
    tenant_id: str
    base_superuser: Optional[str] = None
    max_connections: int = 500
    reserved_connections: int = 0

    ext_schema: str = "edgedbext"
    """A Postgres schema where extensions can be created."""

    existing_exts: Optional[Mapping[str, str]] = None
    """A map of preexisting extensions in the target backend with schemas."""


class BackendRuntimeParams(NamedTuple):

    instance_params: BackendInstanceParams
    session_authorization_role: Optional[str] = None

    @property
    def tenant_id(self) -> str:
        return self.instance_params.tenant_id

    @property
    def has_superuser_access(self) -> bool:
        return bool(
            self.instance_params.capabilities
            & BackendCapabilities.SUPERUSER_ACCESS
        )

    @property
    def has_configfile_access(self) -> bool:
        return bool(
            self.instance_params.capabilities
            & BackendCapabilities.CONFIGFILE_ACCESS
        )

    @property
    def has_c_utf8_locale(self) -> bool:
        return bool(
            self.instance_params.capabilities
            & BackendCapabilities.C_UTF8_LOCALE
        )

    @property
    def has_create_role(self) -> bool:
        return bool(
            self.instance_params.capabilities
            & BackendCapabilities.CREATE_ROLE
        )

    @property
    def has_create_database(self) -> bool:
        return bool(
            self.instance_params.capabilities
            & BackendCapabilities.CREATE_DATABASE
        )

    @property
    def has_stat_statements(self) -> bool:
        return self.has_superuser_access and bool(
            self.instance_params.capabilities
            & BackendCapabilities.STAT_STATEMENTS
        )


@functools.lru_cache
def get_default_runtime_params(
    **instance_params: Any,
) -> BackendRuntimeParams:
    capabilities = ALL_BACKEND_CAPABILITIES
    if not _is_c_utf8_locale_present():
        capabilities &= ~BackendCapabilities.C_UTF8_LOCALE
    instance_params.setdefault('capabilities', capabilities)
    if 'tenant_id' not in instance_params:
        instance_params = dict(
            tenant_id=buildmeta.get_default_tenant_id(),
            **instance_params,
        )
    if 'version' not in instance_params:
        try:
            version = buildmeta.get_pg_version()
        except buildmeta.MetadataError as _:
            # HACK: if get_pg_version fails, this means we have no pg_config,
            # which happens for edgedb-ls. It is invoking pg compiler from
            # schema delta. Ideally, schema delta would not need pg compiler,
            # but that would require a lot of cleanups.
            version = BackendVersion(
                major=100,
                minor=0,
                micro=0,
                releaselevel='final',
                serial=0,
                string='100.0'
            )

        instance_params = dict(
            version=version,
            **instance_params,
        )

    return BackendRuntimeParams(
        instance_params=BackendInstanceParams(**instance_params),
    )


def _is_c_utf8_locale_present() -> bool:
    try:
        locale.setlocale(locale.LC_CTYPE, 'C.UTF-8')
    except Exception:
        return False
    else:
        # We specifically don't use locale.getlocale(), because
        # it can lie and return a non-existent locale due to PEP 538.
        locale.setlocale(locale.LC_CTYPE, '')
        return True
