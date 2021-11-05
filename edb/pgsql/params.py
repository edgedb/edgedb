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
from typing import *

import enum
import functools
import locale

from edb import buildmeta


class BackendCapabilities(enum.IntFlag):

    NONE = 0
    #: Whether CREATE ROLE .. SUPERUSER is allowed
    SUPERUSER_ACCESS = 1 << 0
    #: Whether reading PostgreSQL configuration files
    #: via pg_file_settings is allowed
    CONFIGFILE_ACCESS = 1 << 1
    #: Whether the PostgreSQL server supports the C.UTF-8 locale
    C_UTF8_LOCALE = 1 << 2


ALL_BACKEND_CAPABILITIES = (
    BackendCapabilities.SUPERUSER_ACCESS
    | BackendCapabilities.CONFIGFILE_ACCESS
    | BackendCapabilities.C_UTF8_LOCALE
)


class BackendInstanceParams(NamedTuple):

    capabilities: BackendCapabilities
    tenant_id: str
    base_superuser: Optional[str] = None
    max_connections: int = 500
    reserved_connections: int = 0


class BackendRuntimeParams(NamedTuple):

    instance_params: BackendInstanceParams
    session_authorization_role: Optional[str] = None


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
