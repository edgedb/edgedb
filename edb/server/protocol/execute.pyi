#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

from typing import (
    Any,
    Mapping,
    Optional,
)
import immutables

from edb import errors
from edb.server import compiler
from edb.server import defines as edbdef
from edb.server.compiler import sertypes
from edb.server.dbview import dbview

async def describe(
    db: dbview.Database,
    query: str,
    *,
    query_cache_enabled: Optional[bool] = None,
    allow_capabilities: compiler.Capability = (
        compiler.Capability.MODIFICATIONS),
    query_tag: str | None = None,
) -> sertypes.TypeDesc:
    ...

async def parse_execute_json(
    db: dbview.Database,
    query: str,
    *,
    variables: Mapping[str, Any] = immutables.Map(),
    globals_: Optional[Mapping[str, Any]] = None,
    output_format: compiler.OutputFormat = compiler.OutputFormat.JSON,
    query_cache_enabled: Optional[bool] = None,
    cached_globally: bool = False,
    use_metrics: bool = True,
    tx_isolation: edbdef.TxIsolationLevel | None = None,
    query_tag: str | None = None,
) -> bytes:
    ...

async def interpret_error(
    exc: Exception,
    db: dbview.Database,
    *,
    global_schema_pickle: object=None,
    user_schema_pickle: object=None,
    from_graphql: bool=False,
) -> errors.EdgeDBError:
    ...
