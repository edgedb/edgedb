#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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

from edb.pgsql import ast as pgast
from edb.schema import schema as s_schema

from . import dispatch
from . import context
from . import resolver


def resolve(
    query: pgast.Base,
    schema: s_schema.Schema,
) -> pgast.Base:
    ctx = context.ResolverContextLevel(
        None, context.ContextSwitchMode.EMPTY, schema=schema
    )

    _ = context.ResolverContext(initial=ctx)

    return dispatch.resolve(query, ctx=ctx)