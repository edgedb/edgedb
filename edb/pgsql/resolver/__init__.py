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
from typing import Tuple, Optional

from edb.pgsql import ast as pgast
from edb.schema import schema as s_schema

from edb.server.compiler import dbstate

from . import dispatch
from . import context
from . import expr  # NOQA
from . import relation  # NOQA
from . import command  # NOQA

Options = context.Options


def resolve(
    query: pgast.Base,
    schema: s_schema.Schema,
    options: context.Options,
) -> Tuple[pgast.Base, Optional[dbstate.CommandCompleteTag]]:
    ctx = context.ResolverContextLevel(
        None, context.ContextSwitchMode.EMPTY, schema=schema, options=options
    )

    _ = context.ResolverContext(initial=ctx)

    top_level_ctes = command.compile_dml(query, ctx=ctx)

    resolved = dispatch.resolve(query, ctx=ctx)

    if top_level_ctes:
        assert isinstance(resolved, pgast.Query)
        if not resolved.ctes:
            resolved.ctes = []
        resolved.ctes.extend(top_level_ctes)

    # when the top-level query is DML statement, clients will expect a tag in
    # the CommandComplete message that describes the number of modified rows.
    # Since our resolved SQL does not have a top-level DML stmt, we need to
    # override that tag.
    command_complete_tag: Optional[dbstate.CommandCompleteTag] = None
    if isinstance(query, pgast.DMLQuery):
        prefix: str
        if isinstance(query, pgast.InsertStmt):
            prefix = 'INSERT 0 '
        elif isinstance(query, pgast.DeleteStmt):
            prefix = 'DELETE '
        elif isinstance(query, pgast.UpdateStmt):
            prefix = 'UPDATE '

        if query.returning_list:
            # resolved SQL will return a result, we count those rows
            command_complete_tag = dbstate.TagCountMessages(prefix=prefix)
        else:
            # resolved SQL will contain an injected COUNT clause
            # we instruct io process to unpack that
            command_complete_tag = dbstate.TagUnpackRow(prefix=prefix)

    return (resolved, command_complete_tag)
