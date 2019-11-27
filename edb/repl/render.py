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


from __future__ import annotations
from typing import *  # NoQA

import json

import edgedb

from edb.errors import base as base_errors

from edb.common.markup.renderers import terminal
from edb.common.markup.renderers import styles

from typing import *  # NoQA
from . import context
from . import render_binary as _binary
from . import render_json as _json
from . import table


style = styles.Dark256


def render_binary(
    repl_ctx: context.ReplContext,
    data: Any,
    max_width: Optional[int] = None
) -> None:
    buf = terminal.Buffer(max_width=max_width, styled=repl_ctx.use_colors)
    _binary.walk(data, repl_ctx, buf)
    print(buf.flush())


def render_json(
    repl_ctx: context.ReplContext,
    data: str,
    max_width: Optional[int] = None
) -> None:
    data = json.loads(data)
    buf = terminal.Buffer(max_width=max_width, styled=repl_ctx.use_colors)
    _json.walk(data, repl_ctx, buf)
    print(buf.flush())


def render_status(
    repl_ctx: context.ReplContext,
    status: str
) -> None:
    if repl_ctx.use_colors:
        print(style.code_comment.apply(status))
    else:
        print(status)


def render_error(
    repl_ctx: context.ReplContext,
    error: str
) -> None:
    if repl_ctx.use_colors:
        print(style.exc_title.apply(error))
    else:
        print(error)


def render_exception(
    repl_ctx: context.ReplContext,
    exc: BaseException,
    *,
    query: Optional[str] = None
) -> None:
    print(f'{type(exc).__name__}: {exc}')

    def read_str_field(
        key: int,
        default: str = ''
    ) -> str:
        val: bytes = exc._attrs.get(key)  # type: ignore
        if val:
            return val.decode('utf-8')
        return default

    if isinstance(exc, edgedb.EdgeDBError):
        exc_hint = read_str_field(base_errors.FIELD_HINT, )
        if exc_hint:
            print(f'Hint: {exc_hint}')

        if query:
            exc_line = int(read_str_field(base_errors.FIELD_LINE, '-1'))
            exc_col = int(read_str_field(base_errors.FIELD_COLUMN, '-1'))
            if exc_line >= 0 and exc_col >= 0:
                for lineno, line in enumerate(query.split('\n'), 1):
                    print('###', line)
                    if lineno == exc_line:
                        print('###', ' ' * (exc_col - 1) + '^')


def render_table(
    repl_ctx: context.ReplContext,
    *,
    title: str,
    columns: Sequence[table.ColumnSpec],
    data: Iterable[edgedb.Object],
    max_width: int,
) -> None:
    table.render_table(
        title=title, columns=columns,
        data=data, max_width=max_width)
