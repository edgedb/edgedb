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

import datetime
import decimal
import functools
import uuid

import edgedb
from edgedb import introspect

from edb.common.markup.renderers import terminal
from edb.common.markup.renderers import styles

from . import context
from . import utils


style = styles.Dark256


@functools.singledispatch
def walk(
    o: Any,
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer
) -> None:
    # The default renderer.  Shouldn't be ever called,
    # but if for some reason we haven't defined a renderer
    # for some edgedb type it's better to render something
    # than crash.
    buf.write(str(o))


def _object_guts(
    o: edgedb.Object,
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer,
    *,
    include_id_when_empty: bool
) -> bool:
    pointers = introspect.introspect_object(o).pointers
    if not repl_ctx.show_implicit_fields:
        pointers = tuple(ptr for ptr in pointers if not ptr.implicit)
    pointers_len = len(pointers)

    pointers_rendered = 0
    for ptr in pointers:
        buf.write(ptr.name, style.key)
        buf.write(': ')

        if ptr.kind is introspect.PointerKind.LINK:
            link = o[ptr.name]
            walk(link, repl_ctx, buf)
        else:
            val = getattr(o, ptr.name)
            walk(val, repl_ctx, buf)

        pointers_rendered += 1
        if pointers_rendered < pointers_len:
            buf.write(',')
            buf.mark_line_break()

    if pointers_rendered == 0 and include_id_when_empty:
        buf.write('id', style.key)
        buf.write(': ')
        walk(o.id, repl_ctx, buf)
        pointers_rendered = 1

    return pointers_rendered > 0


def _object_name(o: edgedb.Object, repl_ctx: context.ReplContext) -> str:
    if not repl_ctx.introspect_types:
        return 'Object'
    assert repl_ctx.typenames
    return repl_ctx.typenames.get(o.__tid__, 'Object')


@walk.register
def _link(
    o: edgedb.Link,
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer,
) -> None:
    with buf.foldable_lines():
        buf.write(_object_name(o.target, repl_ctx), style.tree_node)
        buf.write(' {', style.tree_node)
        buf.folded_space()
        with buf.indent():
            pointers = o.__dir__()
            pointers = tuple(ptr for ptr in pointers
                             if ptr not in {'source', 'target'})
            pointers_len = len(pointers)

            non_empty = _object_guts(
                o.target, repl_ctx, buf,
                include_id_when_empty=pointers_len == 0)

            if pointers_len > 0:
                if non_empty:
                    buf.write(',')
                    buf.mark_line_break()

                i = 0
                for name in pointers:
                    val = getattr(o, name)

                    buf.write(f'@{name}', style.code_tag)
                    buf.write(': ')
                    walk(val, repl_ctx, buf)
                    non_empty = True

                    i += 1
                    if i < pointers_len:
                        buf.write(',')
                        buf.mark_line_break()

        if non_empty:
            buf.folded_space()
        buf.write('}', style.tree_node)


@walk.register
def _object(
    o: edgedb.Object,
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer
) -> None:
    with buf.foldable_lines():
        buf.write(_object_name(o, repl_ctx), style.tree_node)
        buf.write(' {', style.tree_node)
        buf.folded_space()
        with buf.indent():
            non_empty = _object_guts(
                o, repl_ctx, buf, include_id_when_empty=True)
        if non_empty:
            buf.folded_space()
        buf.write('}', style.tree_node)


@walk.register
def _namedtuple(
    o: edgedb.NamedTuple,
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer
) -> None:
    with buf.foldable_lines():
        buf.write('(', style.bracket)
        with buf.indent():
            # Call __dir__ directly as dir() scrambles the order.
            for idx, name in enumerate(o.__dir__()):
                val = getattr(o, name)

                buf.write(name)
                buf.write(' := ')
                walk(val, repl_ctx, buf)

                if idx < (len(o) - 1):
                    buf.write(',')
                    buf.mark_line_break()
        buf.write(')', style.bracket)


@walk.register(edgedb.Array)
@walk.register(edgedb.Tuple)
@walk.register(edgedb.Set)
@walk.register(edgedb.LinkSet)
def _set(
    o: Union[edgedb.Array, edgedb.Tuple, edgedb.Set, edgedb.LinkSet],
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer
) -> None:
    if isinstance(o, edgedb.Array):
        begin, end = '[', ']'
    elif isinstance(o, edgedb.Tuple):
        begin, end = '(', ')'
    else:
        begin, end = '{', '}'

    last_idx = len(o) - 1
    with buf.foldable_lines():
        buf.write(begin, style.bracket)
        with buf.indent():
            for idx, el in enumerate(o):
                walk(el, repl_ctx, buf)
                if idx < last_idx:
                    buf.write(',')
                    buf.mark_line_break()
                if (repl_ctx.implicit_limit
                        and (idx + 1) == repl_ctx.implicit_limit):
                    if idx == last_idx:
                        buf.write(',')
                        buf.mark_line_break()

                    buf.write('...')
                    if repl_ctx.implicit_limit > 10:
                        buf.write(f'(further results hidden '
                                  f'\\limit {repl_ctx.implicit_limit})')
                    break
        buf.write(end, style.bracket)


@walk.register
def _uuid(
    o: uuid.UUID,
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer
) -> None:
    buf.write(f'<uuid>{repr(str(o))}', style.code_comment)


@walk.register(int)
@walk.register(float)
def _numeric(
    o: Union[int, float],
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer
) -> None:
    buf.write(str(o), style.code_number)


@walk.register
def _bigint(
    o: utils.BigInt,
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer
) -> None:
    buf.write(f'{o}n', style.code_number)


@walk.register
def _str(
    o: str,
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer
) -> None:
    if "'" in o:
        rs = '"' + o.replace('"', r'\"') + '"'
    else:
        rs = "'" + o.replace("'", r"\'") + "'"
    buf.write(rs, style.code_string)


@walk.register
def _bytes(
    o: bytes,
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer
) -> None:
    buf.write(repr(o), style.code_string)


@walk.register
def _bool(
    o: bool,
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer
) -> None:
    buf.write(str(o).lower(), style.code_constant)


@walk.register
def _decimal(
    o: decimal.Decimal,
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer
) -> None:
    buf.write(f'{o}n', style.code_number)


@walk.register
def _empty(
    o: None,
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer
) -> None:
    buf.write('{}', style.bracket)


@walk.register
def _datetime(
    o: datetime.datetime,
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer
) -> None:
    if o.tzinfo:
        buf.write("<datetime>", style.code_comment)
    else:
        buf.write("<local_datetime>", style.code_comment)

    buf.write(repr(o.isoformat()), style.code_string)


@walk.register
def _date(
    o: datetime.date,
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer
) -> None:
    buf.write("<local_date>", style.code_comment)
    buf.write(repr(o.isoformat()), style.code_string)


@walk.register
def _time(
    o: datetime.time,
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer
) -> None:
    buf.write("<local_time>", style.code_comment)
    buf.write(repr(o.isoformat()), style.code_string)


@walk.register
def _duration(
    o: datetime.timedelta,
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer
) -> None:
    buf.write("<duration>", style.code_comment)
    buf.write(repr(str(o)), style.code_string)


@walk.register
def _enum(
    o: edgedb.EnumValue,
    repl_ctx: context.ReplContext,
    buf: terminal.Buffer
) -> None:
    if not repl_ctx.introspect_types:
        typename = 'enum'
    else:
        assert repl_ctx.typenames
        typename = repl_ctx.typenames.get(o.__tid__, 'enum')

    buf.write(f"<{typename}>", style.code_comment)
    buf.write(f"'{o}'", style.code_string)
