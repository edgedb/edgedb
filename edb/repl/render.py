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

import datetime
import decimal
import functools
import json
import re
import uuid

import edgedb
from edgedb import introspect

from edb.errors import base as base_errors

from edb.common.markup.renderers import terminal
from edb.common.markup.renderers import styles

from typing import *  # NoQA
from . import context
from . import table


style = styles.Dark256


class BinaryRenderer:

    @functools.singledispatch
    def walk(o, repl_ctx: context.ReplContext, buf):
        # The default renderer.  Shouldn't be ever called,
        # but if for some reason we haven't defined a renderer
        # for some edgedb type it's better to render something
        # than crash.
        buf.write(str(o))

    def _object_guts(o, repl_ctx: context.ReplContext, buf, *,
                     include_id_when_empty: bool):
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
                BinaryRenderer.walk(link, repl_ctx, buf)
            else:
                val = getattr(o, ptr.name)
                BinaryRenderer.walk(val, repl_ctx, buf)

            pointers_rendered += 1
            if pointers_rendered < pointers_len:
                buf.write(',')
                buf.mark_line_break()

        if pointers_rendered == 0 and include_id_when_empty:
            buf.write('id', style.key)
            buf.write(': ')
            BinaryRenderer.walk(o.id, repl_ctx, buf)
            pointers_rendered = 1

        return pointers_rendered > 0

    def _object_name(o, repl_ctx):
        if not repl_ctx.introspect_types:
            return 'Object'
        return repl_ctx.typenames.get(o.__tid__, 'Object')

    @walk.register
    def _link(o: edgedb.Link, repl_ctx: context.ReplContext, buf):
        with buf.foldable_lines():
            buf.write(BinaryRenderer._object_name(o.target, repl_ctx),
                      style.tree_node)
            buf.write(' {', style.tree_node)
            buf.folded_space()
            with buf.indent():
                pointers = o.__dir__()
                pointers = tuple(ptr for ptr in pointers
                                 if ptr not in {'source', 'target'})
                pointers_len = len(pointers)

                non_empty = BinaryRenderer._object_guts(
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
                        BinaryRenderer.walk(val, repl_ctx, buf)
                        non_empty = True

                        i += 1
                        if i < pointers_len:
                            buf.write(',')
                            buf.mark_line_break()

            if non_empty:
                buf.folded_space()
            buf.write('}', style.tree_node)

    @walk.register
    def _object(o: edgedb.Object, repl_ctx: context.ReplContext, buf):
        with buf.foldable_lines():
            buf.write(BinaryRenderer._object_name(o, repl_ctx),
                      style.tree_node)
            buf.write(' {', style.tree_node)
            buf.folded_space()
            with buf.indent():
                non_empty = BinaryRenderer._object_guts(
                    o, repl_ctx, buf, include_id_when_empty=True)
            if non_empty:
                buf.folded_space()
            buf.write('}', style.tree_node)

    @walk.register
    def _namedtuple(o: edgedb.NamedTuple, repl_ctx: context.ReplContext, buf):
        with buf.foldable_lines():
            buf.write('(', style.bracket)
            with buf.indent():
                # Call __dir__ directly as dir() scrambles the order.
                for idx, name in enumerate(o.__dir__()):
                    val = getattr(o, name)

                    buf.write(name)
                    buf.write(' := ')
                    BinaryRenderer.walk(val, repl_ctx, buf)

                    if idx < (len(o) - 1):
                        buf.write(',')
                        buf.mark_line_break()
            buf.write(')', style.bracket)

    @walk.register(edgedb.Array)
    @walk.register(edgedb.Tuple)
    @walk.register(edgedb.Set)
    @walk.register(edgedb.LinkSet)
    def _set(o, repl_ctx: context.ReplContext, buf):
        if isinstance(o, edgedb.Array):
            begin, end = '[', ']'
        elif isinstance(o, edgedb.Tuple):
            begin, end = '(', ')'
        else:
            begin, end = '{', '}'

        with buf.foldable_lines():
            buf.write(begin, style.bracket)
            with buf.indent():
                for idx, el in enumerate(o):
                    BinaryRenderer.walk(el, repl_ctx, buf)
                    if idx < (len(o) - 1):
                        buf.write(',')
                        buf.mark_line_break()
            buf.write(end, style.bracket)

    @walk.register
    def _uuid(o: uuid.UUID, repl_ctx: context.ReplContext, buf):
        buf.write(f'<uuid>{repr(str(o))}', style.code_comment)

    @walk.register(int)
    @walk.register(float)
    def _numeric(o, repl_ctx: context.ReplContext, buf):
        buf.write(str(o), style.code_number)

    @walk.register(str)
    def _str(o, repl_ctx: context.ReplContext, buf):
        if "'" in o:
            rs = '"' + o.replace('"', r'\"') + '"'
        else:
            rs = "'" + o.replace("'", r"\'") + "'"
        buf.write(rs, style.code_string)

    @walk.register(bytes)
    def _bytes(o, repl_ctx: context.ReplContext, buf):
        buf.write(repr(o), style.code_string)

    @walk.register(bool)
    def _bool(o, repl_ctx: context.ReplContext, buf):
        buf.write(str(o).lower(), style.code_constant)

    @walk.register(decimal.Decimal)
    def _decimal(o, repl_ctx: context.ReplContext, buf):
        buf.write(f'{o}n', style.code_number)

    @walk.register(type(None))
    def _empty(o, repl_ctx: context.ReplContext, buf):
        buf.write('{}', style.bracket)

    @walk.register(datetime.datetime)
    def _datetime(o, repl_ctx: context.ReplContext, buf):
        if o.tzinfo:
            buf.write("<datetime>", style.code_comment)
        else:
            buf.write("<local_datetime>", style.code_comment)

        buf.write(repr(o.isoformat()), style.code_string)

    @walk.register(datetime.date)
    def _date(o, repl_ctx: context.ReplContext, buf):
        buf.write("<local_date>", style.code_comment)
        buf.write(repr(o.isoformat()), style.code_string)

    @walk.register(datetime.time)
    def _time(o, repl_ctx: context.ReplContext, buf):
        buf.write("<local_time>", style.code_comment)
        buf.write(repr(o.isoformat()), style.code_string)

    @walk.register(edgedb.Duration)
    def _duration(o: edgedb.Duration, repl_ctx: context.ReplContext, buf):
        buf.write("<duration>", style.code_comment)
        buf.write(repr(str(o)), style.code_string)

    @walk.register(edgedb.EnumValue)
    def _enum(o: edgedb.EnumValue, repl_ctx: context.ReplContext, buf):
        if not repl_ctx.introspect_types:
            typename = 'enum'
        else:
            typename = repl_ctx.typenames.get(o.__tid__, 'enum')

        buf.write(f"<{typename}>", style.code_comment)
        buf.write(f"'{o}'", style.code_string)


class JSONRenderer:

    ESCAPE = re.compile(r'[\x00-\x1f\\"\b\f\n\r\t]')
    ESCAPE_DCT = {
        '\\': '\\\\',
        '"': '\\"',
        '\b': '\\b',
        '\f': '\\f',
        '\n': '\\n',
        '\r': '\\r',
        '\t': '\\t',
    }
    for i in range(0x20):
        ESCAPE_DCT.setdefault(chr(i), '\\u{0:04x}'.format(i))

    def _encode_str(s):
        def replace(match):
            return JSONRenderer.ESCAPE_DCT[match.group(0)]
        return '"' + JSONRenderer.ESCAPE.sub(replace, s) + '"'

    @functools.singledispatch
    def walk(o, repl_ctx: context.ReplContext, buf):
        # The default renderer.  Shouldn't be ever called,
        # but if for some reason we haven't defined a renderer
        # for some edgedb type it's better to render something
        # than crash.
        buf.write(str(o))

    @walk.register(list)
    @walk.register(tuple)
    def _set(o, repl_ctx: context.ReplContext, buf):
        with buf.foldable_lines():
            buf.write('[', style.bracket)
            with buf.indent():
                for idx, el in enumerate(o):
                    JSONRenderer.walk(el, repl_ctx, buf)
                    if idx < (len(o) - 1):
                        buf.write(',')
                        buf.mark_line_break()
            buf.write(']', style.bracket)

    @walk.register(dict)
    def _dict(o, repl_ctx: context.ReplContext, buf):
        with buf.foldable_lines():
            buf.write('{', style.bracket)
            with buf.indent():
                for idx, (key, el) in enumerate(o.items()):
                    JSONRenderer.walk(key, repl_ctx, buf)
                    buf.write(': ')
                    JSONRenderer.walk(el, repl_ctx, buf)
                    if idx < (len(o) - 1):
                        buf.write(',')
                        buf.mark_line_break()
            buf.write('}', style.bracket)

    @walk.register(int)
    @walk.register(float)
    def _numeric(o, repl_ctx: context.ReplContext, buf):
        buf.write(str(o), style.code_number)

    @walk.register(str)
    @walk.register(uuid.UUID)
    def _str(o, repl_ctx: context.ReplContext, buf):
        o = str(o)
        buf.write(JSONRenderer._encode_str(str(o)), style.code_string)

    @walk.register(bool)
    def _bool(o, repl_ctx: context.ReplContext, buf):
        buf.write(str(o).lower(), style.code_constant)

    @walk.register(type(None))
    def _empty(o, repl_ctx: context.ReplContext, buf):
        buf.write('null', style.code_constant)


def render_binary(repl_ctx: context.ReplContext,
                  data, max_width=None):
    buf = terminal.Buffer(max_width=max_width, styled=repl_ctx.use_colors)
    BinaryRenderer.walk(data, repl_ctx, buf)
    print(buf.flush())


def render_json(repl_ctx: context.ReplContext,
                data: str, max_width=None):
    data = json.loads(data)
    buf = terminal.Buffer(max_width=max_width, styled=repl_ctx.use_colors)
    JSONRenderer.walk(data, repl_ctx, buf)
    print(buf.flush())


def render_status(repl_ctx: context.ReplContext, status):
    if repl_ctx.use_colors:
        print(style.code_comment.apply(status))
    else:
        print(status)


def render_error(repl_ctx: context.ReplContext, error):
    if repl_ctx.use_colors:
        print(style.exc_title.apply(error))
    else:
        print(error)


def render_exception(repl_ctx: context.ReplContext, exc, *, query=None):
    print(f'{type(exc).__name__}: {exc}')

    def read_str_field(key, default=None):
        val = exc._attrs.get(key)
        if val:
            return val.decode('utf-8')
        return default

    if isinstance(exc, edgedb.EdgeDBError):
        exc_hint = read_str_field(base_errors.FIELD_HINT)
        if exc_hint:
            print(f'Hint: {exc_hint}')

        if query:
            exc_line = int(read_str_field(base_errors.FIELD_LINE, -1))
            exc_col = int(read_str_field(base_errors.FIELD_COLUMN, -1))
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
    max_width=None,
) -> None:
    table.render_table(
        title=title, columns=columns, data=data, max_width=max_width)
