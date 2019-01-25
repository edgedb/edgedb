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


import datetime
import decimal
import functools
import json
import re
import uuid

import edgedb

from edb.common.markup.renderers import terminal
from edb.common.markup.renderers import styles


style = styles.Dark256


class BinaryRenderer:

    @functools.singledispatch
    def walk(o, buf):
        # The default renderer.  Shouldn't be ever called,
        # but if for some reason we haven't defined a renderer
        # for some edgedb type it's better to render something
        # than crash.
        buf.write(str(o))

    def _object_guts(o, buf):
        fields = dir(o)
        fields_len = len(fields)
        printable_fields = fields_len - 1  # __tid__ isn't printable

        i = 0
        for name in fields:
            if name == '__tid__':
                continue

            buf.write(name, style.key)
            buf.write(': ')

            try:
                link = o[name]
            except KeyError:
                link = None

            if link is None:
                val = getattr(o, name)
                BinaryRenderer.walk(val, buf)
            else:
                BinaryRenderer.walk(link, buf)

            i += 1
            if i < printable_fields:
                buf.write(',')
                buf.smart_break()

    @walk.register
    def _link(o: edgedb.Link, buf):
        with buf.smart_lines():
            buf.write('Object{', style.tree_node)
            with buf.indent():
                BinaryRenderer._object_guts(o.target, buf)

                fields = dir(o)
                fields_len = len(fields)
                printable_fields = fields_len - 2  # skip source, target

                if printable_fields > 0:
                    buf.write(',')
                    buf.smart_break()

                    i = 0
                    for name in fields:
                        if name in {'source', 'target'}:
                            continue

                        val = getattr(o, name)

                        buf.write(f'@{name}', style.code_tag)
                        buf.write(': ')
                        BinaryRenderer.walk(val, buf)

                        i += 1
                        if i < printable_fields:
                            buf.write(',')
                            buf.smart_break()

            buf.write('}', style.tree_node)

    @walk.register
    def _object(o: edgedb.Object, buf):
        with buf.smart_lines():
            buf.write('Object{', style.tree_node)
            with buf.indent():
                BinaryRenderer._object_guts(o, buf)
            buf.write('}', style.tree_node)

    @walk.register
    def _namedtuple(o: edgedb.NamedTuple, buf):
        with buf.smart_lines():
            buf.write('(', style.bracket)
            with buf.indent():
                for idx, name in enumerate(dir(o)):
                    val = getattr(o, name)

                    buf.write(name)
                    buf.write(' := ')
                    BinaryRenderer.walk(val, buf)

                    if idx < (len(o) - 1):
                        buf.write(',')
                        buf.smart_break()
            buf.write(')', style.bracket)

    @walk.register(edgedb.Array)
    @walk.register(edgedb.Tuple)
    @walk.register(edgedb.Set)
    @walk.register(edgedb.LinkSet)
    def _set(o, buf):
        if isinstance(o, edgedb.Array):
            begin, end = '[', ']'
        elif isinstance(o, edgedb.Tuple):
            begin, end = '(', ')'
        else:
            begin, end = '{', '}'

        with buf.smart_lines():
            buf.write(begin, style.bracket)
            with buf.indent():
                for idx, el in enumerate(o):
                    BinaryRenderer.walk(el, buf)
                    if idx < (len(o) - 1):
                        buf.write(',')
                        buf.smart_break()
            buf.write(end, style.bracket)

    @walk.register
    def _uuid(o: uuid.UUID, buf):
        buf.write(f'<uuid>{repr(str(o))}', style.code_comment)

    @walk.register(int)
    @walk.register(float)
    def _numeric(o, buf):
        buf.write(str(o), style.code_number)

    @walk.register(str)
    @walk.register(bytes)
    def _str(o, buf):
        buf.write(repr(o), style.code_string)

    @walk.register(bool)
    def _bool(o, buf):
        buf.write(str(o).lower(), style.code_constant)

    @walk.register(decimal.Decimal)
    def _decimal(o, buf):
        buf.write(f'{o}n', style.code_number)

    @walk.register(type(None))
    def _empty(o, buf):
        buf.write('{}', style.bracket)

    @walk.register(datetime.datetime)
    def _datetime(o, buf):
        if o.tzinfo:
            buf.write("<datetime>", style.code_comment)
        else:
            buf.write("<naive_datetime>", style.code_comment)

        buf.write(repr(o.isoformat()), style.code_string)

    @walk.register(datetime.date)
    def _date(o, buf):
        buf.write("<naive_date>", style.code_comment)
        buf.write(repr(o.isoformat()), style.code_string)

    @walk.register(datetime.time)
    def _time(o, buf):
        buf.write("<naive_time>", style.code_comment)
        buf.write(repr(o.isoformat()), style.code_string)

    @walk.register(datetime.timedelta)
    def _timedelta(o: datetime.timedelta, buf):
        buf.write("<timedelta>", style.code_comment)
        buf.write(repr(str(o)), style.code_string)


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
    def walk(o, buf):
        # The default renderer.  Shouldn't be ever called,
        # but if for some reason we haven't defined a renderer
        # for some edgedb type it's better to render something
        # than crash.
        buf.write(str(o))

    @walk.register(list)
    @walk.register(tuple)
    def _set(o, buf):
        with buf.smart_lines():
            buf.write('[', style.bracket)
            with buf.indent():
                for idx, el in enumerate(o):
                    JSONRenderer.walk(el, buf)
                    if idx < (len(o) - 1):
                        buf.write(',')
                        buf.smart_break()
            buf.write(']', style.bracket)

    @walk.register(dict)
    def _dict(o, buf):
        with buf.smart_lines():
            buf.write('{', style.bracket)
            with buf.indent():
                for idx, (key, el) in enumerate(o.items()):
                    JSONRenderer.walk(key, buf)
                    buf.write(': ')
                    JSONRenderer.walk(el, buf)
                    if idx < (len(o) - 1):
                        buf.write(',')
                        buf.smart_break()
            buf.write('}', style.bracket)

    @walk.register(int)
    @walk.register(float)
    def _numeric(o, buf):
        buf.write(str(o), style.code_number)

    @walk.register(str)
    @walk.register(uuid.UUID)
    def _str(o, buf):
        o = str(o)
        buf.write(JSONRenderer._encode_str(str(o)), style.code_string)

    @walk.register(bool)
    def _bool(o, buf):
        buf.write(str(o).lower(), style.code_constant)

    @walk.register(type(None))
    def _empty(o, buf):
        buf.write('null', style.code_constant)


def render_binary(data, max_width=None, use_colors=False):
    buf = terminal.Buffer(max_width=max_width, styled=use_colors)
    BinaryRenderer.walk(data, buf)
    return buf.flush()


def render_json(data: str, max_width=None, use_colors=False):
    data = json.loads(data)
    buf = terminal.Buffer(max_width=max_width, styled=use_colors)
    JSONRenderer.walk(data, buf)
    return buf.flush()
