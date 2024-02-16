#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


"""This module contains tools for maintaining parser context.

Maintaining parser context explicitly is significant overhead and can
be difficult in the face of changing AST structures. Certain parser
productions require various nesting or unnesting of previously parsed
nodes. Both of these operations can result in the parser context not
being correctly updated.

The tools in this module attempt to automatically maintain parser
context based on the context information found in lexer tokens. The
general approach is to infer context information by propagating known
contexts through the AST structure.
"""

from __future__ import annotations

import re
import bisect

import edb._edgeql_parser as ql_parser

from edb.common import ast
from edb.common import markup
from edb.common import typeutils


NEW_LINE = re.compile(br'\r\n?|\n')


class ParserContext(markup.MarkupExceptionContext):
    title = 'Source Context'

    def __init__(self, name, buffer, start: int, end: int, document=None, *,
                 filename=None, context_lines=1):
        self.name = name
        self.buffer = buffer
        self.start = start
        self.end = end
        self.document = document
        self.filename = filename
        self.context_lines = context_lines
        self._points = None
        assert start is not None
        assert end is not None

    def __getstate__(self):
        dic = self.__dict__.copy()
        dic['_points'] = None
        return dic

    def _calc_points(self):
        self._points = ql_parser.SourcePoint.from_offsets(
            self.buffer.encode('utf-8'),
            [self.start, self.end]
        )

    @property
    def start_point(self):
        if self._points is None:
            self._calc_points()
        return self._points[0]

    @property
    def end_point(self):
        if self._points is None:
            self._calc_points()
        return self._points[1]

    @classmethod
    @markup.serializer.no_ref_detect
    def as_markup(cls, self, *, ctx):
        me = markup.elements

        start = self.start_point
        # TODO: do more with end?
        end = self.end_point

        buf_bytes = self.buffer.encode('utf-8')
        offset = 0
        buf_lines = []
        line_offsets = [0]
        for match in NEW_LINE.finditer(buf_bytes):
            buf_lines.append(buf_bytes[offset:match.start()].decode('utf-8'))
            offset = match.end()
            line_offsets.append(offset)

        line_no = bisect.bisect_right(line_offsets, start.offset) - 1

        context_start = max(0, line_no - self.context_lines)
        context_end = min(line_no + self.context_lines + 1, len(buf_lines))

        endcol = end.column if start.line == end.line else None
        tbp = me.lang.TracebackPoint(
            name=self.name, filename=self.name, lineno=start.line,
            colno=start.column, end_colno=endcol,
            lines=buf_lines[context_start:context_end],
            # Line numbers are 1 indexed here
            line_numbers=list(range(context_start + 1, context_end + 1)),
            context=True,
        )

        return me.lang.ExceptionContext(title=self.title, body=[tbp])


def _get_context(items, *, reverse=False):
    ctx = None

    items = reversed(items) if reverse else items
    # find non-empty start and end
    #
    for item in items:
        if isinstance(item, (list, tuple)):
            ctx = _get_context(item, reverse=reverse)
            if ctx:
                return ctx
        else:
            ctx = getattr(item, 'context', None)
            if ctx:
                return ctx

    return None


def empty_context():
    """Return a dummy context that points to an empty string."""
    return ParserContext(
        name='<empty>',
        buffer='',
        start=0,
        end=0,
    )


def get_context(*kids):
    start_ctx = _get_context(kids)
    end_ctx = _get_context(kids, reverse=True)

    if not start_ctx:
        return None

    return ParserContext(
        name=start_ctx.name,
        buffer=start_ctx.buffer,
        start=start_ctx.start,
        end=end_ctx.end,
    )


def merge_context(ctxlist):
    ctxlist.sort(key=lambda x: (x.start, x.end))

    # assume same name and buffer apply to all
    #
    return ParserContext(
        name=ctxlist[0].name,
        buffer=ctxlist[0].buffer,
        start=ctxlist[0].start,
        end=ctxlist[-1].end,
    )


def force_context(node, context):
    if hasattr(node, 'context'):
        ContextPropagator.run(node, default=context)
        node.context = context


def has_context(func):
    """Provide automatic context for Nonterm production rules."""
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        obj, *args = args

        if len(args) == 1:
            # apparently it's a production rule that just returns its
            # only arg, so don't need to change the context
            #
            arg = args[0]
            if getattr(arg, 'val', None) is obj.val:
                if hasattr(arg, 'context'):
                    obj.context = arg.context
                if hasattr(obj.val, 'context'):
                    obj.val.context = obj.context
                return result

        # Avoid mangling any existing context.
        if getattr(obj, 'context', None) is None:
            obj.context = get_context(*args)

        # we have the context for the nonterminal, but now we need to
        # enforce context in the obj.val, recursively, in case it was
        # a complex production with nested AST nodes
        #
        force_context(obj.val, obj.context)
        return result

    return wrapper


class ContextVisitor(ast.NodeVisitor):
    pass


class ContextPropagator(ContextVisitor):
    """Propagate context from children to root.

    It is assumed that if a node has a context, all of its children
    also have correct context. For a node that has no context, its
    context is derived as a superset of all of the contexts of its
    descendants.
    """

    def __init__(self, default=None):
        super().__init__()
        self._default = default

    def container_visit(self, node):
        ctxlist = []
        for el in node:
            if isinstance(el, ast.AST) or typeutils.is_container(el):
                ctx = self.visit(el)

                if isinstance(ctx, list):
                    ctxlist.extend(ctx)
                else:
                    ctxlist.append(ctx)
        return ctxlist

    def generic_visit(self, node):
        # base case: we already have context
        #
        if getattr(node, 'context', None) is not None:
            return node.context

        # we need to derive context based on the children
        #
        ctxlist = self.container_visit(v[1] for v in ast.iter_fields(node))

        # now that we have all of the children contexts, let's merge
        # them into one
        #
        if ctxlist:
            node.context = merge_context(ctxlist)
        else:
            node.context = self._default

        return node.context


class ContextValidator(ContextVisitor):
    def generic_visit(self, node):
        if getattr(node, 'context', None) is None:
            raise RuntimeError('node {} has no context'.format(node))
        super().generic_visit(node)
