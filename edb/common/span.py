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

from typing import Iterable, List
import re
import bisect

import edb._edgeql_parser as ql_parser

from edb.common import ast
from edb.common import markup
from edb.common import typeutils


NEW_LINE = re.compile(br'\r\n?|\n')


class Span(markup.MarkupExceptionContext):
    '''
    Parser Source Context
    '''

    def __init__(
        self,
        name,
        buffer,
        start: int,
        end: int,
        document=None,
        *,
        filename=None,
        context_lines=1,
    ):
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

    @classmethod
    def empty(cls) -> Span:
        return Span(
            name='<empty>',
            buffer='',
            start=0,
            end=0,
        )

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


def _get_span(items, *, reverse=False):
    ctx = None

    items = reversed(items) if reverse else items
    # find non-empty start and end
    #
    for item in items:
        if isinstance(item, (list, tuple)):
            ctx = _get_span(item, reverse=reverse)
            if ctx:
                return ctx
        else:
            ctx = getattr(item, 'span', None)
            if ctx:
                return ctx

    return None


def get_span(*kids: List[ast.AST]):
    start_ctx = _get_span(kids)
    end_ctx = _get_span(kids, reverse=True)

    if not start_ctx:
        return None

    return Span(
        name=start_ctx.name,
        buffer=start_ctx.buffer,
        start=start_ctx.start,
        end=end_ctx.end,
    )


def merge_spans(spans: Iterable[Span]) -> Span | None:
    span_list = list(spans)
    if not span_list:
        return None

    span_list.sort(key=lambda x: (x.start, x.end))

    # assume same name and buffer apply to all
    #
    return Span(
        name=span_list[0].name,
        buffer=span_list[0].buffer,
        start=span_list[0].start,
        end=span_list[-1].end,
    )


def infer_span_from_children(node, span: Span):
    if hasattr(node, 'span'):
        SpanPropagator.run(node, default=span)
        node.span = span


def wrap_function_to_infer_spans(func):
    """Provide automatic span for Nonterm production rules."""
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        obj, *args = args

        if len(args) == 1:
            # apparently it's a production rule that just returns its
            # only arg, so don't need to change the context
            #
            arg = args[0]
            if getattr(arg, 'val', None) is obj.val:
                if hasattr(arg, 'span'):
                    obj.span = arg.span
                if hasattr(obj.val, 'span'):
                    obj.val.span = obj.span
                return result

        # Avoid mangling existing span
        if getattr(obj, 'span', None) is None:
            obj.span = get_span(*args)

        # we have the span for the nonterminal, but now we need to
        # enforce span in the obj.val, recursively, in case it was
        # a complex production with nested AST nodes
        infer_span_from_children(obj.val, obj.span)
        return result

    return wrapper


class SpanPropagator(ast.NodeVisitor):
    """Propagate span from children to root.

    It is assumed that if a node has a span, all of its children
    also have correct span. For a node that has no span, its
    span is derived as a superset of all of the spans of its
    descendants.

    If full_pass is True, nodes with span will still recurse into
    children and their new span will also be superset of the existing span.
    """

    def __init__(self, default=None, full_pass=False):
        super().__init__()
        self._default = default
        self._full_pass = full_pass

    def repeated_node_visit(self, node):
        return self.memo[node]

    def container_visit(self, node) -> List[Span | None]:
        span_list: list[Span | None] = []
        for el in node:
            if isinstance(el, ast.AST) or typeutils.is_container(el):
                span = self.visit(el)

                if not span:
                    pass
                elif isinstance(span, (list, tuple)):
                    span_list.extend(span)
                elif isinstance(span, dict):
                    span_list.extend(span.values())
                else:
                    span_list.append(span)
        return span_list

    def generic_visit(self, node):
        # base case: we already have span
        if not self._full_pass and getattr(node, 'span', None) is not None:
            return node.span

        # recurse into children fields
        span_list = self.container_visit(v for _, v in ast.iter_fields(node))

        # also include own span (this can only happen in full_pass)
        if existing := getattr(node, 'span', None):
            span_list.append(existing)

        # merge spans into one
        node.span = merge_spans(s for s in span_list if s) or self._default

        return node.span


class SpanValidator(ast.NodeVisitor):
    def generic_visit(self, node):
        if getattr(node, 'span', None) is None:
            raise RuntimeError('node {} has no span'.format(node))
        super().generic_visit(node)
