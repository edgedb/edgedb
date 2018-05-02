##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
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

import bisect

from edgedb.lang.common import ast, markup


class SourcePoint:
    def __init__(self, line, column, pointer):
        self.line = line
        self.column = column
        self.pointer = pointer


class ParserContext(markup.MarkupExceptionContext):
    title = 'Source Context'

    def __init__(self, name, buffer, start, end, document=None, *,
                 filename=None):
        self.name = name
        self.buffer = buffer
        self.start = start
        self.end = end
        self.document = document
        self.filename = filename

    @classmethod
    @markup.serializer.no_ref_detect
    def as_markup(cls, self, *, ctx):
        me = markup.elements

        body = []

        lines = []
        line_numbers = []

        buf_lines = self.buffer.split('\n')
        line_offsets = []
        i = 0
        for line in buf_lines:
            line_offsets.append(i)
            i += len(line) + 1

        if self.start.line > 1:
            ctx_line, _ = self.get_line_snippet(
                self.start, offset=-1, line_offsets=line_offsets)
            lines.append(ctx_line)
            line_numbers.append(self.start.line - 1)

        snippet, offset = self.get_line_snippet(
            self.start, line_offsets=line_offsets)
        lines.append(snippet)
        line_numbers.append(self.start.line)

        try:
            ctx_line, _ = self.get_line_snippet(
                self.start, offset=1, line_offsets=line_offsets)
        except ValueError:
            pass
        else:
            lines.append(ctx_line)
            line_numbers.append(self.start.line + 1)

        tbp = me.lang.TracebackPoint(
            name=self.name, filename=self.name, lineno=self.start.line,
            colno=self.start.column, lines=lines, line_numbers=line_numbers,
            context=True)

        body.append(tbp)

        return me.lang.ExceptionContext(title=self.title, body=body)

    def _find_line(self, point, offset=0, *, line_offsets):
        if point.line == 0:
            if offset < 0:
                raise ValueError('not enough lines in buffer')
            else:
                return 0, len(self.buffer)

        line_no = bisect.bisect_right(line_offsets, point.pointer) - 1 + offset
        if line_no >= len(line_offsets):
            raise ValueError('not enough lines in buffer')

        linestart = line_offsets[line_no]
        try:
            lineend = line_offsets[line_no + 1] - 1
        except IndexError:
            lineend = len(self.buffer)

        return linestart, lineend

    def get_line_snippet(
            self, point, max_length=120, *, offset=0, line_offsets):
        line_start, line_end = self._find_line(
            point, offset=offset, line_offsets=line_offsets)
        line_len = line_end - line_start

        if line_len > max_length:
            before = min(max_length // 2, point.pointer - line_start)
            after = max_length - before
        else:
            before = point.pointer - line_start
            after = line_len - before

        start = point.pointer - before
        end = point.pointer + after

        return self.buffer[start:end], before


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


def get_context(*kids):
    start_ctx = _get_context(kids)
    end_ctx = _get_context(kids, reverse=True)

    if not start_ctx:
        return None

    return ParserContext(
        name=start_ctx.name, buffer=start_ctx.buffer,
        start=SourcePoint(
            start_ctx.start.line, start_ctx.start.column,
            start_ctx.start.pointer), end=SourcePoint(
                end_ctx.end.line, end_ctx.end.column, end_ctx.end.pointer))


def merge_context(ctxlist):
    ctxlist.sort(key=lambda x: (x.start.pointer, x.end.pointer))

    # assume same name and buffer apply to all
    #
    return ParserContext(
        name=ctxlist[0].name, buffer=ctxlist[0].buffer,
        start=SourcePoint(
            ctxlist[0].start.line, ctxlist[0].start.column,
            ctxlist[0].start.pointer), end=SourcePoint(
                ctxlist[-1].end.line, ctxlist[-1].end.column,
                ctxlist[-1].end.pointer))


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

        obj.context = get_context(*args)
        # we have the context for the nonterminal, but now we need to
        # enforce context in the obj.val, recursively, in case it was
        # a complex production with nested AST nodes
        #
        force_context(obj.val, obj.context)
        return result

    return wrapper


def rebase_context(base, context, *, offset_column=0, indent=0):
    if not context:
        return

    context.name = base.name
    context.buffer = base.buffer

    if context.start.line == 1:
        context.start.column += base.start.column - 1 + offset_column
    # indentation is always added
    context.start.column += indent
    context.start.line += base.start.line - 1
    context.start.pointer += base.start.pointer + offset_column + indent


class ContextVisitor(ast.NodeVisitor):
    pass


class ContextRebaser(ContextVisitor):
    def __init__(self, base, *, offset_column=0, indent=0):
        super().__init__()
        self._base = base
        self._offset_column = offset_column
        self._indent = indent

    def generic_visit(self, node):
        rebase_context(self._base, node.context,
                       offset_column=self._offset_column,
                       indent=self._indent)
        super().generic_visit(node)


def rebase_ast_context(base, root, *, offset_column=0, indent=0):
    return ContextRebaser.run(root, base=base, offset_column=offset_column,
                              indent=indent)


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
            if isinstance(el, ast.AST) or ast.is_container(el):
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
