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

import types
from edgedb.lang.common import ast, parsing
from importkit import context as lang_context


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

    return parsing.ParserContext(
        name=start_ctx.name, buffer=start_ctx.buffer,
        start=lang_context.SourcePoint(
            start_ctx.start.line, start_ctx.start.column,
            start_ctx.start.pointer), end=lang_context.SourcePoint(
                end_ctx.end.line, end_ctx.end.column, end_ctx.end.pointer))


def merge_context(ctxlist):
    ctxlist.sort(key=lambda x: (x.start.pointer, x.end.pointer))

    # assume same name and buffer apply to all
    #
    return parsing.ParserContext(
        name=ctxlist[0].name, buffer=ctxlist[0].buffer,
        start=lang_context.SourcePoint(
            ctxlist[0].start.line, ctxlist[0].start.column,
            ctxlist[0].start.pointer), end=lang_context.SourcePoint(
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


def rebase_context(base, context):
    if not context:
        return

    context.name = base.name
    context.buffer = base.buffer

    if context.start.line == 1:
        context.start.column += base.start.column - 1
    context.start.line += base.start.line - 1
    context.start.pointer += base.start.pointer


class ContextVisitor(ast.NodeVisitor):
    pass


class ContextRebaser(ContextVisitor):
    def __init__(self, base):
        super().__init__()
        self._base = base

    def generic_visit(self, node):
        rebase_context(self._base, node.context)
        super().generic_visit(node)


def rebase_ast_context(base, root):
    return ContextRebaser.run(root, base=base)


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
            raise parsing.ParserError('node {} has no context'.format(node))
        super().generic_visit(node)


class ContextNontermMeta(parsing.NontermMeta):
    def __new__(mcls, name, bases, dct):
        result = super().__new__(mcls, name, bases, dct)

        if name == 'Nonterm' or name == 'ListNonterm':
            return result

        for name, attr in result.__dict__.items():
            if (name.startswith('reduce_') and
                    isinstance(attr, types.FunctionType)):
                a = has_context(attr)
                a.__doc__ = attr.__doc__
                setattr(result, name, a)

        return result


class ContextListNontermMeta(parsing.ListNontermMeta, ContextNontermMeta):
    pass


class Nonterm(parsing.Nonterm, metaclass=ContextNontermMeta):
    pass


class ListNonterm(
        parsing.ListNonterm, metaclass=ContextListNontermMeta, element=None):
    pass
