##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


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

    return parsing.ParserContext(name=start_ctx.name,
                                 buffer=start_ctx.buffer,
                                 start=lang_context.SourcePoint(
                                     start_ctx.start.line,
                                     start_ctx.start.column,
                                     start_ctx.start.pointer),
                                 end=lang_context.SourcePoint(
                                     end_ctx.end.line,
                                     end_ctx.end.column,
                                     end_ctx.end.pointer))


def merge_context(ctxlist):
    ctxlist.sort(key=lambda x: (x.start.pointer, x.end.pointer))

    # assume same name and buffer apply to all
    #
    return parsing.ParserContext(name=ctxlist[0].name,
                                 buffer=ctxlist[0].buffer,
                                 start=lang_context.SourcePoint(
                                     ctxlist[0].start.line,
                                     ctxlist[0].start.column,
                                     ctxlist[0].start.pointer),
                                 end=lang_context.SourcePoint(
                                     ctxlist[-1].end.line,
                                     ctxlist[-1].end.column,
                                     ctxlist[-1].end.pointer))


def force_context(node, context):
    if hasattr(node, 'context'):
        ContextPropagator(default=context).visit(node)
        node.context = context


def has_context(func):
    '''This is a decorator meant to be used with Nonterm production rules.'''

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
    def visit_list(self, node):
        for el in node:
            if isinstance(el, (list, ast.AST)):
                self.visit(el)

    def generic_visit(self, node):
        for field, value in ast.iter_fields(node):
            if isinstance(value, (list, ast.AST)):
                self.visit(value)


class ContextRebaser(ContextVisitor):
    def __init__(self, base):
        self._base = base

    def generic_visit(self, node):
        rebase_context(self._base, node.context)
        super().generic_visit(node)


def rebase_ast_context(base, root):
    rebaser = ContextRebaser(base)
    return rebaser.visit(root)


class ContextPropagator(ContextVisitor):
    """Propagate context from children to root.

    It is assumed that if a node has a context, all of its children
    also have correct context. For a node that has no context, its
    context is derived as a superset of all of the contexts of its
    descendants.
    """

    def __init__(self, default=None):
        self._default = default

    def visit_list(self, node):
        ctxlist = []
        for el in node:
            if isinstance(el, (list, ast.AST)):
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
        ctxlist = []
        for field, value in ast.iter_fields(node):
            if isinstance(value, list):
                ctxlist.extend(self.visit(value))
            if isinstance(value, ast.AST):
                ctxlist.append(self.visit(value))

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

            # from edgedb.lang.common import markup
            # markup.dump(node)

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


class ListNonterm(parsing.ListNonterm, metaclass=ContextListNontermMeta,
                  element=None):
    pass
