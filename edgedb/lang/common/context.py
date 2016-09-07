##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types
from edgedb.lang.common import ast, parsing
from importkit import context as lang_context


def _get_start_context(items):
    ctx = None
    # find non-empty start and end
    #
    for item in items:
        if isinstance(item, (list, tuple)):
            ctx = _get_start_context(item)
            if ctx:
                return ctx
        else:
            ctx = getattr(item, 'context', None)
            if ctx:
                return ctx

    return None


def get_context(*kids):
    start_ctx = _get_start_context(kids)
    end_ctx = _get_start_context(reversed(kids))

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


def has_context(func):
    '''This is a decorator meant to be used with Nonterm production rules.'''

    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        _parsing = isinstance(args[0], parsing.Nonterm)  # "parsing" style

        if _parsing:
            obj, *args = args
        else:
            obj = result

        if len(args) == 1:
            # apparently it's a production rule that just returns its
            # only arg, so don't need to change the context
            #
            arg = args[0]
            if _parsing and getattr(arg, 'val', None) is obj.val:
                if hasattr(arg, 'context'):
                    obj.context = arg.context
                if hasattr(obj.val, 'context'):
                    obj.val.context = obj.context
                return result
            elif not _parsing and arg is obj:
                return result

        obj.context = get_context(*args)
        if hasattr(obj.val, 'context'):
            obj.val.context = obj.context
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


class ContextRebaser(ast.NodeVisitor):
    def __init__(self, base):
        self._base = base

    def generic_visit(self, node):
        rebase_context(self._base, node.context)
        super().generic_visit(node)


def rebase_ast_context(base, root):
    rebaser = ContextRebaser(base)
    return rebaser.visit(root)


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


class Nonterm(parsing.Nonterm, metaclass=ContextNontermMeta):
    pass
