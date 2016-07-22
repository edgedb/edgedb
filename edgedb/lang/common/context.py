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
    ctx = val = None
    # find non-empty start and end
    #
    for item in items:
        if isinstance(item, parsing.Nonterm):
            val = item.val
        else:
            val = item

        if isinstance(val, (list, tuple)):
            ctx = _get_start_context(val)
            if ctx:
                return ctx
        else:
            ctx = getattr(val, 'context', None)
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
            obj = obj.val
        else:
            obj = result

        if len(args) == 1:
            # apparently it's a production rule that just returns its
            # only arg, so don't need to change the context
            #
            if _parsing and getattr(args[0], 'val', None) is obj:
                return result
            elif not _parsing and args[0] is obj:
                return result

        if hasattr(obj, 'context'):
            obj.context = get_context(*args)
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
