##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types
from edgedb.lang.common import ast, parsing


def get_context(*kids):
    start = end = None
    # find non-empty start and end
    #
    for kid in kids:
        if kid.val:
            start = kid
            break
    for kid in reversed(kids):
        if kid.val:
            end = kid
            break

    if isinstance(start.val, (list, tuple)):
        start = start.val[0]
    if isinstance(end.val, (list, tuple)):
        end = end.val[-1]

    if isinstance(start, parsing.Nonterm):
        start = start.val
    if isinstance(end, parsing.Nonterm):
        end = end.val

    return parsing.ParserContext(name=start.context.name,
                                 buffer=start.context.buffer,
                                 start=start.context.start,
                                 end=end.context.end)


def has_context(func):
    '''This is a decorator meant to be used with Nonterm production rules.'''

    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if isinstance(args[0], parsing.Nonterm):
            obj, *args = args
            obj = obj.val
        else:
            obj = result

        if len(args) == 1 and args[0] is obj:
            # apparently it's a production rule that just returns its
            # only arg, so don't need to change the context
            #
            return result

        if hasattr(obj, 'context'):
            obj.context = get_context(*args)
        return result

    return wrapper


def rebase_context(base, context):
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
