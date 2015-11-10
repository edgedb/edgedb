##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import ast

from . import ast as py_ast
from metamagic.exceptions import MetamagicError


class AstToPyAstConverter:
    @classmethod
    def convert(cls, node:ast.AST):
        name = node.__class__.__name__

        new_cls = getattr(py_ast, 'Py' + name, None)

        if new_cls is None:
            raise MetamagicError('unknown python ast class "%s"' % name)

        new_node = new_cls()

        for field, value in py_ast.iter_fields(node):
            if isinstance(value, list):
                new_value = []
                for item in value:
                    if isinstance(item, ast.AST):
                        new_item = cls.convert(item)
                        if hasattr(item, 'lineno'):
                            new_item.lineno = item.lineno
                            new_item.col_offset = item.col_offset
                        new_item.parent = new_node
                        new_value.append(new_item)
                    else:
                        new_value.append(item)
                setattr(new_node, field, new_value)
            elif isinstance(value, ast.AST):
                new_item = cls.convert(value)
                new_item.parent = new_node
                if hasattr(value, 'lineno'):
                    new_item.lineno = value.lineno
                    new_item.col_offset = value.col_offset
                setattr(new_node, field, new_item)
            else:
                setattr(new_node, field, value)

        return new_node
