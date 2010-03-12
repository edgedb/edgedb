##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import weakref

from semantix import ast
from semantix.utils import datastructures

class Base(ast.AST):
    pass

class ArgListNode(Base):
    __fields = ['name', ('args', list)]

class BinOpNode(Base):
    __fields = ['left', 'op', 'right']

class CallFunctionNode(Base):
    __fields = ['func', ('args', list)]

class VarNode(Base):
    __fields = ['name']

class PathVarNode(VarNode):
    pass

class ConstantNode(Base):
    __fields = ['value', 'index']

class UnaryOpNode(Base):
    __fields = ['op', 'operand']

class PredicateNode(Base):
    __fields = [('expr', Base, None)]

class SelectExprNode(Base):
    __fields = ['expr', 'alias']

class FromExprNode(Base):
    __fields = ['expr', 'alias']

class RelationNode(Base):
    __fields = [('concepts', frozenset), 'alias', ('_bonds', dict), 'caosnode']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        if not self._bonds:
            self._bonds = {}

    def bonds(self, key):
        return self._bonds[key]

    def addbond(self, key, bond):
        if key not in self._bonds:
            self._bonds[key] = [bond]
        else:
            self._bonds[key].append(bond)

        return bond

    def updatebonds(self, node):
        for key, values in node._bonds.items():
            if key not in self._bonds:
                self._bonds[key] = values
            else:
                self._bonds[key].extend(values)

class TableNode(RelationNode):
    __fields = ['name', 'schema']


class SelectQueryNode(RelationNode):
    __fields = ['distinct', ('fromlist', list), ('targets', list), 'where',
                ('orderby', list), ('ctes', datastructures.OrderedSet), ('concept_node_map', dict),
                ('linkmap', dict)]

class CompositeNode(RelationNode):
    __fields = [('queries', list), ('ctes', datastructures.OrderedSet),
                ('concept_node_map', dict)]

class UnionNode(CompositeNode):
    __fields = ['distinct']

class IntersectNode(CompositeNode):
    pass

class CTENode(SelectQueryNode):
    __fields = [('referrers', weakref.WeakSet)]

class CTEAttrRefNode(Base):
    __fields = ['cte', 'attr']

class JoinNode(RelationNode):
    __fields = ['left', 'right', 'condition', 'type']

class ExistsNode(Base):
    __fields = ['expr']

class FieldRefNode(Base):
    __fields = ['table', 'field', 'origin', 'origin_field']

class SequenceNode(Base):
    __fields = [('elements', list)]

class SortExprNode(Base):
    __fields = ['expr', 'direction']

class FunctionCallNode(Base):
    __fields = ['name', ('args', list)]

class IgnoreNode(Base):
    pass
