##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import weakref

from semantix.utils import datastructures, ast


class Base(ast.AST):
    pass

class ArgListNode(Base):
    __fields = ['name', ('args', list)]

class BinOpNode(Base):
    __fields = ['left', 'op', 'right', ('aggregates', bool)]

class VarNode(Base):
    __fields = ['name']

class PathVarNode(VarNode):
    pass

class ConstantNode(Base):
    __fields = ['value', 'index', 'expr', 'type']

class UnaryOpNode(Base):
    __fields = ['op', 'operand']

class PostfixOpNode(Base):
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
    __fields = ['distinct', ('fromlist', list), ('targets', list),
                'where', 'where_weak', 'where_strong',
                ('from_only', bool),
                ('orderby', list), 'offset', 'limit', ('groupby', list), 'having',
                ('ctes', datastructures.OrderedSet),
                ('concept_node_map', dict), ('link_node_map', dict), ('linkmap', dict)]

class UpdateQueryNode(Base):
    __fields = ['fromexpr', ('values', list), 'where', ('targets', list)]

class UpdateExprNode(Base):
    __fields = ['expr', 'value']

class DeleteQueryNode(Base):
    __fields = ['fromexpr', 'where', ('targets', list)]

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
    __fields = ['table', 'field', 'origin', 'origin_field', 'indirection']

class SequenceNode(Base):
    __fields = [('elements', list)]

class SortExprNode(Base):
    __fields = ['expr', 'direction']

class FunctionCallNode(Base):
    __fields = ['name', ('args', list), 'over', ('aggregate', bool)]

class IgnoreNode(Base):
    pass


class ArrayNode(Base):
    __fields = [('elements', list)]


class TypeCastNode(Base):
    __fields = ['expr', 'type']

class ParamRefNode(Base):
    __fields = ['param']

class IndirectionNode(Base):
    __fields = ['expr', 'indirection']

class RowExprNode(Base):
    __fields = [('args', list)]

class TypeNode(Base):
    __fields = ['name', 'typmods', 'array_bounds', ('setof', bool)]

class StarIndirectionNode(Base):
    pass

class IndexIndirectionNode(Base):
    __fields = ['lower', 'upper']


class PgSQLOperator(ast.ops.Operator):
    pass


LIKE = PgSQLOperator('~~')
NOT_LIKE = PgSQLOperator('!~~')
ILIKE = PgSQLOperator('~~*')
NOT_ILIKE = PgSQLOperator('!~~*')
SIMILAR_TO = PgSQLOperator('~')
NOT_SIMILAR_TO = PgSQLOperator('!~')
IS_DISTINCT = PgSQLOperator('IS DISTINCT')
IS_NOT_DISTINCT = PgSQLOperator('IS NOT DISTINCT')
IS_OF = PgSQLOperator('IS OF')
IS_NOT_OF = PgSQLOperator('IS NOT OF')
