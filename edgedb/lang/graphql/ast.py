##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast, parsing


class Base(ast.AST):
    ns = 'graphql'
    __fields = [('context', parsing.ParserContext, parsing.ParserContext,
                 True, None, True  # this last True is "hidden" attribute
                 )]

    def _extra_repr(self):
        return ''

    def __repr__(self):
        ar = self._extra_repr()
        return '<{}.{} at {:#x}{}>'.format(self.__class__.ns,
                                           self.__class__.__name__,
                                           id(self),
                                           ar)


class LiteralNode(Base):
    __fields = ['value']


class StringLiteral(LiteralNode):
    pass


class IntegerLiteral(LiteralNode):
    pass


class FloatLiteral(LiteralNode):
    pass


class BooleanLiteral(LiteralNode):
    pass


class EnumLiteral(LiteralNode):
    pass


class ListLiteral(LiteralNode):
    pass


class ObjectLiteral(LiteralNode):
    pass


class Variable(Base):
    __fields = ['value']


class Document(Base):
    __fields = [('definitions', list, list)]


class Definition(Base):
    __fields = [
        ('name', str, None),
        'selection_set',
    ]


class OperationDefinition(Definition):
    __fields = [
        ('type', str, None),
        ('variables', list, list),
        ('directives', list, list),
    ]


class FragmentDefinition(Definition):
    __fields = [
        'on',
        ('directives', list, list),
    ]


class VariableDefinition(Base):
    __fields = ['name', 'type', 'value']


class VariableType(Base):
    __fields = [
        'name',
        ('nullable', bool, True),
        ('list', bool, False),
    ]


class SelectionSet(Base):
    __fields = [('selections', list, list)]


class Selection(Base):
    pass


class Field(Selection):
    __fields = [
        ('alias', str, None),
        'name',
        ('arguments', list, list),
        ('directives', list, list),
        'selection_set',
    ]


class FragmentSpread(Selection):
    __fields = [
        'name',
        ('directives', list, list),
    ]


class InlineFragment(Selection):
    __fields = [
        'on',
        ('directives', list, list),
        'selection_set',
    ]


class Directive(Base):
    __fields = [
        'name',
        ('arguments', list, list),
    ]


class Argument(Base):
    __fields = ['name', 'value']


class ObjectField(Base):
    __fields = ['name', 'value']
