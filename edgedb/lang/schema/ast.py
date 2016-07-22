##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast, parsing


class Base(ast.AST):
    ns = 'eschema'
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


class QualName(Base):
    pass


class Declaration(Base):
    __fields = [
        ('abstract', bool, False),
        ('final', bool, False),
        ('name', str),
        ('extends', list, list),
        # usually there are some attributes allowed, e.g. "description"
        ('attributes', list, list),
        # some declarations may not allow constraints
        ('constraints', list, list),
        # only links will actually allow policies
        ('policies', list, list),
    ]

    def __init__(self, base=None, **kwargs):
        if base is not None:
            kwargs['abstract'] = kwargs.get('abstract', base.abstract)
            kwargs['final'] = kwargs.get('final', base.final)
            kwargs['name'] = kwargs.get('name', base.name)
            kwargs['extends'] = kwargs.get('extends', base.extends)
            kwargs['attributes'] = kwargs.get('attributes', base.attributes)
            kwargs['constraints'] = kwargs.get('constraints', base.constraints)
            kwargs['policies'] = kwargs.get('policies', base.policies)
            kwargs['context'] = kwargs.get('context', base.context)

        super().__init__(**kwargs)


class Specialization(Base):
    __fields = [
        ('required', bool, False),
        'name',
        'target',
        ('attributes', list, list),
        ('constraints', list, list),
        # only links will actually allow policies
        ('policies', list, list),
        # only links will actually allow properties
        ('properties', list, list),
    ]

    def __init__(self, base=None, **kwargs):
        if base is not None:
            kwargs['required'] = kwargs.get('required', base.required)
            kwargs['name'] = kwargs.get('name', base.name)
            kwargs['target'] = kwargs.get('target', base.target)
            kwargs['attributes'] = kwargs.get('attributes', base.attributes)
            kwargs['constraints'] = kwargs.get('constraints', base.constraints)
            kwargs['policies'] = kwargs.get('policies', base.policies)
            kwargs['properties'] = kwargs.get('properties', base.properties)
            kwargs['context'] = kwargs.get('context', base.context)

        super().__init__(**kwargs)


# expressions
#
class LiteralNode(Base):
    __fields = ['value']


class StringLiteral(LiteralNode):
    pass


class RawLiteral(LiteralNode):
    pass


class MappingLiteral(LiteralNode):
    pass


class IntegerLiteral(LiteralNode):
    pass


class FloatLiteral(LiteralNode):
    pass


class BooleanLiteral(LiteralNode):
    pass


class ObjectName(Base):
    __fields = ['name', ('module', str, None)]

    def _extra_repr(self):
        return ' {!r}'.format(self.name)


class NamespaceExpression(QualName):
    __fields = ['left', 'right']


# property definitions
#
class Attribute(Base):
    __fields = ['name', 'value']


class Constraint(Attribute):
    def __init__(self, prop=None, **kwargs):
        if prop is not None:
            kwargs['name'] = kwargs.get('name', prop.name)
            kwargs['value'] = kwargs.get('value', prop.value)

        super().__init__(**kwargs)


# Statements
#
class Schema(Base):
    __fields = [('declarations', list, list)]


class ActionDeclaration(Declaration):
    pass


class AtomDeclaration(Declaration):
    pass


class AttributeDeclaration(Declaration):
    __fields = ['target']


class ConceptDeclaration(Declaration):
    __fields = [('links', list, list)]


class ConstraintDeclaration(Declaration):
    pass


class EventDeclaration(Declaration):
    pass


class LinkDeclaration(Declaration):
    __fields = [('properties', list, list)]


class LinkPropertyDeclaration(Declaration):
    pass


class Link(Specialization):
    pass


class LinkProperty(Specialization):
    pass


class Policy(Base):
    __fields = ['event', 'action']
