##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import parsing, context
from edgedb.lang.common.exceptions import _get_context

from edgedb.lang import caosql
from edgedb.lang.schema import ast as esast

from .tokens import *


def parse_edgeql(expression):
    ctx = expression.context

    try:
        node = caosql.parse(expression.value)
    except parsing.ParserError as err:
        context.rebase_context(ctx, _get_context(err, parsing.ParserContext))
        raise err

    return node


class Nonterm(context.Nonterm):
    pass


class DotName(Nonterm):
    def reduce_IDENT(self, kid):
        self.val = esast.ObjectName(name=None, module=kid.val)

    def reduce_DotName_DOT_IDENT(self, *kids):
        self.val = kids[0].val
        self.val.module += '.' + kids[2].val


class ObjectName(Nonterm):
    def reduce_IDENT(self, kid):
        self.val = esast.ObjectName(name=kid.val)

    def reduce_DotName_DOUBLECOLON_IDENT(self, *kids):
        self.val = kids[0].val
        self.val.name = kids[2].val


class Value(Nonterm):
    def reduce_ICONST(self, kid):
        self.val = esast.IntegerLiteral(value=kid.normalized_value)

    def reduce_FCONST(self, kid):
        self.val = esast.FloatLiteral(value=kid.normalized_value)

    def reduce_STRING(self, kid):
        self.val = esast.StringLiteral(value=kid.normalized_value)

    def reduce_TRUE(self, kid):
        self.val = esast.BooleanLiteral(value=True)

    def reduce_FALSE(self, kid):
        self.val = esast.BooleanLiteral(value=False)

    def reduce_MAPPING(self, kid):
        self.val = esast.MappingLiteral(value=kid.val)


class RawString(Nonterm):
    def reduce_RawString_RAWLEADWS_RAWSTRING(self, *kids):
        self.val = kids[0].val
        self.val.value += kids[1].val + kids[2].val

    def reduce_RawString_RAWSTRING(self, *kids):
        self.val = kids[0].val
        self.val.value += kids[1].val

    def reduce_RAWLEADWS_RAWSTRING(self, *kids):
        self.val = esast.RawLiteral(value=kids[0].val + kids[1].val)

    def reduce_RAWSTRING(self, kid):
        self.val = esast.RawLiteral(value=kid.val)


class Schema(Nonterm):
    "%start"

    def reduce_NL_DeclarationList(self, *kids):
        self.val = esast.Schema(declarations=kids[1].val)

    def reduce_DeclarationList(self, kid):
        self.val = esast.Schema(declarations=kid.val)


class Declaration(Nonterm):
    def reduce_ABSTRACT_DeclarationBase(self, *kids):
        self.val = kids[1].val
        self.val.abstract = True

    def reduce_FINAL_DeclarationBase(self, *kids):
        self.val = kids[1].val
        self.val.final = True

    def reduce_DeclarationBase(self, kid):
        self.val = kid.val


class DeclarationList(parsing.ListNonterm, element=Declaration):
    pass


class DeclarationBase(Nonterm):
    def reduce_ActionDeclaration(self, kid):
        self.val = kid.val

    def reduce_AtomDeclaration(self, kid):
        self.val = kid.val

    def reduce_AttributeDeclaration(self, kid):
        self.val = kid.val

    def reduce_ConceptDeclaration(self, kid):
        self.val = kid.val

    def reduce_ConstraintDeclaration(self, kid):
        self.val = kid.val

    def reduce_LinkDeclaration(self, kid):
        self.val = kid.val

    def reduce_LinkPropertyDeclaration(self, kid):
        self.val = kid.val

    def reduce_EventDeclaration(self, kid):
        self.val = kid.val


class ActionDeclaration(Nonterm):
    def reduce_ACTION_NameAndExtends_NL(self, *kids):
        self.val = esast.ActionDeclaration(kids[1].val)

    def reduce_ACTION_NameAndExtends_DeclarationSpecsBlob(
            self, *kids):
        attributes = []

        for spec in kids[2].val:
            if isinstance(spec, esast.Attribute):
                attributes.append(spec)
            else:
                raise Exception('parse error')

        self.val = esast.ActionDeclaration(kids[1].val, attributes=attributes)


class AtomDeclaration(Nonterm):
    def reduce_ATOM_NameAndExtends_NL(self, *kids):
        self.val = esast.AtomDeclaration(kids[1].val)

    def reduce_ATOM_NameAndExtends_DeclarationSpecsBlob(
            self, *kids):
        constraints = []
        attributes = []

        for spec in kids[2].val:
            if isinstance(spec, esast.Constraint):
                constraints.append(spec)
            elif isinstance(spec, esast.Attribute):
                attributes.append(spec)
            else:
                raise Exception('parse error')

        self.val = esast.AtomDeclaration(kids[1].val,
                                         attributes=attributes,
                                         constraints=constraints)


class AttributeDeclaration(Nonterm):
    def reduce_ATTRIBUTE_NameAndExtends_NL(self, *kids):
        self.val = esast.AttributeDeclaration(kids[1].val)

    def reduce_ATTRIBUTE_NameAndExtends_DeclarationSpecsBlob(
            self, *kids):
        attributes = []
        attr_target = None

        for spec in kids[2].val:
            if isinstance(spec, esast.Attribute):
                if spec.name == 'target':
                    attr_target = spec.value
                else:
                    attributes.append(spec)
            else:
                raise Exception('parse error')

        self.val = esast.AttributeDeclaration(kids[1].val,
                                              target=attr_target,
                                              attributes=attributes)


class ConceptDeclaration(Nonterm):
    def reduce_CONCEPT_NameAndExtends_NL(self, *kids):
        self.val = esast.ConceptDeclaration(kids[1].val)

    def reduce_CONCEPT_NameAndExtends_DeclarationSpecsBlob(
            self, *kids):
        constraints = []
        links = []
        attributes = []

        for spec in kids[2].val:
            if isinstance(spec, esast.Constraint):
                constraints.append(spec)
            elif isinstance(spec, esast.Attribute):
                attributes.append(spec)
            elif isinstance(spec, esast.Link):
                links.append(spec)
            else:
                raise Exception('parse error')

        self.val = esast.ConceptDeclaration(kids[1].val,
                                            attributes=attributes,
                                            constraints=constraints,
                                            links=links)


class ConstraintDeclaration(Nonterm):
    def reduce_CONSTRAINT_NameAndExtends_NL(self, *kids):
        self.val = esast.ConstraintDeclaration(kids[1].val)

    def reduce_CONSTRAINT_NameAndExtends_DeclarationSpecsBlob(
            self, *kids):
        attributes = []

        for spec in kids[2].val:
            if isinstance(spec, esast.Attribute):
                attributes.append(spec)
            else:
                raise Exception('parse error')

        self.val = esast.ConstraintDeclaration(kids[1].val,
                                               attributes=attributes)


class LinkDeclaration(Nonterm):
    def reduce_LINK_NameAndExtends_NL(self, *kids):
        self.val = esast.LinkDeclaration(kids[1].val)

    def reduce_LINK_NameAndExtends_DeclarationSpecsBlob(
            self, *kids):
        constraints = []
        properties = []
        policies = []
        attributes = []

        for spec in kids[2].val:
            if isinstance(spec, esast.Constraint):
                constraints.append(spec)
            elif isinstance(spec, esast.Attribute):
                attributes.append(spec)
            elif isinstance(spec, esast.LinkProperty):
                properties.append(spec)
            elif isinstance(spec, esast.Policy):
                policies.append(spec)
            else:
                raise Exception('parse error')

        self.val = esast.LinkDeclaration(kids[1].val,
                                         attributes=attributes,
                                         constraints=constraints,
                                         properties=properties,
                                         policies=policies)


class LinkPropertyDeclaration(Nonterm):
    def reduce_LINKPROPERTY_NameAndExtends_NL(self, *kids):
        self.val = esast.LinkPropertyDeclaration(kids[1].val)

    def reduce_LINKPROPERTY_NameAndExtends_DeclarationSpecsBlob(
            self, *kids):
        attributes = []

        for spec in kids[2].val:
            if isinstance(spec, esast.Attribute):
                attributes.append(spec)
            else:
                raise Exception('parse error')

        self.val = esast.LinkPropertyDeclaration(kids[1].val,
                                                 attributes=attributes)


class EventDeclaration(Nonterm):
    def reduce_EVENT_NameAndExtends_NL(self, *kids):
        self.val = esast.EventDeclaration(kids[1].val)

    def reduce_EVENT_NameAndExtends_DeclarationSpecsBlob(
            self, *kids):
        attributes = []

        for spec in kids[2].val:
            if isinstance(spec, esast.Attribute):
                attributes.append(spec)
            else:
                raise Exception('parse error')

        self.val = esast.EventDeclaration(kids[1].val, attributes=attributes)


class NameAndExtends(Nonterm):
    def reduce_IDENT_EXTENDS_NameList(self, *kids):
        self.val = esast.Declaration(name=kids[0].val, extends=kids[2].val)

    def reduce_IDENT(self, kid):
        self.val = esast.Declaration(name=kid.val)


class NameList(parsing.ListNonterm, element=ObjectName, separator=T_COMMA):
    pass


class DeclarationSpec(Nonterm):
    def reduce_REQUIRED_DeclarationSpecBase(self, *kids):
        self.val = kids[1].val
        self.val.required = True

    def reduce_DeclarationSpecBase(self, kid):
        self.val = kid.val

    def reduce_Policy(self, kid):
        self.val = kid.val


class DeclarationSpecs(parsing.ListNonterm, element=DeclarationSpec):
    pass


class DeclarationSpecBase(Nonterm):
    def reduce_Link(self, kid):
        self.val = kid.val

    def reduce_Constraint(self, kid):
        self.val = kid.val

    def reduce_LinkProperty(self, kid):
        self.val = kid.val

    def reduce_Attribute(self, kid):
        self.val = kid.val


class DeclarationSpecsBlob(Nonterm):
    def reduce_COLON_NL_INDENT_DeclarationSpecs_DEDENT(self, *kids):
        self.val = kids[3].val


class Link(Nonterm):
    def reduce_LINK_Spec(self, *kids):
        self.val = esast.Link(kids[1].val)


class LinkProperty(Nonterm):
    def reduce_LINKPROPERTY_Spec(self, *kids):
        if kids[1].val.policies or kids[1].val.properties:
            raise Exception('parse error')
        self.val = esast.LinkProperty(kids[1].val)


class Spec(Nonterm):
    def reduce_ObjectName_NL(self, *kids):
        self.val = esast.Specialization(name=kids[0].val, target=None)

    def reduce_ObjectName_DeclarationSpecsBlob(self, *kids):
        self._processdecl_specs(kids[0], None, kids[1])

    def reduce_ObjectName_ARROW_NameList_NL(self, *kids):
        self.val = esast.Specialization(name=kids[0].val, target=kids[2].val)

    def reduce_ObjectName_ARROW_NameList_DeclarationSpecsBlob(
            self, *kids):
        self._processdecl_specs(kids[0], kids[2], kids[3])

    def reduce_ObjectName_TURNSTILE_RawString_NL(self, *kids):
        self.val = esast.Specialization(
            name=kids[0].val,
            target=parse_edgeql(kids[2].val))

    def reduce_ObjectName_TURNSTILE_NL_INDENT_RawString_NL_DEDENT(self, *kids):
        self.val = esast.Specialization(
            name=kids[0].val,
            target=parse_edgeql(kids[4].val))

    def _processdecl_specs(self, name, target, specs):
        constraints = []
        attributes = []
        policies = []
        properties = []

        for spec in specs.val:
            if isinstance(spec, esast.Constraint):
                constraints.append(spec)
            elif isinstance(spec, esast.Attribute):
                attributes.append(spec)
            elif isinstance(spec, esast.LinkProperty):
                properties.append(spec)
            elif isinstance(spec, esast.Policy):
                policies.append(spec)
            else:
                raise Exception('parse error')

        if target:
            target = target.val
            if len(target) == 1:
                target = target[0]

        self.val = esast.Specialization(
            name=name.val, target=target,
            attributes=attributes,
            constraints=constraints,
            policies=policies,
            properties=properties)


class Policy(Nonterm):
    def reduce_ON_ObjectName_ObjectName_NL(self, *kids):
        self.val = esast.Policy(event=kids[1].val, action=kids[2].val)


class Constraint(Nonterm):
    def reduce_CONSTRAINT_ObjectName_NL(self, *kids):
        self.val = esast.Constraint(name=kids[1].val)

    def reduce_CONSTRAINT_Attribute(self, *kids):
        self.val = esast.Constraint(kids[1].val)


class Attribute(Nonterm):
    def reduce_ObjectName_COLON_Value_NL(self, *kids):
        self.val = esast.Attribute(name=kids[0].val, value=kids[2].val)

    def reduce_ObjectName_COLON_NL_INDENT_Value_NL_DEDENT(self, *kids):
        self.val = esast.Attribute(name=kids[0].val, value=kids[4].val)

    def reduce_ObjectName_TURNSTILE_RawString_NL(self, *kids):
        self.val = esast.Attribute(
            name=kids[0].val,
            value=parse_edgeql(kids[2].val))

    def reduce_ObjectName_TURNSTILE_NL_INDENT_RawString_NL_DEDENT(self, *kids):
        self.val = esast.Attribute(
            name=kids[0].val,
            value=parse_edgeql(kids[4].val))
