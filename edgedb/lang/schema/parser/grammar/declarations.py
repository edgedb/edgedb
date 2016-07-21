##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import parsing
from edgedb.lang.common.exceptions import _get_context

from edgedb.lang import caosql
from edgedb.lang.schema import ast as esast

from .tokens import *


def parse_edgeql(expression):
    ctx = expression.context

    try:
        return caosql.parse(expression.value)
    except parsing.ParserError as err:
        err_ctx = _get_context(err, parsing.ParserContext)

        err_ctx.name = ctx.name
        err_ctx.buffer = ctx.buffer

        if err_ctx.start.line == 1:
            err_ctx.start.column += ctx.start.column - 1
        err_ctx.start.line += ctx.start.line - 1
        err_ctx.start.pointer += ctx.start.pointer

        raise err


def get_context(*kids):
    start = kids[0]
    end = kids[-1]

    if isinstance(start.val, list):
        start = start.val[-1]
    if isinstance(end.val, list):
        end = end.val[-1]

    if isinstance(start, Nonterm):
        start = start.val
    if isinstance(end, Nonterm):
        end = end.val

    return parsing.ParserContext(name=start.context.name,
                                 buffer=start.context.buffer,
                                 start=start.context.start,
                                 end=end.context.end)


class Nonterm(parsing.Nonterm):
    pass


class DotName(Nonterm):
    def reduce_IDENT(self, kid):
        self.val = esast.ObjectName(name=None, module=kid.val,
                                    context=get_context(kid))

    def reduce_DotName_DOT_IDENT(self, *kids):
        self.val = kids[0].val
        self.val.module += '.' + kids[2].val
        self.context = get_context(*kids)


class ObjectName(Nonterm):
    def reduce_IDENT(self, kid):
        self.val = esast.ObjectName(name=kid.val,
                                    context=get_context(kid))

    def reduce_DotName_DOUBLECOLON_IDENT(self, *kids):
        self.val = kids[0].val
        self.val.name = kids[2].val
        self.context = get_context(*kids)


class Value(Nonterm):
    def reduce_ICONST(self, kid):
        self.val = esast.IntegerLiteral(value=kid.val,
                                        context=get_context(kid))

    def reduce_FCONST(self, kid):
        self.val = esast.FloatLiteral(value=kid.val, context=get_context(kid))

    def reduce_STRING(self, kid):
        self.val = esast.StringLiteral(value=kid.val, context=get_context(kid))

    def reduce_TRUE(self, kid):
        self.val = esast.BooleanLiteral(value=True, context=get_context(kid))

    def reduce_FALSE(self, kid):
        self.val = esast.BooleanLiteral(value=False, context=get_context(kid))

    def reduce_MAPPING(self, kid):
        self.val = esast.MappingLiteral(value=kid.val,
                                        context=get_context(kid))


class RawString(Nonterm):
    def reduce_RawString_RAWLEADWS_RAWSTRING(self, *kids):
        self.val = kids[0].val
        self.val.value += kids[1].val + kids[2].val
        self.val.context = get_context(*kids)

    def reduce_RawString_RAWSTRING(self, *kids):
        self.val = kids[0].val
        self.val.value += kids[1].val
        self.val.context = get_context(*kids)

    def reduce_RAWLEADWS_RAWSTRING(self, *kids):
        self.val = esast.RawLiteral(value=kids[0].val + kids[1].val,
                                    context=get_context(*kids))

    def reduce_RAWSTRING(self, kid):
        self.val = esast.RawLiteral(value=kid.val, context=get_context(kid))


class Schema(Nonterm):
    "%start"

    def reduce_NL_DeclarationList(self, *kids):
        self.val = esast.Schema(declarations=kids[1].val,
                                context=get_context(*kids))

    def reduce_DeclarationList(self, kid):
        self.val = esast.Schema(declarations=kid.val, context=get_context(kid))


class Declaration(Nonterm):
    def reduce_ABSTRACT_DeclarationBase(self, *kids):
        self.val = kids[1].val
        self.val.abstract = True
        self.val.context = get_context(*kids)

    def reduce_FINAL_DeclarationBase(self, *kids):
        self.val = kids[1].val
        self.val.final = True
        self.val.context = get_context(*kids)

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
        self.val = esast.ActionDeclaration(kids[1].val,
                                           context=get_context(*kids))

    def reduce_ACTION_NameAndExtends_DeclarationSpecsBlob(
            self, *kids):
        attributes = []

        for spec in kids[2].val:
            if isinstance(spec, esast.Attribute):
                attributes.append(spec)
            else:
                raise Exception('parse error')

        self.val = esast.ActionDeclaration(kids[1].val,
                                           attributes=attributes,
                                           context=get_context(*kids))


class AtomDeclaration(Nonterm):
    def reduce_ATOM_NameAndExtends_NL(self, *kids):
        self.val = esast.AtomDeclaration(kids[1].val,
                                         context=get_context(*kids))

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
                                         constraints=constraints,
                                         context=get_context(*kids))


class AttributeDeclaration(Nonterm):
    def reduce_ATTRIBUTE_NameAndExtends_NL(self, *kids):
        self.val = esast.AttributeDeclaration(kids[1].val,
                                              context=get_context(*kids))

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
                                              attributes=attributes,
                                              context=get_context(*kids))


class ConceptDeclaration(Nonterm):
    def reduce_CONCEPT_NameAndExtends_NL(self, *kids):
        self.val = esast.ConceptDeclaration(kids[1].val,
                                            context=get_context(*kids))

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
                                            links=links,
                                            context=get_context(*kids))


class ConstraintDeclaration(Nonterm):
    def reduce_CONSTRAINT_NameAndExtends_NL(self, *kids):
        self.val = esast.ConstraintDeclaration(kids[1].val,
                                               context=get_context(*kids))

    def reduce_CONSTRAINT_NameAndExtends_DeclarationSpecsBlob(
            self, *kids):
        attributes = []

        for spec in kids[2].val:
            if isinstance(spec, esast.Attribute):
                attributes.append(spec)
            else:
                raise Exception('parse error')

        self.val = esast.ConstraintDeclaration(kids[1].val,
                                               attributes=attributes,
                                               context=get_context(*kids))


class LinkDeclaration(Nonterm):
    def reduce_LINK_NameAndExtends_NL(self, *kids):
        self.val = esast.LinkDeclaration(kids[1].val,
                                         context=get_context(*kids))

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
                                         policies=policies,
                                         context=get_context(*kids))


class LinkPropertyDeclaration(Nonterm):
    def reduce_LINKPROPERTY_NameAndExtends_NL(self, *kids):
        self.val = esast.LinkPropertyDeclaration(kids[1].val,
                                                 context=get_context(*kids))

    def reduce_LINKPROPERTY_NameAndExtends_DeclarationSpecsBlob(
            self, *kids):
        attributes = []

        for spec in kids[2].val:
            if isinstance(spec, esast.Attribute):
                attributes.append(spec)
            else:
                raise Exception('parse error')

        self.val = esast.LinkPropertyDeclaration(kids[1].val,
                                                 attributes=attributes,
                                                 context=get_context(*kids))


class EventDeclaration(Nonterm):
    def reduce_EVENT_NameAndExtends_NL(self, *kids):
        self.val = esast.EventDeclaration(kids[1].val,
                                          context=get_context(*kids))

    def reduce_EVENT_NameAndExtends_DeclarationSpecsBlob(
            self, *kids):
        attributes = []

        for spec in kids[2].val:
            if isinstance(spec, esast.Attribute):
                attributes.append(spec)
            else:
                raise Exception('parse error')

        self.val = esast.EventDeclaration(kids[1].val, attributes=attributes,
                                          context=get_context(*kids))


class NameAndExtends(Nonterm):
    def reduce_IDENT_EXTENDS_ExtendsList(self, *kids):
        self.val = esast.Declaration(name=kids[0].val, extends=kids[2].val,
                                     context=get_context(*kids))

    def reduce_IDENT(self, kid):
        self.val = esast.Declaration(name=kid.val, context=get_context(kid))


class ExtendsList(parsing.ListNonterm, element=ObjectName, separator=T_COMMA):
    pass


class DeclarationSpec(Nonterm):
    def reduce_REQUIRED_DeclarationSpecBase(self, *kids):
        self.val = kids[1].val
        self.val.required = True
        self.val.context = get_context(*kids)

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
        self.val = esast.Link(kids[1].val, context=get_context(*kids))


class LinkProperty(Nonterm):
    def reduce_LINKPROPERTY_Spec(self, *kids):
        if kids[1].val.policies:
            raise Exception('parse error')
        self.val = esast.LinkProperty(kids[1].val, context=get_context(*kids))


class Spec(Nonterm):
    def reduce_ObjectName_NL(self, *kids):
        self.val = esast.Spec(name=kids[0].val, target=None,
                              context=get_context(*kids))

    def reduce_ObjectName_DeclarationSpecsBlob(self, *kids):
        self._processdecl_specs(kids[0], None, kids[1],
                                context=get_context(*kids))

    def reduce_ObjectName_ARROW_ObjectName_NL(self, *kids):
        self.val = esast.Spec(name=kids[0].val, target=kids[2].val,
                              context=get_context(*kids))

    def reduce_ObjectName_ARROW_ObjectName_DeclarationSpecsBlob(
            self, *kids):
        self._processdecl_specs(kids[0], kids[2], kids[3],
                                context=get_context(*kids))

    def reduce_ObjectName_TURNSTILE_RawString_NL(self, *kids):
        self.val = esast.Spec(
            name=kids[0].val,
            target=parse_edgeql(kids[2].val),
            context=get_context(*kids))

    def reduce_ObjectName_TURNSTILE_NL_INDENT_RawString_NL_DEDENT(self, *kids):
        self.val = esast.Spec(
            name=kids[0].val,
            target=parse_edgeql(kids[4].val),
            context=get_context(*kids))

    def _processdecl_specs(self, name, target, specs, context):
        constraints = []
        attributes = []
        policies = []

        for spec in specs.val:
            if isinstance(spec, esast.Constraint):
                constraints.append(spec)
            elif isinstance(spec, esast.Attribute):
                attributes.append(spec)
            elif isinstance(spec, esast.Policy):
                policies.append(spec)
            else:
                raise Exception('parse error')

        self.val = esast.Spec(name=name.val, target=target.val,
                              attributes=attributes,
                              constraints=constraints,
                              policies=policies,
                              context=context)


class Policy(Nonterm):
    def reduce_ON_ObjectName_ObjectName_NL(self, *kids):
        self.val = esast.Policy(event=kids[1].val, action=kids[2].val,
                                context=get_context(*kids))


class Constraint(Nonterm):
    def reduce_CONSTRAINT_ObjectName_NL(self, *kids):
        self.val = esast.Constraint(name=kids[1].val,
                                    context=get_context(*kids))

    def reduce_CONSTRAINT_Attribute(self, *kids):
        self.val = esast.Constraint(kids[1].val, context=get_context(*kids))


class Attribute(Nonterm):
    def reduce_ObjectName_COLON_Value_NL(self, *kids):
        self.val = esast.Attribute(name=kids[0].val, value=kids[2].val,
                                   context=get_context(*kids))

    def reduce_ObjectName_COLON_NL_INDENT_Value_NL_DEDENT(self, *kids):
        self.val = esast.Attribute(name=kids[0].val, value=kids[4].val,
                                   context=get_context(*kids))

    def reduce_ObjectName_TURNSTILE_RawString_NL(self, *kids):
        self.val = esast.Attribute(
            name=kids[0].val,
            value=parse_edgeql(kids[2].val),
            context=get_context(*kids))

    def reduce_ObjectName_TURNSTILE_NL_INDENT_RawString_NL_DEDENT(self, *kids):
        self.val = esast.Attribute(
            name=kids[0].val,
            value=parse_edgeql(kids[4].val),
            context=get_context(*kids))
