##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import textwrap

from edgedb.lang.common import parsing, context
from edgedb.lang.common.exceptions import get_context

from edgedb.lang import edgeql
from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.schema import ast as esast

from ...error import SchemaSyntaxError

from . import keywords
from . import tokens

from .tokens import *  # NOQA


def parse_edgeql(expression):
    ctx = expression.context

    try:
        node = edgeql.parse(expression.value)
    except parsing.ParserError as err:
        context.rebase_context(ctx, get_context(err, parsing.ParserContext))
        raise err

    context.rebase_ast_context(ctx, node)

    return node


def _parse_language(node):
    try:
        return esast.Language(node.val.upper())
    except ValueError as ex:
        raise SchemaSyntaxError(
            f'{node.val} is not a valid language',
            context=node.context) from None


class Nonterm(parsing.Nonterm):
    pass


class ListNonterm(parsing.ListNonterm, element=None):
    pass


class Identifier(Nonterm):
    def reduce_IDENT(self, *kids):
        self.val = kids[0].val

    def reduce_UnreservedKeyword(self, *kids):
        self.val = kids[0].val


class DotName(Nonterm):
    def reduce_Identifier(self, kid):
        self.val = esast.ObjectName(name=None, module=kid.val)

    def reduce_DotName_DOT_Identifier(self, *kids):
        self.val = kids[0].val
        self.val.module += '.' + kids[2].val


class ObjectName(Nonterm):
    def reduce_Identifier(self, kid):
        self.val = esast.ObjectName(name=kid.val)

    def reduce_DotName_DOUBLECOLON_Identifier(self, *kids):
        self.val = kids[0].val
        self.val.name = kids[2].val


class TypeName(Nonterm):
    def reduce_ObjectName(self, *kids):
        self.val = kids[0].val

    def reduce_ObjectName_LANGBRACKET_TypeList_RANGBRACKET(self, *kids):
        self.val = kids[0].val
        self.val.subtypes = kids[2].val


class BaseValue(Nonterm):
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


class Value(Nonterm):
    def reduce_BaseValue(self, *kids):
        self.val = kids[0].val

    def reduce_LBRACKET_ValueList_RBRACKET(self, *kids):
        self.val = esast.ArrayLiteral(value=[el.value for el in kids[1].val])


class ValueList(ListNonterm, element=BaseValue, separator=tokens.T_COMMA):
    pass


class RawString(Nonterm):
    def reduce_RawStr(self, *kids):
        self.val = kids[0].val
        text = self.val.value
        text = textwrap.dedent(text).strip().replace('\\\n', '')
        self.val.value = text


class RawStr(Nonterm):
    def reduce_RawStr_RAWLEADWS_RAWSTRING(self, *kids):
        self.val = kids[0].val
        self.val.value += kids[1].val + kids[2].val

    def reduce_RawStr_RAWSTRING(self, *kids):
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

    def reduce_NL_INDENT_DeclarationList_DEDENT(self, *kids):
        self.val = esast.Schema(declarations=kids[2].val)

    def reduce_INDENT_DeclarationList_DEDENT(self, *kids):
        self.val = esast.Schema(declarations=kids[1].val)


class ImportModule(Nonterm):
    def reduce_DotName(self, kid):
        self.val = esast.ImportModule(module=kid.val.module)

    def reduce_DotName_AS_Identifier(self, *kids):
        self.val = esast.ImportModule(module=kids[0].val.module,
                                      alias=kids[2].val)


class ImportModuleList(ListNonterm, element=ImportModule,
                       separator=tokens.T_COMMA):
    pass


class Declaration(Nonterm):
    def reduce_IMPORT_ImportModuleList_NL(self, *kids):
        self.val = esast.Import(modules=kids[1].val)

    def reduce_IMPORT_LPAREN_ImportModuleList_RPAREN_NL(self, *kids):
        self.val = esast.Import(modules=kids[2].val)

    def reduce_ABSTRACT_DeclarationBase(self, *kids):
        self.val = kids[1].val
        self.val.abstract = True

    def reduce_FINAL_DeclarationBase(self, *kids):
        self.val = kids[1].val
        self.val.final = True

    def reduce_DeclarationBase(self, kid):
        self.val = kid.val

    def reduce_FunctionDeclaration(self, kid):
        self.val = kid.val

    def reduce_AggregateDeclaration(self, kid):
        self.val = kid.val

    def reduce_ViewDeclaration(self, kid):
        self.val = kid.val


class DeclarationList(ListNonterm, element=Declaration):
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
                raise SchemaSyntaxError(
                    'illegal definition', context=spec.context)

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
                raise SchemaSyntaxError(
                    'illegal definition', context=spec.context)

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
                raise SchemaSyntaxError(
                    'illegal definition', context=spec.context)

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
        indexes = []

        for spec in kids[2].val:
            if isinstance(spec, esast.Constraint):
                constraints.append(spec)
            elif isinstance(spec, esast.Attribute):
                attributes.append(spec)
            elif isinstance(spec, esast.Index):
                indexes.append(spec)
            elif isinstance(spec, esast.Link):
                links.append(spec)
            else:
                raise SchemaSyntaxError(
                    'illegal definition', context=spec.context)

        self.val = esast.ConceptDeclaration(kids[1].val,
                                            attributes=attributes,
                                            constraints=constraints,
                                            indexes=indexes,
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
                raise SchemaSyntaxError(
                    'illegal definition', context=spec.context)

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
                raise SchemaSyntaxError(
                    'illegal definition', context=spec.context)

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
                raise SchemaSyntaxError(
                    'illegal definition', context=spec.context)

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
                raise SchemaSyntaxError(
                    'illegal definition', context=spec.context)

        self.val = esast.EventDeclaration(kids[1].val, attributes=attributes)


class ViewDeclaration(Nonterm):
    def reduce_VIEW_Identifier_COLON_NL_INDENT_Attributes_DEDENT(self, *kids):
        self.val = esast.ViewDeclaration(name=kids[1].val,
                                         attributes=kids[5].val)


class FunctionDeclaration(Nonterm):
    def reduce_FUNCTION_FunctionDeclCore(self, *kids):
        self.val = kids[1].val

        if self.val.initial_value is not None:
            raise SchemaSyntaxError(
                "unexpected 'initial value' in function definition",
                context=self.val.initial_value.context)

        if self.val.code is None:
            raise SchemaSyntaxError("missing 'from' in function definition",
                                    context=kids[0].context)


class AggregateDeclaration(Nonterm):
    def reduce_AGGREGATE_FunctionDeclCore(self, *kids):
        self.val = kids[1].val
        self.val.aggregate = True

        if self.val.initial_value is None:
            raise SchemaSyntaxError(
                "missing 'initial value' in aggregate definition",
                context=kids[0].context)

        if self.val.code is None:
            raise SchemaSyntaxError("missing 'from' in aggregate definition",
                                    context=kids[0].context)


class OptSetOf(Nonterm):
    def reduce_SET_OF(self, *kids):
        self.val = True

    def reduce_empty(self):
        self.val = False


class FunctionDeclCore(Nonterm):
    def reduce_FunctionDeclCore(self, *kids):
        r"""%reduce \
                Identifier FunctionArgs \
                ARROW OptSetOf TypeName FunctionSpecsBlob \
        """
        attributes = []
        init_val = None
        code = None

        for spec in kids[5].val:
            if isinstance(spec, esast.Attribute):
                if spec.name.name == 'initial value':
                    init_val = spec.value
                else:
                    attributes.append(spec)
            elif code is None and isinstance(spec, esast.FunctionCode):
                code = spec
            else:
                raise SchemaSyntaxError(
                    'illegal definition', context=spec.context)

        self.val = esast.FunctionDeclaration(
            name=kids[0].val,
            args=kids[1].val,
            set_returning=kids[3].val,
            returning=kids[4].val,
            attributes=attributes,
            initial_value=init_val,
            code=code,
        )


class FunctionSpecsBlob(Nonterm):
    def reduce_COLON_NL_INDENT_FunctionSpecs_DEDENT(self, *kids):
        self.val = kids[3].val


class FunctionSpec(Nonterm):
    def reduce_FROM_Identifier_ColonValue(self, *kids):
        self.val = esast.FunctionCode(language=_parse_language(kids[1]),
                                      code=kids[2].val.value)

    def reduce_FROM_Identifier_FUNCTION_COLON_Identifier_NL(self, *kids):
        self.val = esast.FunctionCode(language=_parse_language(kids[1]),
                                      from_name=kids[4].val)

    def reduce_DeclarationSpec(self, *kids):
        self.val = kids[0].val

    def reduce_INITIAL_VALUE_ColonValue(self, *kids):
        self.val = esast.Attribute(
            name=esast.ObjectName(name='initial value'),
            value=kids[2].val)


class FunctionSpecs(ListNonterm, element=FunctionSpec):
    pass


class OptDefault(Nonterm):
    def reduce_empty(self):
        self.val = None

    def reduce_EQUALS_Value(self, *kids):
        self.val = kids[1].val


class OptVariadic(Nonterm):
    def reduce_empty(self):
        self.val = False

    def reduce_STAR(self, *kids):
        self.val = True


class FuncDeclArg(Nonterm):
    def reduce_OptVariadic_TypeName_OptDefault(self, *kids):
        self.val = esast.FuncArg(
            variadic=kids[0].val,
            name=None,
            type=kids[1].val,
            default=kids[2].val
        )

    def reduce_OptVariadic_Identifier_COLON_TypeName_OptDefault(self, *kids):
        self.val = esast.FuncArg(
            variadic=kids[0].val,
            name=kids[1].val,
            type=kids[3].val,
            default=kids[4].val
        )


class FuncDeclArgList(ListNonterm, element=FuncDeclArg,
                      separator=tokens.T_COMMA):
    pass


class FunctionArgs(Nonterm):
    def reduce_LPAREN_RPAREN(self, *kids):
        self.val = []

    def reduce_LPAREN_FuncDeclArgList_RPAREN(self, *kids):
        args = kids[1].val

        default_arg_seen = False
        variadic_arg_seen = False
        for arg in args:
            if arg.variadic:
                if variadic_arg_seen:
                    raise SchemaSyntaxError('more than one variadic argument',
                                            context=arg.context)
                else:
                    variadic_arg_seen = True
            else:
                if variadic_arg_seen:
                    raise SchemaSyntaxError(
                        'non-variadic argument follows variadic argument',
                        context=arg.context)

            if arg.default is None:
                if default_arg_seen and not arg.variadic:
                    raise SchemaSyntaxError(
                        'non-default argument follows default argument',
                        context=arg.context)
            else:
                default_arg_seen = True

        self.val = args


class NameAndExtends(Nonterm):
    def reduce_Identifier_EXTENDS_NameList(self, *kids):
        self.val = esast.Declaration(name=kids[0].val, extends=kids[2].val)

    def reduce_Identifier_EXTENDS_LPAREN_NameList_RPAREN(self, *kids):
        self.val = esast.Declaration(name=kids[0].val, extends=kids[3].val)

    def reduce_Identifier(self, kid):
        self.val = esast.Declaration(name=kid.val)


class NameList(ListNonterm, element=ObjectName, separator=tokens.T_COMMA):
    pass


class TypeList(ListNonterm, element=TypeName, separator=tokens.T_COMMA):
    pass


class DeclarationSpec(Nonterm):
    def reduce_DeclarationSpecBase(self, kid):
        self.val = kid.val

    def reduce_Policy(self, kid):
        self.val = kid.val


class DeclarationSpecs(ListNonterm, element=DeclarationSpec):
    pass


class DeclarationSpecBase(Nonterm):
    def reduce_Link(self, kid):
        self.val = kid.val

    def reduce_ABSTRACT_Constraint(self, *kids):
        self.val = kids[1].val
        self.val.abstract = True

    def reduce_Constraint(self, kid):
        self.val = kid.val

    def reduce_LinkProperty(self, kid):
        self.val = kid.val

    def reduce_Attribute(self, kid):
        self.val = kid.val

    def reduce_Index(self, kid):
        self.val = kid.val


class DeclarationSpecsBlob(Nonterm):
    def reduce_COLON_NL_INDENT_DeclarationSpecs_DEDENT(self, *kids):
        self.val = kids[3].val


# this Nonterminal should NOT automatically compute context as it
# relies on an external parser
#
class TurnstileBlob(parsing.Nonterm):
    def reduce_TURNSTILE_RawString_NL(self, *kids):
        self.val = parse_edgeql(kids[1].val)

    def reduce_TURNSTILE_NL_INDENT_RawString_NL_DEDENT(self, *kids):
        self.val = parse_edgeql(kids[3].val)


class Link(Nonterm):
    def reduce_LINK_Spec(self, *kids):
        self.val = esast.Link(kids[1].val)

    def reduce_REQUIRED_LINK_Spec(self, *kids):
        self.val = esast.Link(kids[2].val)
        self.val.required = True


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

    def reduce_ObjectName_TO_TypeList_NL(self, *kids):
        self.val = esast.Specialization(name=kids[0].val, target=kids[2].val)

    def reduce_ObjectName_TO_TypeList_DeclarationSpecsBlob(
            self, *kids):
        self._processdecl_specs(kids[0], kids[2], kids[3])

    def reduce_ObjectName_TurnstileBlob(self, *kids):
        self.val = esast.Specialization(name=kids[0].val, target=kids[1].val)

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
                raise SchemaSyntaxError(
                    'illegal definition', context=spec.context)

        if target:
            target = target.val

        self.val = esast.Specialization(
            name=name.val, target=target,
            attributes=attributes,
            constraints=constraints,
            policies=policies,
            properties=properties)


class Policy(Nonterm):
    def reduce_ON_ObjectName_ObjectName_NL(self, *kids):
        self.val = esast.Policy(event=kids[1].val, action=kids[2].val)


class Index(Nonterm):
    def reduce_INDEX_ObjectName_TurnstileBlob(self, *kids):
        self.val = esast.Index(name=kids[1].val, expression=kids[2].val)


class Constraint(Nonterm):
    def reduce_CONSTRAINT_ObjectName_NL(self, *kids):
        self.val = esast.Constraint(name=kids[1].val)

    def reduce_CONSTRAINT_ObjectName_COLON_NL_INDENT_Attributes_DEDENT(
            self, *kids):
        self.val = esast.Constraint(name=kids[1].val, attributes=kids[5].val)

    def reduce_CONSTRAINT_ObjectName_TurnstileBlob(self, *kids):
        attributes = [
            esast.Attribute(
                name=esast.ObjectName(name='args'),
                value=qlast.NamedTuple(
                    elements=[
                        qlast.TupleElement(
                            name=qlast.ClassRef(name='param'),
                            val=kids[2].val)
                    ]))
        ]

        self.val = esast.Constraint(
            name=kids[1].val, attributes=attributes)


class Attribute(Nonterm):
    def reduce_ObjectName_ColonValue(self, *kids):
        self.val = esast.Attribute(name=kids[0].val, value=kids[1].val)

    def reduce_ObjectName_TurnstileBlob(self, *kids):
        self.val = esast.Attribute(name=kids[0].val, value=kids[1].val)


class Attributes(ListNonterm, element=Attribute):
    pass


class ColonValue(parsing.Nonterm):
    def reduce_COLON_Value_NL(self, *kids):
        self.val = kids[1].val

    def reduce_COLONGT_NL_INDENT_RawString_NL_DEDENT(self, *kids):
        self.val = kids[3].val

    def reduce_COLON_NL_INDENT_Value_NL_DEDENT(self, *kids):
        self.val = kids[3].val


class KeywordMeta(parsing.NontermMeta):
    def __new__(mcls, name, bases, dct, *, type):
        result = super().__new__(mcls, name, bases, dct)

        assert type in keywords.keyword_types

        for val, token in keywords.by_type[type].items():
            def method(inst, *kids):
                inst.val = kids[0].val
            method = context.has_context(method)
            method.__doc__ = "%%reduce %s" % token
            method.__name__ = 'reduce_%s' % token
            setattr(result, method.__name__, method)

        return result

    def __init__(cls, name, bases, dct, *, type):
        super().__init__(name, bases, dct)


class UnreservedKeyword(Nonterm, metaclass=KeywordMeta,
                        type=keywords.UNRESERVED_KEYWORD):
    pass
