##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import parsing
from edgedb.lang.graphql import ast as gqlast

from .tokens import *


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


class DefaultValue(Nonterm):
    def reduce_INTEGER(self, kid):
        self.val = gqlast.IntegerLiteral(value=kid.normalized_value,
                                         context=get_context(kid))

    def reduce_FLOAT(self, kid):
        self.val = gqlast.FloatLiteral(value=kid.normalized_value,
                                       context=get_context(kid))

    def reduce_TRUE(self, kid):
        self.val = gqlast.BooleanLiteral(value=True,
                                         context=get_context(kid))

    def reduce_FALSE(self, kid):
        self.val = gqlast.BooleanLiteral(value=False,
                                         context=get_context(kid))

    def reduce_STRING(self, kid):
        self.val = gqlast.StringLiteral(value=kid.normalized_value,
                                        context=get_context(kid))

    def reduce_IDENT(self, kid):
        self.val = gqlast.EnumLiteral(value=kid.val,
                                      context=get_context(kid))

    def reduce_LSBRACKET_RSBRACKET(self, *kids):
        self.val = gqlast.ListLiteral(value=[],
                                      context=get_context(*kids))

    def reduce_LSBRACKET_ValueList_RSBRACKET(self, *kids):
        self.val = gqlast.ListLiteral(value=kids[1].val,
                                      context=get_context(*kids))

    def reduce_LCBRACKET_RCBRACKET(self, *kids):
        self.val = gqlast.ObjectLiteral(value={},
                                        context=get_context(*kids))

    def reduce_LCBRACKET_ObjectFieldList_RCBRACKET(self, *kids):
        self.val = gqlast.ObjectLiteral(value=kids[1].val,
                                        context=get_context(*kids))


class Value(Nonterm):
    def reduce_DefaultValue(self, kid):
        self.val = kid.val

    def reduce_VAR(self, kid):
        self.val = gqlast.Variable(value=kid.val,
                                   context=get_context(kid))


class ValueList(parsing.ListNonterm, element=Value):
    pass


class ObjectField(Nonterm):
    def reduce_IDENT_COLON_Value(self, *kids):
        self.val = gqlast.ObjectField(name=kids[0].val, value=kids[2].val,
                                      context=get_context(*kids))


class ObjectFieldList(parsing.ListNonterm, element=ObjectField):
    pass


class OptValue(Nonterm):
    def reduce_DefaultValue(self, kid):
        self.val = kid.val

    def reduce_empty(self):
        self.val = None


class OptNameTok(Nonterm):
    def reduce_IDENT(self, kid):
        self.val = kid

    def reduce_empty(self):
        self.val = None


class Document(Nonterm):
    "%start"

    def reduce_Definitions(self, kid):
        self.val = gqlast.Document(definitions=kid.val,
                                   context=get_context(kid))


class Definition(Nonterm):
    def reduce_Query(self, kid):
        self.val = kid.val

    def reduce_Fragment(self, kid):
        self.val = kid.val


class Definitions(parsing.ListNonterm, element=Definition):
    pass


class Query(Nonterm):
    def reduce_QueryTypeTok_OptNameTok_OptVariables_OptDirectives_SelectionSet(
            self, *kids):
        self.val = gqlast.OperationDefinition(
            type=kids[0].val.val,
            name=kids[1].val.val if kids[1].val else None,
            variables=kids[2].val,
            directives=kids[3].val,
            selection_set=kids[4].val,
            context=get_context(*kids))

    def reduce_SelectionSet(self, kid):
        self.val = gqlast.OperationDefinition(selection_set=kid.val,
                                              context=get_context(kid))


class QueryTypeTok(Nonterm):
    def reduce_QUERY(self, kid):
        self.val = kid

    def reduce_MUTATION(self, kid):
        self.val = kid


class Fragment(Nonterm):
    def reduce_FRAGMENT_IDENT_TypeCondition_OptDirectives_SelectionSet(self,
                                                                       *kids):
        self.val = gqlast.FragmentDefinition(name=kids[1].val,
                                             on=kids[2].val,
                                             directives=kids[3].val,
                                             selection_set=kids[4].val,
                                             context=get_context(*kids))


class SelectionSet(Nonterm):
    def reduce_LCBRACKET_Selections_RCBRACKET(self, *kids):
        self.val = gqlast.SelectionSet(selections=kids[1].val,
                                       context=get_context(*kids))


class OptSelectionSet(Nonterm):
    def reduce_SelectionSet(self, kid):
        self.val = kid.val

    def reduce_empty(self):
        self.val = None


class Field(Nonterm):
    def reduce_AliasedField_OptArgs_OptDirectives_OptSelectionSet(self, *kids):
        self.val = kids[0].val
        self.val.arguments = kids[1].val
        self.val.directives = kids[2].val
        self.val.selection_set = kids[3].val


class AliasedField(Nonterm):
    def reduce_IDENT(self, kid):
        self.val = gqlast.Field(name=kid.val, context=get_context(kid))

    def reduce_IDENT_COLON_IDENT(self, *kids):
        self.val = gqlast.Field(alias=kids[0].val, name=kids[2].val,
                                context=get_context(*kids))


class FragmentSpread(Nonterm):
    def reduce_ELLIPSIS_IDENT_OptDirectives(self, *kids):
        self.val = gqlast.FragmentSpread(name=kids[1].val,
                                         directives=kids[2].val,
                                         context=get_context(*kids))


class InlineFragment(Nonterm):
    def reduce_ELLIPSIS_OptTypeCondition_OptDirectives_SelectionSet(self,
                                                                    *kids):
        self.val = gqlast.InlineFragment(selection_set=kids[3].val,
                                         on=kids[1].val,
                                         directives=kids[2].val,
                                         context=get_context(*kids))


class Selection(Nonterm):
    def reduce_Field(self, kid):
        self.val = kid.val

    def reduce_FragmentSpread(self, kid):
        self.val = kid.val

    def reduce_InlineFragment(self, kid):
        self.val = kid.val


class Selections(parsing.ListNonterm, element=Selection):
    pass


class OptArgs(Nonterm):
    def reduce_Arguments(self, kid):
        self.val = kid.val

    def reduce_empty(self):
        self.val = []


class Arguments(Nonterm):
    def reduce_LPAREN_ArgumentList_RPAREN(self, *kids):
        self.val = kids[1].val


class Argument(Nonterm):
    def reduce_IDENT_COLON_Value(self, *kids):
        self.val = gqlast.Argument(name=kids[0].val, value=kids[2].val,
                                   context=get_context(*kids))


class ArgumentList(parsing.ListNonterm, element=Argument):
    pass


class OptDirectives(Nonterm):
    def reduce_Directives(self, kid):
        self.val = kid.val

    def reduce_empty(self):
        self.val = []


class Directive(Nonterm):
    def reduce_AT_IDENT_OptArgs(self, *kids):
        self.val = gqlast.Directive(name=kids[1].val, arguments=kids[2].val,
                                    context=get_context(*kids))


class Directives(parsing.ListNonterm, element=Directive):
    pass


class OptTypeCondition(Nonterm):
    def reduce_TypeCondition(self, kid):
        self.val = kid.val

    def reduce_empty(self):
        self.val = None


class TypeCondition(Nonterm):
    def reduce_ON_IDENT(self, *kids):
        self.val = kids[1].val


class OptVariables(Nonterm):
    def reduce_Variables(self, kid):
        self.val = kid.val

    def reduce_empty(self):
        self.val = None


class Variables(Nonterm):
    def reduce_LPAREN_VariableList_RPAREN(self, *kids):
        self.val = kids[1].val


class Variable(Nonterm):
    def reduce_VAR_COLON_VarType_OptValue(self, *kids):
        self.val = gqlast.VariableDefinition(name=kids[0].val,
                                             type=kids[2].val,
                                             value=kids[3].val,
                                             context=get_context(*kids))


class VariableList(parsing.ListNonterm, element=Variable):
    pass


class VarType(Nonterm):
    def reduce_IDENT(self, kid):
        self.val = gqlast.VariableType(name=kid.val,
                                       context=get_context(kid))

    def reduce_IDENT_BANG(self, kid):
        self.val = gqlast.VariableType(name=kid.val,
                                       nullable=False,
                                       context=get_context(kid))

    def reduce_LSBRACKET_VarType_RSBRACKET(self, *kids):
        self.val = gqlast.VariableType(name=kids[1].val,
                                       list=True,
                                       context=get_context(*kids))

    def reduce_LSBRACKET_VarType_RSBRACKET_BANG(self, *kids):
        self.val = gqlast.VariableType(name=kids[1].val,
                                       list=True,
                                       nullable=False,
                                       context=get_context(*kids))
