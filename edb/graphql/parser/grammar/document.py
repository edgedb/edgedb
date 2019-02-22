#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import re

from edb import errors

from edb.common import parsing
from edb.graphql import ast as gqlast

from .tokens import *  # NOQA
from . import keywords


def check_const(expr):
    if isinstance(expr, gqlast.Variable):
        raise errors.GraphQLSyntaxError(
            'unexpected variable, must be a constant value',
            context=expr.context)
    elif isinstance(expr, gqlast.ListValue):
        for val in expr.value:
            check_const(val)
    elif isinstance(expr, gqlast.ObjectValue):
        for field in expr.fields:
            check_const(field.value)


class Nonterm(parsing.Nonterm):
    pass


class ListNonterm(parsing.ListNonterm, element=None):
    pass


class NameTokNontermMeta(parsing.NontermMeta):
    def __new__(mcls, name, bases, dct, *, exceptions=tuple()):
        if name != 'NameTokNonTerm':
            prod = NameTokNonTerm._reduce_token

            tokens = ['IDENT']
            tokens.extend([
                tok for tok in
                keywords.by_type[keywords.UNRESERVED_KEYWORD].values()
                if tok not in exceptions])

            for tok in tokens:
                dct['reduce_' + tok] = prod

        cls = super().__new__(mcls, name, bases, dct)
        return cls

    def __init__(cls, name, bases, dct, *, exceptions=tuple()):
        super().__init__(name, bases, dct)


class NameTokNonTerm(Nonterm, metaclass=NameTokNontermMeta):
    def _reduce_token(self, *kids):
        val = kids[0].val
        if isinstance(val, str):
            val = gqlast.Name(value=val)
        self.val = val


class NameTok(NameTokNonTerm):
    pass


class NameNotONTok(NameTokNonTerm, exceptions=('ON',)):
    pass


class NameNotBoolTok(NameTokNonTerm, exceptions=('TRUE', 'FALSE', 'NULL')):
    pass


class NameNotDunderTok(NameTokNonTerm,
                       exceptions=('SCHEMA', 'TYPE', 'TYPENAME',)):
    pass


INVALID_STRING_RE = re.compile(r'''
    (?<!\\)(?:\\{2})*(\\u(?![0-9A-Fa-f]{4})) |
    ([\n\f\v\b]) |
    (?<!\\)(?:\\{2})*(\\[^"/bfnrtu\\])
''', re.X)


FORWARD_SLASH_ESCAPE_RE = re.compile(r'(?<!\\)((?:\\{2})*)(\\/)')


class StringValue(Nonterm):

    def reduce_STRING(self, *kids):
        val = kids[0].val
        context = kids[0].context
        invalid = INVALID_STRING_RE.search(val, 1, len(val) - 1)
        if invalid:
            # pick whichever group actually matched
            inv = next(filter(None, invalid.groups()))
            context.start.column += invalid.end() - len(inv)
            context.end.line = context.start.line
            context.end.column = context.start.column + len(inv)
            raise errors.GraphQLSyntaxError(
                f"invalid {invalid.group()!r} within string token",
                context=context)

        val = FORWARD_SLASH_ESCAPE_RE.sub(r'\1/', val)

        self.val = gqlast.StringValue(value=val[1:-1])


class BaseValue(Nonterm):
    def reduce_INTEGER(self, *kids):
        self.val = gqlast.IntValue(value=kids[0].val)

    def reduce_FLOAT(self, *kids):
        self.val = gqlast.FloatValue(value=kids[0].val)

    def reduce_TRUE(self, *kids):
        self.val = gqlast.BooleanValue(value=True)

    def reduce_FALSE(self, *kids):
        self.val = gqlast.BooleanValue(value=False)

    def reduce_NULL(self, *kids):
        self.val = gqlast.NullValue()

    def reduce_StringValue(self, *kids):
        self.val = kids[0].val

    def reduce_NameNotBoolTok(self, *kids):
        self.val = gqlast.EnumValue(value=kids[0].val.value)

    def reduce_LSBRACKET_RSBRACKET(self, *kids):
        self.val = gqlast.ListValue(values=[])

    def reduce_LSBRACKET_ValueList_RSBRACKET(self, *kids):
        self.val = gqlast.ListValue(values=kids[1].val)

    def reduce_LCBRACKET_RCBRACKET(self, *kids):
        self.val = gqlast.ObjectValue(fields=[])

    def reduce_LCBRACKET_ObjectFieldList_RCBRACKET(self, *kids):
        self.val = gqlast.ObjectValue(fields=kids[1].val)


class Value(Nonterm):
    def reduce_BaseValue(self, *kids):
        self.val = kids[0].val

    def reduce_VAR(self, *kids):
        self.val = gqlast.Variable(
            name=gqlast.Name(value=kids[0].val[1:]))


class ValueList(ListNonterm, element=Value):
    pass


class ObjectField(Nonterm):
    def reduce_NameTok_COLON_Value(self, *kids):
        self.val = gqlast.ObjectField(name=kids[0].val, value=kids[2].val)


class ObjectFieldList(ListNonterm, element=ObjectField):
    pass


class OptValue(Nonterm):
    def reduce_BaseValue(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self):
        self.val = None


class OptNameTok(Nonterm):
    def reduce_NameTok(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self):
        self.val = None


class Document(Nonterm):
    "%start"

    def reduce_Definitions(self, *kids):
        short = unnamed = None
        fragnames = set()
        opnames = set()
        for defn in kids[0].val:
            if isinstance(defn, gqlast.OperationDefinition):
                if defn.name is not None:
                    if defn.name.value not in opnames:
                        opnames.add(defn.name.value)
                    else:
                        raise errors.GraphQLSyntaxError(
                            f"redefinition of operation {defn.name.value!r}",
                            context=defn.context)

                elif (short is None and
                        defn.operation is None and
                        not defn.variable_definitions and
                        not defn.directives):
                    short = unnamed = defn
                elif unnamed is None:
                    unnamed = defn

            elif isinstance(defn, gqlast.FragmentDefinition):
                if defn.name.value not in fragnames:
                    fragnames.add(defn.name.value)
                else:
                    raise errors.GraphQLSyntaxError(
                        f"redefinition of fragment {defn.name.value!r}",
                        context=defn.context)

        if len(kids[0].val) - len(fragnames) > 1:
            if short is not None:
                # we have more than one query definition, so short
                # form is not allowed
                #
                raise errors.GraphQLSyntaxError(
                    'short form is not allowed here',
                    context=short.context)
            elif unnamed is not None:
                # we have more than one query definition, so unnamed operations
                # are not allowed
                #
                raise errors.GraphQLSyntaxError(
                    'unnamed operation is not allowed here',
                    context=unnamed.context)

        self.val = gqlast.Document(definitions=kids[0].val)


class Definition(Nonterm):
    def reduce_Query(self, *kids):
        self.val = kids[0].val

    def reduce_Fragment(self, *kids):
        self.val = kids[0].val


class Definitions(ListNonterm, element=Definition):
    pass


class Query(Nonterm):
    def reduce_QueryTypeTok_OptNameTok_OptVariables_OptDirectives_SelectionSet(
            self, *kids):
        self.val = gqlast.OperationDefinition(
            operation=kids[0].val,
            name=kids[1].val if kids[1].val else None,
            variable_definitions=kids[2].val,
            directives=kids[3].val,
            selection_set=kids[4].val)

    def reduce_SelectionSet(self, *kids):
        self.val = gqlast.OperationDefinition(selection_set=kids[0].val)


class QueryTypeTok(Nonterm):
    def reduce_QUERY(self, *kids):
        self.val = kids[0].val

    def reduce_MUTATION(self, *kids):
        self.val = kids[0].val

    def reduce_SUBSCRIPTION(self, *kids):
        self.val = kids[0].val


class Fragment(Nonterm):
    def reduce_FRAGMENT_NameNotONTok_TypeCondition_OptDirectives_SelectionSet(
            self, *kids):
        self.val = gqlast.FragmentDefinition(name=kids[1].val,
                                             type_condition=kids[2].val,
                                             directives=kids[3].val,
                                             selection_set=kids[4].val)


class SelectionSet(Nonterm):
    def reduce_LCBRACKET_Selections_RCBRACKET(self, *kids):
        self.val = gqlast.SelectionSet(selections=kids[1].val)


class OptSelectionSet(Nonterm):
    def reduce_SelectionSet(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self):
        self.val = None


class Field(Nonterm):
    def reduce_AliasedField_OptArgs_OptDirectives_OptSelectionSet(self, *kids):
        # there are some special fields that need to be checked here
        self.val = kids[0].val
        self.val.arguments = kids[1].val
        self.val.directives = kids[2].val
        self.val.selection_set = kids[3].val


class AliasedField(Nonterm):
    def reduce_FieldName(self, *kids):
        self.val = kids[0].val

    def reduce_NameTok_COLON_FieldName(self, *kids):
        self.val = kids[2].val
        self.val.alias = kids[0].val


class FieldName(Nonterm):
    def reduce_SCHEMA(self, *kids):
        self.val = gqlast.Field(name=gqlast.Name(value=kids[0].val))

    def reduce_TYPE(self, *kids):
        self.val = gqlast.Field(name=gqlast.Name(value=kids[0].val))

    def reduce_TYPENAME(self, *kids):
        self.val = gqlast.Field(name=gqlast.Name(value=kids[0].val))

    def reduce_NameNotDunderTok(self, *kids):
        self.val = gqlast.Field(name=kids[0].val)


class FragmentSpread(Nonterm):
    def reduce_ELLIPSIS_NameNotONTok_OptDirectives(self, *kids):
        self.val = gqlast.FragmentSpread(name=kids[1].val,
                                         directives=kids[2].val)


class InlineFragment(Nonterm):
    def reduce_ELLIPSIS_OptTypeCondition_OptDirectives_SelectionSet(self,
                                                                    *kids):
        self.val = gqlast.InlineFragment(selection_set=kids[3].val,
                                         type_condition=kids[1].val,
                                         directives=kids[2].val)


class Selection(Nonterm):
    def reduce_Field(self, *kids):
        self.val = kids[0].val

    def reduce_FragmentSpread(self, *kids):
        self.val = kids[0].val

    def reduce_InlineFragment(self, *kids):
        self.val = kids[0].val


class Selections(ListNonterm, element=Selection):
    pass


class OptArgs(Nonterm):
    def reduce_Arguments(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self):
        self.val = []


class Arguments(Nonterm):
    def reduce_LPAREN_ArgumentList_RPAREN(self, *kids):
        self.val = kids[1].val
        # validate argument name uniqueness
        #
        argnames = set()
        for arg in self.val:
            if arg.name.value not in argnames:
                argnames.add(arg.name.value)
            else:
                raise errors.GraphQLSyntaxError(
                    f"redefinition of argument with name {arg.name.value!r}",
                    context=arg.context)


class Argument(Nonterm):
    def reduce_NameTok_COLON_Value(self, *kids):
        self.val = gqlast.Argument(name=kids[0].val, value=kids[2].val)


class ArgumentList(ListNonterm, element=Argument):
    pass


class OptDirectives(Nonterm):
    def reduce_Directives(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self):
        self.val = []


class Directive(Nonterm):
    def reduce_AT_NameTok_OptArgs(self, *kids):
        self.val = gqlast.Directive(name=kids[1].val,
                                    arguments=kids[2].val)


class Directives(ListNonterm, element=Directive):
    pass


class OptTypeCondition(Nonterm):
    def reduce_TypeCondition(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self):
        self.val = None


class TypeCondition(Nonterm):
    def reduce_ON_NameTok(self, *kids):
        self.val = gqlast.NamedType(name=kids[1].val)


class OptVariables(Nonterm):
    def reduce_Variables(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self):
        self.val = None


class Variables(Nonterm):
    def reduce_LPAREN_VariableList_RPAREN(self, *kids):
        self.val = kids[1].val
        # validate argument name uniqueness
        #
        variables = set()
        for var in self.val:
            if var.variable.name.value not in variables:
                variables.add(var.variable.name.value)
            else:
                raise errors.GraphQLSyntaxError(
                    f"redefinition of variable {var.variable.name.value!r}",
                    context=var.context)


class Variable(Nonterm):
    def reduce_VAR_COLON_VarType_DefaultValue(self, *kids):
        self.val = gqlast.VariableDefinition(
            variable=gqlast.Variable(name=gqlast.Name(value=kids[0].val[1:])),
            type=kids[2].val,
            default_value=kids[3].val)


class VariableList(ListNonterm, element=Variable):
    pass


class VarType(Nonterm):
    def reduce_NameTok(self, *kids):
        self.val = gqlast.NamedType(name=kids[0].val)

    def reduce_NameTok_BANG(self, *kids):
        self.val = gqlast.NonNullType(
            type=gqlast.NamedType(name=kids[0].val))

    def reduce_LSBRACKET_VarType_RSBRACKET(self, *kids):
        self.val = gqlast.ListType(type=kids[1].val)

    def reduce_LSBRACKET_VarType_RSBRACKET_BANG(self, *kids):
        self.val = gqlast.NonNullType(
            type=gqlast.ListType(type=kids[1].val))


class DefaultValue(Nonterm):
    def reduce_EQUAL_BaseValue(self, *kids):
        check_const(kids[1])
        self.val = kids[1].val

    def reduce_empty(self):
        self.val = None
