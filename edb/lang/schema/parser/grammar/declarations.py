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


import collections
import re
import textwrap
import typing

from edb.lang.common import parsing, context
from edb.lang.common.exceptions import get_context

from edb.lang import edgeql
from edb.lang.edgeql import ast as qlast
from edb.lang.schema import ast as esast

from edb.errors import SchemaSyntaxError, EdgeQLSyntaxError

from . import keywords
from . import tokens

from .tokens import *  # NOQA


class RawLiteral(typing.NamedTuple):
    value: str


class NameWithParents(typing.NamedTuple):
    name: str
    extends: typing.List[qlast.TypeName]


class PointerSpec(typing.NamedTuple):
    name: str
    target: typing.List[qlast.TypeName]
    spec: typing.List[esast.Base]
    expr: typing.Optional[qlast.Base]


def parse_edgeql(expr: str, ctx, *, offset_column=0, indent=0):
    try:
        node = edgeql.parse(expr)
    except EdgeQLSyntaxError as err:
        context.rebase_context(
            ctx, get_context(err, parsing.ParserContext),
            offset_column=offset_column, indent=indent)
        raise err from None

    context.rebase_ast_context(ctx, node,
                               offset_column=offset_column, indent=indent)
    return node


def parse_edgeql_constant(expr: str, ctx, *, indent=0):
    node = parse_edgeql(expr, ctx, indent=indent)
    if (isinstance(node, qlast.SelectQuery) and
            isinstance(node.result, qlast.BaseConstant)):
        node = node.result
    return node


def _parse_language(node):
    try:
        return esast.Language(node.val.upper())
    except ValueError:
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
        self.val = qlast.ObjectRef(name=None, module=kid.val)

    def reduce_DotName_DOT_Identifier(self, *kids):
        self.val = kids[0].val
        self.val.module += '.' + kids[2].val


class UnqualifiedObjectName(Nonterm):
    def reduce_Identifier(self, kid):
        self.val = qlast.ObjectRef(name=kid.val)


class ObjectName(Nonterm):
    def reduce_UnqualifiedObjectName(self, kid):
        self.val = kid.val

    def reduce_DotName_DOUBLECOLON_Identifier(self, *kids):
        self.val = kids[0].val
        self.val.name = kids[2].val


class TypeList(Nonterm):
    def reduce_LPAREN_TypeList_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_RowRawString(self, *kids):
        self.val = kids[0].parse_as_typelist()


class RowRawString(Nonterm):
    def reduce_RowRawString_RowRawStr(self, *kids):
        self.val = RawLiteral(
            value=kids[0].val.value + kids[1].val.value)

    def reduce_RowRawStr(self, *kids):
        self.val = kids[0].val

    def parse_as_typelist(self):
        expr = self.val.value
        context = self.context

        prefix, postfix = '<tuple<', '>>X'
        eql_query = f'{prefix}{expr}{postfix}'
        eql = parse_edgeql(eql_query, context, offset_column=-len(prefix))

        if not isinstance(eql, qlast.SelectQuery):
            raise SchemaSyntaxError(
                f'Could not parse EdgeQL parameters declaration {expr!r}',
                context=context) from None

        return eql.result.type.subtypes

    def parse_as_function_type(self):
        expr = self.val.value
        context = self.context

        prefix, postfix = 'CREATE FUNCTION f() ->', ' FROM SQL ""'
        eql_query = f'{prefix}{expr}{postfix}'
        eql = parse_edgeql(eql_query, context, offset_column=-len(prefix))

        if not isinstance(eql, qlast.CreateFunction):
            raise SchemaSyntaxError(
                f'Could not parse EdgeQL parameters declaration {expr!r}',
                context=context) from None

        return (eql.returning_typemod, eql.returning)


class RowRawStr(Nonterm):
    def reduce_STRING(self, kid):
        self.val = RawLiteral(value=kid.val)

    def reduce_IDENT(self, kid):
        # this can actually only be QIDENT
        self.val = RawLiteral(value=f'`{kid.val}`')

    def reduce_RAWSTRING(self, kid):
        self.val = RawLiteral(value=kid.val)


class RawString(Nonterm):
    def reduce_RawStr(self, *kids):
        self.val = RawLiteral(value=kids[0].val.value)


class RawStr(Nonterm):
    def reduce_RawStr_RAWLEADWS_RAWSTRING(self, *kids):
        self.val = RawLiteral(
            value=kids[0].val.value + kids[1].val + kids[2].val)

    def reduce_RawStr_RAWSTRING(self, *kids):
        self.val = RawLiteral(
            value=kids[0].val.value + kids[1].val)

    def reduce_RAWLEADWS_RAWSTRING(self, *kids):
        self.val = RawLiteral(
            value=kids[0].val + kids[1].val)

    def reduce_RAWSTRING(self, kid):
        self.val = RawLiteral(
            value=kid.val)


class Schema(Nonterm):
    "%start"

    def reduce_NL(self, *kids):
        self.val = esast.Schema(declarations=[])

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

    def reduce_DeclarationBase(self, kid):
        self.val = kid.val

    def reduce_FunctionDeclaration(self, kid):
        self.val = kid.val

    def reduce_ViewDeclaration(self, kid):
        self.val = kid.val


class DeclarationList(ListNonterm, element=Declaration):
    pass


class DeclarationBase(Nonterm):

    def reduce_ABSTRACT_AttributeDeclaration(self, *kids):
        self.val = kids[1].val
        self.val.abstract = True

    def reduce_ABSTRACT_LinkDeclaration(self, *kids):
        self.val = kids[1].val
        self.val.abstract = True

    def reduce_ABSTRACT_PropertyDeclaration(self, *kids):
        self.val = kids[1].val
        self.val.abstract = True

    def reduce_ABSTRACT_ScalarTypeDeclaration(self, *kids):
        self.val = kids[1].val
        self.val.abstract = True

    def reduce_ScalarTypeDeclaration(self, kid):
        self.val = kid.val

    def reduce_ABSTRACT_ObjectTypeDeclaration(self, *kids):
        self.val = kids[1].val
        self.val.abstract = True

    def reduce_FINAL_ScalarTypeDeclaration(self, *kids):
        self.val = kids[1].val
        self.val.final = True

    def reduce_FINAL_ObjectTypeDeclaration(self, *kids):
        self.val = kids[1].val
        self.val.final = True

    def reduce_ObjectTypeDeclaration(self, kid):
        self.val = kid.val

    def reduce_ABSTRACT_ConstraintDeclaration(self, *kids):
        self.val = kids[1].val
        self.val.abstract = True

    def reduce_DELEGATED_ConstraintDeclaration(self, *kids):
        raise SchemaSyntaxError(
            'only concrete constraints can be delegated',
            context=kids[0].context)


def _astname_to_propname(name: str) -> str:
    return re.sub(
        '([a-z0-9])([A-Z])',
        r'\1_\2',
        re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    ).lower()


def _pluralize(name: str) -> str:
    if name[-1] in {'s', 'x', 'z'}:
        return f'{name}es'
    elif name[-1] == 'y':
        return f'{name[:-1]}ies'
    else:
        return f'{name}s'


def _process_decl_body(
    specs: typing.List[esast.Spec],
    allowed: typing.Tuple[type(esast.Spec), ...], *,
    once: typing.AbstractSet[type(esast.Spec)]=frozenset(),
) -> typing.Mapping[str, typing.List[esast.Spec]]:
    result = collections.defaultdict(list)

    for spec in specs:
        if not isinstance(spec, allowed):
            raise SchemaSyntaxError(
                'illegal definition', context=spec.context)

        propname = _astname_to_propname(type(spec).__name__)
        if type(spec) in once:
            if result[propname]:
                raise SchemaSyntaxError(
                    f'more than one {propname.replace("_", " ")!r} '
                    f'specification',
                    context=spec.context)
            result[propname] = spec
        else:
            propname = _pluralize(propname)
            result[propname].append(spec)

    return result


class ScalarTypeDeclaration(Nonterm):
    def reduce_SCALAR_TYPE_NameAndExtends_NL(self, *kids):
        np: NameWithParents = kids[2].val
        self.val = esast.ScalarTypeDeclaration(
            name=np.name,
            extends=np.extends)

    def reduce_SCALAR_TYPE_NameAndExtends_DeclarationSpecsBlob(self, *kids):
        np: NameWithParents = kids[2].val

        self.val = esast.ScalarTypeDeclaration(
            name=np.name,
            extends=np.extends,
            **_process_decl_body(
                kids[3].val,
                (
                    esast.Constraint,
                    esast.Attribute,
                    esast.Field
                )
            )
        )


class OptInheritable(Nonterm):
    def reduce_empty(self):
        self.val = False

    def reduce_INHERITABLE(self, *kids):
        self.val = True


class AttributeDeclaration(Nonterm):
    def reduce_OptInheritable_ATTRIBUTE_NameAndExtends_NL(self, *kids):
        np: NameWithParents = kids[2].val
        self.val = esast.AttributeDeclaration(
            name=np.name,
            extends=np.extends,
            inheritable=kids[0].val
        )

    def reduce_attribute_with_specs(self, *kids):
        """%reduce OptInheritable ATTRIBUTE NameAndExtends
                   DeclarationSpecsBlob
        """
        np: NameWithParents = kids[2].val

        self.val = esast.AttributeDeclaration(
            name=np.name,
            extends=np.extends,
            **_process_decl_body(
                kids[3].val,
                (
                    esast.Attribute,
                    esast.Field
                )
            )
        )


class RawTypeList(Nonterm):
    def reduce_LPAREN_RawTypeList_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_RowRawString(self, *kids):
        self.val = kids[0].val


class OptRawExtending(Nonterm):
    def reduce_EXTENDING_RawTypeList(self, *kids):
        self.val = kids[1].val

    def reduce_empty(self, *kids):
        self.val = None


class ObjectTypeDeclaration(Nonterm):
    def reduce_TYPE_NameAndExtends_NL(self, *kids):
        np: NameWithParents = kids[1].val
        self.val = esast.ObjectTypeDeclaration(
            name=np.name,
            extends=np.extends)

    def reduce_TYPE_NameAndExtends_DeclarationSpecsBlob(
            self, *kids):
        np: NameWithParents = kids[1].val

        self.val = esast.ObjectTypeDeclaration(
            name=np.name,
            extends=np.extends,
            **_process_decl_body(
                kids[2].val,
                (
                    esast.Constraint,
                    esast.Attribute,
                    esast.Field,
                    esast.Index,
                    esast.Link,
                    esast.Property,
                )
            )
        )


class ConstraintCallableAndExtends(Nonterm):
    def reduce_Identifier_OptFunctionParameters_OptOnExpr_ExtendingNameList(
            self, *kids):
        self.val = esast.ConstraintDeclaration(
            name=kids[0].val,
            params=kids[1].val,
            subject=kids[2].val,
            extends=kids[3].val)

    def reduce_Identifier_OptFunctionParameters_OptOnExpr(self, *kids):
        self.val = esast.ConstraintDeclaration(
            name=kids[0].val,
            params=kids[1].val,
            subject=kids[2].val)


class ConstraintDeclaration(Nonterm):
    def reduce_CONSTRAINT_ConstraintCallableAndExtends_NL(
            self, c_tok, decl, nl_tok):
        self.val = decl.val

    def reduce_CONSTRAINT_ConstraintCallableAndExtends_DeclarationSpecsBlob(
            self, c_tok, decl, specs):
        decl: esast.ConstraintDeclaration = decl.val
        specs: list = specs.val

        attrs = _process_decl_body(
            specs,
            (
                esast.Attribute,
                esast.Field,
            )
        )
        for attr, val in attrs.items():
            setattr(decl, attr, val)

        self.val = decl


class LinkDeclaration(Nonterm):
    def reduce_LINK_NameAndExtends_NL(self, *kids):
        np: NameWithParents = kids[1].val
        self.val = esast.LinkDeclaration(
            name=np.name,
            extends=np.extends)

    def reduce_LINK_NameAndExtends_DeclarationSpecsBlob(
            self, *kids):
        np: NameWithParents = kids[1].val

        self.val = esast.LinkDeclaration(
            name=np.name,
            extends=np.extends,
            **_process_decl_body(
                kids[2].val,
                (
                    esast.Constraint,
                    esast.Attribute,
                    esast.Field,
                    esast.Index,
                    esast.Property,
                )
            )
        )

        for prop in self.val.properties:
            if prop.required:
                raise SchemaSyntaxError(
                    'link properties cannot be "required".',
                    context=prop.context)


class PropertyDeclaration(Nonterm):
    def reduce_PROPERTY_NameAndExtends_NL(self, *kids):
        np: NameWithParents = kids[1].val
        self.val = esast.PropertyDeclaration(
            name=np.name,
            extends=np.extends)

    def reduce_PROPERTY_NameAndExtends_DeclarationSpecsBlob(
            self, *kids):
        np: NameWithParents = kids[1].val

        self.val = esast.PropertyDeclaration(
            name=np.name,
            extends=np.extends,
            **_process_decl_body(
                kids[2].val,
                (
                    esast.Attribute,
                    esast.Field
                )
            )
        )


class ViewDeclaration(Nonterm):
    def reduce_VIEW_Identifier_DeclarationSpecsBlob(self, *kids):
        self.val = esast.ViewDeclaration(
            name=kids[1].val,
            **_process_decl_body(
                kids[2].val,
                (
                    esast.Attribute,
                    esast.Field,
                )
            )
        )

    def reduce_VIEW_Identifier_AssignmentBlob(self, *kids):
        self.val = esast.ViewDeclaration(
            name=kids[1].val,
            fields=[esast.Field(
                name=qlast.ObjectRef(name='expr'),
                value=kids[2].val)
            ])


class FunctionDeclaration(Nonterm):
    def reduce_FUNCTION_FunctionDeclCore(self, *kids):
        self.val = kids[1].val

        for field in self.val.fields:
            if field.name.name == 'initial_value':
                raise SchemaSyntaxError(
                    "unexpected 'initial_value' in function definition",
                    context=field.context)

        if self.val.function_code is None:
            raise SchemaSyntaxError("missing 'from' in function definition",
                                    context=kids[0].context)


class FunctionDeclCore(Nonterm):
    def reduce_FunctionDeclCore(self, *kids):
        r"""%reduce \
                Identifier FunctionParameters \
                ARROW RowRawString FunctionSpecsBlob \
        """
        returning_typemod, returning = kids[3].parse_as_function_type()

        self.val = esast.FunctionDeclaration(
            name=kids[0].val,
            params=kids[1].val,
            returning_typemod=returning_typemod,
            returning=returning,
            **_process_decl_body(
                kids[4].val,
                (
                    esast.Attribute,
                    esast.Field,
                    esast.FunctionCode,
                ),
                once={esast.FunctionCode}
            )
        )


class FunctionSpecsBlob(Nonterm):
    def reduce_COLON_NL_INDENT_FunctionSpecs_DEDENT(self, *kids):
        self.val = kids[3].val


class FunctionSpec(Nonterm):
    def reduce_FROM_Identifier_Value(self, *kids):
        self.val = esast.FunctionCode(language=_parse_language(kids[1]),
                                      code=kids[2].val)

    def reduce_FROM_Identifier_FUNCTION_COLON_Identifier_NL(self, *kids):
        self.val = esast.FunctionCode(language=_parse_language(kids[1]),
                                      from_name=kids[4].val)

    def reduce_DeclarationSpec(self, *kids):
        self.val = kids[0].val


class FunctionSpecs(ListNonterm, element=FunctionSpec):
    pass


class ParenRawString(Nonterm):
    def reduce_ParenRawString_LPAREN_ParenRawString_RPAREN(self, *kids):
        self.val = RawLiteral(
            value=f'{kids[0].val.value}({kids[2].val.value})')

    def reduce_LPAREN_RPAREN(self, *kids):
        self.val = RawLiteral(
            value='()')

    def reduce_ParenRawString_LPAREN_RPAREN(self, *kids):
        self.val = RawLiteral(
            value=f'{kids[0].val.value}()')

    def reduce_LPAREN_ParenRawString_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_ParenRawString_RowRawStr(self, *kids):
        self.val = RawLiteral(
            value=f'{kids[0].val.value}{kids[1].val.value}')

    def reduce_RowRawStr(self, *kids):
        self.val = kids[0].val

    def parse_as_call_args(self):
        expr = self.val.value
        context = self.context

        prefix, postfix = 'SELECT f(', ')'
        eql_query = f'{prefix}{expr}{postfix}'
        eql = parse_edgeql(eql_query, context, offset_column=-len(prefix))

        if (not isinstance(eql, qlast.SelectQuery) or
                not isinstance(eql.result, qlast.FunctionCall)):
            raise SchemaSyntaxError(
                f'Could not parse EdgeQL call arguments {expr!r}',
                context=context) from None

        return eql.result.args

    def parse_as_parameters_decl(self):
        expr = self.val.value
        context = self.context

        prefix, postfix = 'CREATE FUNCTION f(', ') -> anytype FROM SQL ""'
        eql_query = f'{prefix}{expr}{postfix}'
        eql = parse_edgeql(eql_query, context, offset_column=-len(prefix))

        if not isinstance(eql, qlast.CreateFunction):
            raise SchemaSyntaxError(
                f'Could not parse EdgeQL parameters declaration {expr!r}',
                context=context) from None

        return eql.params

    def parse_as_expr(self):
        expr = self.val.value
        context = self.context

        prefix, postfix = '(', ')'
        eql_query = f'{prefix}{expr}{postfix}'
        eql = parse_edgeql(eql_query, context, offset_column=-len(prefix))

        return eql


class OnExpr(Nonterm):
    def reduce_ON_LPAREN_ParenRawString_RPAREN(self, *kids):
        expr = kids[2].parse_as_expr()
        self.val = expr


class OptOnExpr(Nonterm):
    def reduce_empty(self, *kids):
        self.val = None

    def reduce_OnExpr(self, *kids):
        self.val = kids[0].val


class FunctionParameters(Nonterm):
    def reduce_LPAREN_RPAREN(self, *kids):
        self.val = []

    def reduce_LPAREN_ParenRawString_RPAREN(self, *kids):
        self.val = kids[1].parse_as_parameters_decl()


class OptFunctionParameters(Nonterm):
    def reduce_FunctionParameters(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self):
        self.val = []


class ExtendingNameList(Nonterm):
    def reduce_EXTENDING_TypeList(self, *kids):
        self.val = kids[1].val


class NameAndExtends(Nonterm):
    def reduce_Identifier_ExtendingNameList(self, *kids):
        self.val = NameWithParents(
            name=kids[0].val,
            extends=kids[1].val)

    def reduce_Identifier(self, kid):
        self.val = NameWithParents(
            name=kid.val,
            extends=[])


class DeclarationSpec(Nonterm):
    def reduce_DeclarationSpecBase(self, kid):
        self.val = kid.val

    def reduce_OnDelete(self, kid):
        self.val = kid.val


class DeclarationSpecs(ListNonterm, element=DeclarationSpec):
    pass


class DeclarationSpecBase(Nonterm):
    def reduce_Link(self, kid):
        self.val = kid.val

    def reduce_DELEGATED_Constraint(self, *kids):
        self.val = kids[1].val
        self.val.delegated = True

    def reduce_ABSTRACT_Constraint(self, *kids):
        raise SchemaSyntaxError(
            'only top-level constraints declarations can be abstract',
            context=kids[0].context)

    def reduce_Constraint(self, kid):
        self.val = kid.val

    def reduce_Property(self, kid):
        self.val = kid.val

    def reduce_Attribute(self, kid):
        self.val = kid.val

    def reduce_Field(self, kid):
        self.val = kid.val

    def reduce_Index(self, kid):
        self.val = kid.val


class DeclarationSpecsBlob(Nonterm):
    def reduce_COLON_NL_INDENT_DeclarationSpecs_DEDENT(self, *kids):
        self.val = kids[3].val


# this Nonterminal should NOT automatically compute context as it
# relies on an external parser
#
class AssignmentBlob(parsing.Nonterm):
    def reduce_ASSIGN_RawString_NL(self, *kids):
        self.val = parse_edgeql_constant(kids[1].val.value, kids[1].context)

    def reduce_ASSIGN_NL_INDENT_RawString_NL_DEDENT(self, *kids):
        text = kids[3].val.value
        indent = len(re.match(r'^\s*', text).group(0))
        text = textwrap.dedent(text).strip()
        self.val = parse_edgeql_constant(text, kids[3].context,
                                         indent=indent)


class Spec(Nonterm):
    def reduce_UnqualifiedObjectName_NL(self, *kids):
        self.val = PointerSpec(
            name=kids[0].val, target=None, spec=[], expr=None)

    def reduce_UnqualifiedObjectName_DeclarationSpecsBlob(self, *kids):
        self.val = PointerSpec(
            name=kids[0].val, target=None, spec=kids[1].val, expr=None)

    def reduce_UnqualifiedObjectName_ARROW_TypeList_NL(self, *kids):
        self.val = PointerSpec(
            name=kids[0].val, target=kids[2].val, spec=[], expr=None)

    def reduce_UnqualifiedObjectName_ARROW_TypeList_DeclarationSpecsBlob(
            self, *kids):
        self.val = PointerSpec(
            name=kids[0].val, target=kids[2].val, spec=kids[3].val, expr=None)

    def reduce_UnqualifiedObjectName_AssignmentBlob(self, *kids):
        self.val = PointerSpec(
            name=kids[0].val, target=None, spec=[], expr=kids[1].val)


class Link(Nonterm):
    def _process_pointerspec(self, p: PointerSpec):
        return esast.Link(
            name=p.name,
            target=p.target,
            expr=p.expr,
            **_process_decl_body(
                p.spec,
                (
                    esast.Constraint,
                    esast.Attribute,
                    esast.Field,
                    esast.Property,
                    esast.OnTargetDelete,
                ),
                once={esast.OnTargetDelete},
            )
        )

    def reduce_LINK_Spec(self, *kids):
        self.val = self._process_pointerspec(kids[1].val)

    def reduce_SINGLE_LINK_Spec(self, *kids):
        self.val = self._process_pointerspec(kids[2].val)
        self.val.cardinality = qlast.Cardinality.ONE

    def reduce_MULTI_LINK_Spec(self, *kids):
        self.val = self._process_pointerspec(kids[2].val)
        self.val.cardinality = qlast.Cardinality.MANY

    def reduce_INHERITED_LINK_Spec(self, *kids):
        link = self._process_pointerspec(kids[2].val)
        link.inherited = True
        self.val = link

    def reduce_INHERITED_SINGLE_LINK_Spec(self, *kids):
        link = self._process_pointerspec(kids[3].val)
        link.inherited = True
        link.cardinality = qlast.Cardinality.ONE
        self.val = link

    def reduce_INHERITED_MULTI_LINK_Spec(self, *kids):
        link = self._process_pointerspec(kids[3].val)
        link.inherited = True
        link.cardinality = qlast.Cardinality.MANY
        self.val = link

    def reduce_REQUIRED_LINK_Spec(self, *kids):
        link = self._process_pointerspec(kids[2].val)
        link.required = True
        self.val = link

    def reduce_REQUIRED_SINGLE_LINK_Spec(self, *kids):
        link = self._process_pointerspec(kids[3].val)
        link.required = True
        link.cardinality = qlast.Cardinality.ONE
        self.val = link

    def reduce_REQUIRED_MULTI_LINK_Spec(self, *kids):
        link = self._process_pointerspec(kids[3].val)
        link.required = True
        link.cardinality = qlast.Cardinality.MANY
        self.val = link

    def reduce_INHERITED_REQUIRED_LINK_Spec(self, *kids):
        link = self._process_pointerspec(kids[3].val)
        link.required = True
        link.inherited = True
        self.val = link

    def reduce_INHERITED_REQUIRED_SINGLE_LINK_Spec(self, *kids):
        link = self._process_pointerspec(kids[4].val)
        link.required = True
        link.inherited = True
        link.cardinality = qlast.Cardinality.ONE
        self.val = link

    def reduce_INHERITED_REQUIRED_MULTI_LINK_Spec(self, *kids):
        link = self._process_pointerspec(kids[4].val)
        link.required = True
        link.inherited = True
        link.cardinality = qlast.Cardinality.MANY
        self.val = link


class Property(Nonterm):
    def _process_pointerspec(self, p: PointerSpec):
        return esast.Property(
            name=p.name,
            target=p.target,
            expr=p.expr,
            **_process_decl_body(
                p.spec,
                (
                    esast.Constraint,
                    esast.Attribute,
                    esast.Field,
                ),
            )
        )

    def reduce_PROPERTY_Spec(self, *kids):
        self.val = self._process_pointerspec(kids[1].val)

    def reduce_SINGLE_PROPERTY_Spec(self, *kids):
        self.val = self._process_pointerspec(kids[2].val)
        self.val.cardinality = qlast.Cardinality.ONE

    def reduce_MULTI_PROPERTY_Spec(self, *kids):
        self.val = self._process_pointerspec(kids[2].val)
        self.val.cardinality = qlast.Cardinality.MANY

    def reduce_INHERITED_PROPERTY_Spec(self, *kids):
        prop = self._process_pointerspec(kids[2].val)
        prop.inherited = True
        self.val = prop

    def reduce_INHERITED_SINGLE_PROPERTY_Spec(self, *kids):
        prop = self._process_pointerspec(kids[3].val)
        prop.inherited = True
        prop.cardinality = qlast.Cardinality.ONE
        self.val = prop

    def reduce_INHERITED_MULTI_PROPERTY_Spec(self, *kids):
        prop = self._process_pointerspec(kids[3].val)
        prop.inherited = True
        prop.cardinality = qlast.Cardinality.MANY
        self.val = prop

    def reduce_REQUIRED_PROPERTY_Spec(self, *kids):
        prop = self._process_pointerspec(kids[2].val)
        prop.required = True
        self.val = prop

    def reduce_REQUIRED_SINGLE_PROPERTY_Spec(self, *kids):
        prop = self._process_pointerspec(kids[3].val)
        prop.required = True
        prop.cardinality = qlast.Cardinality.ONE
        self.val = prop

    def reduce_REQUIRED_MULTI_PROPERTY_Spec(self, *kids):
        prop = self._process_pointerspec(kids[3].val)
        prop.required = True
        prop.cardinality = qlast.Cardinality.MANY
        self.val = prop

    def reduce_INHERITED_REQUIRED_PROPERTY_Spec(self, *kids):
        prop = self._process_pointerspec(kids[3].val)
        prop.required = True
        prop.inherited = True
        self.val = prop

    def reduce_INHERITED_REQUIRED_SINGLE_PROPERTY_Spec(self, *kids):
        prop = self._process_pointerspec(kids[4].val)
        prop.required = True
        prop.inherited = True
        prop.cardinality = qlast.Cardinality.ONE
        self.val = prop

    def reduce_INHERITED_REQUIRED_MULTI_PROPERTY_Spec(self, *kids):
        prop = self._process_pointerspec(kids[4].val)
        prop.required = True
        prop.inherited = True
        prop.cardinality = qlast.Cardinality.MANY
        self.val = prop


class OnDelete(Nonterm):
    def reduce_ON_TARGET_DELETE_RESTRICT_NL(self, *kids):
        self.val = esast.OnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.RESTRICT)

    def reduce_ON_TARGET_DELETE_DELETE_SOURCE_NL(self, *kids):
        self.val = esast.OnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.DELETE_SOURCE)

    def reduce_ON_TARGET_DELETE_SET_EMPTY_NL(self, *kids):
        self.val = esast.OnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.SET_EMPTY)

    def reduce_ON_TARGET_DELETE_SET_DEFAULT_NL(self, *kids):
        self.val = esast.OnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.SET_DEFAULT)

    def reduce_ON_TARGET_DELETE_DEFERRED_RESTRICT_NL(self, *kids):
        self.val = esast.OnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.DEFERRED_RESTRICT)


class Index(Nonterm):
    def reduce_INDEX_ObjectName_OnExpr_NL(self, *kids):
        self.val = esast.Index(
            name=kids[1].val,
            expression=kids[2].val)


class ConstraintCallArguments(Nonterm):
    def reduce_LPAREN_RPAREN(self, *kids):
        self.val = []

    def reduce_LPAREN_ParenRawString_RPAREN(self, *kids):
        call_args = kids[1].parse_as_call_args()
        self.val = call_args


class OptConstraintCallArguments(Nonterm):
    def reduce_empty(self, *kids):
        self.val = []

    def reduce_ConstraintCallArguments(self, *kids):
        self.val = kids[0].val


class Constraint(Nonterm):
    def reduce_constraint(self, *kids):
        r"""%reduce \
                CONSTRAINT ObjectName OptConstraintCallArguments \
                OptOnExpr NL \
        """
        self.val = esast.Constraint(
            name=kids[1].val,
            args=kids[2].val,
            subject=kids[3].val)

    def reduce_constraint_with_attributes(self, *kids):
        """%reduce
                CONSTRAINT ObjectName OptConstraintCallArguments
                OptOnExpr DeclarationSpecsBlob
        """
        self.val = esast.Constraint(
            name=kids[1].val,
            args=kids[2].val,
            subject=kids[3].val,
            **_process_decl_body(
                kids[4].val,
                (
                    esast.Attribute,
                    esast.Field,
                ),
            )
        )


class Attribute(Nonterm):
    def reduce_ATTRIBUTE_ObjectName_Value(self, *kids):
        self.val = esast.Attribute(name=kids[1].val, value=kids[2].val)


class Field(Nonterm):
    def reduce_ObjectName_Value(self, *kids):
        self.val = esast.Field(name=kids[0].val, value=kids[1].val)


class Value(Nonterm):
    def reduce_AssignmentBlob(self, *kids):
        self.val = kids[0].val


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
