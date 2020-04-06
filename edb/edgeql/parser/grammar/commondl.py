#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


from __future__ import annotations

import sys
import types
from typing import *  # noqa

from edb.errors import EdgeQLSyntaxError

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb.common import parsing

from . import expressions
from . import tokens

from .precedence import *  # NOQA
from .tokens import *  # NOQA
from .expressions import *  # NOQA


Nonterm = expressions.Nonterm
ListNonterm = parsing.ListNonterm


def _parse_language(node):
    try:
        return qlast.Language(node.val.upper())
    except ValueError:
        raise EdgeQLSyntaxError(
            f'{node.val} is not a valid language',
            context=node.context) from None


def _validate_declarations(
    declarations: Sequence[Union[qlast.ModuleDeclaration, qlast.DDLCommand]]
) -> None:
    # Check that top-level declarations either use fully-qualified
    # names or are module blocks.
    for decl in declarations:
        if (not isinstance(decl, qlast.ModuleDeclaration) and
                decl.name.module is None):
            raise EdgeQLSyntaxError(
                "only fully-qualified name is allowed in "
                "top-level declaration",
                context=decl.name.context)


class NewNontermHelper:
    def __init__(self, modname):
        self.name = modname

    def _new_nonterm(self, clsname, clsdict=None, clskwds=None,
                     clsbases=(Nonterm,)):
        if clsdict is None:
            clsdict = {}
        if clskwds is None:
            clskwds = {}
        mod = sys.modules[self.name]

        def clsexec(ns):
            ns['__module__'] = self.name
            for k, v in clsdict.items():
                ns[k] = v
            return ns

        cls = types.new_class(clsname, clsbases, clskwds, clsexec)
        setattr(mod, clsname, cls)
        return cls


class Semicolons(Nonterm):
    # one or more semicolons
    def reduce_SEMICOLON(self, tok):
        self.val = tok

    def reduce_Semicolons_SEMICOLON(self, *kids):
        self.val = kids[0].val


class OptSemicolons(Nonterm):
    def reduce_Semicolons(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self):
        self.val = None


class ExtendingSimple(Nonterm):
    def reduce_EXTENDING_SimpleTypeNameList(self, *kids):
        self.val = kids[1].val


class OptExtendingSimple(Nonterm):
    def reduce_ExtendingSimple(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = []


class Extending(Nonterm):
    def reduce_EXTENDING_TypeNameList(self, *kids):
        self.val = kids[1].val


class OptExtending(Nonterm):
    def reduce_Extending(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = []


class OnExpr(Nonterm):
    # NOTE: the reason why we need parentheses around the expression
    # is to disambiguate whether the '{' following the expression is
    # meant to be a shape or a nested DDL/SDL block.
    def reduce_ON_ParenExpr(self, *kids):
        self.val = kids[1].val


class OptOnExpr(Nonterm):
    def reduce_empty(self, *kids):
        self.val = None

    def reduce_OnExpr(self, *kids):
        self.val = kids[0].val


class OptConcreteConstraintArgList(Nonterm):
    def reduce_LPAREN_OptPosCallArgList_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_empty(self):
        self.val = []


class OptDefault(Nonterm):
    def reduce_empty(self):
        self.val = None

    def reduce_EQUALS_Expr(self, *kids):
        self.val = kids[1].val


class OptParameterKind(Nonterm):
    def reduce_empty(self):
        self.val = qltypes.ParameterKind.POSITIONAL

    def reduce_VARIADIC(self, kid):
        self.val = qltypes.ParameterKind.VARIADIC

    def reduce_NAMEDONLY(self, *kids):
        self.val = qltypes.ParameterKind.NAMED_ONLY


class FuncDeclArgName(Nonterm):
    def reduce_Identifier(self, dp):
        self.val = dp.val
        self.context = dp.context

    def reduce_ARGUMENT(self, dp):
        if dp.val[1].isdigit():
            raise EdgeQLSyntaxError(
                f'numeric parameters are not supported',
                context=dp.context)
        else:
            raise EdgeQLSyntaxError(
                f"function parameters do not need a $ prefix, "
                f"rewrite as '{dp.val[1:]}'",
                context=dp.context)


class FuncDeclArg(Nonterm):
    def reduce_kwarg(self, *kids):
        r"""%reduce OptParameterKind FuncDeclArgName COLON \
                OptTypeQualifier FullTypeExpr OptDefault \
        """
        self.val = qlast.FuncParam(
            kind=kids[0].val,
            name=kids[1].val,
            typemod=kids[3].val,
            type=kids[4].val,
            default=kids[5].val
        )

    def reduce_OptParameterKind_FuncDeclArgName_OptDefault(self, *kids):
        raise EdgeQLSyntaxError(
            f'missing type declaration for the `{kids[1].val}` parameter',
            context=kids[1].context)


class FuncDeclArgList(ListNonterm, element=FuncDeclArg,
                      separator=tokens.T_COMMA):
    pass


class CreateFunctionArgs(Nonterm):
    def reduce_LPAREN_RPAREN(self, *kids):
        self.val = []

    def reduce_LPAREN_FuncDeclArgList_RPAREN(self, *kids):
        args = kids[1].val

        last_pos_default_arg = None
        last_named_arg = None
        variadic_arg = None
        names = set()
        for arg in args:
            if arg.name in names:
                raise EdgeQLSyntaxError(
                    f'duplicate parameter name `{arg.name}`',
                    context=arg.context)
            names.add(arg.name)

            if arg.kind is qltypes.ParameterKind.VARIADIC:
                if variadic_arg is not None:
                    raise EdgeQLSyntaxError(
                        'more than one variadic argument',
                        context=arg.context)
                elif last_named_arg is not None:
                    raise EdgeQLSyntaxError(
                        f'NAMED ONLY argument `{last_named_arg.name}` '
                        f'before VARIADIC argument `{arg.name}`',
                        context=last_named_arg.context)
                else:
                    variadic_arg = arg

                if arg.default is not None:
                    raise EdgeQLSyntaxError(
                        f'VARIADIC argument `{arg.name}` '
                        f'cannot have a default value',
                        context=arg.context)

            elif arg.kind is qltypes.ParameterKind.NAMED_ONLY:
                last_named_arg = arg

            else:
                if last_named_arg is not None:
                    raise EdgeQLSyntaxError(
                        f'positional argument `{arg.name}` '
                        f'follows NAMED ONLY argument `{last_named_arg.name}`',
                        context=arg.context)

                if variadic_arg is not None:
                    raise EdgeQLSyntaxError(
                        f'positional argument `{arg.name}` '
                        f'follows VARIADIC argument `{variadic_arg.name}`',
                        context=arg.context)

            if arg.kind is qltypes.ParameterKind.POSITIONAL:
                if arg.default is None:
                    if last_pos_default_arg is not None:
                        raise EdgeQLSyntaxError(
                            f'positional argument `{arg.name}` without '
                            f'default follows positional argument '
                            f'`{last_pos_default_arg.name}` with default',
                            context=arg.context)
                else:
                    last_pos_default_arg = arg

        self.val = args


class OptTypeQualifier(Nonterm):
    def reduce_SET_OF(self, *kids):
        self.val = qltypes.TypeModifier.SET_OF

    def reduce_OPTIONAL(self, *kids):
        self.val = qltypes.TypeModifier.OPTIONAL

    def reduce_empty(self):
        self.val = qltypes.TypeModifier.SINGLETON


class FunctionType(Nonterm):
    def reduce_FullTypeExpr(self, *kids):
        self.val = kids[0].val


class FromFunction(Nonterm):
    def reduce_USING_ParenExpr(self, *kids):
        lang = qlast.Language.EdgeQL
        self.val = qlast.FunctionCode(
            language=lang,
            nativecode=kids[1].val)

    def reduce_USING_Identifier_BaseStringConstant(self, *kids):
        lang = _parse_language(kids[1])
        code = kids[2].val.value
        self.val = qlast.FunctionCode(language=lang, code=code)

    def reduce_USING_Identifier_FUNCTION_BaseStringConstant(self, *kids):
        lang = _parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in USING FUNCTION clause',
                context=kids[1].context) from None

        self.val = qlast.FunctionCode(language=lang,
                                      from_function=kids[3].val.value)

    def reduce_USING_Identifier_EXPRESSION(self, *kids):
        lang = _parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in USING clause',
                context=kids[1].context) from None

        self.val = qlast.FunctionCode(language=lang)


class ProcessFunctionBlockMixin:
    def _process_function_body(self, block):
        props = {}

        commands = []
        code = None
        nativecode = None
        language = qlast.Language.SQL
        from_expr = False
        from_function = None

        for node in block.val:
            if isinstance(node, qlast.FunctionCode):
                if node.from_function:
                    if from_function is not None:
                        raise EdgeQLSyntaxError(
                            'more than one USING FUNCTION clause',
                            context=node.context)
                    from_function = node.from_function

                elif node.nativecode:
                    if code is not None or nativecode is not None:
                        raise EdgeQLSyntaxError(
                            'more than one USING <code> clause',
                            context=node.context)
                    nativecode = node.nativecode
                    language = node.language

                elif node.code:
                    if code is not None or nativecode is not None:
                        raise EdgeQLSyntaxError(
                            'more than one USING <code> clause',
                            context=node.context)
                    code = node.code
                    language = node.language

                else:
                    # USING SQL EXPRESSION
                    from_expr = True
            else:
                commands.append(node)

        if (
            nativecode is None and
            code is None and
            from_function is None and
            not from_expr
        ):
            raise EdgeQLSyntaxError(
                'CREATE FUNCTION requires at least one USING clause',
                context=block.context)

        else:
            if from_expr and (from_function or code):
                raise EdgeQLSyntaxError(
                    'USING SQL EXPRESSION is mutually exclusive with other '
                    'USING variants',
                    context=block.context)

            props['code'] = qlast.FunctionCode(
                language=language,
                from_function=from_function,
                from_expr=from_expr,
                code=code,
            )

            props['nativecode'] = nativecode

        if commands:
            props['commands'] = commands

        return props


#
# CREATE TYPE ... { CREATE LINK ... { ON TARGET DELETE ...
#
class OnTargetDeleteStmt(Nonterm):
    def reduce_ON_TARGET_DELETE_RESTRICT(self, *kids):
        self.val = qlast.OnTargetDelete(
            cascade=qltypes.LinkTargetDeleteAction.RESTRICT)

    def reduce_ON_TARGET_DELETE_DELETE_SOURCE(self, *kids):
        self.val = qlast.OnTargetDelete(
            cascade=qltypes.LinkTargetDeleteAction.DELETE_SOURCE)

    def reduce_ON_TARGET_DELETE_ALLOW(self, *kids):
        self.val = qlast.OnTargetDelete(
            cascade=qltypes.LinkTargetDeleteAction.ALLOW)

    def reduce_ON_TARGET_DELETE_DEFERRED_RESTRICT(self, *kids):
        self.val = qlast.OnTargetDelete(
            cascade=qltypes.LinkTargetDeleteAction.DEFERRED_RESTRICT)
