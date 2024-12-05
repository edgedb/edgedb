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
import typing


from edb.errors import EdgeQLSyntaxError

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb.common import parsing
from edb.common import verutils

from . import expressions
from . import tokens

from .precedence import *  # NOQA
from .tokens import *  # NOQA
from .expressions import *  # NOQA


Nonterm = expressions.Nonterm  # type: ignore[misc]


def _parse_language(node):
    try:
        return qlast.Language(node.val.upper())
    except ValueError:
        raise EdgeQLSyntaxError(
            f'{node.val} is not a valid language',
            span=node.span) from None


def _validate_declarations(
    declarations: typing.Sequence[
        typing.Union[qlast.ModuleDeclaration, qlast.ObjectDDL]]
) -> None:
    # Check that top-level declarations either use fully-qualified
    # names or are module blocks.
    for decl in declarations:
        if (
            not isinstance(
                decl,
                (qlast.ModuleDeclaration, qlast.ExtensionCommand,
                 qlast.FutureCommand)
            ) and decl.name.module is None
        ):
            raise EdgeQLSyntaxError(
                "only fully-qualified name is allowed in "
                "top-level declaration",
                span=decl.name.span)


def extract_bases(bases, commands):
    vbases = bases
    vcommands = []
    for command in commands:
        if isinstance(command, qlast.AlterAddInherit):
            if vbases:
                raise EdgeQLSyntaxError(
                    "specifying EXTENDING twice is not allowed",
                    span=command.span)
            vbases = command.bases
        else:
            vcommands.append(command)
    return vbases, vcommands


class NewNontermHelper:
    def __init__(self, modname):
        self.name = modname

    def _new_nonterm(
        self, clsname, clsdict=None, clskwds=None, clsbases=(Nonterm,)
    ):
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
    @parsing.inline(0)
    def reduce_SEMICOLON(self, tok):
        pass

    @parsing.inline(0)
    def reduce_Semicolons_SEMICOLON(self, semicolons, semicolon):
        pass


class OptSemicolons(Nonterm):
    @parsing.inline(0)
    def reduce_Semicolons(self, semicolons):
        pass

    def reduce_empty(self):
        self.val = None


class ExtendingSimple(Nonterm):
    @parsing.inline(1)
    def reduce_EXTENDING_SimpleTypeNameList(self, _, list):
        pass


class OptExtendingSimple(Nonterm):
    @parsing.inline(0)
    def reduce_ExtendingSimple(self, extending):
        pass

    def reduce_empty(self):
        self.val = []


class Extending(Nonterm):
    @parsing.inline(1)
    def reduce_EXTENDING_TypeNameList(self, _, list):
        pass


class OptExtending(Nonterm):
    @parsing.inline(0)
    def reduce_Extending(self, extending):
        pass

    def reduce_empty(self):
        self.val = []


class CreateSimpleExtending(Nonterm):
    def reduce_EXTENDING_SimpleTypeNameList(self, *kids):
        self.val = qlast.AlterAddInherit(bases=kids[1].val)


class OnExpr(Nonterm):
    # NOTE: the reason why we need parentheses around the expression
    # is to disambiguate whether the '{' following the expression is
    # meant to be a shape or a nested DDL/SDL block.
    @parsing.inline(1)
    def reduce_ON_ParenExpr(self, _, expr):
        pass


class OptOnExpr(Nonterm):
    def reduce_empty(self):
        self.val = None

    @parsing.inline(0)
    def reduce_OnExpr(self, expr):
        pass


class OptDeferred(Nonterm):
    def reduce_empty(self):
        self.val = None

    def reduce_DEFERRED(self, _):
        self.val = True


class OptExceptExpr(Nonterm):
    def reduce_empty(self):
        self.val = None

    @parsing.inline(1)
    def reduce_EXCEPT_ParenExpr(self, _, expr):
        pass


class OptConcreteConstraintArgList(Nonterm):
    @parsing.inline(1)
    def reduce_LPAREN_OptPosCallArgList_RPAREN(self, _lparen, list, _rparen):
        pass

    def reduce_empty(self):
        self.val = []


class OptDefault(Nonterm):
    def reduce_empty(self):
        self.val = None

    @parsing.inline(1)
    def reduce_EQUALS_Expr(self, _, expr):
        pass


class ParameterKind(Nonterm):
    def reduce_VARIADIC(self, *kids):
        self.val = qltypes.ParameterKind.VariadicParam

    def reduce_NAMEDONLY(self, _):
        self.val = qltypes.ParameterKind.NamedOnlyParam


class OptParameterKind(Nonterm):
    def reduce_empty(self):
        self.val = qltypes.ParameterKind.PositionalParam

    @parsing.inline(0)
    def reduce_ParameterKind(self, *kids):
        pass


class FuncDeclArgName(Nonterm):
    def reduce_Identifier(self, dp):
        self.val = dp.val
        self.span = dp.span

    def reduce_PARAMETER(self, dp):
        if dp.val[1].isdigit():
            raise EdgeQLSyntaxError(
                f'numeric parameters are not supported',
                span=dp.span)
        else:
            raise EdgeQLSyntaxError(
                f"function parameters do not need a $ prefix, "
                f"rewrite as '{dp.val[1:]}'",
                span=dp.span)


class FuncDeclArg(Nonterm):
    def reduce_kwarg(self, kind, name, _, typemod, type, default):
        r"""%reduce OptParameterKind FuncDeclArgName COLON \
                OptTypeQualifier FullTypeExpr OptDefault \
        """
        self.val = qlast.FuncParam(
            kind=kind.val,
            name=name.val,
            typemod=typemod.val,
            type=type.val,
            default=default.val
        )

    def reduce_OptParameterKind_FuncDeclArgName_OptDefault(
        self, kind, name, default
    ):
        raise EdgeQLSyntaxError(
            f'missing type declaration for the `{name.val}` parameter',
            span=name.span
        )


class FuncDeclArgList(parsing.ListNonterm, element=FuncDeclArg,
                      separator=tokens.T_COMMA, allow_trailing_separator=True):
    pass


class FuncDeclArgs(Nonterm):
    @parsing.inline(0)
    def reduce_FuncDeclArgList(self, list):
        pass


class ProcessFunctionParamsMixin:
    def _validate_params(self, params):
        last_pos_default_arg = None
        last_named_arg = None
        variadic_arg = None
        names = set()

        for arg in params:
            if isinstance(arg, tuple):
                # A tuple here means that it's part of the "param := val"
                raise EdgeQLSyntaxError(
                    f"Unexpected ':='",
                    span=arg[1])

            if arg.name in names:
                raise EdgeQLSyntaxError(
                    f'duplicate parameter name `{arg.name}`',
                    span=arg.span)
            names.add(arg.name)

            if arg.kind is qltypes.ParameterKind.VariadicParam:
                if variadic_arg is not None:
                    raise EdgeQLSyntaxError(
                        'more than one variadic argument',
                        span=arg.span)
                elif last_named_arg is not None:
                    raise EdgeQLSyntaxError(
                        f'NAMED ONLY argument `{last_named_arg.name}` '
                        f'before VARIADIC argument `{arg.name}`',
                        span=last_named_arg.span)
                else:
                    variadic_arg = arg

                if arg.default is not None:
                    raise EdgeQLSyntaxError(
                        f'VARIADIC argument `{arg.name}` '
                        f'cannot have a default value',
                        span=arg.span)

            elif arg.kind is qltypes.ParameterKind.NamedOnlyParam:
                last_named_arg = arg

            else:
                if last_named_arg is not None:
                    raise EdgeQLSyntaxError(
                        f'positional argument `{arg.name}` '
                        f'follows NAMED ONLY argument `{last_named_arg.name}`',
                        span=arg.span)

                if variadic_arg is not None:
                    raise EdgeQLSyntaxError(
                        f'positional argument `{arg.name}` '
                        f'follows VARIADIC argument `{variadic_arg.name}`',
                        span=arg.span)

            if arg.kind is qltypes.ParameterKind.PositionalParam:
                if arg.default is None:
                    if last_pos_default_arg is not None:
                        raise EdgeQLSyntaxError(
                            f'positional argument `{arg.name}` without '
                            f'default follows positional argument '
                            f'`{last_pos_default_arg.name}` with default',
                            span=arg.span)
                else:
                    last_pos_default_arg = arg


class CreateFunctionArgs(Nonterm, ProcessFunctionParamsMixin):
    def reduce_LPAREN_RPAREN(self, _lparen, _rparen):
        self.val = []

    def reduce_LPAREN_FuncDeclArgs_RPAREN(self, _lparen, args, _rparen):
        args = args.val
        self._validate_params(args)
        self.val = args


class OptTypeQualifier(Nonterm):
    def reduce_SET_OF(self, _s, _o):
        self.val = qltypes.TypeModifier.SetOfType

    def reduce_OPTIONAL(self, _):
        self.val = qltypes.TypeModifier.OptionalType

    def reduce_empty(self):
        self.val = qltypes.TypeModifier.SingletonType


class FunctionType(Nonterm):
    @parsing.inline(0)
    def reduce_FullTypeExpr(self, expr):
        pass


class FromFunction(Nonterm):
    def reduce_USING_ParenExpr(self, _, expr):
        lang = qlast.Language.EdgeQL
        self.val = qlast.FunctionCode(
            language=lang,
            nativecode=expr.val)

    def reduce_USING_Identifier_BaseStringConstant(self, _, ident, const):
        lang = _parse_language(ident)
        code = const.val.value
        self.val = qlast.FunctionCode(language=lang, code=code)

    def reduce_USING_Identifier_FUNCTION_BaseStringConstant(
        self, _using, ident, _function, const
    ):
        lang = _parse_language(ident)
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in USING FUNCTION clause',
                span=ident.span) from None

        self.val = qlast.FunctionCode(
            language=lang,
            from_function=const.val.value
        )

    def reduce_USING_Identifier_EXPRESSION(self, _using, ident, _expression):
        lang = _parse_language(ident)
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in USING clause',
                span=ident.span) from None

        self.val = qlast.FunctionCode(language=lang)


class ProcessFunctionBlockMixin:
    def _process_function_body(self, block, *, optional_using: bool=False):
        props: typing.Dict[str, typing.Any] = {}

        commands = []
        code = None
        nativecode = None
        language = qlast.Language.EdgeQL
        from_expr = False
        from_function = None

        for node in block.val:
            if isinstance(node, qlast.FunctionCode):
                if node.from_function:
                    if from_function is not None:
                        raise EdgeQLSyntaxError(
                            'more than one USING FUNCTION clause',
                            span=node.span)
                    from_function = node.from_function
                    language = qlast.Language.SQL

                elif node.nativecode:
                    if code is not None or nativecode is not None:
                        raise EdgeQLSyntaxError(
                            'more than one USING <code> clause',
                            span=node.span)
                    nativecode = node.nativecode
                    language = node.language

                elif node.code:
                    if code is not None or nativecode is not None:
                        raise EdgeQLSyntaxError(
                            'more than one USING <code> clause',
                            span=node.span)
                    code = node.code
                    language = node.language

                else:
                    # USING SQL EXPRESSION
                    from_expr = True
                    language = qlast.Language.SQL
            else:
                commands.append(node)

        if (
            nativecode is None and
            code is None and
            from_function is None and
            not from_expr and
            not optional_using
        ):
            raise EdgeQLSyntaxError(
                'missing a USING clause',
                span=block.span)

        else:
            if from_expr and (from_function or code):
                raise EdgeQLSyntaxError(
                    'USING SQL EXPRESSION is mutually exclusive with other '
                    'USING variants',
                    span=block.span)

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
    def reduce_ON_TARGET_DELETE_RESTRICT(self, *_):
        self.val = qlast.OnTargetDelete(
            cascade=qltypes.LinkTargetDeleteAction.Restrict)

    def reduce_ON_TARGET_DELETE_DELETE_SOURCE(self, *_):
        self.val = qlast.OnTargetDelete(
            cascade=qltypes.LinkTargetDeleteAction.DeleteSource)

    def reduce_ON_TARGET_DELETE_ALLOW(self, *_):
        self.val = qlast.OnTargetDelete(
            cascade=qltypes.LinkTargetDeleteAction.Allow)

    def reduce_ON_TARGET_DELETE_DEFERRED_RESTRICT(self, *_):
        self.val = qlast.OnTargetDelete(
            cascade=qltypes.LinkTargetDeleteAction.DeferredRestrict)


class OnSourceDeleteStmt(Nonterm):
    def reduce_ON_SOURCE_DELETE_DELETE_TARGET(self, *_):
        self.val = qlast.OnSourceDelete(
            cascade=qltypes.LinkSourceDeleteAction.DeleteTarget)

    def reduce_ON_SOURCE_DELETE_ALLOW(self, *_):
        self.val = qlast.OnSourceDelete(
            cascade=qltypes.LinkSourceDeleteAction.Allow)

    def reduce_ON_SOURCE_DELETE_DELETE_TARGET_IF_ORPHAN(self, *_):
        self.val = qlast.OnSourceDelete(
            cascade=qltypes.LinkSourceDeleteAction.DeleteTargetIfOrphan)


class OptWhenBlock(Nonterm):
    @parsing.inline(1)
    def reduce_WHEN_ParenExpr(self, _, expr):
        pass

    def reduce_empty(self):
        self.val = None


class OptUsingBlock(Nonterm):
    @parsing.inline(1)
    def reduce_USING_ParenExpr(self, _, expr):
        pass

    def reduce_empty(self):
        self.val = None


class AccessKind(Nonterm):

    def reduce_ALL(self, _):
        self.val = list(qltypes.AccessKind)

    def reduce_SELECT(self, _):
        self.val = [qltypes.AccessKind.Select]

    def reduce_UPDATE(self, _):
        self.val = [
            qltypes.AccessKind.UpdateRead, qltypes.AccessKind.UpdateWrite]

    def reduce_UPDATE_READ(self, _u, _r):
        self.val = [qltypes.AccessKind.UpdateRead]

    def reduce_UPDATE_WRITE(self, _u, _w):
        self.val = [qltypes.AccessKind.UpdateWrite]

    def reduce_INSERT(self, _):
        self.val = [qltypes.AccessKind.Insert]

    def reduce_DELETE(self, _):
        self.val = [qltypes.AccessKind.Delete]


class AccessKindList(parsing.ListNonterm, element=AccessKind,
                     separator=tokens.T_COMMA):
    pass


class AccessPolicyAction(Nonterm):

    def reduce_ALLOW(self, _):
        self.val = qltypes.AccessPolicyAction.Allow

    def reduce_DENY(self, _):
        self.val = qltypes.AccessPolicyAction.Deny


class TriggerTiming(Nonterm):
    def reduce_AFTER(self, *kids):
        self.val = qltypes.TriggerTiming.After

    def reduce_AFTER_COMMIT_OF(self, *kids):
        self.val = qltypes.TriggerTiming.AfterCommitOf


class TriggerKind(Nonterm):
    def reduce_UPDATE(self, *kids):
        self.val = qltypes.TriggerKind.Update

    def reduce_INSERT(self, *kids):
        self.val = qltypes.TriggerKind.Insert

    def reduce_DELETE(self, *kids):
        self.val = qltypes.TriggerKind.Delete


class TriggerKindList(parsing.ListNonterm, element=TriggerKind,
                      separator=tokens.T_COMMA):
    pass


class TriggerScope(Nonterm):
    def reduce_EACH(self, *kids):
        self.val = qltypes.TriggerScope.Each

    def reduce_ALL(self, *kids):
        self.val = qltypes.TriggerScope.All


class RewriteKind(Nonterm):
    def reduce_UPDATE(self, *kids):
        self.val = qltypes.RewriteKind.Update

    def reduce_INSERT(self, *kids):
        self.val = qltypes.RewriteKind.Insert


class RewriteKindList(parsing.ListNonterm, element=RewriteKind,
                      separator=tokens.T_COMMA):
    pass


class ExtensionVersion(Nonterm):

    def reduce_VERSION_BaseStringConstant(self, _, const):
        version = const.val

        try:
            verutils.parse_version(version.value)
        except ValueError:
            raise EdgeQLSyntaxError(
                'invalid extension version format',
                details='Expected a SemVer-compatible format.',
                span=version.span,
            ) from None

        self.val = version


class OptExtensionVersion(Nonterm):

    @parsing.inline(0)
    def reduce_ExtensionVersion(self, version):
        pass

    def reduce_empty(self):
        self.val = None


class IndexArg(Nonterm):
    def reduce_kwarg_bad_definition(self, *kids):
        r"""%reduce FuncDeclArgName COLON \
                OptTypeQualifier FullTypeExpr OptDefault \
        """
        raise EdgeQLSyntaxError(
            f'index parameters have to be NAMED ONLY',
            span=kids[0].span)

    def reduce_kwarg_definition(self, kind, name, _, typemod, type, default):
        r"""%reduce ParameterKind FuncDeclArgName COLON \
                OptTypeQualifier FullTypeExpr OptDefault \
        """
        if kind.val is not qltypes.ParameterKind.NamedOnlyParam:
            raise EdgeQLSyntaxError(
                f'index parameters have to be NAMED ONLY',
                span=kind.span)

        self.val = qlast.FuncParam(
            kind=kind.val,
            name=name.val,
            typemod=typemod.val,
            type=type.val,
            default=default.val
        )

    def reduce_AnyIdentifier_ASSIGN_Expr(self, ident, _, expr):
        self.val = (
            ident.val,
            ident.span,
            expr.val,
        )

    def reduce_FuncDeclArgName_OptDefault(self, name, default):
        raise EdgeQLSyntaxError(
            f'missing type declaration for the `{name.val}` parameter',
            span=name.span)


class IndexArgList(parsing.ListNonterm, element=IndexArg,
                   separator=tokens.T_COMMA, allow_trailing_separator=True):
    pass


class OptIndexArgList(Nonterm):
    @parsing.inline(0)
    def reduce_IndexArgList(self, list):
        pass

    def reduce_empty(self):
        self.val = []


class IndexExtArgList(Nonterm):

    @parsing.inline(1)
    def reduce_LPAREN_OptIndexArgList_RPAREN(self, *_):
        pass


class OptIndexExtArgList(Nonterm):

    @parsing.inline(0)
    def reduce_IndexExtArgList(self, list):
        pass

    def reduce_empty(self):
        self.val = []


class ProcessIndexMixin(ProcessFunctionParamsMixin):
    def _process_arguments(self, arguments):
        kwargs = {}
        for argval in arguments:
            if isinstance(argval, qlast.FuncParam):
                raise EdgeQLSyntaxError(
                    f"unexpected new parameter definition `{argval.name}`",
                    span=argval.span)

            argname, argname_ctx, arg = argval
            if argname in kwargs:
                raise EdgeQLSyntaxError(
                    f"duplicate named argument `{argname}`",
                    span=argname_ctx)

            kwargs[argname] = arg

        return kwargs

    def _process_params_or_kwargs(self, bases, arguments):
        params = []
        kwargs = dict()

        # If the definition is extending another abstract index, then we
        # cannot define new parameters, but can only supply some arguments.
        if bases:
            kwargs = self._process_arguments(arguments)
        else:
            params = arguments
            self._validate_params(params)

        return params, kwargs

    def _process_sql_body(self, block, *, optional_using: bool=False):
        props: typing.Dict[str, typing.Any] = {}

        commands = []
        code = None

        for node in block.val:
            if isinstance(node, qlast.IndexCode):
                if code is not None:
                    raise EdgeQLSyntaxError(
                        'more than one USING <code> clause',
                        span=node.span)
                props['code'] = node
            else:
                commands.append(node)

        if commands:
            props['commands'] = commands

        return props
