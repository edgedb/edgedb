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

from edb.edgeql import ast as qlast

from edb.common import parsing
from edb import errors

from . import expressions
from . import commondl

from .precedence import *  # NOQA
from .tokens import *  # NOQA
from .commondl import *  # NOQA


Nonterm = expressions.Nonterm  # type: ignore[misc]
OptSemicolons = commondl.OptSemicolons  # type: ignore[misc]


sdl_nontem_helper = commondl.NewNontermHelper(__name__)
_new_nonterm = sdl_nontem_helper._new_nonterm


# top-level SDL statements
class SDLStatement(Nonterm):
    def reduce_SDLBlockStatement(self, *kids):
        self.val = kids[0].val

    def reduce_SDLShortStatement_SEMICOLON(self, *kids):
        self.val = kids[0].val


# a list of SDL statements with optional semicolon separators
class SDLStatements(parsing.ListNonterm, element=SDLStatement,
                    separator=OptSemicolons):
    pass


# These statements have a block
class SDLBlockStatement(Nonterm):
    def reduce_ModuleDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_ScalarTypeDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_AnnotationDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_ObjectTypeDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_AliasDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_ConstraintDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_LinkDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_PropertyDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_FunctionDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_GlobalDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_IndexDeclaration(self, *kids):
        self.val = kids[0].val


# these statements have no {} block
class SDLShortStatement(Nonterm):

    def reduce_ExtensionRequirementDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_FutureRequirementDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_ScalarTypeDeclarationShort(self, *kids):
        self.val = kids[0].val

    def reduce_AnnotationDeclarationShort(self, *kids):
        self.val = kids[0].val

    def reduce_ObjectTypeDeclarationShort(self, *kids):
        self.val = kids[0].val

    def reduce_AliasDeclarationShort(self, *kids):
        self.val = kids[0].val

    def reduce_ConstraintDeclarationShort(self, *kids):
        self.val = kids[0].val

    def reduce_LinkDeclarationShort(self, *kids):
        self.val = kids[0].val

    def reduce_PropertyDeclarationShort(self, *kids):
        self.val = kids[0].val

    def reduce_FunctionDeclarationShort(self, *kids):
        self.val = kids[0].val

    def reduce_GlobalDeclarationShort(self, *kids):
        self.val = kids[0].val

    def reduce_IndexDeclarationShort(self, *kids):
        self.val = kids[0].val


# A rule for an SDL block, either as part of `module` declaration or
# as top-level schema used in MIGRATION DDL.
class SDLCommandBlock(Nonterm):
    # this command block can be empty
    def reduce_LBRACE_OptSemicolons_RBRACE(self, *kids):
        self.val = []

    def reduce_statement_without_semicolons(self, *kids):
        r"""%reduce LBRACE \
                OptSemicolons SDLShortStatement \
            RBRACE
        """
        _, _, stmt, _ = kids
        self.val = [stmt.val]

    def reduce_statements_without_optional_trailing_semicolons(self, *kids):
        r"""%reduce LBRACE \
                OptSemicolons SDLStatements \
                OptSemicolons SDLShortStatement \
            RBRACE
        """
        _, _, stmts, _, stmt, _ = kids
        self.val = stmts.val + [stmt.val]

    def reduce_LBRACE_OptSemicolons_SDLStatements_RBRACE(self, *kids):
        _, _, stmts, _ = kids
        self.val = stmts.val

    def reduce_statements_without_optional_trailing_semicolons2(self, *kids):
        r"""%reduce LBRACE \
                OptSemicolons SDLStatements \
                Semicolons \
            RBRACE
        """
        _, _, stmts, _, _ = kids
        self.val = stmts.val


class SDLProductionHelper:
    def _passthrough(self, *cmds):
        self.val = cmds[0].val

    def _singleton_list(self, cmd):
        self.val = [cmd.val]

    def _empty(self, *kids):
        self.val = []

    def _block(self, lbrace, sc1, cmdl, rbrace):
        self.val = [cmdl.val]

    def _block2(self, lbrace, sc1, cmdlist, sc2, rbrace):
        self.val = cmdlist.val

    def _block3(self, lbrace, sc1, cmdlist, sc2, cmd, rbrace):
        self.val = cmdlist.val + [cmd.val]


def sdl_commands_block(parent, *commands, opt=True):
    if parent is None:
        parent = ''

    # SDLCommand := SDLCommand1 | SDLCommand2 ...
    #
    # All the "short" commands, ones that need a ";" are gathered as
    # SDLCommandShort.
    #
    # All the "block" commands, ones that have a "{...}" and don't
    # need a ";" are gathered as SDLCommandBlock.
    clsdict_b = {}
    clsdict_s = {}

    for command in commands:
        if command.__name__.endswith('Block'):
            clsdict_b[f'reduce_{command.__name__}'] = \
                SDLProductionHelper._passthrough
        else:
            clsdict_s[f'reduce_{command.__name__}'] = \
                SDLProductionHelper._passthrough

    cmd_s = _new_nonterm(f'{parent}SDLCommandShort', clsdict=clsdict_s)
    cmd_b = _new_nonterm(f'{parent}SDLCommandBlock', clsdict=clsdict_b)

    # Merged command which has minimal ";"
    #
    # SDLCommandFull := SDLCommandShort ; | SDLCommandBlock
    clsdict = {}
    clsdict[f'reduce_{cmd_s.__name__}_SEMICOLON'] = \
        SDLProductionHelper._passthrough
    clsdict[f'reduce_{cmd_b.__name__}'] = \
        SDLProductionHelper._passthrough
    cmd = _new_nonterm(f'{parent}SDLCommandFull', clsdict=clsdict)

    # SDLCommandsList := SDLCommandFull [; SDLCommandFull ...]
    cmdlist = _new_nonterm(f'{parent}SDLCommandsList',
                           clsbases=(parsing.ListNonterm,),
                           clskwds=dict(element=cmd, separator=OptSemicolons))

    # Command block is tricky, but the inner commands must terminate
    # without a ";", is possible.
    #
    # SDLCommandsBlock :=
    #
    #   { [ ; ] SDLCommandFull }
    #   { [ ; ] SDLCommandsList [ ; ]} |
    #   { [ ; ] SDLCommandsList [ ; ] SDLCommandFull }
    clsdict = {}
    clsdict[f'reduce_LBRACE_OptSemicolons_{cmd_s.__name__}_RBRACE'] = \
        SDLProductionHelper._block
    clsdict[f'reduce_LBRACE_OptSemicolons_{cmdlist.__name__}_' +
            f'OptSemicolons_RBRACE'] = \
        SDLProductionHelper._block2
    clsdict[f'reduce_LBRACE_OptSemicolons_{cmdlist.__name__}_OptSemicolons_' +
            f'{cmd_s.__name__}_RBRACE'] = \
        SDLProductionHelper._block3
    clsdict[f'reduce_LBRACE_OptSemicolons_RBRACE'] = \
        SDLProductionHelper._empty
    _new_nonterm(f'{parent}SDLCommandsBlock', clsdict=clsdict)

    if opt is False:
        #   | Command
        clsdict = {}
        clsdict[f'reduce_{cmd_s.__name__}'] = \
            SDLProductionHelper._singleton_list
        clsdict[f'reduce_{cmd_b.__name__}'] = \
            SDLProductionHelper._singleton_list
        _new_nonterm(parent + 'SingleSDLCommandBlock', clsdict=clsdict)


class Using(Nonterm):
    def reduce_USING_ParenExpr(self, *kids):
        _, paren_expr = kids
        self.val = qlast.SetField(
            name='expr',
            value=paren_expr.val,
            special_syntax=True,
        )


class SetField(Nonterm):
    # field := <expr>
    def reduce_Identifier_ASSIGN_Expr(self, *kids):
        identifier, _, expr = kids
        self.val = qlast.SetField(name=identifier.val, value=expr.val)


class SetAnnotation(Nonterm):
    def reduce_ANNOTATION_NodeName_ASSIGN_Expr(self, *kids):
        _, name, _, expr = kids
        self.val = qlast.CreateAnnotationValue(name=name.val, value=expr.val)


sdl_commands_block(
    'Create',
    Using,
    SetField,
    SetAnnotation)


class ExtensionRequirementDeclaration(Nonterm):

    def reduce_USING_EXTENSION_ShortNodeName_OptExtensionVersion(self, *kids):
        _, _, name, version = kids
        self.val = qlast.CreateExtension(
            name=name.val,
            version=version.val,
        )


class FutureRequirementDeclaration(Nonterm):

    def reduce_USING_FUTURE_ShortNodeName(self, *kids):
        _, _, name = kids
        self.val = qlast.CreateFuture(
            name=name.val,
        )


class ModuleDeclaration(Nonterm):
    def reduce_MODULE_ModuleName_SDLCommandBlock(self, *kids):
        _, module_name, block = kids

        # Check that top-level declarations DO NOT use fully-qualified
        # names and aren't nested module blocks.
        declarations = block.val
        for decl in declarations:
            if isinstance(decl, qlast.ExtensionCommand):
                raise errors.EdgeQLSyntaxError(
                    "'using extension' cannot be used inside a module block",
                    context=decl.context)
            elif isinstance(decl, qlast.FutureCommand):
                raise errors.EdgeQLSyntaxError(
                    "'using future' cannot be used inside a module block",
                    context=decl.context)
            elif decl.name.module is not None:
                raise errors.EdgeQLSyntaxError(
                    "fully-qualified name is not allowed in "
                    "a module declaration",
                    context=decl.name.context)

        self.val = qlast.ModuleDeclaration(
            # mirror what we do in CREATE MODULE
            name=qlast.ObjectRef(module=None, name='::'.join(module_name.val)),
            declarations=declarations,
        )


#
# Constraints
#
class ConstraintDeclaration(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT NodeName OptOnExpr \
                    OptExtendingSimple CreateSDLCommandsBlock"""
        _, _, name, on_expr, extending, commands = kids
        self.val = qlast.CreateConstraint(
            name=name.val,
            subjectexpr=on_expr.val,
            bases=extending.val,
            commands=commands.val,
        )

    def reduce_CreateConstraint_CreateFunctionArgs(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT NodeName CreateFunctionArgs \
                    OptOnExpr OptExtendingSimple CreateSDLCommandsBlock"""
        _, _, name, args, on_expr, extending, commands = kids
        self.val = qlast.CreateConstraint(
            name=name.val,
            params=args.val,
            subjectexpr=on_expr.val,
            bases=extending.val,
            commands=commands.val,
        )


class ConstraintDeclarationShort(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT NodeName OptOnExpr \
                    OptExtendingSimple"""
        _, _, name, on_expr, extending = kids
        self.val = qlast.CreateConstraint(
            name=name.val,
            subject=on_expr.val,
            bases=extending.val,
        )

    def reduce_CreateConstraint_CreateFunctionArgs(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT NodeName CreateFunctionArgs \
                    OptOnExpr OptExtendingSimple"""
        _, _, name, args, on_expr, extending = kids
        self.val = qlast.CreateConstraint(
            name=name.val,
            params=args.val,
            subject=on_expr.val,
            bases=extending.val,
        )


class ConcreteConstraintBlock(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr \
                    OptExceptExpr \
                    CreateSDLCommandsBlock"""
        _, name, arg_list, on_expr, except_expr, commands = kids
        self.val = qlast.CreateConcreteConstraint(
            name=name.val,
            args=arg_list.val,
            subjectexpr=on_expr.val,
            except_expr=except_expr.val,
            commands=commands.val,
        )

    def reduce_CreateDelegatedConstraint(self, *kids):
        r"""%reduce DELEGATED CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr \
                    OptExceptExpr \
                    CreateSDLCommandsBlock"""
        _, _, name, arg_list, on_expr, except_expr, commands = kids
        self.val = qlast.CreateConcreteConstraint(
            delegated=True,
            name=name.val,
            args=arg_list.val,
            subjectexpr=on_expr.val,
            except_expr=except_expr.val,
            commands=commands.val,
        )


class ConcreteConstraintShort(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr \
                    OptExceptExpr"""
        _, name, arg_list, on_expr, except_expr = kids
        self.val = qlast.CreateConcreteConstraint(
            name=name.val,
            args=arg_list.val,
            subjectexpr=on_expr.val,
            except_expr=except_expr.val,
        )

    def reduce_CreateDelegatedConstraint(self, *kids):
        r"""%reduce DELEGATED CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr \
                    OptExceptExpr"""
        _, _, name, arg_list, on_expr, except_expr = kids
        self.val = qlast.CreateConcreteConstraint(
            delegated=True,
            name=name.val,
            args=arg_list.val,
            subjectexpr=on_expr.val,
            except_expr=except_expr.val,
        )


#
# Scalar Types
#

sdl_commands_block(
    'CreateScalarType',
    SetField,
    SetAnnotation,
    ConcreteConstraintBlock,
    ConcreteConstraintShort,
)


class ScalarTypeDeclaration(Nonterm):
    def reduce_CreateAbstractScalarTypeStmt(self, *kids):
        r"""%reduce \
            ABSTRACT SCALAR TYPE NodeName \
            OptExtending CreateScalarTypeSDLCommandsBlock \
        """
        _, _, _, name, extending, commands = kids
        self.val = qlast.CreateScalarType(
            abstract=True,
            name=name.val,
            bases=extending.val,
            commands=commands.val,
        )

    def reduce_ScalarTypeDeclaration(self, *kids):
        r"""%reduce \
            SCALAR TYPE NodeName \
            OptExtending CreateScalarTypeSDLCommandsBlock \
        """
        _, _, name, extending, commands = kids
        self.val = qlast.CreateScalarType(
            name=name.val,
            bases=extending.val,
            commands=commands.val,
        )


class ScalarTypeDeclarationShort(Nonterm):
    def reduce_CreateAbstractScalarTypeStmt(self, *kids):
        r"""%reduce \
            ABSTRACT SCALAR TYPE NodeName \
            OptExtending \
        """
        _, _, _, name, extending = kids
        self.val = qlast.CreateScalarType(
            abstract=True,
            name=name.val,
            bases=extending.val,
        )

    def reduce_ScalarTypeDeclaration(self, *kids):
        r"""%reduce \
            SCALAR TYPE NodeName \
            OptExtending \
        """
        _, _, name, extending = kids
        self.val = qlast.CreateScalarType(
            name=name.val,
            bases=extending.val,
        )


#
# Annotations
#
class AnnotationDeclaration(Nonterm):
    def reduce_CreateAnnotation(self, *kids):
        r"""%reduce ABSTRACT ANNOTATION NodeName OptExtendingSimple \
                    CreateSDLCommandsBlock"""
        _, _, name, extending, commands = kids
        self.val = qlast.CreateAnnotation(
            abstract=True,
            name=name.val,
            bases=extending.val,
            inheritable=False,
            commands=commands.val,
        )

    def reduce_CreateInheritableAnnotation(self, *kids):
        r"""%reduce ABSTRACT INHERITABLE ANNOTATION
                    NodeName OptExtendingSimple CreateSDLCommandsBlock"""
        _, _, _, name, extending, commands = kids
        self.val = qlast.CreateAnnotation(
            abstract=True,
            name=name.val,
            bases=extending.val,
            inheritable=True,
            commands=commands.val,
        )


class AnnotationDeclarationShort(Nonterm):
    def reduce_CreateAnnotation(self, *kids):
        r"""%reduce ABSTRACT ANNOTATION NodeName OptExtendingSimple"""
        _, _, name, extending = kids
        self.val = qlast.CreateAnnotation(
            abstract=True,
            name=name.val,
            bases=extending.val,
            inheritable=False,
        )

    def reduce_CreateInheritableAnnotation(self, *kids):
        r"""%reduce ABSTRACT INHERITABLE ANNOTATION
                    NodeName OptExtendingSimple"""
        _, _, _, name, extending = kids
        self.val = qlast.CreateAnnotation(
            abstract=True,
            name=name.val,
            bases=extending.val,
            inheritable=True,
        )


#
# Indexes
#
sdl_commands_block(
    'CreateIndex',
    Using,
    SetField,
    SetAnnotation,
)


class IndexDeclaration(
    Nonterm,
    commondl.ProcessFunctionParamsMixin,
    commondl.ProcessIndexMixin,
):
    def reduce_CreateIndex(self, *kids):
        r"""%reduce ABSTRACT INDEX NodeName \
                    CreateIndexSDLCommandsBlock"""
        _, _, name, commands = kids
        self.val = qlast.CreateIndex(
            name=name.val,
            commands=commands.val,
        )

    def reduce_CreateIndex_CreateFunctionArgs(self, *kids):
        r"""%reduce ABSTRACT INDEX NodeName IndexExtArgList \
                    CreateIndexSDLCommandsBlock"""
        _, _, name, arg_list, commands = kids
        self._validate_params(kids[3].val)
        self.val = qlast.CreateIndex(
            name=name.val,
            params=arg_list.val,
            commands=commands.val,
        )


class IndexDeclarationShort(
    Nonterm,
    commondl.ProcessFunctionParamsMixin,
    commondl.ProcessIndexMixin,
):
    def reduce_CreateIndex(self, *kids):
        r"""%reduce ABSTRACT INDEX NodeName"""
        _, _, name = kids
        self.val = qlast.CreateIndex(
            name=name.val,
        )

    def reduce_CreateIndex_CreateFunctionArgs(self, *kids):
        r"""%reduce ABSTRACT INDEX NodeName IndexExtArgList"""
        _, _, name, arg_list = kids
        self._validate_params(arg_list.val)
        self.val = qlast.CreateIndex(
            name=name.val,
            params=arg_list.val,
        )


sdl_commands_block(
    'CreateConcreteIndex',
    SetField,
    SetAnnotation)


class ConcreteIndexDeclarationBlock(Nonterm, commondl.ProcessIndexMixin):
    def reduce_INDEX_OnExpr_OptExceptExpr_CreateConcreteIndexSDLCommandsBlock(
            self, *kids):
        _, on_expr, except_expr, commands = kids
        self.val = qlast.CreateConcreteIndex(
            name=qlast.ObjectRef(module='__', name='idx'),
            expr=on_expr.val,
            except_expr=except_expr.val,
            commands=commands.val,
        )

    def reduce_CreateConcreteIndex(self, *kids):
        r"""%reduce INDEX NodeName \
                    OnExpr OptExceptExpr \
                    CreateConcreteIndexSDLCommandsBlock \
        """
        _, name, on_expr, except_expr, commands = kids
        self.val = qlast.CreateConcreteIndex(
            name=name.val,
            expr=on_expr.val,
            except_expr=except_expr.val,
            commands=commands.val,
        )


class ConcreteIndexDeclarationShort(Nonterm, commondl.ProcessIndexMixin):
    def reduce_INDEX_OnExpr_OptExceptExpr(self, *kids):
        _, on_expr, except_expr = kids
        self.val = qlast.CreateConcreteIndex(
            name=qlast.ObjectRef(module='__', name='idx'),
            expr=on_expr.val,
            except_expr=except_expr.val,
        )

    def reduce_CreateConcreteIndex(self, *kids):
        r"""%reduce INDEX NodeName \
                    OnExpr OptExceptExpr \
        """
        _, name, on_expr, except_expr = kids
        self.val = qlast.CreateConcreteIndex(
            name=name.val,
            expr=on_expr.val,
            except_expr=except_expr.val,
        )


#
# Properties
#
class PropertyDeclaration(Nonterm):
    def reduce_CreateProperty(self, *kids):
        r"""%reduce ABSTRACT PROPERTY PtrNodeName OptExtendingSimple \
                    CreateSDLCommandsBlock \
        """
        _, _, name, extending, commands_block = kids

        self.val = qlast.CreateProperty(
            name=name.val,
            bases=extending.val,
            commands=commands_block.val,
            abstract=True,
        )


class PropertyDeclarationShort(Nonterm):
    def reduce_CreateProperty(self, *kids):
        r"""%reduce ABSTRACT PROPERTY PtrNodeName OptExtendingSimple"""
        _, _, name, extending = kids
        self.val = qlast.CreateProperty(
            name=name.val,
            bases=extending.val,
            abstract=True,
        )


sdl_commands_block(
    'CreateConcreteProperty',
    Using,
    SetField,
    SetAnnotation,
    ConcreteConstraintBlock,
    ConcreteConstraintShort,
)


class PtrTarget(Nonterm):

    def reduce_ARROW_FullTypeExpr(self, *kids):
        _arrow, type_expr = kids

        self.val = type_expr.val
        self.context = type_expr.val.context


class OptPtrTarget(Nonterm):

    def reduce_empty(self, *kids):
        self.val = None

    def reduce_PtrTarget(self, *kids):
        (ptr,) = kids
        self.val = ptr.val


class ConcretePropertyBlock(Nonterm):
    def _extract_target(self, target, cmds, context, *, overloaded=False):
        if target:
            return target, cmds

        for cmd in cmds:
            if isinstance(cmd, qlast.SetField) and cmd.name == 'expr':
                if target is not None:
                    raise errors.EdgeQLSyntaxError(
                        f'computed property with more than one expression',
                        context=context)
                target = cmd.value

        if not overloaded and target is None:
            raise errors.EdgeQLSyntaxError(
                f'computed property without expression',
                context=context)

        return target, cmds

    def reduce_CreateRegularProperty(self, *kids):
        """%reduce
            PROPERTY PathNodeName OptExtendingSimple
            OptPtrTarget CreateConcretePropertySDLCommandsBlock
        """
        _, name, extending, target, commands_block = kids

        target, cmds = self._extract_target(
            target.val, commands_block.val, name.context
        )
        self.val = qlast.CreateConcreteProperty(
            name=name.val,
            bases=extending.val,
            target=target,
            commands=cmds,
        )

    def reduce_CreateRegularQualifiedProperty(self, *kids):
        """%reduce
            PtrQuals PROPERTY PathNodeName OptExtendingSimple
            OptPtrTarget CreateConcretePropertySDLCommandsBlock
        """
        (quals, property, name, extending, target, commands) = kids

        target, cmds = self._extract_target(
            target.val, commands.val, property.context
        )
        self.val = qlast.CreateConcreteProperty(
            name=name.val,
            bases=extending.val,
            is_required=quals.val.required,
            cardinality=quals.val.cardinality,
            target=target,
            commands=cmds,
        )

    def reduce_CreateOverloadedProperty(self, *kids):
        """%reduce
            OVERLOADED OptPtrQuals PROPERTY PathNodeName OptExtendingSimple
            OptPtrTarget CreateConcretePropertySDLCommandsBlock
        """
        _, quals, _, name, extending, target, commands = kids
        target, cmds = self._extract_target(
            target.val, commands.val, name.context, overloaded=True
        )
        self.val = qlast.CreateConcreteProperty(
            name=name.val,
            bases=extending.val,
            declared_overloaded=True,
            is_required=quals.val.required,
            cardinality=quals.val.cardinality,
            target=target,
            commands=cmds,
        )


class ConcretePropertyShort(Nonterm):
    def reduce_CreateRegularProperty(self, *kids):
        """%reduce
            PROPERTY PathNodeName OptExtendingSimple PtrTarget
        """
        _, name, extending, target = kids
        self.val = qlast.CreateConcreteProperty(
            name=name.val,
            bases=extending.val,
            target=target.val,
        )

    def reduce_CreateRegularQualifiedProperty(self, *kids):
        """%reduce
            PtrQuals PROPERTY PathNodeName OptExtendingSimple PtrTarget
        """
        quals, _, name, extending, target = kids
        self.val = qlast.CreateConcreteProperty(
            name=name.val,
            bases=extending.val,
            is_required=quals.val.required,
            cardinality=quals.val.cardinality,
            target=target.val,
        )

    def reduce_CreateOverloadedProperty(self, *kids):
        """%reduce
            OVERLOADED OptPtrQuals PROPERTY PathNodeName OptExtendingSimple
            OptPtrTarget
        """
        _, quals, _, name, extending, target = kids
        self.val = qlast.CreateConcreteProperty(
            name=name.val,
            bases=extending.val,
            declared_overloaded=True,
            is_required=quals.val.required,
            cardinality=quals.val.cardinality,
            target=target.val,
        )

    def reduce_CreateComputableProperty(self, *kids):
        """%reduce
            PROPERTY PathNodeName ASSIGN Expr
        """
        _, name, _, expr = kids
        self.val = qlast.CreateConcreteProperty(
            name=name.val,
            target=expr.val,
        )

    def reduce_CreateQualifiedComputableProperty(self, *kids):
        """%reduce
            PtrQuals PROPERTY PathNodeName ASSIGN Expr
        """
        quals, _, name, _, expr = kids
        self.val = qlast.CreateConcreteProperty(
            name=name.val,
            is_required=quals.val.required,
            cardinality=quals.val.cardinality,
            target=expr.val,
        )


#
# Links
#

sdl_commands_block(
    'CreateLink',
    SetField,
    SetAnnotation,
    ConcreteConstraintBlock,
    ConcreteConstraintShort,
    ConcretePropertyBlock,
    ConcretePropertyShort,
    ConcreteIndexDeclarationBlock,
    ConcreteIndexDeclarationShort,
)


class LinkDeclaration(Nonterm):
    def reduce_CreateLink(self, *kids):
        r"""%reduce \
            ABSTRACT LINK PtrNodeName OptExtendingSimple \
            CreateLinkSDLCommandsBlock \
        """
        _, _, name, extending, commands = kids
        self.val = qlast.CreateLink(
            name=name.val,
            bases=extending.val,
            commands=commands.val,
            abstract=True,
        )


class LinkDeclarationShort(Nonterm):
    def reduce_CreateLink(self, *kids):
        r"""%reduce \
            ABSTRACT LINK PtrNodeName OptExtendingSimple"""
        _, _, name, extending = kids
        self.val = qlast.CreateLink(
            name=name.val,
            bases=extending.val,
            abstract=True,
        )


sdl_commands_block(
    'CreateConcreteLink',
    Using,
    SetField,
    SetAnnotation,
    ConcreteConstraintBlock,
    ConcreteConstraintShort,
    ConcretePropertyBlock,
    ConcretePropertyShort,
    ConcreteIndexDeclarationBlock,
    ConcreteIndexDeclarationShort,
    commondl.OnTargetDeleteStmt,
    commondl.OnSourceDeleteStmt,
)


class ConcreteLinkBlock(Nonterm):
    def _validate(self):
        on_target_delete = None
        for cmd in self.val.commands:
            if isinstance(cmd, qlast.OnTargetDelete):
                if on_target_delete:
                    raise errors.EdgeQLSyntaxError(
                        f"more than one 'on target delete' specification",
                        context=cmd.context)
                else:
                    on_target_delete = cmd

    def _extract_target(self, target, cmds, context, *, overloaded=False):
        if target:
            return target, cmds

        for cmd in cmds:
            if isinstance(cmd, qlast.SetField) and cmd.name == 'expr':
                if target is not None:
                    raise errors.EdgeQLSyntaxError(
                        f'computed link with more than one expression',
                        context=context)
                target = cmd.value

        if not overloaded and target is None:
            raise errors.EdgeQLSyntaxError(
                f'computed link without expression',
                context=context)

        return target, cmds

    def reduce_CreateRegularLink(self, *kids):
        """%reduce
            LINK PathNodeName OptExtendingSimple
            OptPtrTarget CreateConcreteLinkSDLCommandsBlock
        """
        _, name, extending, target, commands = kids
        target, cmds = self._extract_target(
            target.val, commands.val, name.context
        )
        self.val = qlast.CreateConcreteLink(
            name=name.val,
            bases=extending.val,
            target=target,
            commands=cmds,
        )
        self._validate()

    def reduce_CreateRegularQualifiedLink(self, *kids):
        """%reduce
            PtrQuals LINK PathNodeName OptExtendingSimple
            OptPtrTarget CreateConcreteLinkSDLCommandsBlock
        """
        quals, _, name, extending, target, commands = kids
        target, cmds = self._extract_target(
            target.val, commands.val, name.context
        )
        self.val = qlast.CreateConcreteLink(
            is_required=quals.val.required,
            cardinality=quals.val.cardinality,
            name=name.val,
            bases=extending.val,
            target=target,
            commands=cmds,
        )
        self._validate()

    def reduce_CreateOverloadedLink(self, *kids):
        """%reduce
            OVERLOADED OptPtrQuals LINK PathNodeName OptExtendingSimple
            OptPtrTarget CreateConcreteLinkSDLCommandsBlock
        """
        _, quals, _, name, extending, target, commands = kids
        target, cmds = self._extract_target(
            target.val, commands.val, name.context, overloaded=True
        )
        self.val = qlast.CreateConcreteLink(
            is_required=quals.val.required,
            cardinality=quals.val.cardinality,
            declared_overloaded=True,
            name=name.val,
            bases=extending.val,
            target=target,
            commands=cmds,
        )
        self._validate()


class ConcreteLinkShort(Nonterm):

    def reduce_CreateRegularLink(self, *kids):
        """%reduce
            LINK PathNodeName OptExtendingSimple
            PtrTarget
        """
        _, name, extending, target = kids
        self.val = qlast.CreateConcreteLink(
            name=name.val,
            bases=extending.val,
            target=target.val,
        )

    def reduce_CreateRegularQualifiedLink(self, *kids):
        """%reduce
            PtrQuals LINK PathNodeName OptExtendingSimple
            PtrTarget
        """
        quals, _, name, extending, target = kids
        self.val = qlast.CreateConcreteLink(
            is_required=quals.val.required,
            cardinality=quals.val.cardinality,
            name=name.val,
            bases=extending.val,
            target=target.val,
        )

    def reduce_CreateOverloadedLink(self, *kids):
        """%reduce
            OVERLOADED OptPtrQuals LINK PathNodeName OptExtendingSimple
            OptPtrTarget
        """
        _, quals, _, name, extending, target = kids
        self.val = qlast.CreateConcreteLink(
            declared_overloaded=True,
            is_required=quals.val.required,
            cardinality=quals.val.cardinality,
            name=name.val,
            bases=extending.val,
            target=target.val,
        )

    def reduce_CreateComputableLink(self, *kids):
        """%reduce
            LINK PathNodeName ASSIGN Expr
        """
        _, name, _, expr = kids
        self.val = qlast.CreateConcreteLink(
            name=name.val,
            target=expr.val,
        )

    def reduce_CreateQualifiedComputableLink(self, *kids):
        """%reduce
            PtrQuals LINK PathNodeName ASSIGN Expr
        """
        quals, _, name, _, expr = kids
        self.val = qlast.CreateConcreteLink(
            is_required=quals.val.required,
            cardinality=quals.val.cardinality,
            name=name.val,
            target=expr.val,
        )


#
# Access Policies
#
sdl_commands_block(
    'CreateAccessPolicy',
    SetField,
    SetAnnotation
)


class AccessPolicyDeclarationBlock(Nonterm):
    def reduce_CreateAccessPolicy(self, *kids):
        """%reduce
            ACCESS POLICY ShortNodeName
            OptWhenBlock AccessPolicyAction AccessKindList
            OptUsingBlock
            CreateAccessPolicySDLCommandsBlock
        """
        _, _, name, when, action, access_kinds, using, commands = kids
        self.val = qlast.CreateAccessPolicy(
            name=name.val,
            condition=when.val,
            action=action.val,
            access_kinds=[y for x in access_kinds.val for y in x],
            expr=using.val,
            commands=commands.val,
        )


class AccessPolicyDeclarationShort(Nonterm):
    def reduce_CreateAccessPolicy(self, *kids):
        """%reduce
            ACCESS POLICY ShortNodeName
            OptWhenBlock AccessPolicyAction AccessKindList
            OptUsingBlock
        """
        _, _, name, when, action, access_kinds, using = kids
        self.val = qlast.CreateAccessPolicy(
            name=name.val,
            condition=when.val,
            action=action.val,
            access_kinds=[y for x in access_kinds.val for y in x],
            expr=using.val,
        )


#
# Object Types
#

sdl_commands_block(
    'CreateObjectType',
    SetField,
    SetAnnotation,
    ConcretePropertyBlock,
    ConcretePropertyShort,
    ConcreteLinkBlock,
    ConcreteLinkShort,
    ConcreteConstraintBlock,
    ConcreteConstraintShort,
    ConcreteIndexDeclarationBlock,
    ConcreteIndexDeclarationShort,
    AccessPolicyDeclarationBlock,
    AccessPolicyDeclarationShort,
)


class ObjectTypeDeclaration(Nonterm):
    def reduce_CreateAbstractObjectTypeStmt(self, *kids):
        r"""%reduce \
            ABSTRACT TYPE NodeName OptExtendingSimple \
            CreateObjectTypeSDLCommandsBlock \
        """
        _, _, name, extending, commands = kids
        self.val = qlast.CreateObjectType(
            abstract=True,
            name=name.val,
            bases=extending.val,
            commands=commands.val,
        )

    def reduce_CreateRegularObjectTypeStmt(self, *kids):
        r"""%reduce \
            TYPE NodeName OptExtendingSimple \
            CreateObjectTypeSDLCommandsBlock \
        """
        _, name, extending, commands = kids
        self.val = qlast.CreateObjectType(
            name=name.val,
            bases=extending.val,
            commands=commands.val,
        )


class ObjectTypeDeclarationShort(Nonterm):
    def reduce_CreateAbstractObjectTypeStmt(self, *kids):
        r"""%reduce \
            ABSTRACT TYPE NodeName OptExtendingSimple"""
        _, _, name, extending = kids
        self.val = qlast.CreateObjectType(
            abstract=True,
            name=name.val,
            bases=extending.val,
        )

    def reduce_CreateRegularObjectTypeStmt(self, *kids):
        r"""%reduce \
            TYPE NodeName OptExtendingSimple"""
        _, name, extending = kids
        self.val = qlast.CreateObjectType(
            name=name.val,
            bases=extending.val,
        )


#
# Aliases
#

sdl_commands_block(
    'CreateAlias',
    Using,
    SetField,
    SetAnnotation,
    opt=False
)


class AliasDeclaration(Nonterm):
    def reduce_CreateAliasRegularStmt(self, *kids):
        r"""%reduce
            ALIAS NodeName CreateAliasSDLCommandsBlock
        """
        _, name, commands = kids
        self.val = qlast.CreateAlias(
            name=name.val,
            commands=commands.val,
        )


class AliasDeclarationShort(Nonterm):
    def reduce_CreateAliasShortStmt(self, *kids):
        r"""%reduce
            ALIAS NodeName ASSIGN Expr
        """
        _, name, _, expr = kids
        self.val = qlast.CreateAlias(
            name=name.val,
            commands=[
                qlast.SetField(
                    name='expr',
                    value=expr.val,
                    special_syntax=True,
                )
            ]
        )

    def reduce_CreateAliasRegularStmt(self, *kids):
        r"""%reduce
            ALIAS NodeName CreateAliasSingleSDLCommandBlock
        """
        _, name, commands = kids
        self.val = qlast.CreateAlias(
            name=name.val,
            commands=commands.val,
        )


#
# Functions
#


sdl_commands_block(
    'CreateFunction',
    commondl.FromFunction,
    SetField,
    SetAnnotation,
    opt=False
)


class FunctionDeclaration(Nonterm, commondl.ProcessFunctionBlockMixin):
    def reduce_CreateFunction(self, *kids):
        r"""%reduce FUNCTION NodeName CreateFunctionArgs \
                ARROW OptTypeQualifier FunctionType \
                CreateFunctionSDLCommandsBlock
        """
        _, name, args, _, type_qualifier, function_type, body = kids
        self.val = qlast.CreateFunction(
            name=name.val,
            params=args.val,
            returning=function_type.val,
            returning_typemod=type_qualifier.val,
            **self._process_function_body(body),
        )


class FunctionDeclarationShort(Nonterm, commondl.ProcessFunctionBlockMixin):
    def reduce_CreateFunction(self, *kids):
        r"""%reduce FUNCTION NodeName CreateFunctionArgs \
                ARROW OptTypeQualifier FunctionType \
                CreateFunctionSingleSDLCommandBlock
        """
        _, name, args, _, type_qualifier, function_type, body = kids
        self.val = qlast.CreateFunction(
            name=name.val,
            params=args.val,
            returning=function_type.val,
            returning_typemod=type_qualifier.val,
            **self._process_function_body(body),
        )


#
# Globals
#

sdl_commands_block(
    'CreateGlobal',
    Using,
    SetField,
    SetAnnotation,
)


class GlobalDeclaration(Nonterm):
    def _extract_target(self, target, cmds, context, *, overloaded=False):
        if target:
            return target, cmds

        for cmd in cmds:
            if isinstance(cmd, qlast.SetField) and cmd.name == 'expr':
                if target is not None:
                    raise errors.EdgeQLSyntaxError(
                        f'computed global with more than one expression',
                        context=context)
                target = cmd.value

        if not overloaded and target is None:
            raise errors.EdgeQLSyntaxError(
                f'computed property without expression',
                context=context)

        return target, cmds

    def reduce_CreateGlobalQuals(self, *kids):
        """%reduce
            PtrQuals GLOBAL NodeName
            OptPtrTarget CreateGlobalSDLCommandsBlock
        """
        quals, glob, name, target, commands = kids
        target, cmds = self._extract_target(
            target.val, commands.val, glob.context
        )
        self.val = qlast.CreateGlobal(
            name=name.val,
            is_required=quals.val.required,
            cardinality=quals.val.cardinality,
            target=target,
            commands=cmds,
        )

    def reduce_CreateGlobal(self, *kids):
        """%reduce
            GLOBAL NodeName
            OptPtrTarget CreateGlobalSDLCommandsBlock
        """
        glob, name, target, commands = kids
        target, cmds = self._extract_target(
            target.val, commands.val, glob.context
        )
        self.val = qlast.CreateGlobal(
            name=name.val,
            target=target,
            commands=cmds,
        )


class GlobalDeclarationShort(Nonterm):
    def reduce_CreateRegularGlobalShortQuals(self, *kids):
        """%reduce
            PtrQuals GLOBAL NodeName PtrTarget
        """
        quals, _, name, target = kids
        self.val = qlast.CreateGlobal(
            name=name.val,
            is_required=quals.val.required,
            cardinality=quals.val.cardinality,
            target=target.val,
        )

    def reduce_CreateRegularGlobalShort(self, *kids):
        """%reduce
            GLOBAL NodeName PtrTarget
        """
        _, name, target = kids
        self.val = qlast.CreateGlobal(
            name=name.val,
            target=target.val,
        )

    def reduce_CreateComputedGlobalShortQuals(self, *kids):
        """%reduce
            PtrQuals GLOBAL NodeName ASSIGN Expr
        """
        quals, _, name, _, expr = kids
        self.val = qlast.CreateGlobal(
            name=name.val,
            is_required=quals.val.required,
            cardinality=quals.val.cardinality,
            target=expr.val,
        )

    def reduce_CreateComputedGlobalShort(self, *kids):
        """%reduce
            GLOBAL NodeName ASSIGN Expr
        """
        _, name, _, expr = kids
        self.val = qlast.CreateGlobal(
            name=name.val,
            target=expr.val,
        )
