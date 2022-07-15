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


# these statements have no {} block
class SDLShortStatement(Nonterm):

    def reduce_ExtensionRequirementDeclaration(self, *kids):
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
        self.val = [kids[2].val]

    def reduce_statements_without_optional_trailing_semicolons(self, *kids):
        r"""%reduce LBRACE \
                OptSemicolons SDLStatements \
                OptSemicolons SDLShortStatement \
            RBRACE
        """
        self.val = kids[2].val + [kids[4].val]

    def reduce_LBRACE_OptSemicolons_SDLStatements_RBRACE(self, *kids):
        self.val = kids[2].val

    def reduce_statements_without_optional_trailing_semicolons2(self, *kids):
        r"""%reduce LBRACE \
                OptSemicolons SDLStatements \
                Semicolons \
            RBRACE
        """
        self.val = kids[2].val


class DotName(Nonterm):
    def reduce_ModuleName(self, *kids):
        self.val = '.'.join(part for part in kids[0].val)


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
        self.val = qlast.SetField(
            name='expr',
            value=kids[1].val,
            special_syntax=True,
        )


class SetField(Nonterm):
    # field := <expr>
    def reduce_Identifier_ASSIGN_Expr(self, *kids):
        self.val = qlast.SetField(name=kids[0].val, value=kids[2].val)


class SetAnnotation(Nonterm):
    def reduce_ANNOTATION_NodeName_ASSIGN_Expr(self, *kids):
        self.val = qlast.CreateAnnotationValue(
            name=kids[1].val, value=kids[3].val)


sdl_commands_block(
    'Create',
    Using,
    SetField,
    SetAnnotation)


class ExtensionRequirementDeclaration(Nonterm):

    def reduce_USING_EXTENSION_ShortNodeName_OptExtensionVersion(self, *kids):
        self.val = qlast.CreateExtension(
            name=kids[2].val,
            version=kids[3].val,
        )


class ModuleDeclaration(Nonterm):
    def reduce_MODULE_ModuleName_SDLCommandBlock(self, *kids):
        # Check that top-level declarations DO NOT use fully-qualified
        # names and aren't nested module blocks.
        declarations = kids[2].val
        for decl in declarations:
            if isinstance(decl, qlast.ModuleDeclaration):
                raise errors.EdgeQLSyntaxError(
                    "nested module declaration is not allowed",
                    context=decl.context)
            elif isinstance(decl, qlast.ExtensionCommand):
                raise errors.EdgeQLSyntaxError(
                    "'using extension' cannot be used inside a module block",
                    context=decl.context)
            elif decl.name.module is not None:
                raise errors.EdgeQLSyntaxError(
                    "fully-qualified name is not allowed in "
                    "a module declaration",
                    context=decl.name.context)

        self.val = qlast.ModuleDeclaration(
            # mirror what we do in CREATE MODULE
            name=qlast.ObjectRef(module=None, name='.'.join(kids[1].val)),
            declarations=declarations,
        )


#
# Constraints
#
class ConstraintDeclaration(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT NodeName OptOnExpr \
                    OptExtendingSimple CreateSDLCommandsBlock"""
        self.val = qlast.CreateConstraint(
            name=kids[2].val,
            subjectexpr=kids[3].val,
            bases=kids[4].val,
            commands=kids[5].val,
        )

    def reduce_CreateConstraint_CreateFunctionArgs(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT NodeName CreateFunctionArgs \
                    OptOnExpr OptExtendingSimple CreateSDLCommandsBlock"""
        self.val = qlast.CreateConstraint(
            name=kids[2].val,
            params=kids[3].val,
            subjectexpr=kids[4].val,
            bases=kids[5].val,
            commands=kids[6].val,
        )


class ConstraintDeclarationShort(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT NodeName OptOnExpr \
                    OptExtendingSimple"""
        self.val = qlast.CreateConstraint(
            name=kids[2].val,
            subject=kids[3].val,
            bases=kids[4].val,
        )

    def reduce_CreateConstraint_CreateFunctionArgs(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT NodeName CreateFunctionArgs \
                    OptOnExpr OptExtendingSimple"""
        self.val = qlast.CreateConstraint(
            name=kids[2].val,
            params=kids[3].val,
            subject=kids[4].val,
            bases=kids[5].val,
        )


class ConcreteConstraintBlock(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr \
                    OptExceptExpr \
                    CreateSDLCommandsBlock"""
        self.val = qlast.CreateConcreteConstraint(
            name=kids[1].val,
            args=kids[2].val,
            subjectexpr=kids[3].val,
            except_expr=kids[4].val,
            commands=kids[5].val,
        )

    def reduce_CreateDelegatedConstraint(self, *kids):
        r"""%reduce DELEGATED CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr \
                    OptExceptExpr \
                    CreateSDLCommandsBlock"""
        self.val = qlast.CreateConcreteConstraint(
            delegated=True,
            name=kids[2].val,
            args=kids[3].val,
            subjectexpr=kids[4].val,
            except_expr=kids[5].val,
            commands=kids[6].val,
        )


class ConcreteConstraintShort(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr \
                    OptExceptExpr"""
        self.val = qlast.CreateConcreteConstraint(
            name=kids[1].val,
            args=kids[2].val,
            subjectexpr=kids[3].val,
            except_expr=kids[4].val,
        )

    def reduce_CreateDelegatedConstraint(self, *kids):
        r"""%reduce DELEGATED CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr \
                    OptExceptExpr"""
        self.val = qlast.CreateConcreteConstraint(
            delegated=True,
            name=kids[2].val,
            args=kids[3].val,
            subjectexpr=kids[4].val,
            except_expr=kids[5].val,
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
        self.val = qlast.CreateScalarType(
            abstract=True,
            name=kids[3].val,
            bases=kids[4].val,
            commands=kids[5].val,
        )

    def reduce_ScalarTypeDeclaration(self, *kids):
        r"""%reduce \
            SCALAR TYPE NodeName \
            OptExtending CreateScalarTypeSDLCommandsBlock \
        """
        self.val = qlast.CreateScalarType(
            name=kids[2].val,
            bases=kids[3].val,
            commands=kids[4].val,
        )


class ScalarTypeDeclarationShort(Nonterm):
    def reduce_CreateAbstractScalarTypeStmt(self, *kids):
        r"""%reduce \
            ABSTRACT SCALAR TYPE NodeName \
            OptExtending \
        """
        self.val = qlast.CreateScalarType(
            abstract=True,
            name=kids[3].val,
            bases=kids[4].val,
        )

    def reduce_ScalarTypeDeclaration(self, *kids):
        r"""%reduce \
            SCALAR TYPE NodeName \
            OptExtending \
        """
        self.val = qlast.CreateScalarType(
            name=kids[2].val,
            bases=kids[3].val,
        )


#
# Annotations
#
class AnnotationDeclaration(Nonterm):
    def reduce_CreateAnnotation(self, *kids):
        r"""%reduce ABSTRACT ANNOTATION NodeName OptExtendingSimple \
                    CreateSDLCommandsBlock"""
        self.val = qlast.CreateAnnotation(
            abstract=True,
            name=kids[2].val,
            bases=kids[3].val,
            inheritable=False,
            commands=kids[4].val,
        )

    def reduce_CreateInheritableAnnotation(self, *kids):
        r"""%reduce ABSTRACT INHERITABLE ANNOTATION
                    NodeName OptExtendingSimple CreateSDLCommandsBlock"""
        self.val = qlast.CreateAnnotation(
            abstract=True,
            name=kids[3].val,
            bases=kids[4].val,
            inheritable=True,
            commands=kids[4].val,
        )


class AnnotationDeclarationShort(Nonterm):
    def reduce_CreateAnnotation(self, *kids):
        r"""%reduce ABSTRACT ANNOTATION NodeName OptExtendingSimple"""
        self.val = qlast.CreateAnnotation(
            abstract=True,
            name=kids[2].val,
            bases=kids[3].val,
            inheritable=False,
        )

    def reduce_CreateInheritableAnnotation(self, *kids):
        r"""%reduce ABSTRACT INHERITABLE ANNOTATION
                    NodeName OptExtendingSimple"""
        self.val = qlast.CreateAnnotation(
            abstract=True,
            name=kids[3].val,
            bases=kids[4].val,
            inheritable=True,
        )


#
# Indexes
#
sdl_commands_block(
    'CreateIndex',
    SetField,
    SetAnnotation)


class IndexDeclarationBlock(Nonterm):
    def reduce_INDEX_OnExpr_OptExceptExpr_CreateIndexSDLCommandsBlock(
            self, *kids):
        self.val = qlast.CreateIndex(
            name=qlast.ObjectRef(name='idx'),
            expr=kids[1].val,
            except_expr=kids[2].val,
            commands=kids[3].val,
        )


class IndexDeclarationShort(Nonterm):
    def reduce_INDEX_OnExpr_OptExceptExpr(self, *kids):
        self.val = qlast.CreateIndex(
            name=qlast.ObjectRef(name='idx'),
            expr=kids[1].val,
            except_expr=kids[2].val,
        )


#
# Properties
#
class PropertyDeclaration(Nonterm):
    def reduce_CreateProperty(self, *kids):
        r"""%reduce ABSTRACT PROPERTY NodeName OptExtendingSimple \
                    CreateSDLCommandsBlock \
        """
        self.val = qlast.CreateProperty(
            name=kids[2].val,
            bases=kids[3].val,
            commands=kids[4].val,
            abstract=True,
        )


class PropertyDeclarationShort(Nonterm):
    def reduce_CreateProperty(self, *kids):
        r"""%reduce ABSTRACT PROPERTY NodeName OptExtendingSimple"""
        self.val = qlast.CreateProperty(
            name=kids[2].val,
            bases=kids[3].val,
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
        self.val = kids[1].val
        self.context = kids[1].val.context


class OptPtrTarget(Nonterm):

    def reduce_empty(self, *kids):
        self.val = None

    def reduce_PtrTarget(self, *kids):
        self.val = kids[0].val


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
            PROPERTY ShortNodeName OptExtendingSimple
            OptPtrTarget CreateConcretePropertySDLCommandsBlock
        """
        target, cmds = self._extract_target(
            kids[3].val, kids[4].val, kids[1].context)
        self.val = qlast.CreateConcreteProperty(
            name=kids[1].val,
            bases=kids[2].val,
            target=target,
            commands=cmds,
        )

    def reduce_CreateRegularQualifiedProperty(self, *kids):
        """%reduce
            PtrQuals PROPERTY ShortNodeName OptExtendingSimple
            OptPtrTarget CreateConcretePropertySDLCommandsBlock
        """
        target, cmds = self._extract_target(
            kids[4].val, kids[5].val, kids[1].context)
        self.val = qlast.CreateConcreteProperty(
            name=kids[2].val,
            bases=kids[3].val,
            is_required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            target=target,
            commands=cmds,
        )

    def reduce_CreateOverloadedProperty(self, *kids):
        """%reduce
            OVERLOADED OptPtrQuals PROPERTY ShortNodeName OptExtendingSimple
            OptPtrTarget CreateConcretePropertySDLCommandsBlock
        """
        target, cmds = self._extract_target(
            kids[5].val, kids[6].val, kids[3].context, overloaded=True)
        self.val = qlast.CreateConcreteProperty(
            name=kids[3].val,
            bases=kids[4].val,
            declared_overloaded=True,
            is_required=kids[1].val.required,
            cardinality=kids[1].val.cardinality,
            target=target,
            commands=cmds,
        )


class ConcretePropertyShort(Nonterm):
    def reduce_CreateRegularProperty(self, *kids):
        """%reduce
            PROPERTY ShortNodeName OptExtendingSimple PtrTarget
        """
        self.val = qlast.CreateConcreteProperty(
            name=kids[1].val,
            bases=kids[2].val,
            target=kids[3].val,
        )

    def reduce_CreateRegularQualifiedProperty(self, *kids):
        """%reduce
            PtrQuals PROPERTY ShortNodeName OptExtendingSimple PtrTarget
        """
        self.val = qlast.CreateConcreteProperty(
            name=kids[2].val,
            bases=kids[3].val,
            is_required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            target=kids[4].val,
        )

    def reduce_CreateOverloadedProperty(self, *kids):
        """%reduce
            OVERLOADED OptPtrQuals PROPERTY ShortNodeName OptExtendingSimple
            OptPtrTarget
        """
        self.val = qlast.CreateConcreteProperty(
            name=kids[3].val,
            bases=kids[4].val,
            declared_overloaded=True,
            is_required=kids[1].val.required,
            cardinality=kids[1].val.cardinality,
            target=kids[5].val,
        )

    def reduce_CreateComputableProperty(self, *kids):
        """%reduce
            PROPERTY ShortNodeName ASSIGN Expr
        """
        self.val = qlast.CreateConcreteProperty(
            name=kids[1].val,
            target=kids[3].val,
        )

    def reduce_CreateQualifiedComputableProperty(self, *kids):
        """%reduce
            PtrQuals PROPERTY ShortNodeName ASSIGN Expr
        """
        self.val = qlast.CreateConcreteProperty(
            name=kids[2].val,
            is_required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            target=kids[4].val,
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
    IndexDeclarationBlock,
    IndexDeclarationShort,
)


class LinkDeclaration(Nonterm):
    def reduce_CreateLink(self, *kids):
        r"""%reduce \
            ABSTRACT LINK NodeName OptExtendingSimple \
            CreateLinkSDLCommandsBlock \
        """
        self.val = qlast.CreateLink(
            name=kids[2].val,
            bases=kids[3].val,
            commands=kids[4].val,
            abstract=True,
        )


class LinkDeclarationShort(Nonterm):
    def reduce_CreateLink(self, *kids):
        r"""%reduce \
            ABSTRACT LINK NodeName OptExtendingSimple"""
        self.val = qlast.CreateLink(
            name=kids[2].val,
            bases=kids[3].val,
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
    IndexDeclarationBlock,
    IndexDeclarationShort,
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
            LINK ShortNodeName OptExtendingSimple
            OptPtrTarget CreateConcreteLinkSDLCommandsBlock
        """
        target, cmds = self._extract_target(
            kids[3].val, kids[4].val, kids[1].context)
        self.val = qlast.CreateConcreteLink(
            name=kids[1].val,
            bases=kids[2].val,
            target=target,
            commands=cmds,
        )
        self._validate()

    def reduce_CreateRegularQualifiedLink(self, *kids):
        """%reduce
            PtrQuals LINK ShortNodeName OptExtendingSimple
            OptPtrTarget CreateConcreteLinkSDLCommandsBlock
        """
        target, cmds = self._extract_target(
            kids[4].val, kids[5].val, kids[2].context)
        self.val = qlast.CreateConcreteLink(
            is_required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            name=kids[2].val,
            bases=kids[3].val,
            target=target,
            commands=cmds,
        )
        self._validate()

    def reduce_CreateOverloadedLink(self, *kids):
        """%reduce
            OVERLOADED OptPtrQuals LINK ShortNodeName OptExtendingSimple
            OptPtrTarget CreateConcreteLinkSDLCommandsBlock
        """
        target, cmds = self._extract_target(
            kids[5].val, kids[6].val, kids[3].context, overloaded=True)
        self.val = qlast.CreateConcreteLink(
            is_required=kids[1].val.required,
            cardinality=kids[1].val.cardinality,
            declared_overloaded=True,
            name=kids[3].val,
            bases=kids[4].val,
            target=target,
            commands=cmds,
        )
        self._validate()


class ConcreteLinkShort(Nonterm):

    def reduce_CreateRegularLink(self, *kids):
        """%reduce
            LINK ShortNodeName OptExtendingSimple
            PtrTarget
        """
        self.val = qlast.CreateConcreteLink(
            name=kids[1].val,
            bases=kids[2].val,
            target=kids[3].val,
        )

    def reduce_CreateRegularQualifiedLink(self, *kids):
        """%reduce
            PtrQuals LINK ShortNodeName OptExtendingSimple
            PtrTarget
        """
        self.val = qlast.CreateConcreteLink(
            is_required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            name=kids[2].val,
            bases=kids[3].val,
            target=kids[4].val,
        )

    def reduce_CreateOverloadedLink(self, *kids):
        """%reduce
            OVERLOADED OptPtrQuals LINK ShortNodeName OptExtendingSimple
            OptPtrTarget
        """
        self.val = qlast.CreateConcreteLink(
            declared_overloaded=True,
            is_required=kids[1].val.required,
            cardinality=kids[1].val.cardinality,
            name=kids[3].val,
            bases=kids[4].val,
            target=kids[5].val,
        )

    def reduce_CreateComputableLink(self, *kids):
        """%reduce
            LINK ShortNodeName ASSIGN Expr
        """
        self.val = qlast.CreateConcreteLink(
            name=kids[1].val,
            target=kids[3].val,
        )

    def reduce_CreateQualifiedComputableLink(self, *kids):
        """%reduce
            PtrQuals LINK ShortNodeName ASSIGN Expr
        """
        self.val = qlast.CreateConcreteLink(
            is_required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            name=kids[2].val,
            target=kids[4].val,
        )


#
# Access Policies
#
sdl_commands_block(
    'CreateAccessPolicy',
    SetAnnotation)


class AccessPolicyDeclarationBlock(Nonterm):
    def reduce_CreateAccessPolicy(self, *kids):
        """%reduce
            ACCESS POLICY ShortNodeName
            OptWhenBlock AccessPolicyAction AccessKindList
            OptUsingBlock
            CreateAccessPolicySDLCommandsBlock
        """
        self.val = qlast.CreateAccessPolicy(
            name=kids[2].val,
            condition=kids[3].val,
            action=kids[4].val,
            access_kinds=[y for x in kids[5].val for y in x],
            expr=kids[6].val,
            commands=kids[7].val,
        )


class AccessPolicyDeclarationShort(Nonterm):
    def reduce_CreateAccessPolicy(self, *kids):
        """%reduce
            ACCESS POLICY ShortNodeName
            OptWhenBlock AccessPolicyAction AccessKindList
            OptUsingBlock
        """
        self.val = qlast.CreateAccessPolicy(
            name=kids[2].val,
            condition=kids[3].val,
            action=kids[4].val,
            access_kinds=[y for x in kids[5].val for y in x],
            expr=kids[6].val,
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
    IndexDeclarationBlock,
    IndexDeclarationShort,
    AccessPolicyDeclarationBlock,
    AccessPolicyDeclarationShort,
)


class ObjectTypeDeclaration(Nonterm):
    def reduce_CreateAbstractObjectTypeStmt(self, *kids):
        r"""%reduce \
            ABSTRACT TYPE NodeName OptExtendingSimple \
            CreateObjectTypeSDLCommandsBlock \
        """
        self.val = qlast.CreateObjectType(
            abstract=True,
            name=kids[2].val,
            bases=kids[3].val,
            commands=kids[4].val,
        )

    def reduce_CreateRegularObjectTypeStmt(self, *kids):
        r"""%reduce \
            TYPE NodeName OptExtendingSimple \
            CreateObjectTypeSDLCommandsBlock \
        """
        self.val = qlast.CreateObjectType(
            name=kids[1].val,
            bases=kids[2].val,
            commands=kids[3].val,
        )


class ObjectTypeDeclarationShort(Nonterm):
    def reduce_CreateAbstractObjectTypeStmt(self, *kids):
        r"""%reduce \
            ABSTRACT TYPE NodeName OptExtendingSimple"""
        self.val = qlast.CreateObjectType(
            abstract=True,
            name=kids[2].val,
            bases=kids[3].val,
        )

    def reduce_CreateRegularObjectTypeStmt(self, *kids):
        r"""%reduce \
            TYPE NodeName OptExtendingSimple"""
        self.val = qlast.CreateObjectType(
            name=kids[1].val,
            bases=kids[2].val,
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
        self.val = qlast.CreateAlias(
            name=kids[1].val,
            commands=kids[2].val,
        )


class AliasDeclarationShort(Nonterm):
    def reduce_CreateAliasShortStmt(self, *kids):
        r"""%reduce
            ALIAS NodeName ASSIGN Expr
        """
        self.val = qlast.CreateAlias(
            name=kids[1].val,
            commands=[
                qlast.SetField(
                    name='expr',
                    value=kids[3].val,
                    special_syntax=True,
                )
            ]
        )

    def reduce_CreateAliasRegularStmt(self, *kids):
        r"""%reduce
            ALIAS NodeName CreateAliasSingleSDLCommandBlock
        """
        self.val = qlast.CreateAlias(
            name=kids[1].val,
            commands=kids[2].val,
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
        self.val = qlast.CreateFunction(
            name=kids[1].val,
            params=kids[2].val,
            returning=kids[5].val,
            returning_typemod=kids[4].val,
            **self._process_function_body(kids[6]),
        )


class FunctionDeclarationShort(Nonterm, commondl.ProcessFunctionBlockMixin):
    def reduce_CreateFunction(self, *kids):
        r"""%reduce FUNCTION NodeName CreateFunctionArgs \
                ARROW OptTypeQualifier FunctionType \
                CreateFunctionSingleSDLCommandBlock
        """
        self.val = qlast.CreateFunction(
            name=kids[1].val,
            params=kids[2].val,
            returning=kids[5].val,
            returning_typemod=kids[4].val,
            **self._process_function_body(kids[6]),
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
        target, cmds = self._extract_target(
            kids[3].val, kids[4].val, kids[1].context)
        self.val = qlast.CreateGlobal(
            name=kids[2].val,
            is_required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            target=target,
            commands=cmds,
        )

    def reduce_CreateGlobal(self, *kids):
        """%reduce
            GLOBAL NodeName
            OptPtrTarget CreateGlobalSDLCommandsBlock
        """
        target, cmds = self._extract_target(
            kids[2].val, kids[3].val, kids[0].context)
        self.val = qlast.CreateGlobal(
            name=kids[1].val,
            target=target,
            commands=cmds,
        )


class GlobalDeclarationShort(Nonterm):
    def reduce_CreateRegularGlobalShortQuals(self, *kids):
        """%reduce
            PtrQuals GLOBAL NodeName PtrTarget
        """
        self.val = qlast.CreateGlobal(
            name=kids[2].val,
            is_required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            target=kids[3].val,
        )

    def reduce_CreateRegularGlobalShort(self, *kids):
        """%reduce
            GLOBAL NodeName PtrTarget
        """
        self.val = qlast.CreateGlobal(
            name=kids[1].val,
            target=kids[2].val,
        )

    def reduce_CreateComputedGlobalShortQuals(self, *kids):
        """%reduce
            PtrQuals GLOBAL NodeName ASSIGN Expr
        """
        self.val = qlast.CreateGlobal(
            name=kids[2].val,
            is_required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            target=kids[4].val,
        )

    def reduce_CreateComputedGlobalShort(self, *kids):
        """%reduce
            GLOBAL NodeName ASSIGN Expr
        """
        self.val = qlast.CreateGlobal(
            name=kids[1].val,
            target=kids[3].val,
        )
