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
from . import tokens

from .precedence import *  # NOQA
from .tokens import *  # NOQA
from .commondl import *  # NOQA


Nonterm = expressions.Nonterm
OptSemicolons = commondl.OptSemicolons
ListNonterm = parsing.ListNonterm


sdl_nontem_helper = commondl.NewNontermHelper(__name__)
_new_nonterm = sdl_nontem_helper._new_nonterm


# top-level SDL statements
class SDLStatement(Nonterm):
    def reduce_SDLBlockStatement(self, *kids):
        self.val = kids[0].val

    def reduce_SDLShortStatement_SEMICOLON(self, *kids):
        self.val = kids[0].val


# a list of SDL statements with optional semicolon separators
class SDLStatements(ListNonterm, element=SDLStatement,
                    separator=OptSemicolons):
    pass


# These statements have a block
class SDLBlockStatement(Nonterm):
    def reduce_ScalarTypeDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_AnnotationDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_ObjectTypeDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_ViewDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_ConstraintDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_LinkDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_PropertyDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_FunctionDeclaration(self, *kids):
        self.val = kids[0].val


# these statements have no {} block
class SDLShortStatement(Nonterm):
    def reduce_IMPORT_ImportModuleList(self, *kids):
        self.val = qlast.Import(modules=kids[1].val)

    def reduce_IMPORT_LPAREN_ImportModuleList_RPAREN(self, *kids):
        self.val = qlast.Import(modules=kids[2].val)

    def reduce_ScalarTypeDeclarationShort(self, *kids):
        self.val = kids[0].val

    def reduce_AnnotationDeclarationShort(self, *kids):
        self.val = kids[0].val

    def reduce_ObjectTypeDeclarationShort(self, *kids):
        self.val = kids[0].val

    def reduce_ViewDeclarationShort(self, *kids):
        self.val = kids[0].val

    def reduce_ConstraintDeclarationShort(self, *kids):
        self.val = kids[0].val

    def reduce_LinkDeclarationShort(self, *kids):
        self.val = kids[0].val

    def reduce_PropertyDeclarationShort(self, *kids):
        self.val = kids[0].val

    def reduce_FunctionDeclarationShort(self, *kids):
        self.val = kids[0].val


class DotName(Nonterm):
    def reduce_ModuleName(self, *kids):
        self.val = '.'.join(part for part in kids[0].val)


class ImportModule(Nonterm):
    def reduce_DotName(self, *kids):
        self.val = qlast.ImportModule(module=kids[0].val)

    def reduce_DotName_AS_AnyIdentifier(self, *kids):
        self.val = qlast.ImportModule(module=kids[0].val,
                                      alias=kids[2].val)


class ImportModuleList(ListNonterm, element=ImportModule,
                       separator=tokens.T_COMMA):
    pass


class SDLProductionHelper:
    def _passthrough(self, *cmds):
        self.val = cmds[0].val

    def _singleton_list(self, cmd):
        self.val = [cmd.val]

    def _empty(self):
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
    cmdlist = _new_nonterm(f'{parent}SDLCommandsList', clsbases=(ListNonterm,),
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
    _new_nonterm(f'{parent}SDLCommandsBlock', clsdict=clsdict)

    if opt is False:
        #   | Command
        clsdict = {}
        clsdict[f'reduce_{cmd_s.__name__}'] = \
            SDLProductionHelper._singleton_list
        clsdict[f'reduce_{cmd_b.__name__}'] = \
            SDLProductionHelper._singleton_list
        _new_nonterm(parent + 'SingleSDLCommandBlock', clsdict=clsdict)


class SetField(Nonterm):
    # field := <expr>
    def reduce_ShortNodeName_ASSIGN_Expr(self, *kids):
        self.val = qlast.SetField(name=kids[0].val, value=kids[2].val)


class SetAnnotation(Nonterm):
    def reduce_ANNOTATION_ShortNodeName_ASSIGN_Expr(self, *kids):
        self.val = qlast.CreateAnnotationValue(
            name=kids[1].val, value=kids[3].val)


sdl_commands_block(
    'Create',
    SetField,
    SetAnnotation)


#
# Constraints
#
class ConstraintDeclaration(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT ShortNodeName OptOnExpr \
                    OptExtendingSimple CreateSDLCommandsBlock"""
        self.val = qlast.CreateConstraint(
            name=kids[2].val,
            subjectexpr=kids[3].val,
            bases=kids[4].val,
            commands=kids[5].val,
        )

    def reduce_CreateConstraint_CreateFunctionArgs(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT ShortNodeName CreateFunctionArgs \
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
        r"""%reduce ABSTRACT CONSTRAINT ShortNodeName OptOnExpr \
                    OptExtendingSimple"""
        self.val = qlast.CreateConstraint(
            name=kids[2].val,
            subject=kids[3].val,
            extends=kids[4].val,
        )

    def reduce_CreateConstraint_CreateFunctionArgs(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT ShortNodeName CreateFunctionArgs \
                    OptOnExpr OptExtendingSimple"""
        self.val = qlast.CreateConstraint(
            name=kids[2].val,
            params=kids[3].val,
            subject=kids[4].val,
            extends=kids[5].val,
        )


class ConcreteConstraintBlock(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr \
                    CreateSDLCommandsBlock"""
        self.val = qlast.CreateConcreteConstraint(
            name=kids[1].val,
            args=kids[2].val,
            subjectexpr=kids[3].val,
            commands=kids[4].val,
        )

    def reduce_CreateDelegatedConstraint(self, *kids):
        r"""%reduce DELEGATED CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr \
                    CreateSDLCommandsBlock"""
        self.val = qlast.CreateConcreteConstraint(
            delegated=True,
            name=kids[2].val,
            args=kids[3].val,
            subjectexpr=kids[4].val,
            commands=kids[5].val,
        )


class ConcreteConstraintShort(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr"""
        self.val = qlast.CreateConcreteConstraint(
            name=kids[1].val,
            args=kids[2].val,
            subjectexpr=kids[3].val,
        )

    def reduce_CreateDelegatedConstraint(self, *kids):
        r"""%reduce DELEGATED CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr"""
        self.val = qlast.CreateConcreteConstraint(
            delegated=True,
            name=kids[2].val,
            args=kids[3].val,
            subjectexpr=kids[4].val,
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
            ABSTRACT SCALAR TYPE ShortNodeName \
            OptExtending CreateScalarTypeSDLCommandsBlock \
        """
        self.val = qlast.CreateScalarType(
            is_abstract=True,
            name=kids[3].val,
            bases=kids[4].val,
            commands=kids[5].val,
        )

    def reduce_CreateFinalScalarTypeStmt(self, *kids):
        r"""%reduce \
            FINAL SCALAR TYPE ShortNodeName \
            OptExtending CreateScalarTypeSDLCommandsBlock \
        """
        self.val = qlast.CreateScalarType(
            is_final=True,
            name=kids[3].val,
            bases=kids[4].val,
            commands=kids[5].val,
        )

    def reduce_ScalarTypeDeclaration(self, *kids):
        r"""%reduce \
            SCALAR TYPE ShortNodeName \
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
            ABSTRACT SCALAR TYPE ShortNodeName \
            OptExtending \
        """
        self.val = qlast.CreateScalarType(
            is_abstract=True,
            name=kids[3].val,
            bases=kids[4].val,
        )

    def reduce_CreateFinalScalarTypeStmt(self, *kids):
        r"""%reduce \
            FINAL SCALAR TYPE ShortNodeName \
            OptExtending \
        """
        self.val = qlast.CreateScalarType(
            is_final=True,
            name=kids[3].val,
            bases=kids[4].val,
        )

    def reduce_ScalarTypeDeclaration(self, *kids):
        r"""%reduce \
            SCALAR TYPE ShortNodeName \
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
        r"""%reduce ABSTRACT ANNOTATION ShortNodeName OptExtendingSimple \
                    CreateSDLCommandsBlock"""
        self.val = qlast.CreateAnnotation(
            is_abstract=True,
            name=kids[2].val,
            bases=kids[3].val,
            inheritable=False,
            commands=kids[4].val,
        )

    def reduce_CreateInheritableAnnotation(self, *kids):
        r"""%reduce ABSTRACT INHERITABLE ANNOTATION
                    ShortNodeName OptExtendingSimple CreateSDLCommandsBlock"""
        self.val = qlast.CreateAnnotation(
            is_abstract=True,
            name=kids[3].val,
            bases=kids[4].val,
            inheritable=True,
            commands=kids[4].val,
        )


class AnnotationDeclarationShort(Nonterm):
    def reduce_CreateAnnotation(self, *kids):
        r"""%reduce ABSTRACT ANNOTATION ShortNodeName OptExtendingSimple"""
        self.val = qlast.CreateAnnotation(
            is_abstract=True,
            name=kids[2].val,
            bases=kids[3].val,
            inheritable=False,
        )

    def reduce_CreateInheritableAnnotation(self, *kids):
        r"""%reduce ABSTRACT INHERITABLE ANNOTATION
                    ShortNodeName OptExtendingSimple"""
        self.val = qlast.CreateAnnotation(
            is_abstract=True,
            name=kids[3].val,
            bases=kids[4].val,
            inheritable=True,
        )


#
# Indexes
#
sdl_commands_block(
    'CreateIndex',
    SetAnnotation)


class IndexDeclarationBlock(Nonterm):
    def reduce_INDEX_OnExpr_CreateIndexSDLCommandsBlock(self, *kids):
        self.val = qlast.CreateIndex(
            name=qlast.ObjectRef(name='idx'),
            expr=kids[1].val,
            commands=kids[2].val,
        )


class IndexDeclarationShort(Nonterm):
    def reduce_INDEX_OnExpr(self, *kids):
        self.val = qlast.CreateIndex(
            name=qlast.ObjectRef(name='idx'),
            expr=kids[1].val,
        )


#
# Properties
#
class PropertyDeclaration(Nonterm):
    def reduce_CreateProperty(self, *kids):
        r"""%reduce ABSTRACT PROPERTY ShortNodeName OptExtendingSimple \
                    CreateSDLCommandsBlock \
        """
        self.val = qlast.CreateProperty(
            name=kids[2].val,
            bases=kids[3].val,
            commands=kids[4].val,
        )


class PropertyDeclarationShort(Nonterm):
    def reduce_CreateProperty(self, *kids):
        r"""%reduce ABSTRACT PROPERTY ShortNodeName OptExtendingSimple"""
        self.val = qlast.CreateProperty(
            name=kids[2].val,
            bases=kids[3].val,
        )


sdl_commands_block(
    'CreateConcreteProperty',
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
    def reduce_CreateRegularProperty(self, *kids):
        """%reduce
            PROPERTY ShortNodeName OptExtendingSimple
            PtrTarget CreateConcretePropertySDLCommandsBlock
        """
        self.val = qlast.CreateConcreteProperty(
            name=kids[1].val,
            bases=kids[2].val,
            target=kids[3].val,
            commands=kids[4].val,
        )

    def reduce_CreateRegularQualifiedProperty(self, *kids):
        """%reduce
            PtrQuals PROPERTY ShortNodeName OptExtendingSimple
            PtrTarget CreateConcretePropertySDLCommandsBlock
        """
        self.val = qlast.CreateConcreteProperty(
            name=kids[2].val,
            bases=kids[3].val,
            is_required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            target=kids[4].val,
            commands=kids[5].val,
        )

    def reduce_CreateOverloadedProperty(self, *kids):
        """%reduce
            OVERLOADED OptPtrQuals PROPERTY ShortNodeName OptExtendingSimple
            OptPtrTarget CreateConcretePropertySDLCommandsBlock
        """
        self.val = qlast.CreateConcreteProperty(
            name=kids[3].val,
            bases=kids[4].val,
            declared_overloaded=True,
            is_required=kids[1].val.required,
            cardinality=kids[1].val.cardinality,
            target=kids[5].val,
            commands=kids[6].val,
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
            ABSTRACT LINK ShortNodeName OptExtendingSimple \
            CreateLinkSDLCommandsBlock \
        """
        self.val = qlast.CreateLink(
            name=kids[2].val,
            bases=kids[3].val,
            commands=kids[4].val,
        )


class LinkDeclarationShort(Nonterm):
    def reduce_CreateLink(self, *kids):
        r"""%reduce \
            ABSTRACT LINK ShortNodeName OptExtendingSimple"""
        self.val = qlast.CreateLink(
            name=kids[2].val,
            bases=kids[3].val,
        )


sdl_commands_block(
    'CreateConcreteLink',
    SetField,
    SetAnnotation,
    ConcreteConstraintBlock,
    ConcreteConstraintShort,
    ConcretePropertyBlock,
    ConcretePropertyShort,
    commondl.OnTargetDeleteStmt,
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

    def reduce_CreateRegularLink(self, *kids):
        """%reduce
            LINK ShortNodeName OptExtendingSimple
            PtrTarget CreateConcreteLinkSDLCommandsBlock
        """
        self.val = qlast.CreateConcreteLink(
            name=kids[1].val,
            bases=kids[2].val,
            target=kids[3].val,
            commands=kids[4].val,
        )
        self._validate()

    def reduce_CreateRegularQualifiedLink(self, *kids):
        """%reduce
            PtrQuals LINK ShortNodeName OptExtendingSimple
            PtrTarget CreateConcreteLinkSDLCommandsBlock
        """
        self.val = qlast.CreateConcreteLink(
            is_required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            name=kids[2].val,
            bases=kids[3].val,
            target=kids[4].val,
            commands=kids[5].val,
        )
        self._validate()

    def reduce_CreateOverloadedLink(self, *kids):
        """%reduce
            OVERLOADED OptPtrQuals LINK ShortNodeName OptExtendingSimple
            OptPtrTarget CreateConcreteLinkSDLCommandsBlock
        """
        self.val = qlast.CreateConcreteLink(
            is_required=kids[1].val.required,
            cardinality=kids[1].val.cardinality,
            declared_overloaded=True,
            name=kids[3].val,
            bases=kids[4].val,
            target=kids[5].val,
            commands=kids[6].val,
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
    IndexDeclarationBlock,
    IndexDeclarationShort,
)


class ObjectTypeDeclaration(Nonterm):
    def reduce_CreateAbstractObjectTypeStmt(self, *kids):
        r"""%reduce \
            ABSTRACT TYPE ShortNodeName OptExtendingSimple \
            CreateObjectTypeSDLCommandsBlock \
        """
        self.val = qlast.CreateObjectType(
            is_abstract=True,
            name=kids[2].val,
            bases=kids[3].val,
            commands=kids[4].val,
        )

    def reduce_CreateRegularObjectTypeStmt(self, *kids):
        r"""%reduce \
            TYPE ShortNodeName OptExtendingSimple \
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
            ABSTRACT TYPE ShortNodeName OptExtendingSimple"""
        self.val = qlast.CreateObjectType(
            is_abstract=True,
            name=kids[2].val,
            bases=kids[3].val,
        )

    def reduce_CreateRegularObjectTypeStmt(self, *kids):
        r"""%reduce \
            TYPE ShortNodeName OptExtendingSimple"""
        self.val = qlast.CreateObjectType(
            name=kids[1].val,
            bases=kids[2].val,
        )


#
# Views
#

sdl_commands_block(
    'CreateView',
    SetField,
    SetAnnotation,
    opt=False
)


class ViewDeclaration(Nonterm):
    def reduce_CreateViewRegularStmt(self, *kids):
        r"""%reduce \
            VIEW ShortNodeName CreateViewSDLCommandsBlock \
        """
        self.val = qlast.CreateView(
            name=kids[1].val,
            commands=kids[2].val,
        )


class ViewDeclarationShort(Nonterm):
    def reduce_CreateViewShortStmt(self, *kids):
        r"""%reduce \
            VIEW ShortNodeName ASSIGN Expr \
        """
        self.val = qlast.CreateView(
            name=kids[1].val,
            commands=[
                qlast.SetField(
                    name=qlast.ObjectRef(name='expr'),
                    value=kids[3].val,
                )
            ]
        )

    def reduce_CreateViewRegularStmt(self, *kids):
        r"""%reduce \
            VIEW ShortNodeName CreateViewSingleSDLCommandBlock \
        """
        self.val = qlast.CreateView(
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
        r"""%reduce FUNCTION ShortNodeName CreateFunctionArgs \
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
        r"""%reduce FUNCTION ShortNodeName CreateFunctionArgs \
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
