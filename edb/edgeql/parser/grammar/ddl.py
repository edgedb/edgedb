#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2015-present MagicStack Inc. and the EdgeDB authors.
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

import collections
import re
import typing

from edb import errors
from edb.errors import EdgeQLSyntaxError

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb.common import parsing

from . import expressions
from . import commondl

from .precedence import *  # NOQA
from .tokens import *  # NOQA
from .commondl import *  # NOQA

from .sdl import *  # NOQA


ListNonterm = parsing.ListNonterm
Nonterm = expressions.Nonterm
Semicolons = commondl.Semicolons


sdl_nontem_helper = commondl.NewNontermHelper(__name__)
_new_nonterm = sdl_nontem_helper._new_nonterm


class DDLStmt(Nonterm):
    def reduce_CreateDatabaseStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropDatabaseStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateRoleStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterRoleStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropRoleStmt(self, *kids):
        self.val = kids[0].val

    def reduce_OptWithDDLStmt(self, *kids):
        self.val = kids[0].val


class DDLWithBlock(Nonterm):
    def reduce_WithBlock(self, *kids):
        self.val = kids[0].val


class OptWithDDLStmt(Nonterm):
    def reduce_DDLWithBlock_WithDDLStmt(self, *kids):
        self.val = kids[1].val
        self.val.aliases = kids[0].val.aliases

    def reduce_WithDDLStmt(self, *kids):
        self.val = kids[0].val


class WithDDLStmt(Nonterm):
    def reduce_CreateMigrationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterMigrationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropMigrationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CommitMigrationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_GetMigrationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_InnerDDLStmt(self, *kids):
        self.val = kids[0].val


# DDL statements that are allowed inside CREATE DATABASE and CREATE MIGRATION
#
class InnerDDLStmt(Nonterm):
    def reduce_CreateScalarTypeStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterScalarTypeStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropScalarTypeStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateAnnotationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropAnnotationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateObjectTypeStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterObjectTypeStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropObjectTypeStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateViewStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterViewStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropViewStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateConstraintStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterConstraintStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropConstraintStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateLinkStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterLinkStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropLinkStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreatePropertyStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterPropertyStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropPropertyStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateModuleStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterModuleStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropModuleStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateFunctionStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropFunctionStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateOperatorStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterOperatorStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropOperatorStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateCastStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterCastStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropCastStmt(self, *kids):
        self.val = kids[0].val


class InnerDDLStmtBlock(ListNonterm, element=InnerDDLStmt,
                        separator=Semicolons):
    pass


class PointerName(Nonterm):
    def reduce_NodeName(self, *kids):
        self.val = kids[0].val

    def reduce_DUNDERTYPE(self, *kids):
        self.val = qlast.ObjectRef(name=kids[0].val)


class UnqualifiedPointerName(Nonterm):
    def reduce_PointerName(self, *kids):
        if kids[0].val.module:
            raise EdgeQLSyntaxError(
                'unexpected fully-qualified name',
                context=kids[0].val.context)
        self.val = kids[0].val


class ProductionHelper:
    def _passthrough(self, cmd):
        self.val = cmd.val

    def _singleton_list(self, cmd):
        self.val = [cmd.val]

    def _empty(self):
        self.val = []

    def _block(self, lbrace, cmdlist, sc2, rbrace):
        self.val = cmdlist.val

    def _block2(self, lbrace, sc1, cmdlist, sc2, rbrace):
        self.val = cmdlist.val


def commands_block(parent, *commands, opt=True):
    if parent is None:
        parent = ''

    clsdict = collections.OrderedDict()

    # Command := Command1 | Command2 ...
    #
    for command in commands:
        clsdict['reduce_{}'.format(command.__name__)] = \
            ProductionHelper._passthrough

    cmd = _new_nonterm(parent + 'Command', clsdict=clsdict)

    # CommandsList := Command [; Command ...]
    cmdlist = _new_nonterm(parent + 'CommandsList', clsbases=(ListNonterm,),
                           clskwds=dict(element=cmd, separator=Semicolons))

    # CommandsBlock :=
    #
    #   { [ ; ] CommandsList ; }
    clsdict = collections.OrderedDict()
    clsdict['reduce_LBRACE_' + cmdlist.__name__ + '_OptSemicolons_RBRACE'] = \
        ProductionHelper._block
    clsdict['reduce_LBRACE_Semicolons_' + cmdlist.__name__ +
            '_OptSemicolons_RBRACE'] = \
        ProductionHelper._block2
    if not opt:
        #
        #   | Command
        clsdict['reduce_{}'.format(cmd.__name__)] = \
            ProductionHelper._singleton_list
    cmdblock = _new_nonterm(parent + 'CommandsBlock', clsdict=clsdict)

    # OptCommandsBlock := CommandsBlock | <e>
    clsdict = collections.OrderedDict()
    clsdict['reduce_{}'.format(cmdblock.__name__)] = \
        ProductionHelper._passthrough
    clsdict['reduce_empty'] = ProductionHelper._empty

    if opt:
        _new_nonterm('Opt' + parent + 'CommandsBlock', clsdict=clsdict)


class SetFieldStmt(Nonterm):
    # field := <expr>
    def reduce_SET_NodeName_ASSIGN_Expr(self, *kids):
        self.val = qlast.SetField(
            name=kids[1].val,
            value=kids[3].val,
        )

    def reduce_SET_NodeName_AS_SchemaItem(self, *kids):
        self.val = qlast.SetField(
            name=kids[1].val,
            value=kids[3].val,
        )


class SetAnnotationValueStmt(Nonterm):
    def reduce_SETANNOTATION_NodeName_ASSIGN_Expr(self, *kids):
        self.val = qlast.CreateAnnotationValue(
            name=kids[1].val,
            value=kids[3].val,
        )


class DropAnnotationValueStmt(Nonterm):
    def reduce_DROP_ANNOTATION_NodeName(self, *kids):
        self.val = qlast.DropAnnotationValue(
            name=kids[2].val,
        )


class RenameStmt(Nonterm):
    def reduce_RENAME_TO_NodeName(self, *kids):
        self.val = qlast.Rename(new_name=kids[2].val)


commands_block(
    'Create',
    SetFieldStmt,
    SetAnnotationValueStmt)


commands_block(
    'Alter',
    RenameStmt,
    SetFieldStmt,
    SetAnnotationValueStmt,
    DropAnnotationValueStmt,
    opt=False)


class AlterAbstract(Nonterm):
    def reduce_DROP_ABSTRACT(self, *kids):
        self.val = qlast.SetSpecialField(
            name=qlast.ObjectRef(name='is_abstract'), value=False)

    def reduce_SET_ABSTRACT(self, *kids):
        self.val = qlast.SetSpecialField(
            name=qlast.ObjectRef(name='is_abstract'), value=True)


class AlterFinal(Nonterm):
    def reduce_DROP_FINAL(self, *kids):
        self.val = qlast.SetSpecialField(
            name=qlast.ObjectRef(name='is_final'), value=False)

    def reduce_SET_FINAL(self, *kids):
        self.val = qlast.SetSpecialField(
            name=qlast.ObjectRef(name='is_final'), value=True)


class OptInheritPosition(Nonterm):
    def reduce_BEFORE_NodeName(self, *kids):
        self.val = qlast.Position(ref=kids[1].val, position='BEFORE')

    def reduce_AFTER_NodeName(self, *kids):
        self.val = qlast.Position(ref=kids[1].val, position='AFTER')

    def reduce_FIRST(self, *kids):
        self.val = qlast.Position(position='FIRST')

    def reduce_LAST(self, *kids):
        self.val = qlast.Position(position='LAST')

    def reduce_empty(self, *kids):
        self.val = None


class AlterSimpleExtending(Nonterm):
    def reduce_EXTENDING_SimpleTypeNameList_OptInheritPosition(self, *kids):
        self.val = qlast.AlterAddInherit(bases=kids[1].val,
                                         position=kids[2].val)

    def reduce_DROP_EXTENDING_SimpleTypeNameList(self, *kids):
        self.val = qlast.AlterDropInherit(bases=kids[2].val)

    def reduce_AlterAbstract(self, *kids):
        self.val = kids[0].val

    def reduce_AlterFinal(self, *kids):
        self.val = kids[0].val


class AlterExtending(Nonterm):
    def reduce_EXTENDING_TypeNameList_OptInheritPosition(self, *kids):
        self.val = qlast.AlterAddInherit(bases=kids[1].val,
                                         position=kids[2].val)

    def reduce_DROP_EXTENDING_TypeNameList(self, *kids):
        self.val = qlast.AlterDropInherit(bases=kids[2].val)

    def reduce_AlterAbstract(self, *kids):
        self.val = kids[0].val

    def reduce_AlterFinal(self, *kids):
        self.val = kids[0].val


# DELTAS

class OptDeltaParents(Nonterm):
    def reduce_empty(self):
        self.val = []

    def reduce_FROM_NodeNameList(self, *kids):
        self.val = kids[1].val


class OptDeltaTarget(Nonterm):
    def reduce_empty(self):
        self.val = None

    def reduce_TO_AnyIdentifier_BaseStringConstant(self, *kids):
        self.val = [kids[1], kids[2]]

#
# DELTAS
#

#
# CREATE MIGRATION
#


class SDLCommandBlock(Nonterm):
    # this command block can be empty
    def reduce_LBRACE_OptSemicolons_RBRACE(self, *kids):
        self.val = qlast.Schema(declarations=[])

    def reduce_statement_without_semicolons(self, *kids):
        r"""%reduce LBRACE \
                OptSemicolons SDLShortStatement \
            RBRACE
        """
        self.val = qlast.Schema(declarations=[kids[2].val])

    def reduce_statements_without_optional_trailing_semicolons(self, *kids):
        r"""%reduce LBRACE \
                OptSemicolons SDLStatements \
                OptSemicolons SDLShortStatement \
            RBRACE
        """
        self.val = qlast.Schema(declarations=kids[2].val + [kids[4].val])

    def reduce_LBRACE_OptSemicolons_SDLStatements_RBRACE(self, *kids):
        self.val = qlast.Schema(declarations=kids[2].val)

    def reduce_statements_without_optional_trailing_semicolons2(self, *kids):
        r"""%reduce LBRACE \
                OptSemicolons SDLStatements \
                Semicolons \
            RBRACE
        """
        self.val = qlast.Schema(declarations=kids[2].val)


class CreateMigrationStmt(Nonterm):
    def reduce_CreateMigration_SDL(self, *kids):
        r"""%reduce CREATE MIGRATION NodeName \
                    OptDeltaParents TO SDLCommandBlock
        """
        self.val = qlast.CreateMigration(
            name=kids[2].val,
            parents=kids[3].val,
            target=kids[5].val,
        )

    def reduce_CreateMigration_Commands(self, *kids):
        r"""%reduce CREATE MIGRATION NodeName \
                    OptDeltaParents LBRACE InnerDDLStmtBlock OptSemicolons \
                    RBRACE
        """
        self.val = qlast.CreateMigration(
            name=kids[2].val,
            parents=kids[3].val,
            commands=kids[5].val,
        )


#
# ALTER MIGRATION
#
commands_block(
    'AlterMigration',
    RenameStmt,
    opt=False
)


class AlterMigrationStmt(Nonterm):
    def reduce_AlterMigration(self, *kids):
        r"""%reduce ALTER MIGRATION NodeName \
                    AlterMigrationCommandsBlock \
        """
        self.val = qlast.AlterMigration(
            name=kids[2].val,
            commands=kids[3].val
        )


#
# DROP MIGRATION
#
class DropMigrationStmt(Nonterm):
    def reduce_DROP_MIGRATION_NodeName(self, *kids):
        self.val = qlast.DropMigration(
            name=kids[2].val,
        )


# COMMIT MIGRATION
class CommitMigrationStmt(Nonterm):
    def reduce_COMMIT_MIGRATION_NodeName(self, *kids):
        self.val = qlast.CommitMigration(
            name=kids[2].val,
        )


# GET MIGRATION
class GetMigrationStmt(Nonterm):
    def reduce_GET_MIGRATION_NodeName(self, *kids):
        self.val = qlast.GetMigration(
            name=kids[2].val,
        )


#
# CREATE DATABASE
#
class CreateDatabaseStmt(Nonterm):
    def reduce_CREATE_DATABASE_AnyNodeName(self, *kids):
        self.val = qlast.CreateDatabase(name=kids[2].val)


#
# DROP DATABASE
#
class DropDatabaseStmt(Nonterm):
    def reduce_DROP_DATABASE_AnyNodeName(self, *kids):
        self.val = qlast.DropDatabase(name=kids[2].val)


#
# CREATE ROLE
#
class ShortExtending(Nonterm):
    def reduce_EXTENDING_ShortNodeNameList(self, *kids):
        self.val = [qlast.TypeName(maintype=v) for v in kids[1].val]


class OptShortExtending(Nonterm):
    def reduce_ShortExtending(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = []


commands_block(
    'CreateRole',
    SetFieldStmt,
)


class CreateRoleStmt(Nonterm):
    def reduce_CreateRoleStmt(self, *kids):
        r"""%reduce CREATE ROLE ShortNodeName OptShortExtending \
                                OptCreateRoleCommandsBlock \
        """
        self.val = qlast.CreateRole(
            name=kids[2].val,
            bases=kids[3].val,
            commands=kids[4].val,
        )


#
# ALTER ROLE
#
class AlterRoleExtending(Nonterm):
    def reduce_EXTENDING_ShortNodeNameList_OptInheritPosition(self, *kids):
        self.val = qlast.AlterAddInherit(
            bases=[qlast.TypeName(maintype=b) for b in kids[1].val],
            position=kids[2].val)

    def reduce_DROP_EXTENDING_ShortNodeNameList(self, *kids):
        self.val = qlast.AlterDropInherit(
            bases=[qlast.TypeName(maintype=b) for b in kids[2].val])


commands_block(
    'AlterRole',
    RenameStmt,
    SetFieldStmt,
    AlterRoleExtending,
    opt=False
)


class AlterRoleStmt(Nonterm):
    def reduce_ALTER_ROLE_ShortNodeName_AlterRoleCommandsBlock(self, *kids):
        self.val = qlast.AlterRole(
            name=kids[2].val,
            commands=kids[3].val,
        )


#
# DROP ROLE
#
class DropRoleStmt(Nonterm):
    def reduce_DROP_ROLE_ShortNodeName(self, *kids):
        self.val = qlast.DropRole(
            name=kids[2].val,
        )


#
# CREATE CONSTRAINT
#
class CreateConstraintStmt(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce CREATE ABSTRACT CONSTRAINT NodeName OptOnExpr \
                    OptExtendingSimple OptCreateCommandsBlock"""
        self.val = qlast.CreateConstraint(
            name=kids[3].val,
            subjectexpr=kids[4].val,
            bases=kids[5].val,
            commands=kids[6].val,
        )

    def reduce_CreateConstraint_CreateFunctionArgs(self, *kids):
        r"""%reduce CREATE ABSTRACT CONSTRAINT NodeName CreateFunctionArgs \
                    OptOnExpr OptExtendingSimple OptCreateCommandsBlock"""
        self.val = qlast.CreateConstraint(
            name=kids[3].val,
            params=kids[4].val,
            subjectexpr=kids[5].val,
            bases=kids[6].val,
            commands=kids[7].val,
        )


class AlterConstraintStmt(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce ALTER ABSTRACT CONSTRAINT NodeName \
                    AlterCommandsBlock"""
        self.val = qlast.AlterConstraint(
            name=kids[3].val,
            commands=kids[4].val,
        )


class DropConstraintStmt(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce DROP ABSTRACT CONSTRAINT NodeName"""
        self.val = qlast.DropConstraint(
            name=kids[3].val
        )


class OptDelegated(Nonterm):
    def reduce_DELEGATED(self, *kids):
        self.val = True

    def reduce_empty(self):
        self.val = False


class CreateConcreteConstraintStmt(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce CREATE OptDelegated CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr \
                    OptCreateCommandsBlock"""
        self.val = qlast.CreateConcreteConstraint(
            delegated=kids[1].val,
            name=kids[3].val,
            args=kids[4].val,
            subjectexpr=kids[5].val,
            commands=kids[6].val,
        )


class SetDelegatedStmt(Nonterm):

    def reduce_SET_DELEGATED(self, *kids):
        self.val = qlast.SetSpecialField(
            name=qlast.ObjectRef(name='delegated'),
            value=True,
        )

    def reduce_DROP_DELEGATED(self, *kids):
        self.val = qlast.SetSpecialField(
            name=qlast.ObjectRef(name='delegated'),
            value=False,
        )


commands_block(
    'AlterConcreteConstraint',
    RenameStmt,
    SetFieldStmt,
    SetDelegatedStmt,
    SetAnnotationValueStmt,
    DropAnnotationValueStmt,
    AlterAbstract,
    opt=False
)


class AlterConcreteConstraintStmt(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce ALTER CONSTRAINT NodeName
                    OptConcreteConstraintArgList OptOnExpr
                    AlterConcreteConstraintCommandsBlock"""
        self.val = qlast.AlterConcreteConstraint(
            name=kids[2].val,
            args=kids[3].val,
            subjectexpr=kids[4].val,
            commands=kids[5].val,
        )


class DropConcreteConstraintStmt(Nonterm):
    def reduce_DropConstraint(self, *kids):
        r"""%reduce DROP CONSTRAINT NodeName
                    OptConcreteConstraintArgList OptOnExpr"""
        self.val = qlast.DropConcreteConstraint(
            name=kids[2].val,
            args=kids[3].val,
            subjectexpr=kids[4].val,
        )


#
# CREATE SCALAR TYPE
#

commands_block(
    'CreateScalarType',
    SetFieldStmt,
    SetAnnotationValueStmt,
    CreateConcreteConstraintStmt)


class CreateScalarTypeStmt(Nonterm):
    def reduce_CreateAbstractScalarTypeStmt(self, *kids):
        r"""%reduce \
            CREATE ABSTRACT SCALAR TYPE NodeName \
            OptExtending OptCreateScalarTypeCommandsBlock \
        """
        self.val = qlast.CreateScalarType(
            name=kids[4].val,
            is_abstract=True,
            bases=kids[5].val,
            commands=kids[6].val
        )

    def reduce_CreateFinalScalarTypeStmt(self, *kids):
        r"""%reduce \
            CREATE FINAL SCALAR TYPE NodeName \
            OptExtending OptCreateScalarTypeCommandsBlock \
        """
        self.val = qlast.CreateScalarType(
            name=kids[4].val,
            is_final=True,
            bases=kids[5].val,
            commands=kids[6].val
        )

    def reduce_CreateScalarTypeStmt(self, *kids):
        r"""%reduce \
            CREATE SCALAR TYPE NodeName \
            OptExtending OptCreateScalarTypeCommandsBlock \
        """
        self.val = qlast.CreateScalarType(
            name=kids[3].val,
            bases=kids[4].val,
            commands=kids[5].val
        )


#
# ALTER SCALAR TYPE
#

commands_block(
    'AlterScalarType',
    RenameStmt,
    SetFieldStmt,
    SetAnnotationValueStmt,
    DropAnnotationValueStmt,
    AlterExtending,
    CreateConcreteConstraintStmt,
    AlterConcreteConstraintStmt,
    DropConcreteConstraintStmt,
    opt=False
)


class AlterScalarTypeStmt(Nonterm):
    def reduce_AlterScalarTypeStmt(self, *kids):
        r"""%reduce \
            ALTER SCALAR TYPE NodeName \
            AlterScalarTypeCommandsBlock \
        """
        self.val = qlast.AlterScalarType(
            name=kids[3].val,
            commands=kids[4].val
        )


class DropScalarTypeStmt(Nonterm):
    def reduce_DROP_SCALAR_TYPE_NodeName(self, *kids):
        self.val = qlast.DropScalarType(
            name=kids[3].val
        )


#
# CREATE ANNOTATION
#
class CreateAnnotationStmt(Nonterm):
    def reduce_CreateAnnotation(self, *kids):
        r"""%reduce CREATE ABSTRACT ANNOTATION NodeName OptExtendingSimple \
                    OptCreateCommandsBlock"""
        self.val = qlast.CreateAnnotation(
            name=kids[3].val,
            bases=kids[4].val,
            commands=kids[5].val,
            inheritable=False,
        )

    def reduce_CreateInheritableAnnotation(self, *kids):
        r"""%reduce CREATE ABSTRACT INHERITABLE ANNOTATION
                    NodeName OptExtendingSimple OptCreateCommandsBlock"""
        self.val = qlast.CreateAnnotation(
            name=kids[4].val,
            bases=kids[5].val,
            commands=kids[6].val,
            inheritable=True,
        )


#
# DROP ANNOTATION
#
class DropAnnotationStmt(Nonterm):
    def reduce_DropAnnotation(self, *kids):
        r"""%reduce DROP ABSTRACT ANNOTATION NodeName"""
        self.val = qlast.DropAnnotation(
            name=kids[3].val,
        )


commands_block(
    'AlterIndex',
    SetFieldStmt,
    SetAnnotationValueStmt,
    DropAnnotationValueStmt,
    opt=False)


#
# CREATE INDEX
#
class CreateIndexStmt(Nonterm):
    def reduce_CREATE_INDEX_OnExpr_OptCreateCommandsBlock(self, *kids):
        self.val = qlast.CreateIndex(
            name=qlast.ObjectRef(name='idx'),
            expr=kids[2].val,
            commands=kids[3].val,
        )


#
# ALTER INDEX
#
class AlterIndexStmt(Nonterm):
    def reduce_ALTER_INDEX_OnExpr_AlterIndexCommandsBlock(self, *kids):
        self.val = qlast.AlterIndex(
            name=qlast.ObjectRef(name='idx'),
            expr=kids[2].val,
            commands=kids[3].val,
        )


#
# DROP INDEX
#
class DropIndexStmt(Nonterm):
    def reduce_DROP_INDEX_OnExpr(self, *kids):
        self.val = qlast.DropIndex(
            name=qlast.ObjectRef(name='idx'),
            expr=kids[2].val,
        )


class SetPropertyTypeStmt(Nonterm):
    def reduce_SETTYPE_FullTypeExpr(self, *kids):
        self.val = qlast.SetPropertyType(type=kids[1].val)


class SetLinkTypeStmt(Nonterm):
    def reduce_SETTYPE_FullTypeExpr(self, *kids):
        self.val = qlast.SetLinkType(type=kids[1].val)


#
# CREATE PROPERTY
#
class CreatePropertyStmt(Nonterm):
    def reduce_CreateProperty(self, *kids):
        r"""%reduce CREATE ABSTRACT PROPERTY NodeName OptExtendingSimple \
                    OptCreateCommandsBlock \
        """
        self.val = qlast.CreateProperty(
            name=kids[3].val,
            bases=kids[4].val,
            commands=kids[5].val
        )


#
# ALTER PROPERTY
#

commands_block(
    'AlterProperty',
    RenameStmt,
    SetFieldStmt,
    SetAnnotationValueStmt,
    DropAnnotationValueStmt,
    opt=False
)


class AlterPropertyStmt(Nonterm):
    def reduce_AlterProperty(self, *kids):
        r"""%reduce \
            ALTER ABSTRACT PROPERTY NodeName \
            AlterPropertyCommandsBlock \
        """
        self.val = qlast.AlterProperty(
            name=kids[3].val,
            commands=kids[4].val
        )


#
# DROP PROPERTY
#
class DropPropertyStmt(Nonterm):
    def reduce_DropProperty(self, *kids):
        r"""%reduce DROP ABSTRACT PROPERTY NodeName"""
        self.val = qlast.DropProperty(
            name=kids[3].val
        )


#
# CREATE LINK ... { CREATE PROPERTY
#

commands_block(
    'CreateConcreteProperty',
    SetFieldStmt,
    SetAnnotationValueStmt,
    CreateConcreteConstraintStmt
)


class CreateConcretePropertyStmt(Nonterm):
    def reduce_CreateRegularProperty(self, *kids):
        """%reduce
            CREATE OptPtrQuals PROPERTY UnqualifiedPointerName
            OptExtendingSimple ARROW FullTypeExpr
            OptCreateConcretePropertyCommandsBlock
        """
        self.val = qlast.CreateConcreteProperty(
            name=kids[3].val,
            bases=kids[4].val,
            is_required=kids[1].val.required,
            cardinality=kids[1].val.cardinality,
            target=kids[6].val,
            commands=kids[7].val,
        )

    def reduce_CreateComputableProperty(self, *kids):
        """%reduce
            CREATE OptPtrQuals PROPERTY UnqualifiedPointerName ASSIGN Expr
        """
        self.val = qlast.CreateConcreteProperty(
            name=kids[3].val,
            is_required=kids[1].val.required,
            cardinality=kids[1].val.cardinality,
            target=kids[5].val,
        )


#
# ALTER LINK ... { ALTER PROPERTY
#

class SetCardinalityStmt(Nonterm):

    def reduce_SET_SINGLE(self, *kids):
        self.val = qlast.SetSpecialField(
            name=qlast.ObjectRef(name='cardinality'),
            value=qltypes.Cardinality.ONE,
        )

    def reduce_SET_MULTI(self, *kids):
        self.val = qlast.SetSpecialField(
            name=qlast.ObjectRef(name='cardinality'),
            value=qltypes.Cardinality.MANY,
        )


class SetRequiredStmt(Nonterm):

    def reduce_SET_REQUIRED(self, *kids):
        self.val = qlast.SetSpecialField(
            name=qlast.ObjectRef(name='required'),
            value=True,
        )

    def reduce_DROP_REQUIRED(self, *kids):
        self.val = qlast.SetSpecialField(
            name=qlast.ObjectRef(name='required'),
            value=False,
        )


commands_block(
    'AlterConcreteProperty',
    RenameStmt,
    SetFieldStmt,
    SetAnnotationValueStmt,
    DropAnnotationValueStmt,
    SetPropertyTypeStmt,
    SetCardinalityStmt,
    SetRequiredStmt,
    AlterSimpleExtending,
    CreateConcreteConstraintStmt,
    AlterConcreteConstraintStmt,
    DropConcreteConstraintStmt,
    opt=False
)


class AlterConcretePropertyStmt(Nonterm):
    def reduce_AlterProperty(self, *kids):
        r"""%reduce \
            ALTER PROPERTY UnqualifiedPointerName \
            AlterConcretePropertyCommandsBlock \
        """
        self.val = qlast.AlterConcreteProperty(
            name=kids[2].val,
            commands=kids[3].val
        )


#
# ALTER LINK ... { DROP PROPERTY
#

class DropConcretePropertyStmt(Nonterm):
    def reduce_DropProperty(self, *kids):
        r"""%reduce \
            DROP PROPERTY UnqualifiedPointerName \
        """
        self.val = qlast.DropConcreteProperty(
            name=kids[2].val
        )


#
# CREATE LINK
#

commands_block(
    'CreateLink',
    SetFieldStmt,
    SetAnnotationValueStmt,
    CreateConcreteConstraintStmt,
    CreateConcretePropertyStmt,
    CreateIndexStmt,
)


class CreateLinkStmt(Nonterm):
    def reduce_CreateLink(self, *kids):
        r"""%reduce \
            CREATE ABSTRACT LINK NodeName OptExtendingSimple \
            OptCreateLinkCommandsBlock \
        """
        self.val = qlast.CreateLink(
            name=kids[3].val,
            bases=kids[4].val,
            commands=kids[5].val
        )


#
# ALTER LINK
#

commands_block(
    'AlterLink',
    RenameStmt,
    SetFieldStmt,
    SetAnnotationValueStmt,
    DropAnnotationValueStmt,
    AlterSimpleExtending,
    CreateConcreteConstraintStmt,
    AlterConcreteConstraintStmt,
    DropConcreteConstraintStmt,
    CreateConcretePropertyStmt,
    AlterConcretePropertyStmt,
    DropConcretePropertyStmt,
    CreateIndexStmt,
    AlterIndexStmt,
    DropIndexStmt,
    opt=False
)


class AlterLinkStmt(Nonterm):
    def reduce_AlterLink(self, *kids):
        r"""%reduce \
            ALTER ABSTRACT LINK NodeName \
            AlterLinkCommandsBlock \
        """
        self.val = qlast.AlterLink(
            name=kids[3].val,
            commands=kids[4].val
        )


#
# DROP LINK
#

commands_block(
    'DropLink',
    DropConcreteConstraintStmt,
    DropConcreteConstraintStmt,
    DropConcretePropertyStmt,
    DropIndexStmt,
)


class DropLinkStmt(Nonterm):
    def reduce_DropLink(self, *kids):
        r"""%reduce \
            DROP ABSTRACT LINK NodeName \
            OptDropLinkCommandsBlock \
        """
        self.val = qlast.DropLink(
            name=kids[3].val,
            commands=kids[4].val
        )


#
# CREATE TYPE ... { CREATE LINK
#

commands_block(
    'CreateConcreteLink',
    SetFieldStmt,
    SetAnnotationValueStmt,
    CreateConcreteConstraintStmt,
    CreateConcretePropertyStmt,
    commondl.OnTargetDeleteStmt,
)


class CreateConcreteLinkStmt(Nonterm):
    def reduce_CreateRegularLink(self, *kids):
        """%reduce
            CREATE OptPtrQuals LINK UnqualifiedPointerName OptExtendingSimple
            ARROW FullTypeExpr OptCreateConcreteLinkCommandsBlock
        """
        self.val = qlast.CreateConcreteLink(
            name=kids[3].val,
            bases=kids[4].val,
            is_required=kids[1].val.required,
            cardinality=kids[1].val.cardinality,
            target=kids[6].val,
            commands=kids[7].val
        )

    def reduce_CreateComputableLink(self, *kids):
        """%reduce
            CREATE OptPtrQuals LINK UnqualifiedPointerName ASSIGN Expr
        """
        self.val = qlast.CreateConcreteLink(
            name=kids[3].val,
            is_required=kids[1].val.required,
            cardinality=kids[1].val.cardinality,
            target=kids[5].val,
        )


commands_block(
    'AlterConcreteLink',
    RenameStmt,
    SetFieldStmt,
    SetAnnotationValueStmt,
    DropAnnotationValueStmt,
    SetCardinalityStmt,
    SetRequiredStmt,
    SetLinkTypeStmt,
    AlterSimpleExtending,
    CreateConcreteConstraintStmt,
    AlterConcreteConstraintStmt,
    DropConcreteConstraintStmt,
    CreateConcretePropertyStmt,
    AlterConcretePropertyStmt,
    DropConcretePropertyStmt,
    commondl.OnTargetDeleteStmt,
    opt=False
)


class AlterConcreteLinkStmt(Nonterm):
    def reduce_AlterLink(self, *kids):
        r"""%reduce \
            ALTER LINK UnqualifiedPointerName AlterConcreteLinkCommandsBlock \
        """
        self.val = qlast.AlterConcreteLink(
            name=kids[2].val,
            commands=kids[3].val
        )


commands_block(
    'DropConcreteLink',
    DropConcreteConstraintStmt,
    DropConcretePropertyStmt,
)


class DropConcreteLinkStmt(Nonterm):
    def reduce_DropLink(self, *kids):
        r"""%reduce \
            DROP LINK UnqualifiedPointerName OptDropConcreteLinkCommandsBlock \
        """
        self.val = qlast.DropConcreteLink(
            name=kids[2].val,
            commands=kids[3].val
        )


#
# CREATE TYPE
#

commands_block(
    'CreateObjectType',
    SetFieldStmt,
    SetAnnotationValueStmt,
    CreateConcretePropertyStmt,
    AlterConcretePropertyStmt,
    CreateConcreteLinkStmt,
    AlterConcreteLinkStmt,
    CreateIndexStmt,
    AlterIndexStmt,
)


class CreateObjectTypeStmt(Nonterm):
    def reduce_CreateAbstractObjectTypeStmt(self, *kids):
        r"""%reduce \
            CREATE ABSTRACT TYPE NodeName \
            OptExtendingSimple OptCreateObjectTypeCommandsBlock \
        """
        self.val = qlast.CreateObjectType(
            name=kids[3].val,
            bases=kids[4].val,
            is_abstract=True,
            commands=kids[5].val,
        )

    def reduce_CreateRegularObjectTypeStmt(self, *kids):
        r"""%reduce \
            CREATE TYPE NodeName \
            OptExtendingSimple OptCreateObjectTypeCommandsBlock \
        """
        self.val = qlast.CreateObjectType(
            name=kids[2].val,
            bases=kids[3].val,
            is_abstract=False,
            commands=kids[4].val,
        )


#
# ALTER TYPE
#

commands_block(
    'AlterObjectType',
    RenameStmt,
    SetFieldStmt,
    SetAnnotationValueStmt,
    DropAnnotationValueStmt,
    AlterSimpleExtending,
    CreateConcretePropertyStmt,
    AlterConcretePropertyStmt,
    DropConcretePropertyStmt,
    CreateConcreteLinkStmt,
    AlterConcreteLinkStmt,
    DropConcreteLinkStmt,
    CreateIndexStmt,
    AlterIndexStmt,
    DropIndexStmt,
    opt=False
)


class AlterObjectTypeStmt(Nonterm):
    def reduce_AlterObjectTypeStmt(self, *kids):
        r"""%reduce \
            ALTER TYPE NodeName \
            AlterObjectTypeCommandsBlock \
        """
        self.val = qlast.AlterObjectType(
            name=kids[2].val,
            commands=kids[3].val
        )


#
# DROP TYPE
#

commands_block(
    'DropObjectType',
    DropConcretePropertyStmt,
    DropConcreteLinkStmt,
    DropConcreteConstraintStmt,
    DropIndexStmt
)


class DropObjectTypeStmt(Nonterm):
    def reduce_DropObjectType(self, *kids):
        r"""%reduce \
            DROP TYPE \
            NodeName OptDropObjectTypeCommandsBlock \
        """
        self.val = qlast.DropObjectType(
            name=kids[2].val,
            commands=kids[3].val
        )


#
# CREATE VIEW
#

commands_block(
    'CreateView',
    SetFieldStmt,
    SetAnnotationValueStmt,
    opt=False
)


class CreateViewStmt(Nonterm):
    def reduce_CreateViewShortStmt(self, *kids):
        r"""%reduce \
            CREATE VIEW NodeName ASSIGN Expr \
        """
        self.val = qlast.CreateView(
            name=kids[2].val,
            commands=[
                qlast.SetField(
                    name=qlast.ObjectRef(name='expr'),
                    value=kids[4].val,
                )
            ]
        )

    def reduce_CreateViewRegularStmt(self, *kids):
        r"""%reduce \
            CREATE VIEW NodeName \
            CreateViewCommandsBlock \
        """
        self.val = qlast.CreateView(
            name=kids[2].val,
            commands=kids[3].val,
        )


#
# ALTER VIEW
#

commands_block(
    'AlterView',
    RenameStmt,
    SetFieldStmt,
    SetAnnotationValueStmt,
    DropAnnotationValueStmt,
    opt=False
)


class AlterViewStmt(Nonterm):
    def reduce_AlterViewStmt(self, *kids):
        r"""%reduce \
            ALTER VIEW NodeName \
            AlterViewCommandsBlock \
        """
        self.val = qlast.AlterView(
            name=kids[2].val,
            commands=kids[3].val
        )


#
# DROP VIEW
#

class DropViewStmt(Nonterm):
    def reduce_DropView(self, *kids):
        r"""%reduce \
            DROP VIEW NodeName \
        """
        self.val = qlast.DropView(
            name=kids[2].val,
        )


#
# CREATE MODULE
#
class CreateModuleStmt(Nonterm):
    def reduce_CREATE_MODULE_ModuleName_OptCreateCommandsBlock(
            self, *kids):
        self.val = qlast.CreateModule(
            name=qlast.ObjectRef(module=None, name='.'.join(kids[2].val)),
            commands=kids[3].val
        )


#
# ALTER MODULE
#
class AlterModuleStmt(Nonterm):
    def reduce_ALTER_MODULE_ModuleName_AlterCommandsBlock(
            self, *kids):
        self.val = qlast.AlterModule(
            name=qlast.ObjectRef(module=None, name='.'.join(kids[2].val)),
            commands=kids[3].val
        )


#
# DROP MODULE
#
class DropModuleStmt(Nonterm):
    def reduce_DROP_MODULE_ModuleName(self, *kids):
        self.val = qlast.DropModule(
            name=qlast.ObjectRef(module=None, name='.'.join(kids[2].val))
        )


#
# CREATE FUNCTION
#


commands_block(
    'CreateFunction',
    commondl.FromFunction,
    SetFieldStmt,
    SetAnnotationValueStmt,
    opt=False
)


class CreateFunctionStmt(Nonterm, commondl.ProcessFunctionBlockMixin):
    def reduce_CreateFunction(self, *kids):
        r"""%reduce CREATE FUNCTION NodeName CreateFunctionArgs \
                ARROW OptTypeQualifier FunctionType \
                CreateFunctionCommandsBlock
        """
        self.val = qlast.CreateFunction(
            name=kids[2].val,
            params=kids[3].val,
            returning=kids[6].val,
            returning_typemod=kids[5].val,
            **self._process_function_body(kids[7])
        )


class DropFunctionStmt(Nonterm):
    def reduce_DropFunction(self, *kids):
        r"""%reduce DROP FUNCTION NodeName CreateFunctionArgs"""
        self.val = qlast.DropFunction(
            name=kids[2].val,
            params=kids[3].val)


#
# CREATE OPERATOR
#

class OperatorKind(Nonterm):

    def reduce_INFIX(self, *kids):
        self.val = kids[0].val

    def reduce_POSTFIX(self, *kids):
        self.val = kids[0].val

    def reduce_PREFIX(self, *kids):
        self.val = kids[0].val

    def reduce_TERNARY(self, *kids):
        self.val = kids[0].val


class OperatorCode(Nonterm):

    def reduce_USING_Identifier_OPERATOR_BaseStringConstant(self, *kids):
        lang = commondl._parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in USING OPERATOR clause',
                context=kids[1].context) from None

        sql_operator = kids[3].val.value
        m = re.match(r'([^(]+)(?:\((\w*(?:,\s*\w*)*)\))?', sql_operator)
        if not m:
            raise EdgeQLSyntaxError(
                f'invalid syntax for USING OPERATOR clause',
                context=kids[3].context) from None

        sql_operator = (m.group(1),)
        if m.group(2):
            operands = tuple(op.strip() for op in m.group(2).split(','))
            sql_operator += operands

        self.val = qlast.OperatorCode(
            language=lang, from_operator=sql_operator)

    def reduce_USING_Identifier_FUNCTION_BaseStringConstant(self, *kids):
        lang = commondl._parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in USING FUNCTION clause',
                context=kids[1].context) from None

        self.val = qlast.OperatorCode(language=lang,
                                      from_function=kids[3].val.value)

    def reduce_USING_Identifier_BaseStringConstant(self, *kids):
        lang = commondl._parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in USING clause',
                context=kids[1].context) from None

        self.val = qlast.OperatorCode(language=lang,
                                      code=kids[2].val.value)

    def reduce_USING_Identifier_EXPRESSION(self, *kids):
        lang = commondl._parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in USING clause',
                context=kids[1].context) from None

        self.val = qlast.OperatorCode(language=lang)


commands_block(
    'CreateOperator',
    SetFieldStmt,
    SetAnnotationValueStmt,
    OperatorCode,
    opt=False
)


class OptCreateOperatorCommandsBlock(Nonterm):

    def reduce_CreateOperatorCommandsBlock(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = []


class CreateOperatorStmt(Nonterm):

    def reduce_CreateOperatorStmt(self, *kids):
        r"""%reduce
            CREATE OperatorKind OPERATOR NodeName CreateFunctionArgs
            ARROW OptTypeQualifier FunctionType
            CreateOperatorCommandsBlock
        """
        self.val = qlast.CreateOperator(
            kind=qltypes.OperatorKind(kids[1].val.upper()),
            name=kids[3].val,
            params=kids[4].val,
            returning_typemod=kids[6].val,
            returning=kids[7].val,
            **self._process_operator_body(kids[8])
        )

    def reduce_CreateAbstractOperatorStmt(self, *kids):
        r"""%reduce
            CREATE ABSTRACT OperatorKind OPERATOR NodeName CreateFunctionArgs
            ARROW OptTypeQualifier FunctionType
            OptCreateOperatorCommandsBlock
        """
        self.val = qlast.CreateOperator(
            kind=qltypes.OperatorKind(kids[2].val.upper()),
            name=kids[4].val,
            params=kids[5].val,
            returning_typemod=kids[7].val,
            returning=kids[8].val,
            is_abstract=True,
            **self._process_operator_body(kids[9], abstract=True)
        )

    def _process_operator_body(self, block, abstract: bool=False):
        props = {}

        commands = []
        from_operator = None
        from_function = None
        from_expr = False
        code = None

        for node in block.val:
            if isinstance(node, qlast.OperatorCode):
                if abstract:
                    raise errors.InvalidOperatorDefinitionError(
                        'unexpected USING clause in abstract '
                        'operator definition',
                        context=node.context,
                    )

                if node.from_function:
                    if from_function is not None:
                        raise errors.InvalidOperatorDefinitionError(
                            'more than one USING FUNCTION clause',
                            context=node.context)
                    from_function = node.from_function

                elif node.from_operator:
                    if from_operator is not None:
                        raise errors.InvalidOperatorDefinitionError(
                            'more than one USING OPERATOR clause',
                            context=node.context)
                    from_operator = node.from_operator

                elif node.code:
                    if code is not None:
                        raise errors.InvalidOperatorDefinitionError(
                            'more than one USING <code> clause',
                            context=node.context)
                    code = node.code

                else:
                    # USING SQL EXPRESSION
                    from_expr = True
            else:
                commands.append(node)

        if not abstract:
            if (code is None and from_operator is None
                    and from_function is None
                    and not from_expr):
                raise errors.InvalidOperatorDefinitionError(
                    'CREATE OPERATOR requires at least one USING clause',
                    context=block.context)

            else:
                if from_expr and (from_operator or from_function or code):
                    raise errors.InvalidOperatorDefinitionError(
                        'USING SQL EXPRESSION is mutually exclusive with '
                        'other USING variants',
                        context=block.context)

                props['code'] = qlast.OperatorCode(
                    language=qlast.Language.SQL,
                    from_function=from_function,
                    from_operator=from_operator,
                    from_expr=from_expr,
                    code=code,
                )

        if commands:
            props['commands'] = commands

        return props


#
# ALTER OPERATOR
#

commands_block(
    'AlterOperator',
    SetFieldStmt,
    SetAnnotationValueStmt,
    DropAnnotationValueStmt,
    opt=False
)


class AlterOperatorStmt(Nonterm):
    def reduce_AlterOperatorStmt(self, *kids):
        """%reduce
           ALTER OperatorKind OPERATOR NodeName CreateFunctionArgs
           AlterOperatorCommandsBlock
        """
        self.val = qlast.AlterOperator(
            kind=qltypes.OperatorKind(kids[1].val.upper()),
            name=kids[3].val,
            params=kids[4].val,
            commands=kids[5].val
        )


#
# DROP OPERATOR
#

class DropOperatorStmt(Nonterm):
    def reduce_DropOperator(self, *kids):
        """%reduce
           DROP OperatorKind OPERATOR NodeName CreateFunctionArgs
        """
        self.val = qlast.DropOperator(
            kind=qltypes.OperatorKind(kids[1].val.upper()),
            name=kids[3].val,
            params=kids[4].val,
        )


#
# CREATE CAST
#


class CastUseValue(typing.NamedTuple):

    use: str


class CastAllowedUse(Nonterm):

    def reduce_ALLOW_IMPLICIT(self, *kids):
        self.val = CastUseValue(use=kids[1].val.upper())

    def reduce_ALLOW_ASSIGNMENT(self, *kids):
        self.val = CastUseValue(use=kids[1].val.upper())


class CastCode(Nonterm):

    def reduce_USING_Identifier_FUNCTION_BaseStringConstant(self, *kids):
        lang = commondl._parse_language(kids[1])
        if lang not in {qlast.Language.SQL, qlast.Language.EdgeQL}:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in USING FUNCTION clause',
                context=kids[1].context) from None

        self.val = qlast.CastCode(language=lang,
                                  from_function=kids[3].val.value)

    def reduce_USING_Identifier_BaseStringConstant(self, *kids):
        lang = commondl._parse_language(kids[1])
        if lang not in {qlast.Language.SQL, qlast.Language.EdgeQL}:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in USING clause',
                context=kids[1].context) from None

        self.val = qlast.CastCode(language=lang,
                                  code=kids[2].val.value)

    def reduce_USING_Identifier_CAST(self, *kids):
        lang = commondl._parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in USING CAST clause',
                context=kids[1].context) from None

        self.val = qlast.CastCode(language=lang, from_cast=True)

    def reduce_USING_Identifier_EXPRESSION(self, *kids):
        lang = commondl._parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in USING EXPRESSION clause',
                context=kids[1].context) from None

        self.val = qlast.CastCode(language=lang)


commands_block(
    'CreateCast',
    SetFieldStmt,
    SetAnnotationValueStmt,
    CastCode,
    CastAllowedUse,
    opt=False
)


class CreateCastStmt(Nonterm):

    def reduce_CreateCastStmt(self, *kids):
        r"""%reduce
            CREATE CAST FROM TypeName TO TypeName
            CreateCastCommandsBlock
        """
        self.val = qlast.CreateCast(
            from_type=kids[3].val,
            to_type=kids[5].val,
            **self._process_cast_body(kids[6])
        )

    def _process_cast_body(self, block):
        props = {}

        commands = []
        from_function = None
        from_expr = False
        from_cast = False
        allow_implicit = False
        allow_assignment = False
        code = None

        for node in block.val:
            if isinstance(node, qlast.CastCode):
                if node.from_function:
                    if from_function is not None:
                        raise EdgeQLSyntaxError(
                            'more than one USING FUNCTION clause',
                            context=node.context)
                    from_function = node.from_function

                elif node.code:
                    if code is not None:
                        raise EdgeQLSyntaxError(
                            'more than one USING <code> clause',
                            context=node.context)
                    code = node.code

                elif node.from_cast:
                    # USING SQL CAST

                    if from_cast:
                        raise EdgeQLSyntaxError(
                            'more than one USING CAST clause',
                            context=node.context)

                    from_cast = True

                else:
                    # USING SQL EXPRESSION

                    if from_expr:
                        raise EdgeQLSyntaxError(
                            'more than one USING EXPRESSION clause',
                            context=node.context)

                    from_expr = True

            elif isinstance(node, CastUseValue):

                if node.use == 'IMPLICIT':
                    allow_implicit = True
                elif node.use == 'ASSIGNMENT':
                    allow_assignment = True
                else:
                    raise EdgeQLSyntaxError(
                        'unexpected ALLOW clause',
                        context=node.context)

            else:
                commands.append(node)

        if (code is None and from_function is None
                and not from_expr and not from_cast):
            raise EdgeQLSyntaxError(
                'CREATE CAST requires at least one USING clause',
                context=block.context)

        else:
            if from_expr and (from_function or code or from_cast):
                raise EdgeQLSyntaxError(
                    'USING SQL EXPRESSION is mutually exclusive with other '
                    'USING variants',
                    context=block.context)

            if from_cast and (from_function or code or from_expr):
                raise EdgeQLSyntaxError(
                    'USING SQL CAST is mutually exclusive with other '
                    'USING variants',
                    context=block.context)

            props['code'] = qlast.CastCode(
                language=qlast.Language.SQL,
                from_function=from_function,
                from_expr=from_expr,
                from_cast=from_cast,
                code=code,
            )

            props['allow_implicit'] = allow_implicit
            props['allow_assignment'] = allow_assignment

        if commands:
            props['commands'] = commands

        return props


#
# ALTER CAST
#

commands_block(
    'AlterCast',
    SetFieldStmt,
    SetAnnotationValueStmt,
    DropAnnotationValueStmt,
    opt=False
)


class AlterCastStmt(Nonterm):
    def reduce_AlterCastStmt(self, *kids):
        """%reduce
           ALTER CAST FROM TypeName TO TypeName
           AlterCastCommandsBlock
        """
        self.val = qlast.AlterCast(
            from_type=kids[3].val,
            to_type=kids[5].val,
            commands=kids[6].val,
        )


#
# DROP CAST
#

class DropCastStmt(Nonterm):
    def reduce_DropCastStmt(self, *kids):
        """%reduce
           DROP CAST FROM TypeName TO TypeName
        """
        self.val = qlast.DropCast(
            from_type=kids[3].val,
            to_type=kids[5].val,
        )
