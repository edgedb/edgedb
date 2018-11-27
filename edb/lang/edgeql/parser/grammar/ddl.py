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


import collections
import re
import sys
import types
import typing

from edb.errors import EdgeQLSyntaxError

from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import functypes as ft

from edb.lang.common import parsing, context
from edb.lang.common.parsing import ListNonterm

from .expressions import Nonterm
from . import tokens

from .precedence import *  # NOQA
from .tokens import *  # NOQA
from .expressions import *  # NOQA


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
    def reduce_CreateDeltaStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterDeltaStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropDeltaStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CommitDeltaStmt(self, *kids):
        self.val = kids[0].val

    def reduce_GetDeltaStmt(self, *kids):
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

    def reduce_CreateAttributeStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropAttributeStmt(self, *kids):
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

    def reduce_CreateIndexStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropIndexStmt(self, *kids):
        self.val = kids[0].val


class Semicolons(Nonterm):
    # one or more semicolons
    #
    def reduce_SEMICOLON(self, tok):
        self.val = tok

    def reduce_Semicolons_SEMICOLON(self, *kids):
        self.val = kids[0].val


class InnerDDLStmtBlock(ListNonterm, element=InnerDDLStmt,
                        separator=Semicolons):
    pass


class LinkName(Nonterm):
    def reduce_NodeName(self, *kids):
        self.val = kids[0].val

    def reduce_DUNDERTYPE(self, *kids):
        self.val = qlast.ObjectRef(name=kids[0].val)


def _new_nonterm(clsname, clsdict={}, clskwds={}, clsbases=(Nonterm,)):
    mod = sys.modules[__name__]

    def clsexec(ns):
        ns['__module__'] = __name__
        for k, v in clsdict.items():
            ns[k] = v
        return ns

    cls = types.new_class(clsname, clsbases, clskwds, clsexec)
    setattr(mod, clsname, cls)
    return cls


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
    clsdict['reduce_LBRACE_' + cmdlist.__name__ + '_Semicolons_RBRACE'] = \
        ProductionHelper._block
    clsdict['reduce_LBRACE_Semicolons_' + cmdlist.__name__ +
            '_Semicolons_RBRACE'] = \
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


class SchemaItemClassValue(typing.NamedTuple):

    itemclass: qlast.SchemaItemClass


class SchemaItemClass(Nonterm):

    def reduce_OPERATOR(self, *kids):
        self.val = SchemaItemClassValue(
            itemclass=qlast.SchemaItemClass.OPERATOR)


class SetFieldStmt(Nonterm):
    # field := <expr>
    def reduce_SET_NodeName_ASSIGN_Expr(self, *kids):
        # if the expression is trivial (a literal or variable), it
        # should be treated as an eager expression
        eager = isinstance(kids[3].val,
                           (qlast.BaseConstant, qlast.Tuple))
        self.val = qlast.SetField(
            name=kids[1].val,
            value=kids[3].val,
            as_expr=not eager
        )

    def reduce_SET_NodeName_AS_SchemaItemClass_NodeName(self, *kids):
        ref = kids[4].val
        ref.itemclass = kids[3].val.itemclass
        self.val = qlast.SetField(
            name=kids[1].val,
            value=ref
        )


class SetAttributeValueStmt(Nonterm):
    def reduce_SETATTRIBUTE_NodeName_ASSIGN_Expr(self, *kids):
        self.val = qlast.CreateAttributeValue(
            name=kids[1].val,
            value=kids[3].val,
        )


class DropAttributeValueStmt(Nonterm):
    def reduce_DROP_ATTRIBUTE_NodeName(self, *kids):
        self.val = qlast.DropAttributeValue(
            name=kids[2].val,
        )


class RenameStmt(Nonterm):
    def reduce_RENAME_TO_NodeName(self, *kids):
        self.val = qlast.Rename(new_name=kids[2].val)


commands_block(
    'Create',
    SetFieldStmt,
    SetAttributeValueStmt)


commands_block(
    'Alter',
    RenameStmt,
    SetFieldStmt,
    SetAttributeValueStmt,
    DropAttributeValueStmt,
    opt=False)


class Extending(Nonterm):
    def reduce_EXTENDING_SimpleTypeName(self, *kids):
        self.val = [kids[1].val]

    def reduce_EXTENDING_LPAREN_SimpleTypeNameList_RPAREN(self, *kids):
        self.val = kids[2].val


class OptExtending(Nonterm):
    def reduce_Extending(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = []


class AlterAbstract(Nonterm):
    def reduce_DROP_ABSTRACT(self, *kids):
        self.val = qlast.SetSpecialField(name='is_abstract', value=False)

    def reduce_SET_ABSTRACT(self, *kids):
        self.val = qlast.SetSpecialField(name='is_abstract', value=True)


class AlterFinal(Nonterm):
    def reduce_DROP_FINAL(self, *kids):
        self.val = qlast.SetSpecialField(name='is_final', value=False)

    def reduce_SET_FINAL(self, *kids):
        self.val = qlast.SetSpecialField(name='is_final', value=True)


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


class AlterExtending(Nonterm):
    def reduce_EXTENDING_SimpleTypeNameList_OptInheritPosition(self, *kids):
        self.val = qlast.AlterAddInherit(bases=kids[1].val,
                                         position=kids[2].val)

    def reduce_DROP_EXTENDING_SimpleTypeNameList(self, *kids):
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


class CreateDeltaStmt(Nonterm):
    def _parse_schema_decl(self, tok):
        from edb.lang.common.exceptions import get_context
        from edb.lang.schema import parser

        ctx = tok.context

        try:
            node = parser.parse(tok.val.value)
        except parsing.ParserError as err:
            context.rebase_context(
                ctx, get_context(err, parsing.ParserContext))
            raise err
        else:
            context.rebase_ast_context(ctx, node)
            return node

    def reduce_CreateDelta_TO(self, *kids):
        r"""%reduce CREATE MIGRATION NodeName \
                    OptDeltaParents OptDeltaTarget \
        """
        if kids[4].val is None:
            lang = target = None
        else:
            lang, target = kids[4].val

        # currently we only support one valid language for migration target
        #
        if lang.val.lower() == 'eschema':
            target = self._parse_schema_decl(target)
        else:
            raise EdgeQLSyntaxError(f'unknown migration language: {lang.val}',
                                    context=lang.context)

        self.val = qlast.CreateDelta(
            name=kids[2].val,
            parents=kids[3].val,
            language=lang.val.lower(),
            target=target,
        )

    def reduce_CreateDelta_Commands(self, *kids):
        r"""%reduce CREATE MIGRATION NodeName \
                    OptDeltaParents LBRACE InnerDDLStmtBlock Semicolons \
                    RBRACE
        """
        self.val = qlast.CreateDelta(
            name=kids[2].val,
            parents=kids[3].val,
            commands=kids[5].val,
        )


#
# ALTER MIGRATION
#
commands_block(
    'AlterDelta',
    RenameStmt,
    opt=False
)


class AlterDeltaStmt(Nonterm):
    def reduce_AlterDelta(self, *kids):
        r"""%reduce ALTER MIGRATION NodeName \
                    AlterDeltaCommandsBlock \
        """
        self.val = qlast.AlterDelta(
            name=kids[2].val,
            commands=kids[3].val
        )


#
# DROP MIGRATION
#
class DropDeltaStmt(Nonterm):
    def reduce_DROP_MIGRATION_NodeName(self, *kids):
        self.val = qlast.DropDelta(
            name=kids[2].val,
        )


# COMMIT MIGRATION
class CommitDeltaStmt(Nonterm):
    def reduce_COMMIT_MIGRATION_NodeName(self, *kids):
        self.val = qlast.CommitDelta(
            name=kids[2].val,
        )


# GET MIGRATION
class GetDeltaStmt(Nonterm):
    def reduce_GET_MIGRATION_NodeName(self, *kids):
        self.val = qlast.GetDelta(
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
    def reduce_EXTENDING_ShortNodeName(self, *kids):
        self.val = [kids[1].val]

    def reduce_EXTENDING_LPAREN_ShortNodeNameList_RPAREN(self, *kids):
        self.val = kids[2].val


class OptShortExtending(Nonterm):
    def reduce_ShortExtending(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = []


class CreateRoleStmt(Nonterm):
    def reduce_CreateRoleStmt(self, *kids):
        r"""%reduce CREATE ROLE ShortNodeName OptShortExtending \
                                OptCreateCommandsBlock \
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
        self.val = qlast.AlterAddInherit(bases=kids[1].val,
                                         position=kids[2].val)

    def reduce_DROP_EXTENDING_ShortNodeNameList(self, *kids):
        self.val = qlast.AlterDropInherit(bases=kids[2].val)


commands_block(
    'AlterRole',
    RenameStmt,
    SetFieldStmt,
    SetAttributeValueStmt,
    DropAttributeValueStmt,
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
                    OptExtending OptCreateCommandsBlock"""
        self.val = qlast.CreateConstraint(
            name=kids[3].val,
            subject=kids[4].val,
            bases=kids[5].val,
            commands=kids[6].val,
        )

    def reduce_CreateConstraint_CreateFunctionArgs(self, *kids):
        r"""%reduce CREATE ABSTRACT CONSTRAINT NodeName CreateFunctionArgs \
                    OptOnExpr OptExtending OptCreateCommandsBlock"""
        self.val = qlast.CreateConstraint(
            name=kids[3].val,
            params=kids[4].val,
            subject=kids[5].val,
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


class OnExpr(Nonterm):
    def reduce_ON_LPAREN_Expr_RPAREN(self, *kids):
        self.val = kids[2].val


class OptOnExpr(Nonterm):
    def reduce_empty(self, *kids):
        self.val = None

    def reduce_OnExpr(self, *kids):
        self.val = kids[0].val


class OptDelegated(Nonterm):
    def reduce_DELEGATED(self, *kids):
        self.val = True

    def reduce_empty(self):
        self.val = False


class OptConcreteConstraintArgList(Nonterm):
    def reduce_LPAREN_OptPosCallArgList_RPAREN(self, *kids):
        self.val = kids[1].val

    def reduce_empty(self):
        self.val = []


class CreateConcreteConstraintStmt(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce CREATE OptDelegated CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr \
                    OptCreateCommandsBlock"""
        self.val = qlast.CreateConcreteConstraint(
            is_abstract=kids[1].val,
            name=kids[3].val,
            args=kids[4].val,
            subject=kids[5].val,
            commands=kids[6].val,
        )


commands_block(
    'AlterConcreteConstraint',
    RenameStmt,
    SetFieldStmt,
    SetAttributeValueStmt,
    DropAttributeValueStmt,
    AlterAbstract,
    opt=False
)


class AlterConcreteConstraintStmt(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce ALTER CONSTRAINT NodeName \
                    AlterConcreteConstraintCommandsBlock"""
        self.val = qlast.AlterConcreteConstraint(
            name=kids[2].val,
            commands=kids[3].val,
        )


class DropConcreteConstraintStmt(Nonterm):
    def reduce_DropConstraint(self, *kids):
        r"""%reduce DROP CONSTRAINT NodeName"""
        self.val = qlast.DropConcreteConstraint(
            name=kids[2].val
        )


#
# CREATE SCALAR TYPE
#

commands_block(
    'CreateScalarType',
    SetFieldStmt,
    SetAttributeValueStmt,
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
    SetAttributeValueStmt,
    DropAttributeValueStmt,
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
# CREATE ATTRIBUTE
#
class CreateAttributeStmt(Nonterm):
    def reduce_CreateAttribute(self, *kids):
        r"""%reduce CREATE ABSTRACT ATTRIBUTE NodeName OptExtending \
                    OptCreateCommandsBlock"""
        self.val = qlast.CreateAttribute(
            name=kids[3].val,
            bases=kids[4].val,
            commands=kids[5].val,
            inheritable=False,
        )

    def reduce_CreateInheritableAttribute(self, *kids):
        r"""%reduce CREATE ABSTRACT INHERITABLE ATTRIBUTE
                    NodeName OptExtending OptCreateCommandsBlock"""
        self.val = qlast.CreateAttribute(
            name=kids[4].val,
            bases=kids[5].val,
            commands=kids[6].val,
            inheritable=True,
        )


#
# DROP ATTRIBUTE
#
class DropAttributeStmt(Nonterm):
    def reduce_DropAttribute(self, *kids):
        r"""%reduce DROP ABSTRACT ATTRIBUTE NodeName"""
        self.val = qlast.DropAttribute(
            name=kids[3].val,
        )


#
# CREATE INDEX
#
class CreateIndexStmt(Nonterm):
    def reduce_CREATE_INDEX_NodeName_ON_Expr(self, *kids):
        self.val = qlast.CreateIndex(
            name=kids[2].val,
            expr=kids[4].val
        )


#
# DROP INDEX
#
class DropIndexStmt(Nonterm):
    def reduce_DROP_INDEX_NodeName(self, *kids):
        self.val = qlast.DropIndex(
            name=kids[2].val
        )


class AlterTargetStmt(Nonterm):
    def reduce_ALTER_TYPE_NodeNameList(self, *kids):
        self.val = qlast.AlterTarget(target=kids[2].val)


#
# CREATE PROPERTY
#
class CreatePropertyStmt(Nonterm):
    def reduce_CreateProperty(self, *kids):
        r"""%reduce CREATE ABSTRACT PROPERTY NodeName OptExtending \
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
    SetAttributeValueStmt,
    DropAttributeValueStmt,
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
    SetAttributeValueStmt,
    CreateConcreteConstraintStmt
)


class CreateConcretePropertyStmt(Nonterm):
    def reduce_CreateRegularProperty(self, *kids):
        """%reduce
            CREATE OptPtrQuals PROPERTY NodeName
            ARROW FullTypeExpr OptCreateConcretePropertyCommandsBlock
        """
        self.val = qlast.CreateConcreteProperty(
            name=kids[3].val,
            is_required=kids[1].val.required,
            cardinality=kids[1].val.cardinality,
            target=kids[5].val,
            commands=kids[6].val,
        )

    def reduce_CREATE_OptPtrQuals_PROPERTY_NodeName_ASSIGN_Expr(self, *kids):
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
            name='cardinality',
            value=qlast.Cardinality.ONE,
        )

    def reduce_SET_MULTI(self, *kids):
        self.val = qlast.SetSpecialField(
            name='cardinality',
            value=qlast.Cardinality.MANY,
        )


class SetRequiredStmt(Nonterm):

    def reduce_SET_REQUIRED(self, *kids):
        self.val = qlast.SetSpecialField(
            name='required',
            value=True,
        )

    def reduce_DROP_REQUIRED(self, *kids):
        self.val = qlast.SetSpecialField(
            name='required',
            value=False,
        )


commands_block(
    'AlterConcreteProperty',
    RenameStmt,
    SetFieldStmt,
    SetAttributeValueStmt,
    DropAttributeValueStmt,
    AlterTargetStmt,
    SetCardinalityStmt,
    SetRequiredStmt,
    CreateConcreteConstraintStmt,
    AlterConcreteConstraintStmt,
    DropConcreteConstraintStmt,
    opt=False
)


class AlterConcretePropertyStmt(Nonterm):
    def reduce_AlterProperty(self, *kids):
        r"""%reduce \
            ALTER PROPERTY NodeName \
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
            DROP PROPERTY NodeName \
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
    SetAttributeValueStmt,
    CreateConcreteConstraintStmt,
    CreateConcretePropertyStmt,
    CreateIndexStmt,
)


class CreateLinkStmt(Nonterm):
    def reduce_CreateLink(self, *kids):
        r"""%reduce \
            CREATE ABSTRACT LINK NodeName OptExtending \
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
    SetAttributeValueStmt,
    DropAttributeValueStmt,
    AlterExtending,
    CreateConcreteConstraintStmt,
    AlterConcreteConstraintStmt,
    DropConcreteConstraintStmt,
    CreateConcretePropertyStmt,
    AlterConcretePropertyStmt,
    DropConcretePropertyStmt,
    CreateIndexStmt,
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
# CREATE TYPE ... { CREATE LINK ... { ON TARGET DELETE ...
#
class OnTargetDeleteStmt(Nonterm):
    def reduce_ON_TARGET_DELETE_RESTRICT(self, *kids):
        self.val = qlast.OnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.RESTRICT)

    def reduce_ON_TARGET_DELETE_DELETE_SOURCE(self, *kids):
        self.val = qlast.OnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.DELETE_SOURCE)

    def reduce_ON_TARGET_DELETE_SET_EMPTY(self, *kids):
        self.val = qlast.OnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.SET_EMPTY)

    def reduce_ON_TARGET_DELETE_SET_DEFAULT(self, *kids):
        self.val = qlast.OnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.SET_DEFAULT)

    def reduce_ON_TARGET_DELETE_DEFERRED_RESTRICT(self, *kids):
        self.val = qlast.OnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.DEFERRED_RESTRICT)


#
# CREATE TYPE ... { CREATE LINK
#

commands_block(
    'CreateConcreteLink',
    SetFieldStmt,
    SetAttributeValueStmt,
    CreateConcreteConstraintStmt,
    CreateConcretePropertyStmt,
    OnTargetDeleteStmt,
)


class CreateConcreteLinkStmt(Nonterm):
    def reduce_CreateRegularLink(self, *kids):
        """%reduce
            CREATE OptPtrQuals LINK LinkName
            ARROW FullTypeExpr OptCreateConcreteLinkCommandsBlock
        """
        self.val = qlast.CreateConcreteLink(
            name=kids[3].val,
            is_required=kids[1].val.required,
            cardinality=kids[1].val.cardinality,
            target=kids[5].val,
            commands=kids[6].val
        )

    def reduce_CREATE_OptPtrQuals_LINK_NodeName_ASSIGN_Expr(self, *kids):
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
    SetAttributeValueStmt,
    DropAttributeValueStmt,
    SetCardinalityStmt,
    SetRequiredStmt,
    AlterTargetStmt,
    AlterExtending,
    CreateConcreteConstraintStmt,
    AlterConcreteConstraintStmt,
    DropConcreteConstraintStmt,
    CreateConcretePropertyStmt,
    AlterConcretePropertyStmt,
    DropConcretePropertyStmt,
    OnTargetDeleteStmt,
    opt=False
)


class AlterConcreteLinkStmt(Nonterm):
    def reduce_AlterLink(self, *kids):
        r"""%reduce \
            ALTER LINK LinkName AlterConcreteLinkCommandsBlock \
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
            DROP LINK LinkName OptDropConcreteLinkCommandsBlock \
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
    SetAttributeValueStmt,
    CreateConcretePropertyStmt,
    CreateConcreteLinkStmt,
    CreateIndexStmt
)


class CreateObjectTypeStmt(Nonterm):
    def reduce_CreateAbstractObjectTypeStmt(self, *kids):
        r"""%reduce \
            CREATE ABSTRACT TYPE NodeName \
            OptExtending OptCreateObjectTypeCommandsBlock \
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
            OptExtending OptCreateObjectTypeCommandsBlock \
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
    SetAttributeValueStmt,
    DropAttributeValueStmt,
    AlterExtending,
    CreateConcretePropertyStmt,
    AlterConcretePropertyStmt,
    DropConcretePropertyStmt,
    CreateConcreteLinkStmt,
    AlterConcreteLinkStmt,
    DropConcreteLinkStmt,
    CreateIndexStmt,
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
    SetAttributeValueStmt,
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
                    as_expr=True
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
    SetAttributeValueStmt,
    DropAttributeValueStmt,
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
# FUNCTIONS
#


class OptDefault(Nonterm):
    def reduce_empty(self):
        self.val = None

    def reduce_EQUALS_Expr(self, *kids):
        self.val = kids[1].val


class OptParameterKind(Nonterm):

    def reduce_empty(self):
        self.val = ft.ParameterKind.POSITIONAL

    def reduce_VARIADIC(self, kid):
        self.val = ft.ParameterKind.VARIADIC

    def reduce_NAMEDONLY(self, *kids):
        self.val = ft.ParameterKind.NAMED_ONLY


class FuncDeclArgName(Nonterm):

    def reduce_Identifier(self, dp):
        self.val = dp.val
        self.context = dp.context

    def reduce_DOLLAR_AnyIdentifier(self, dk, dp):
        raise EdgeQLSyntaxError(
            f"function parameters do not need a $ prefix, "
            f"rewrite as '{dp.val}'",
            context=dk.context)

    def reduce_DOLLAR_ICONST(self, dk, di):
        raise EdgeQLSyntaxError(
            f'numeric parameters are not supported',
            context=dk.context)


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

            if arg.kind is ft.ParameterKind.VARIADIC:
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

            elif arg.kind is ft.ParameterKind.NAMED_ONLY:
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

            if arg.kind is ft.ParameterKind.POSITIONAL:
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


def _parse_language(node):
    try:
        return qlast.Language(node.val.upper())
    except ValueError:
        raise EdgeQLSyntaxError(
            f'{node.val} is not a valid language',
            context=node.context) from None


class FromFunction(Nonterm):
    def reduce_FROM_Identifier_BaseStringConstant(self, *kids):
        lang = _parse_language(kids[1])
        code = kids[2].val.value
        self.val = qlast.FunctionCode(language=lang, code=code)

    def reduce_FROM_Identifier_FUNCTION_BaseStringConstant(self, *kids):
        lang = _parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in FROM FUNCTION clause',
                context=kids[1].context) from None

        self.val = qlast.FunctionCode(language=lang,
                                      from_name=kids[3].val.value)


#
# CREATE FUNCTION
#


class _ProcessFunctionBlockMixin:
    def _process_function_body(self, block):
        props = {}

        commands = []
        code = None
        for node in block.val:
            if isinstance(node, qlast.FunctionCode):
                if code is not None:
                    raise EdgeQLSyntaxError('more than one FROM clause',
                                            context=node.context)
                else:
                    code = node
            else:
                commands.append(node)

        if code is None:
            raise EdgeQLSyntaxError('FROM clause is missing',
                                    context=block.context)

        else:
            props['code'] = code

        if commands:
            props['commands'] = commands

        return props


commands_block(
    'CreateFunction',
    FromFunction,
    SetFieldStmt,
    SetAttributeValueStmt,
    opt=False
)


class OptTypeQualifier(Nonterm):
    def reduce_SET_OF(self, *kids):
        self.val = ft.TypeModifier.SET_OF

    def reduce_OPTIONAL(self, *kids):
        self.val = ft.TypeModifier.OPTIONAL

    def reduce_empty(self):
        self.val = ft.TypeModifier.SINGLETON


class CreateFunctionStmt(Nonterm, _ProcessFunctionBlockMixin):
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


class FunctionType(Nonterm):
    def reduce_FullTypeExpr(self, *kids):
        self.val = kids[0].val


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


class OperatorCode(Nonterm):

    def reduce_FROM_Identifier_OPERATOR_BaseStringConstant(self, *kids):
        lang = _parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in FROM OPERATOR clause',
                context=kids[1].context) from None

        sql_operator = kids[3].val.value
        m = re.match(r'([^(]+)(?:\((\w*(?:,\s*\w*)*)\))?', sql_operator)
        if not m:
            raise EdgeQLSyntaxError(
                f'invalid syntax for FROM OPERATOR clause',
                context=kids[3].context) from None

        sql_operator = (m.group(1),)
        if m.group(2):
            operands = tuple(op.strip() for op in m.group(2).split(','))
            sql_operator += operands

        self.val = qlast.OperatorCode(
            language=lang, from_operator=sql_operator)

    def reduce_FROM_Identifier_FUNCTION_BaseStringConstant(self, *kids):
        lang = _parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in FROM FUNCTION clause',
                context=kids[1].context) from None

        self.val = qlast.OperatorCode(language=lang,
                                      from_function=kids[3].val.value)

    def reduce_FROM_Identifier_BaseStringConstant(self, *kids):
        lang = _parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in FROM clause',
                context=kids[1].context) from None

        self.val = qlast.OperatorCode(language=lang,
                                      code=kids[2].val.value)

    def reduce_FROM_Identifier_EXPRESSION(self, *kids):
        lang = _parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in FROM clause',
                context=kids[1].context) from None

        self.val = qlast.OperatorCode(language=lang)


commands_block(
    'CreateOperator',
    SetFieldStmt,
    SetAttributeValueStmt,
    OperatorCode,
    opt=False
)


class CreateOperatorStmt(Nonterm):

    def reduce_CreateOperatorStmt(self, *kids):
        r"""%reduce
            CREATE OperatorKind OPERATOR NodeName CreateFunctionArgs
            ARROW OptTypeQualifier FunctionType
            CreateOperatorCommandsBlock
        """
        self.val = qlast.CreateOperator(
            kind=ft.OperatorKind(kids[1].val.upper()),
            name=kids[3].val,
            params=kids[4].val,
            returning_typemod=kids[6].val,
            returning=kids[7].val,
            **self._process_operator_body(kids[8])
        )

    def _process_operator_body(self, block):
        props = {}

        commands = []
        from_operator = None
        from_function = None
        from_expr = False
        code = None

        for node in block.val:
            if isinstance(node, qlast.OperatorCode):
                if node.from_function:
                    if from_function is not None:
                        raise EdgeQLSyntaxError(
                            'more than one FROM FUNCTION clause',
                            context=node.context)
                    from_function = node.from_function

                elif node.from_operator:
                    if from_operator is not None:
                        raise EdgeQLSyntaxError(
                            'more than one FROM OPERATOR clause',
                            context=node.context)
                    from_operator = node.from_operator

                elif node.code:
                    if code is not None:
                        raise EdgeQLSyntaxError(
                            'more than one FROM <code> clause',
                            context=node.context)
                    code = node.code

                else:
                    # FROM SQL EXPRESSION
                    from_expr = True
            else:
                commands.append(node)

        if (code is None and from_operator is None and from_function is None
                and not from_expr):
            raise EdgeQLSyntaxError(
                'CREATE OPERATOR requires at least one FROM clause',
                context=block.context)

        else:
            if from_expr and (from_operator or from_function or code):
                raise EdgeQLSyntaxError(
                    'FROM SQL EXPRESSION is mutually exclusive with other '
                    'FROM variants',
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
    SetAttributeValueStmt,
    DropAttributeValueStmt,
    opt=False
)


class AlterOperatorStmt(Nonterm):
    def reduce_AlterOperatorStmt(self, *kids):
        """%reduce
           ALTER OperatorKind OPERATOR NodeName CreateFunctionArgs
           AlterOperatorCommandsBlock
        """
        self.val = qlast.AlterOperator(
            kind=ft.OperatorKind(kids[1].val.upper()),
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
            kind=ft.OperatorKind(kids[1].val.upper()),
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

    def reduce_FROM_Identifier_FUNCTION_BaseStringConstant(self, *kids):
        lang = _parse_language(kids[1])
        if lang not in {qlast.Language.SQL, qlast.Language.EdgeQL}:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in FROM FUNCTION clause',
                context=kids[1].context) from None

        self.val = qlast.CastCode(language=lang,
                                  from_function=kids[3].val.value)

    def reduce_FROM_Identifier_BaseStringConstant(self, *kids):
        lang = _parse_language(kids[1])
        if lang not in {qlast.Language.SQL, qlast.Language.EdgeQL}:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in FROM clause',
                context=kids[1].context) from None

        self.val = qlast.CastCode(language=lang,
                                  code=kids[2].val.value)

    def reduce_FROM_Identifier_CAST(self, *kids):
        lang = _parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in FROM CAST clause',
                context=kids[1].context) from None

        self.val = qlast.CastCode(language=lang, from_cast=True)

    def reduce_FROM_Identifier_EXPRESSION(self, *kids):
        lang = _parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in FROM EXPRESSION clause',
                context=kids[1].context) from None

        self.val = qlast.CastCode(language=lang)


commands_block(
    'CreateCast',
    SetFieldStmt,
    SetAttributeValueStmt,
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
                            'more than one FROM FUNCTION clause',
                            context=node.context)
                    from_function = node.from_function

                elif node.code:
                    if code is not None:
                        raise EdgeQLSyntaxError(
                            'more than one FROM <code> clause',
                            context=node.context)
                    code = node.code

                elif node.from_cast:
                    # FROM SQL CAST

                    if from_cast:
                        raise EdgeQLSyntaxError(
                            'more than one FROM CAST clause',
                            context=node.context)

                    from_cast = True

                else:
                    # FROM SQL EXPRESSION

                    if from_expr:
                        raise EdgeQLSyntaxError(
                            'more than one FROM EXPRESSION clause',
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
                'CREATE CAST requires at least one FROM clause',
                context=block.context)

        else:
            if from_expr and (from_function or code or from_cast):
                raise EdgeQLSyntaxError(
                    'FROM SQL EXPRESSION is mutually exclusive with other '
                    'FROM variants',
                    context=block.context)

            if from_cast and (from_function or code or from_expr):
                raise EdgeQLSyntaxError(
                    'FROM SQL CAST is mutually exclusive with other '
                    'FROM variants',
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
    SetAttributeValueStmt,
    DropAttributeValueStmt,
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
