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
import types
import sys

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.common import parsing, context
from edgedb.lang.common.parsing import ListNonterm

from ...errors import EdgeQLSyntaxError

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

    def reduce_OptWithDDLStmt(self, *kids):
        self.val = kids[0].val


class DDLWithBlock(Nonterm):
    def reduce_WithBlock(self, *kids):
        self.val = kids[0].val
        if self.val.cardinality is not None:
            raise EdgeQLSyntaxError(
                'CARDINALITY specification is not allowed here',
                context=kids[0].context)


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
    def reduce_CreateActionStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterActionStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropActionStmt(self, *kids):
        self.val = kids[0].val

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

    def reduce_CreateEventStmt(self, *kids):
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


class InnerDDLStmtBlock(ListNonterm, element=InnerDDLStmt):
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


class Semicolons(Nonterm):
    # one or more semicolons
    #
    def reduce_SEMICOLON(self, tok):
        self.val = tok

    def reduce_Semicolons_SEMICOLON(self, *kids):
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


class SetFieldStmt(Nonterm):
    # field := <expr>
    def reduce_SET_NodeName_TURNSTILE_Expr(self, *kids):
        # if the expression is trivial (a literal or variable), it
        # should be treated as an eager expression
        #
        eager = isinstance(kids[3].val,
                           (qlast.Constant, qlast.Tuple,
                            qlast.Mapping))
        self.val = qlast.CreateAttributeValue(
            name=kids[1].val,
            value=kids[3].val,
            as_expr=not eager
        )


commands_block('Create', SetFieldStmt)


class DropFieldStmt(Nonterm):
    def reduce_DROP_ATTRIBUTE_NodeName(self, *kids):
        self.val = qlast.DropAttributeValue(
            name=kids[2].val,
        )


class RenameStmt(Nonterm):
    def reduce_RENAME_TO_NodeName(self, *kids):
        self.val = qlast.Rename(new_name=kids[2].val)


commands_block('Alter', RenameStmt, SetFieldStmt, DropFieldStmt, opt=False)


class Extending(Nonterm):
    def reduce_EXTENDING_NodeName(self, *kids):
        self.val = [kids[1].val]

    def reduce_EXTENDING_LPAREN_NodeNameList_RPAREN(self, *kids):
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
    def reduce_EXTENDING_NodeNameList_OptInheritPosition(self, *kids):
        self.val = qlast.AlterAddInherit(bases=kids[1].val,
                                         position=kids[2].val)

    def reduce_DROP_EXTENDING_NodeNameList(self, *kids):
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

    def reduce_TO_AnyIdentifier_SCONST(self, *kids):
        self.val = [kids[1], kids[2]]


#
# DELTAS
#

#
# CREATE MIGRATION
#
class CreateDeltaStmt(Nonterm):
    def _parse_schema_decl(self, tok):
        from edgedb.lang.common.exceptions import get_context
        from edgedb.lang.schema import parser

        ctx = tok.context

        try:
            node = parser.parse(tok.string)
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
                    OptDeltaParents LBRACE InnerDDLStmtBlock RBRACE \
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
# CREATE ACTION
#
class CreateActionStmt(Nonterm):
    def reduce_CREATE_ACTION_NodeName_OptCreateCommandsBlock(
            self, *kids):
        self.val = qlast.CreateAction(
            name=kids[2].val,
            commands=kids[3].val
        )


#
# ALTER ACTION
#
class AlterActionStmt(Nonterm):
    def reduce_ALTER_ACTION_NodeName_AlterCommandsBlock(self, *kids):
        self.val = qlast.AlterAction(
            name=kids[2].val,
            commands=kids[3].val
        )


#
# DROP ACTION
#
class DropActionStmt(Nonterm):
    def reduce_DROP_ACTION_NodeName(self, *kids):
        self.val = qlast.DropAction(
            name=kids[2].val,
        )


#
# CREATE POLICY
#
class CreateLocalPolicyStmt(Nonterm):
    def reduce_CREATE_POLICY_FOR_NodeName_TO_NodeNameList(
            self, *kids):
        self.val = qlast.CreateLocalPolicy(
            event=kids[3].val,
            actions=kids[5].val
        )


#
# ALTER POLICY
#
class AlterLocalPolicyStmt(Nonterm):
    def reduce_ALTER_POLICY_FOR_NodeName_TO_NodeNameList(
            self, *kids):
        self.val = qlast.AlterLocalPolicy(
            event=kids[3].val,
            actions=kids[5].val
        )


#
# DROP POLICY
#
class DropLocalPolicyStmt(Nonterm):
    def reduce_DROP_POLICY_FOR_NodeName(self, *kids):
        self.val = qlast.DropLocalPolicy(
            event=kids[3].val
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
            args=kids[4].val,
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
    def reduce_LPAREN_OptFuncArgList_RPAREN(self, *kids):
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
    DropFieldStmt,
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

commands_block('CreateScalarType', SetFieldStmt, CreateConcreteConstraintStmt)


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
    DropFieldStmt,
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
    def reduce_CreateAttributeWithType(self, *kids):
        r"""%reduce CREATE ABSTRACT ATTRIBUTE NodeName TypeName OptExtending \
                    OptCreateCommandsBlock"""
        self.val = qlast.CreateAttribute(
            name=kids[3].val,
            type=kids[4].val,
            bases=kids[5].val,
            commands=kids[6].val,
        )

    def reduce_CreateAttributeWithoutType(self, *kids):
        r"""%reduce CREATE ABSTRACT ATTRIBUTE NodeName Extending \
                    OptCreateCommandsBlock"""
        self.val = qlast.CreateAttribute(
            name=kids[3].val,
            bases=kids[4].val,
            commands=kids[5].val,
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
    def reduce_CREATE_INDEX_NodeName_TURNSTILE_Expr(self, *kids):
        self.val = qlast.CreateIndex(
            name=kids[2].val,
            expr=kids[3].val
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
        self.val = qlast.AlterTarget(targets=kids[2].val)


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
    DropFieldStmt,
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
    CreateConcreteConstraintStmt
)


class CreateConcretePropertyStmt(Nonterm):
    def reduce_CreateRegularRequiredProperty(self, *kids):
        r"""%reduce \
            CREATE REQUIRED PROPERTY NodeName \
            ARROW TypeName OptCreateConcretePropertyCommandsBlock \
        """
        self.val = qlast.CreateConcreteProperty(
            name=kids[3].val,
            is_required=True,
            target=kids[5].val,
            commands=kids[6].val
        )

    def reduce_CreateRegularProperty(self, *kids):
        r"""%reduce \
            CREATE PROPERTY NodeName \
            ARROW TypeName OptCreateConcretePropertyCommandsBlock \
        """
        self.val = qlast.CreateConcreteProperty(
            name=kids[2].val,
            is_required=False,
            target=kids[4].val,
            commands=kids[5].val
        )

    def reduce_CREATE_PROPERTY_NodeName_AS_Expr(self, *kids):
        self.val = qlast.CreateConcreteProperty(
            name=kids[2].val,
            target=kids[4].val
        )


#
# ALTER LINK ... { ALTER PROPERTY
#

commands_block(
    'AlterConcreteProperty',
    RenameStmt,
    SetFieldStmt,
    DropFieldStmt,
    AlterTargetStmt,
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
    CreateConcreteConstraintStmt,
    CreateConcretePropertyStmt,
    CreateIndexStmt,
    CreateLocalPolicyStmt
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
    DropFieldStmt,
    AlterExtending,
    CreateConcreteConstraintStmt,
    AlterConcreteConstraintStmt,
    DropConcreteConstraintStmt,
    CreateConcretePropertyStmt,
    AlterConcretePropertyStmt,
    DropConcretePropertyStmt,
    CreateIndexStmt,
    DropIndexStmt,
    CreateLocalPolicyStmt,
    AlterLocalPolicyStmt,
    DropLocalPolicyStmt,
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
    DropLocalPolicyStmt,
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
    CreateConcreteConstraintStmt,
    CreateConcretePropertyStmt,
    CreateLocalPolicyStmt
)


class CreateConcreteLinkStmt(Nonterm):
    def reduce_CreateRegularRequiredLink(self, *kids):
        r"""%reduce \
            CREATE REQUIRED LINK LinkName \
            ARROW TypeNameList OptCreateConcreteLinkCommandsBlock \
        """
        self.val = qlast.CreateConcreteLink(
            name=kids[3].val,
            is_required=True,
            targets=kids[5].val,
            commands=kids[6].val
        )

    def reduce_CreateRegularLink(self, *kids):
        r"""%reduce \
            CREATE LINK LinkName \
            ARROW TypeNameList OptCreateConcreteLinkCommandsBlock \
        """
        self.val = qlast.CreateConcreteLink(
            name=kids[2].val,
            is_required=False,
            targets=kids[4].val,
            commands=kids[5].val
        )

    def reduce_CREATE_LINK_NodeName_TURNSTILE_Expr(self, *kids):
        self.val = qlast.CreateConcreteLink(
            name=kids[2].val,
            targets=[kids[4].val]
        )


commands_block(
    'AlterConcreteLink',
    RenameStmt,
    SetFieldStmt,
    DropFieldStmt,
    AlterTargetStmt,
    AlterExtending,
    CreateConcreteConstraintStmt,
    AlterConcreteConstraintStmt,
    DropConcreteConstraintStmt,
    CreateConcretePropertyStmt,
    AlterConcretePropertyStmt,
    DropConcretePropertyStmt,
    CreateLocalPolicyStmt,
    AlterLocalPolicyStmt,
    DropLocalPolicyStmt,
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
    DropLocalPolicyStmt,
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
    DropFieldStmt,
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
    opt=False
)


class CreateViewStmt(Nonterm):
    def reduce_CreateViewShortStmt(self, *kids):
        r"""%reduce \
            CREATE VIEW NodeName TURNSTILE Expr \
        """
        self.val = qlast.CreateView(
            name=kids[2].val,
            commands=[
                qlast.CreateAttributeValue(
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
    DropFieldStmt,
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
# CREATE EVENT
#
class CreateEventStmt(Nonterm):
    def reduce_CreateEvent(self, *kids):
        r"""%reduce CREATE EVENT NodeName \
                    OptExtending OptCreateCommandsBlock \
        """
        self.val = qlast.CreateEvent(
            name=kids[2].val,
            bases=kids[3].val,
            commands=kids[4].val
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


class FuncDeclArg(Nonterm):
    def reduce_ArgQualifier_TypeName_OptDefault(self, *kids):
        self.val = qlast.FuncParam(
            name=None,
            qualifier=kids[0].val,
            type=kids[1].val,
            default=kids[2].val
        )

    def reduce_kwarg(self, *kids):
        r"""%reduce DOLLAR Identifier COLON \
                ArgQualifier TypeName OptDefault \
        """
        self.val = qlast.FuncParam(
            name=kids[1].val,
            qualifier=kids[3].val,
            type=kids[4].val,
            default=kids[5].val
        )

    def reduce_DOLLAR_Identifier_OptDefault(self, *kids):
        raise EdgeQLSyntaxError(
            f'missing type declaration for function parameter ${kids[1].val}',
            context=kids[0].context)


class FuncDeclArgList(ListNonterm, element=FuncDeclArg,
                      separator=tokens.T_COMMA):
    pass


class CreateFunctionArgs(Nonterm):
    def reduce_LPAREN_RPAREN(self, *kids):
        self.val = []

    def reduce_LPAREN_FuncDeclArgList_RPAREN(self, *kids):
        args = kids[1].val

        default_arg_seen = False
        variadic_arg_seen = False
        for arg in args:
            if arg.qualifier == qlast.SetQualifier.VARIADIC:
                if variadic_arg_seen:
                    raise EdgeQLSyntaxError('more than one variadic argument',
                                            context=arg.context)
                else:
                    variadic_arg_seen = True
            else:
                if variadic_arg_seen:
                    raise EdgeQLSyntaxError(
                        'non-variadic argument follows variadic argument',
                        context=arg.context)

            if arg.default is None:
                if (default_arg_seen and
                        not arg.qualifier == qlast.SetQualifier.VARIADIC):
                    raise EdgeQLSyntaxError(
                        'non-default argument follows default argument',
                        context=arg.context)
            else:
                default_arg_seen = True

        self.val = args


def _parse_language(node):
    try:
        return qlast.Language(node.val.upper())
    except ValueError as ex:
        raise EdgeQLSyntaxError(
            f'{node.val} is not a valid language',
            context=node.context) from None


class FromFunction(Nonterm):
    def reduce_FROM_Identifier_SCONST(self, *kids):
        lang = _parse_language(kids[1])
        self.val = qlast.FunctionCode(language=lang, code=kids[2].string)

    def reduce_FROM_Identifier_FUNCTION_SCONST(self, *kids):
        lang = _parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in FROM FUNCTION clause',
                context=kids[1].context) from None

        self.val = qlast.FunctionCode(language=lang, from_name=kids[3].string)


class FromAggregate(Nonterm):
    def reduce_FROM_Identifier_AGGREGATE_SCONST(self, *kids):
        lang = _parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in FROM AGGREGATE clause',
                context=kids[1].context) from None

        self.val = qlast.FunctionCode(language=lang, from_name=kids[3].string)


class InitialValue(Nonterm):
    def reduce_INITIAL_VALUE_Expr(self, *kids):
        val = kids[2].val

        # make sure that the initial value is a literal for now
        #
        if not isinstance(val, (qlast.Constant, qlast.EmptyCollection,
                                qlast.Array, qlast.Mapping)):
            raise EdgeQLSyntaxError("initial value must be a literal",
                                    context=val.context)

        self.val = qlast.FunctionIV(val=val)


#
# CREATE FUNCTION|AGGREGATE
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
            elif isinstance(node, qlast.FunctionIV):
                props['initial_value'] = node.val
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
    FromAggregate,
    SetFieldStmt,
    InitialValue,
    opt=False
)


class ArgQualifier(Nonterm):
    def reduce_SET_OF(self, *kids):
        self.val = qlast.SetQualifier.SET_OF

    def reduce_OPTIONAL(self, *kids):
        self.val = qlast.SetQualifier.OPTIONAL

    def reduce_VARIADIC(self, *kids):
        self.val = qlast.SetQualifier.VARIADIC

    def reduce_empty(self):
        self.val = qlast.SetQualifier.DEFAULT


class CreateFunctionStmt(Nonterm, _ProcessFunctionBlockMixin):
    def reduce_CreateFunction(self, *kids):
        r"""%reduce CREATE FUNCTION NodeName CreateFunctionArgs \
                ARROW ArgQualifier FunctionType \
                CreateFunctionCommandsBlock
        """
        if kids[5].val == qlast.SetQualifier.VARIADIC:
            raise EdgeQLSyntaxError(f'Unexpected {kids[5].val.value!r}',
                                    context=kids[5].context)

        self.val = qlast.CreateFunction(
            name=kids[2].val,
            args=kids[3].val,
            returning=kids[6].val,
            set_returning=kids[5].val,
            **self._process_function_body(kids[7])
        )


class DropFunctionStmt(Nonterm):
    def reduce_DropFunction(self, *kids):
        r"""%reduce DROP FUNCTION NodeName CreateFunctionArgs"""
        self.val = qlast.DropFunction(
            name=kids[2].val,
            args=kids[3].val)


class FunctionType(Nonterm):
    def reduce_TypeName(self, *kids):
        self.val = kids[0].val
