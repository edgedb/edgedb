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
import textwrap
import typing

from edb import errors
from edb.errors import EdgeQLSyntaxError

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb.common import context as pctx
from edb.common import parsing

from . import expressions
from . import commondl

from .precedence import *  # NOQA
from .tokens import *  # NOQA
from .commondl import *  # NOQA

from .sdl import *  # NOQA


Nonterm = expressions.Nonterm  # type: ignore[misc]
Semicolons = commondl.Semicolons  # type: ignore[misc]


sdl_nontem_helper = commondl.NewNontermHelper(__name__)
_new_nonterm = sdl_nontem_helper._new_nonterm


class DDLStmt(Nonterm):

    def reduce_DatabaseStmt(self, *kids):
        self.val = kids[0].val

    def reduce_RoleStmt(self, *kids):
        self.val = kids[0].val

    def reduce_ExtensionPackageStmt(self, *kids):
        self.val = kids[0].val

    def reduce_OptWithDDLStmt(self, *kids):
        self.val = kids[0].val

    def reduce_MigrationStmt(self, *kids):
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
    def reduce_InnerDDLStmt(self, *kids):
        self.val = kids[0].val


class InnerDDLStmt(Nonterm):

    def reduce_CreatePseudoTypeStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateScalarTypeStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterScalarTypeStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropScalarTypeStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateAnnotationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterAnnotationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropAnnotationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateObjectTypeStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterObjectTypeStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropObjectTypeStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateAliasStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterAliasStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropAliasStmt(self, *kids):
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

    def reduce_AlterFunctionStmt(self, *kids):
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

    def reduce_CreateGlobalStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterGlobalStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropGlobalStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropCastStmt(self, *kids):
        self.val = kids[0].val

    def reduce_ExtensionStmt(self, *kids):
        self.val = kids[0].val

    def reduce_FutureStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateIndexStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterIndexStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropIndexStmt(self, *kids):
        self.val = kids[0].val


class InnerDDLStmtBlock(parsing.ListNonterm, element=InnerDDLStmt,
                        separator=Semicolons):
    pass


class PointerName(Nonterm):
    def reduce_PtrNodeName(self, *kids):
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


class OptIfNotExists(Nonterm):
    def reduce_IF_NOT_EXISTS(self, *kids):
        self.val = True

    def reduce_empty(self, *kids):
        self.val = False


class ProductionTpl:
    def _passthrough(self, cmd):
        self.val = cmd.val

    def _singleton_list(self, cmd):
        self.val = [cmd.val]

    def _empty(self, *kids):
        self.val = []

    def _block(self, lbrace, cmdlist, sc2, rbrace):
        self.val = cmdlist.val

    def _block2(self, lbrace, sc1, cmdlist, sc2, rbrace):
        self.val = cmdlist.val


def commands_block(parent, *commands, opt=True, production_tpl=ProductionTpl):
    if parent is None:
        parent = ''

    clsdict = collections.OrderedDict()

    # Command := Command1 | Command2 ...
    #
    for command in commands:
        clsdict['reduce_{}'.format(command.__name__)] = \
            production_tpl._passthrough

    cmd = _new_nonterm(parent + 'Command', clsdict=clsdict)

    # CommandsList := Command [; Command ...]
    cmdlist = _new_nonterm(parent + 'CommandsList',
                           clsbases=(parsing.ListNonterm,),
                           clskwds=dict(element=cmd, separator=Semicolons))

    # CommandsBlock :=
    #
    #   { [ ; ] CommandsList ; }
    clsdict = collections.OrderedDict()
    clsdict['reduce_LBRACE_' + cmdlist.__name__ + '_OptSemicolons_RBRACE'] = \
        production_tpl._block
    clsdict['reduce_LBRACE_Semicolons_' + cmdlist.__name__ +
            '_OptSemicolons_RBRACE'] = \
        production_tpl._block2
    clsdict['reduce_LBRACE_OptSemicolons_RBRACE'] = \
        production_tpl._empty
    if not opt:
        #
        #   | Command
        clsdict['reduce_{}'.format(cmd.__name__)] = \
            production_tpl._singleton_list
    cmdblock = _new_nonterm(
        parent + 'CommandsBlock',
        clsdict=clsdict,
        clsbases=(Nonterm, production_tpl),
    )

    # OptCommandsBlock := CommandsBlock | <e>
    clsdict = collections.OrderedDict()
    clsdict['reduce_{}'.format(cmdblock.__name__)] = \
        production_tpl._passthrough
    clsdict['reduce_empty'] = production_tpl._empty

    if opt:
        _new_nonterm(
            'Opt' + parent + 'CommandsBlock',
            clsdict=clsdict,
            clsbases=(Nonterm, production_tpl),
        )


class NestedQLBlockStmt(Nonterm):

    def reduce_Stmt(self, *kids):
        self.val = kids[0].val

    def reduce_OptWithDDLStmt(self, *kids):
        self.val = kids[0].val

    def reduce_SetFieldStmt(self, *kids):
        self.val = kids[0].val


class NestedQLBlock(ProductionTpl):

    @property
    def allowed_fields(self) -> typing.FrozenSet[str]:
        raise NotImplementedError

    @property
    def result(self) -> typing.Any:
        raise NotImplementedError

    def _process_body(self, body):
        fields = []
        stmts = []
        uniq_check = set()
        for stmt in body:
            if isinstance(stmt, qlast.SetField):
                if stmt.name not in self.allowed_fields:
                    raise errors.InvalidSyntaxError(
                        f'unexpected field: {stmt.name!r}',
                        context=stmt.context,
                    )
                if stmt.name in uniq_check:
                    raise errors.InvalidSyntaxError(
                        f'duplicate `SET {stmt.name} := ...`',
                        context=stmt.context,
                    )
                uniq_check.add(stmt.name)
                fields.append(stmt)
            else:
                stmts.append(stmt)

        return fields, stmts

    def _get_text(self, body):
        # XXX: Workaround the rust lexer issue of returning
        # byte token offsets instead of character offsets.
        src_start = body.context.start
        src_end = body.context.end
        buffer = body.context.buffer.encode('utf-8')
        text = buffer[src_start:src_end].decode('utf-8').strip().strip('}{\n')
        return textwrap.dedent(text).strip('\n')

    def _block(self, lbrace, cmdlist, sc2, rbrace):
        # LBRACE NestedQLBlock OptSemicolons RBRACE
        fields, stmts = self._process_body(cmdlist.val)
        body = qlast.NestedQLBlock(commands=stmts)
        contexts = [lbrace.context, cmdlist.context]
        if sc2.context is not None:
            contexts.append(sc2.context)
        contexts.append(rbrace.context)
        body.context = pctx.merge_context(contexts)
        body.text = self._get_text(body)
        self.val = self.result(body=body, fields=fields)

    def _block2(self, lbrace, sc1, cmdlist, sc2, rbrace):
        # LBRACE Semicolons NestedQLBlock OptSemicolons RBRACE
        fields, stmts = self._process_body(cmdlist.val)
        body = qlast.NestedQLBlock(commands=stmts)
        body.context = pctx.merge_context(
            [sc1.context, cmdlist.context, sc2.context])
        body.text = self._get_text(body)
        self.val = self.result(body=body, fields=fields)

    def _empty(self, *kids):
        # LBRACE OptSemicolons RBRACE | <e>
        self.val = []
        body = qlast.NestedQLBlock(commands=[])
        if len(kids) > 1:
            body.context = kids[1].context
        if body.context is None:
            body.context = pctx.empty_context()
        body.text = self._get_text(body)
        self.val = self.result(body=body, fields=[])


def nested_ql_block(parent, *commands, opt=True, production_tpl):
    if not commands:
        commands = (NestedQLBlockStmt,)

    commands_block(parent, *commands, opt=opt, production_tpl=production_tpl)


class UsingStmt(Nonterm):

    def reduce_USING_ParenExpr(self, *kids):
        self.val = qlast.SetField(
            name='expr',
            value=kids[1].val,
            special_syntax=True,
        )

    def reduce_RESET_EXPRESSION(self, *kids):
        self.val = qlast.SetField(
            name='expr',
            value=None,
            special_syntax=True,
        )


class SetFieldStmt(Nonterm):
    # field := <expr>
    def reduce_SET_Identifier_ASSIGN_Expr(self, *kids):
        self.val = qlast.SetField(
            name=kids[1].val.lower(),
            value=kids[3].val,
        )


class ResetFieldStmt(Nonterm):
    # RESET field
    def reduce_RESET_IDENT(self, *kids):
        self.val = qlast.SetField(
            name=kids[1].val.lower(),
            value=None,
        )

    def reduce_RESET_DEFAULT(self, *kids):
        self.val = qlast.SetField(
            name='default',
            value=None,
        )


class CreateAnnotationValueStmt(Nonterm):
    def reduce_CREATE_ANNOTATION_NodeName_ASSIGN_Expr(self, *kids):
        self.val = qlast.CreateAnnotationValue(
            name=kids[2].val,
            value=kids[4].val,
        )


class AlterAnnotationValueStmt(Nonterm):
    def reduce_ALTER_ANNOTATION_NodeName_ASSIGN_Expr(self, *kids):
        self.val = qlast.AlterAnnotationValue(
            name=kids[2].val,
            value=kids[4].val,
        )

    def reduce_ALTER_ANNOTATION_NodeName_DROP_OWNED(self, *kids):
        self.val = qlast.AlterAnnotationValue(
            name=kids[2].val,
        )
        self.val.commands = [qlast.SetField(
            name='owned',
            value=qlast.BooleanConstant(value='false'),
            special_syntax=True,
        )]


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
    UsingStmt,
    SetFieldStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
)


commands_block(
    'Alter',
    UsingStmt,
    RenameStmt,
    SetFieldStmt,
    ResetFieldStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    DropAnnotationValueStmt,
    opt=False)


class AlterAbstract(Nonterm):

    def reduce_DROP_ABSTRACT(self, *kids):
        # TODO: Raise a DeprecationWarning once we have facility for that.
        self.val = qlast.SetField(
            name='abstract',
            value=qlast.BooleanConstant.from_python(False),
            special_syntax=True,
        )

    def reduce_SET_NOT_ABSTRACT(self, *kids):
        self.val = qlast.SetField(
            name='abstract',
            value=qlast.BooleanConstant.from_python(False),
            special_syntax=True,
        )

    def reduce_SET_ABSTRACT(self, *kids):
        self.val = qlast.SetField(
            name='abstract',
            value=qlast.BooleanConstant.from_python(True),
            special_syntax=True,
        )

    def reduce_RESET_ABSTRACT(self, *kids):
        self.val = qlast.SetField(
            name='abstract',
            value=None,
            special_syntax=True,
        )


class OptPosition(Nonterm):
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
    def reduce_EXTENDING_SimpleTypeNameList_OptPosition(self, *kids):
        self.val = qlast.AlterAddInherit(bases=kids[1].val,
                                         position=kids[2].val)

    def reduce_DROP_EXTENDING_SimpleTypeNameList(self, *kids):
        self.val = qlast.AlterDropInherit(bases=kids[2].val)

    def reduce_AlterAbstract(self, *kids):
        self.val = kids[0].val


class AlterExtending(Nonterm):
    def reduce_EXTENDING_TypeNameList_OptPosition(self, *kids):
        self.val = qlast.AlterAddInherit(bases=kids[1].val,
                                         position=kids[2].val)

    def reduce_DROP_EXTENDING_TypeNameList(self, *kids):
        self.val = qlast.AlterDropInherit(bases=kids[2].val)

    def reduce_AlterAbstract(self, *kids):
        self.val = kids[0].val


class AlterOwnedStmt(Nonterm):

    def reduce_DROP_OWNED(self, *kids):
        self.val = qlast.SetField(
            name='owned',
            value=qlast.BooleanConstant(value='false'),
            special_syntax=True,
        )

    def reduce_SET_OWNED(self, *kids):
        self.val = qlast.SetField(
            name='owned',
            value=qlast.BooleanConstant(value='true'),
            special_syntax=True,
        )


#
# DATABASE
#


class DatabaseName(Nonterm):

    def reduce_Identifier(self, kid):
        self.val = qlast.ObjectRef(
            module=None,
            name=kid.val
        )

    def reduce_ReservedKeyword(self, *kids):
        name = kids[0].val
        if (
            name[:2] == '__' and name[-2:] == '__' and
            name not in {'__edgedbsys__', '__edgedbtpl__'}
        ):
            # There are a few reserved keywords like __std__ and __subject__
            # that can be used in paths but are prohibited to be used
            # anywhere else. So just as the tokenizer prohibits using
            # __names__ in general, we enforce the rule here for the
            # few remaining reserved __keywords__.
            raise EdgeQLSyntaxError(
                "identifiers surrounded by double underscores are forbidden",
                context=kids[0].context)

        self.val = qlast.ObjectRef(
            module=None,
            name=name
        )


class DatabaseStmt(Nonterm):

    def reduce_CreateDatabaseStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropDatabaseStmt(self, *kids):
        self.val = kids[0].val


#
# CREATE DATABASE
#


commands_block(
    'CreateDatabase',
    SetFieldStmt,
)


class CreateDatabaseStmt(Nonterm):
    def reduce_CREATE_DATABASE_regular(self, *kids):
        """%reduce CREATE DATABASE DatabaseName OptCreateDatabaseCommandsBlock
        """
        self.val = qlast.CreateDatabase(name=kids[2].val, commands=kids[3].val)

    def reduce_CREATE_DATABASE_from_template(self, *kids):
        """%reduce
            CREATE DATABASE DatabaseName FROM AnyNodeName
            OptCreateDatabaseCommandsBlock
        """
        self.val = qlast.CreateDatabase(
            name=kids[2].val,
            commands=kids[5].val,
            template=kids[4].val,
        )


#
# DROP DATABASE
#
class DropDatabaseStmt(Nonterm):
    def reduce_DROP_DATABASE_DatabaseName(self, *kids):
        self.val = qlast.DropDatabase(name=kids[2].val)


#
# EXTENSION PACKAGE
#

class ExtensionPackageStmt(Nonterm):

    def reduce_CreateExtensionPackageStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropExtensionPackageStmt(self, *kids):
        self.val = kids[0].val


#
# CREATE EXTENSION PACKAGE
#
class ExtensionPackageBody(typing.NamedTuple):

    body: qlast.NestedQLBlock
    fields: typing.List[qlast.SetField]


class CreateExtensionPackageBodyBlock(NestedQLBlock):

    @property
    def allowed_fields(self) -> typing.FrozenSet[str]:
        return frozenset({'internal'})

    @property
    def result(self) -> typing.Any:
        return ExtensionPackageBody


nested_ql_block(
    'CreateExtensionPackage',
    production_tpl=CreateExtensionPackageBodyBlock,
)


class CreateExtensionPackageStmt(Nonterm):

    def reduce_CreateExtensionPackageStmt(self, *kids):
        r"""%reduce CREATE EXTENSIONPACKAGE ShortNodeName
                    ExtensionVersion
                    OptCreateExtensionPackageCommandsBlock
        """
        self.val = qlast.CreateExtensionPackage(
            name=kids[2].val,
            version=kids[3].val,
            body=kids[4].val.body,
            commands=kids[4].val.fields,
        )


#
# DROP EXTENSION PACKAGE
#
class DropExtensionPackageStmt(Nonterm):

    def reduce_DropExtensionPackageStmt(self, *kids):
        r"""%reduce DROP EXTENSIONPACKAGE ShortNodeName ExtensionVersion"""
        self.val = qlast.DropExtensionPackage(
            name=kids[2].val,
            version=kids[3].val,
        )


#
# EXTENSIONS
#


class ExtensionStmt(Nonterm):

    def reduce_CreateExtensionStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropExtensionStmt(self, *kids):
        self.val = kids[0].val


#
# CREATE EXTENSION
#


commands_block(
    'CreateExtension',
    SetFieldStmt,
)


class CreateExtensionStmt(Nonterm):

    def reduce_CreateExtensionStmt(self, *kids):
        r"""%reduce CREATE EXTENSION ShortNodeName OptExtensionVersion
                    OptCreateExtensionCommandsBlock
        """
        self.val = qlast.CreateExtension(
            name=kids[2].val,
            version=kids[3].val,
            commands=kids[4].val,
        )


#
# DROP EXTENSION
#
class DropExtensionStmt(Nonterm):

    def reduce_DropExtensionPackageStmt(self, *kids):
        r"""%reduce DROP EXTENSION ShortNodeName OptExtensionVersion"""
        self.val = qlast.DropExtension(
            name=kids[2].val,
            version=kids[3].val,
        )


#
# FUTURE
#


class FutureStmt(Nonterm):

    def reduce_CreateFutureStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropFutureStmt(self, *kids):
        self.val = kids[0].val


#
# CREATE FUTURE
#


class CreateFutureStmt(Nonterm):

    def reduce_CreateFutureStmt(self, *kids):
        r"""%reduce CREATE FUTURE ShortNodeName
        """
        self.val = qlast.CreateFuture(
            name=kids[2].val,
        )


#
# DROP FUTURE
#
class DropFutureStmt(Nonterm):

    def reduce_DropFutureStmt(self, *kids):
        r"""%reduce DROP FUTURE ShortNodeName"""
        self.val = qlast.DropFuture(
            name=kids[2].val,
        )


#
# ROLE
#

class RoleStmt(Nonterm):

    def reduce_CreateRoleStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterRoleStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropRoleStmt(self, *kids):
        self.val = kids[0].val


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


class OptSuperuser(Nonterm):

    def reduce_SUPERUSER(self, *kids):
        self.val = True

    def reduce_empty(self, *kids):
        self.val = False


class CreateRoleStmt(Nonterm):
    def reduce_CreateRoleStmt(self, *kids):
        r"""%reduce CREATE OptSuperuser ROLE ShortNodeName
                    OptShortExtending OptIfNotExists OptCreateRoleCommandsBlock
        """
        self.val = qlast.CreateRole(
            name=kids[3].val,
            bases=kids[4].val,
            create_if_not_exists=kids[5].val,
            commands=kids[6].val,
            superuser=kids[1].val,
        )


#
# ALTER ROLE
#
class AlterRoleExtending(Nonterm):
    def reduce_EXTENDING_ShortNodeNameList_OptPosition(self, *kids):
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
    ResetFieldStmt,
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
                    OptExceptExpr \
                    OptCreateCommandsBlock"""
        self.val = qlast.CreateConcreteConstraint(
            delegated=kids[1].val,
            name=kids[3].val,
            args=kids[4].val,
            subjectexpr=kids[5].val,
            except_expr=kids[6].val,
            commands=kids[7].val,
        )


class SetDelegatedStmt(Nonterm):

    def reduce_SET_DELEGATED(self, *kids):
        self.val = qlast.SetField(
            name='delegated',
            value=qlast.BooleanConstant.from_python(True),
            special_syntax=True,
        )

    def reduce_SET_NOT_DELEGATED(self, *kids):
        self.val = qlast.SetField(
            name='delegated',
            value=qlast.BooleanConstant.from_python(False),
            special_syntax=True,
        )

    def reduce_RESET_DELEGATED(self, *kids):
        self.val = qlast.SetField(
            name='delegated',
            value=None,
            special_syntax=True,
        )


commands_block(
    'AlterConcreteConstraint',
    SetFieldStmt,
    ResetFieldStmt,
    SetDelegatedStmt,
    AlterOwnedStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    DropAnnotationValueStmt,
    AlterAbstract,
    opt=False
)


class AlterConcreteConstraintStmt(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce ALTER CONSTRAINT NodeName
                    OptConcreteConstraintArgList OptOnExpr OptExceptExpr
                    AlterConcreteConstraintCommandsBlock"""
        self.val = qlast.AlterConcreteConstraint(
            name=kids[2].val,
            args=kids[3].val,
            subjectexpr=kids[4].val,
            except_expr=kids[5].val,
            commands=kids[6].val,
        )


class DropConcreteConstraintStmt(Nonterm):
    def reduce_DropConstraint(self, *kids):
        r"""%reduce DROP CONSTRAINT NodeName
                    OptConcreteConstraintArgList OptOnExpr OptExceptExpr"""
        self.val = qlast.DropConcreteConstraint(
            name=kids[2].val,
            args=kids[3].val,
            subjectexpr=kids[4].val,
            except_expr=kids[5].val,
        )


#
# CREATE PSEUDO TYPE
#

commands_block(
    'CreatePseudoType',
    SetFieldStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
)


class CreatePseudoTypeStmt(Nonterm):

    def reduce_CreatePseudoTypeStmt(self, *kids):
        r"""%reduce
            CREATE PSEUDO TYPE NodeName OptCreatePseudoTypeCommandsBlock
        """
        self.val = qlast.CreatePseudoType(
            name=kids[3].val,
            commands=kids[4].val,
        )


#
# CREATE SCALAR TYPE
#

commands_block(
    'CreateScalarType',
    SetFieldStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    CreateConcreteConstraintStmt)


class CreateScalarTypeStmt(Nonterm):
    def reduce_CreateAbstractScalarTypeStmt(self, *kids):
        r"""%reduce \
            CREATE ABSTRACT SCALAR TYPE NodeName \
            OptExtending OptCreateScalarTypeCommandsBlock \
        """
        self.val = qlast.CreateScalarType(
            name=kids[4].val,
            abstract=True,
            bases=kids[5].val,
            commands=kids[6].val
        )

    def reduce_CreateFinalScalarTypeStmt(self, *kids):
        r"""%reduce \
            CREATE FINAL SCALAR TYPE NodeName \
            OptExtending OptCreateScalarTypeCommandsBlock \
        """
        # Old dumps (1.0-beta.3 and earlier) specify FINAL for all
        # scalar types, despite it not doing anything and being
        # undocumented. So we need to support it in the syntax, and we
        # reject later it when not reading an old dump.
        self.val = qlast.CreateScalarType(
            name=kids[4].val,
            final=True,
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
    ResetFieldStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
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
commands_block(
    'CreateAnnotation',
    CreateAnnotationValueStmt,
)


class CreateAnnotationStmt(Nonterm):
    def reduce_CreateAnnotation(self, *kids):
        r"""%reduce CREATE ABSTRACT ANNOTATION NodeName \
                    OptCreateAnnotationCommandsBlock"""
        self.val = qlast.CreateAnnotation(
            name=kids[3].val,
            commands=kids[4].val,
            inheritable=False,
        )

    def reduce_CreateInheritableAnnotation(self, *kids):
        r"""%reduce CREATE ABSTRACT INHERITABLE ANNOTATION
                    NodeName OptCreateCommandsBlock"""
        self.val = qlast.CreateAnnotation(
            name=kids[4].val,
            commands=kids[5].val,
            inheritable=True,
        )


#
# ALTER ANNOTATION
#
commands_block(
    'AlterAnnotation',
    RenameStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    DropAnnotationValueStmt,
    opt=False,
)


class AlterAnnotationStmt(Nonterm):
    def reduce_AlterAnnotation(self, *kids):
        r"""%reduce ALTER ABSTRACT ANNOTATION NodeName \
                    AlterAnnotationCommandsBlock"""
        self.val = qlast.AlterAnnotation(
            name=kids[3].val,
            commands=kids[4].val
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


#
# CREATE INDEX
#
commands_block(
    'CreateIndex',
    UsingStmt,
    SetFieldStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
)


commands_block(
    'AlterIndex',
    UsingStmt,
    RenameStmt,
    SetFieldStmt,
    ResetFieldStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    DropAnnotationValueStmt,
    opt=False)


class CreateIndexStmt(
    Nonterm,
    commondl.ProcessIndexMixin,
):
    def reduce_CreateIndex(self, *kids):
        r"""%reduce CREATE ABSTRACT INDEX NodeName \
                    OptExtendingSimple OptCreateIndexCommandsBlock"""
        self.val = qlast.CreateIndex(
            name=kids[3].val,
            bases=kids[4].val,
            **self._process_sql_body(kids[5])
        )

    def reduce_CreateIndex_CreateFunctionArgs(self, *kids):
        r"""%reduce CREATE ABSTRACT INDEX NodeName IndexExtArgList \
                    OptExtendingSimple OptCreateIndexCommandsBlock"""
        bases = kids[5].val
        params, kwargs = self._process_params_or_kwargs(bases, kids[4].val)

        self.val = qlast.CreateIndex(
            name=kids[3].val,
            params=params,
            kwargs=kwargs,
            bases=bases,
            **self._process_sql_body(kids[6])
        )


#
# ALTER INDEX
#
class AlterIndexStmt(Nonterm, commondl.ProcessIndexMixin):
    def reduce_AlterIndex(self, *kids):
        r"""%reduce ALTER ABSTRACT INDEX NodeName \
                    AlterIndexCommandsBlock"""
        self.val = qlast.AlterIndex(
            name=kids[3].val,
            **self._process_sql_body(kids[4])
        )


#
# DROP INDEX
#
class DropIndexStmt(Nonterm):
    def reduce_DropIndex(self, *kids):
        r"""%reduce DROP ABSTRACT INDEX NodeName"""
        self.val = qlast.DropIndex(
            name=kids[3].val
        )


commands_block(
    'AlterConcreteIndex',
    SetFieldStmt,
    ResetFieldStmt,
    AlterOwnedStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    DropAnnotationValueStmt,
    opt=False)


#
# CREATE CONCRETE INDEX
#
class CreateConcreteIndexStmt(Nonterm, commondl.ProcessIndexMixin):
    def reduce_CREATE_INDEX_OnExpr_OptExceptExpr_OptCreateCommandsBlock(
            self, *kids):
        self.val = qlast.CreateConcreteIndex(
            name=qlast.ObjectRef(module='__', name='idx'),
            expr=kids[2].val,
            except_expr=kids[3].val,
            commands=kids[4].val,
        )

    def reduce_CreateConcreteIndex(self, *kids):
        r"""%reduce CREATE INDEX NodeName \
                    OptIndexExtArgList OnExpr OptExceptExpr \
                    OptCreateCommandsBlock \
        """
        kwargs = self._process_arguments(kids[3].val)
        self.val = qlast.CreateConcreteIndex(
            name=kids[2].val,
            kwargs=kwargs,
            expr=kids[4].val,
            except_expr=kids[5].val,
            commands=kids[6].val,
        )


#
# ALTER CONCRETE INDEX
#
class AlterConcreteIndexStmt(Nonterm):
    def reduce_AlterConcreteIndex(self, *kids):
        r"""%reduce ALTER INDEX OnExpr OptExceptExpr \
                    AlterConcreteIndexCommandsBlock \
        """
        self.val = qlast.AlterConcreteIndex(
            name=qlast.ObjectRef(module='__', name='idx'),
            expr=kids[2].val,
            except_expr=kids[3].val,
            commands=kids[4].val,
        )


commands_block(
    'DropConcreteIndex',
    SetFieldStmt,
    opt=True,
)


#
# DROP CONCRETE INDEX
#
class DropConcreteIndexStmt(Nonterm):
    def reduce_DropConcreteIndex(self, *kids):
        r"""%reduce DROP INDEX OnExpr OptExceptExpr \
                    OptDropConcreteIndexCommandsBlock \
        """
        self.val = qlast.DropConcreteIndex(
            name=qlast.ObjectRef(module='__', name='idx'),
            expr=kids[2].val,
            except_expr=kids[3].val,
            commands=kids[4].val,
        )


#
# CREATE PROPERTY
#
class CreatePropertyStmt(Nonterm):
    def reduce_CreateProperty(self, *kids):
        r"""%reduce CREATE ABSTRACT PROPERTY PtrNodeName OptExtendingSimple \
                    OptCreateCommandsBlock \
        """
        self.val = qlast.CreateProperty(
            name=kids[3].val,
            bases=kids[4].val,
            commands=kids[5].val,
            abstract=True,
        )


#
# ALTER PROPERTY
#

commands_block(
    'AlterProperty',
    RenameStmt,
    SetFieldStmt,
    ResetFieldStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    DropAnnotationValueStmt,
    opt=False
)


class AlterPropertyStmt(Nonterm):
    def reduce_AlterProperty(self, *kids):
        r"""%reduce \
            ALTER ABSTRACT PROPERTY PtrNodeName \
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
        r"""%reduce DROP ABSTRACT PROPERTY PtrNodeName"""
        self.val = qlast.DropProperty(
            name=kids[3].val
        )


#
# CREATE LINK ... { CREATE PROPERTY
#

class SetRequiredInCreateStmt(Nonterm):

    def reduce_SET_REQUIRED_OptAlterUsingClause(self, *kids):
        self.val = qlast.SetPointerOptionality(
            name='required',
            value=qlast.BooleanConstant.from_python(True),
            special_syntax=True,
            fill_expr=kids[2].val,
        )


commands_block(
    'CreateConcreteProperty',
    UsingStmt,
    SetFieldStmt,
    SetRequiredInCreateStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
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

    def reduce_CreateComputablePropertyWithUsing(self, *kids):
        """%reduce
            CREATE OptPtrQuals PROPERTY UnqualifiedPointerName
            OptCreateConcretePropertyCommandsBlock
        """
        cmds = kids[4].val
        target = None

        for cmd in cmds:
            if isinstance(cmd, qlast.SetField) and cmd.name == 'expr':
                if target is not None:
                    raise EdgeQLSyntaxError(
                        f'computed property with more than one expression',
                        context=kids[3].context)
                target = cmd.value

        if target is None:
            raise EdgeQLSyntaxError(
                f'computed property without expression',
                context=kids[3].context)

        self.val = qlast.CreateConcreteProperty(
            name=kids[3].val,
            is_required=kids[1].val.required,
            cardinality=kids[1].val.cardinality,
            target=target,
            commands=cmds,
        )


#
# ALTER LINK/PROPERTY
#


class OptAlterUsingClause(Nonterm):
    def reduce_USING_ParenExpr(self, *kids):
        self.val = kids[1].val

    def reduce_empty(self):
        self.val = None


class SetCardinalityStmt(Nonterm):

    def reduce_SET_SINGLE_OptAlterUsingClause(self, *kids):
        self.val = qlast.SetPointerCardinality(
            name='cardinality',
            value=qlast.StringConstant.from_python(
                qltypes.SchemaCardinality.One),
            special_syntax=True,
            conv_expr=kids[2].val,
        )

    def reduce_SET_MULTI(self, *kids):
        self.val = qlast.SetPointerCardinality(
            name='cardinality',
            value=qlast.StringConstant.from_python(
                qltypes.SchemaCardinality.Many),
            special_syntax=True,
        )

    def reduce_RESET_CARDINALITY_OptAlterUsingClause(self, *kids):
        self.val = qlast.SetPointerCardinality(
            name='cardinality',
            value=None,
            special_syntax=True,
            conv_expr=kids[2].val,
        )


class SetRequiredStmt(Nonterm):

    def reduce_SET_REQUIRED_OptAlterUsingClause(self, *kids):
        self.val = qlast.SetPointerOptionality(
            name='required',
            value=qlast.BooleanConstant.from_python(True),
            special_syntax=True,
            fill_expr=kids[2].val,
        )

    def reduce_SET_OPTIONAL(self, *kids):
        self.val = qlast.SetPointerOptionality(
            name='required',
            value=qlast.BooleanConstant.from_python(False),
            special_syntax=True,
        )

    def reduce_DROP_REQUIRED(self, *kids):
        # TODO: Raise a DeprecationWarning once we have facility for that.
        self.val = qlast.SetPointerOptionality(
            name='required',
            value=qlast.BooleanConstant.from_python(False),
            special_syntax=True,
        )

    def reduce_RESET_OPTIONALITY(self, *kids):
        self.val = qlast.SetPointerOptionality(
            name='required',
            value=None,
            special_syntax=True,
        )


class SetPointerTypeStmt(Nonterm):

    def reduce_SETTYPE_FullTypeExpr_OptAlterUsingClause(self, *kids):
        self.val = qlast.SetPointerType(
            value=kids[1].val,
            cast_expr=kids[2].val,
        )

    def reduce_RESET_TYPE(self, *kids):
        self.val = qlast.SetPointerType(
            value=None,
        )


commands_block(
    'AlterConcreteProperty',
    UsingStmt,
    RenameStmt,
    SetFieldStmt,
    ResetFieldStmt,
    AlterOwnedStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    DropAnnotationValueStmt,
    SetPointerTypeStmt,
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
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    CreateConcreteConstraintStmt,
    CreateConcretePropertyStmt,
    CreateConcreteIndexStmt,
)


class CreateLinkStmt(Nonterm):
    def reduce_CreateLink(self, *kids):
        r"""%reduce \
            CREATE ABSTRACT LINK PtrNodeName OptExtendingSimple \
            OptCreateLinkCommandsBlock \
        """
        self.val = qlast.CreateLink(
            name=kids[3].val,
            bases=kids[4].val,
            commands=kids[5].val,
            abstract=True,
        )


#
# ALTER LINK
#

commands_block(
    'AlterLink',
    RenameStmt,
    SetFieldStmt,
    ResetFieldStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    DropAnnotationValueStmt,
    AlterSimpleExtending,
    CreateConcreteConstraintStmt,
    AlterConcreteConstraintStmt,
    DropConcreteConstraintStmt,
    CreateConcretePropertyStmt,
    AlterConcretePropertyStmt,
    DropConcretePropertyStmt,
    CreateConcreteIndexStmt,
    AlterConcreteIndexStmt,
    DropConcreteIndexStmt,
    opt=False
)


class AlterLinkStmt(Nonterm):
    def reduce_AlterLink(self, *kids):
        r"""%reduce \
            ALTER ABSTRACT LINK PtrNodeName \
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
    DropConcreteIndexStmt,
)


class DropLinkStmt(Nonterm):
    def reduce_DropLink(self, *kids):
        r"""%reduce \
            DROP ABSTRACT LINK PtrNodeName \
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
    UsingStmt,
    SetFieldStmt,
    SetRequiredInCreateStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    CreateConcreteConstraintStmt,
    CreateConcretePropertyStmt,
    CreateConcreteIndexStmt,
    commondl.OnTargetDeleteStmt,
    commondl.OnSourceDeleteStmt,
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

    def reduce_CreateComputableLinkWithUsing(self, *kids):
        """%reduce
            CREATE OptPtrQuals LINK UnqualifiedPointerName
            OptCreateConcreteLinkCommandsBlock
        """
        cmds = kids[4].val
        target = None

        for cmd in cmds:
            if isinstance(cmd, qlast.SetField) and cmd.name == 'expr':
                if target is not None:
                    raise EdgeQLSyntaxError(
                        f'computed link with more than one expression',
                        context=kids[3].context)
                target = cmd.value

        if target is None:
            raise EdgeQLSyntaxError(
                f'computed link without expression',
                context=kids[3].context)

        self.val = qlast.CreateConcreteLink(
            name=kids[3].val,
            is_required=kids[1].val.required,
            cardinality=kids[1].val.cardinality,
            target=target,
            commands=cmds,
        )


class OnTargetDeleteResetStmt(Nonterm):
    def reduce_RESET_ON_TARGET_DELETE(self, *kids):
        self.val = qlast.OnTargetDelete(cascade=None)


class OnSourceDeleteResetStmt(Nonterm):
    def reduce_RESET_ON_SOURCE_DELETE(self, *kids):
        self.val = qlast.OnSourceDelete(cascade=None)


commands_block(
    'AlterConcreteLink',
    UsingStmt,
    RenameStmt,
    SetFieldStmt,
    ResetFieldStmt,
    AlterOwnedStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    DropAnnotationValueStmt,
    SetCardinalityStmt,
    SetRequiredStmt,
    SetPointerTypeStmt,
    AlterSimpleExtending,
    CreateConcreteConstraintStmt,
    AlterConcreteConstraintStmt,
    DropConcreteConstraintStmt,
    CreateConcretePropertyStmt,
    AlterConcretePropertyStmt,
    DropConcretePropertyStmt,
    CreateConcreteIndexStmt,
    AlterConcreteIndexStmt,
    DropConcreteIndexStmt,
    commondl.OnTargetDeleteStmt,
    commondl.OnSourceDeleteStmt,
    OnTargetDeleteResetStmt,
    OnSourceDeleteResetStmt,
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
    DropConcreteIndexStmt,
)


class DropConcreteLinkStmt(Nonterm):
    def reduce_DropLink(self, *kids):
        r"""%reduce \
            DROP LINK UnqualifiedPointerName \
            OptDropConcreteLinkCommandsBlock \
        """
        self.val = qlast.DropConcreteLink(
            name=kids[2].val,
            commands=kids[3].val
        )


#
# CREATE GLOBAL
#

commands_block(
    'CreateAccessPolicy',
    CreateAnnotationValueStmt,
    SetFieldStmt,
)


class CreateAccessPolicyStmt(Nonterm):
    def reduce_CreateAccessPolicy(self, *kids):
        """%reduce
            CREATE ACCESS POLICY UnqualifiedPointerName
            OptWhenBlock AccessPolicyAction AccessKindList
            OptUsingBlock
            OptCreateAccessPolicyCommandsBlock
        """
        self.val = qlast.CreateAccessPolicy(
            name=kids[3].val,
            condition=kids[4].val,
            action=kids[5].val,
            access_kinds=[y for x in kids[6].val for y in x],
            expr=kids[7].val,
            commands=kids[8].val,
        )


class AccessPermStmt(Nonterm):
    def reduce_AccessPolicyAction_AccessKindList(self, *kids):
        self.val = qlast.SetAccessPerms(
            action=kids[0].val,
            access_kinds=[y for x in kids[1].val for y in x],
        )


class AccessUsingStmt(Nonterm):
    def reduce_USING_ParenExpr(self, *kids):
        self.val = qlast.SetField(
            name='expr',
            value=kids[1].val,
            special_syntax=True,
        )

    def reduce_RESET_EXPRESSION(self, *kids):
        self.val = qlast.SetField(
            name='expr',
            value=None,
            special_syntax=True,
        )


class AccessWhenStmt(Nonterm):

    def reduce_WHEN_ParenExpr(self, *kids):
        self.val = qlast.SetField(
            name='condition',
            value=kids[1].val,
            special_syntax=True,
        )

    def reduce_RESET_WHEN(self, *kids):
        self.val = qlast.SetField(
            name='condition',
            value=None,
            special_syntax=True,
        )


commands_block(
    'AlterAccessPolicy',
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    DropAnnotationValueStmt,
    RenameStmt,
    AccessPermStmt,
    AccessUsingStmt,
    AccessWhenStmt,
    SetFieldStmt,
    ResetFieldStmt,
    opt=False
)


class AlterAccessPolicyStmt(Nonterm):
    def reduce_AlterAccessPolicy(self, *kids):
        r"""%reduce \
            ALTER ACCESS POLICY UnqualifiedPointerName \
            AlterAccessPolicyCommandsBlock \
        """
        self.val = qlast.AlterAccessPolicy(
            name=kids[3].val,
            commands=kids[4].val,
        )


class DropAccessPolicyStmt(Nonterm):
    def reduce_DropAccessPolicy(self, *kids):
        r"""%reduce DROP ACCESS POLICY UnqualifiedPointerName"""
        self.val = qlast.DropAccessPolicy(
            name=kids[3].val
        )


#
# CREATE TYPE
#

commands_block(
    'CreateObjectType',
    SetFieldStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    CreateConcretePropertyStmt,
    AlterConcretePropertyStmt,
    CreateConcreteLinkStmt,
    AlterConcreteLinkStmt,
    CreateConcreteConstraintStmt,
    AlterConcreteConstraintStmt,
    CreateConcreteIndexStmt,
    AlterConcreteIndexStmt,
    CreateAccessPolicyStmt,
    AlterAccessPolicyStmt,
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
            abstract=True,
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
            abstract=False,
            commands=kids[4].val,
        )


#
# ALTER TYPE
#

commands_block(
    'AlterObjectType',
    RenameStmt,
    SetFieldStmt,
    ResetFieldStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    DropAnnotationValueStmt,
    AlterSimpleExtending,
    CreateConcretePropertyStmt,
    AlterConcretePropertyStmt,
    DropConcretePropertyStmt,
    CreateConcreteLinkStmt,
    AlterConcreteLinkStmt,
    DropConcreteLinkStmt,
    CreateConcreteConstraintStmt,
    AlterConcreteConstraintStmt,
    DropConcreteConstraintStmt,
    CreateConcreteIndexStmt,
    AlterConcreteIndexStmt,
    DropConcreteIndexStmt,
    CreateAccessPolicyStmt,
    AlterAccessPolicyStmt,
    DropAccessPolicyStmt,
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
    DropConcreteIndexStmt
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
# CREATE ALIAS
#

commands_block(
    'CreateAlias',
    UsingStmt,
    SetFieldStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    opt=False
)


class CreateAliasStmt(Nonterm):
    def reduce_CreateAliasShortStmt(self, *kids):
        r"""%reduce
            CREATE ALIAS NodeName ASSIGN Expr
        """
        self.val = qlast.CreateAlias(
            name=kids[2].val,
            commands=[
                qlast.SetField(
                    name='expr',
                    value=kids[4].val,
                    special_syntax=True,
                )
            ]
        )

    def reduce_CreateAliasRegularStmt(self, *kids):
        r"""%reduce
            CREATE ALIAS NodeName
            CreateAliasCommandsBlock
        """
        self.val = qlast.CreateAlias(
            name=kids[2].val,
            commands=kids[3].val,
        )


#
# ALTER ALIAS
#

commands_block(
    'AlterAlias',
    UsingStmt,
    RenameStmt,
    SetFieldStmt,
    ResetFieldStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    DropAnnotationValueStmt,
    opt=False
)


class AlterAliasStmt(Nonterm):
    def reduce_AlterAliasStmt(self, *kids):
        r"""%reduce
            ALTER ALIAS NodeName
            AlterAliasCommandsBlock
        """
        self.val = qlast.AlterAlias(
            name=kids[2].val,
            commands=kids[3].val
        )


#
# DROP ALIAS
#

class DropAliasStmt(Nonterm):
    def reduce_DropAlias(self, *kids):
        r"""%reduce
            DROP ALIAS NodeName
        """
        self.val = qlast.DropAlias(
            name=kids[2].val,
        )


#
# CREATE MODULE
#
class CreateModuleStmt(Nonterm):
    def reduce_CREATE_MODULE_ModuleName_OptIfNotExists_OptCreateCommandsBlock(
            self, *kids):
        self.val = qlast.CreateModule(
            name=qlast.ObjectRef(module=None, name='.'.join(kids[2].val)),
            create_if_not_exists=kids[3].val,
            commands=kids[4].val
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
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
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
# ALTER FUNCTION
#

commands_block(
    'AlterFunction',
    commondl.FromFunction,
    SetFieldStmt,
    ResetFieldStmt,
    RenameStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    DropAnnotationValueStmt,
    opt=False
)


class AlterFunctionStmt(Nonterm, commondl.ProcessFunctionBlockMixin):
    def reduce_AlterFunctionStmt(self, *kids):
        """%reduce
           ALTER FUNCTION NodeName CreateFunctionArgs
           AlterFunctionCommandsBlock
        """
        self.val = qlast.AlterFunction(
            name=kids[2].val,
            params=kids[3].val,
            **self._process_function_body(kids[4], optional_using=True)
        )


#
# CREATE OPERATOR
#

class OperatorKind(Nonterm):

    def reduce_INFIX(self, *kids):
        self.val = qltypes.OperatorKind.Infix

    def reduce_POSTFIX(self, *kids):
        self.val = qltypes.OperatorKind.Postfix

    def reduce_PREFIX(self, *kids):
        self.val = qltypes.OperatorKind.Prefix

    def reduce_TERNARY(self, *kids):
        self.val = qltypes.OperatorKind.Ternary


SQL_OP_RE = r"([^(]+)(?:\(([\w\.]*(?:,\s*[\w\.]*)*)\))?"


class OperatorCode(Nonterm):

    def reduce_USING_Identifier_OPERATOR_BaseStringConstant(self, *kids):
        lang = commondl._parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in USING OPERATOR clause',
                context=kids[1].context) from None

        m = re.match(SQL_OP_RE, kids[3].val.value)
        if not m:
            raise EdgeQLSyntaxError(
                f'invalid syntax for USING OPERATOR clause',
                context=kids[3].context) from None

        sql_operator = (m.group(1),)
        if m.group(2):
            sql_operator += tuple(op.strip() for op in m.group(2).split(","))

        self.val = qlast.OperatorCode(
            language=lang, from_operator=sql_operator)

    def reduce_USING_Identifier_FUNCTION_BaseStringConstant(self, *kids):
        lang = commondl._parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in USING FUNCTION clause',
                context=kids[1].context) from None

        m = re.match(SQL_OP_RE, kids[3].val.value)
        if not m:
            raise EdgeQLSyntaxError(
                f'invalid syntax for USING FUNCTION clause',
                context=kids[3].context) from None

        sql_function = (m.group(1),)
        if m.group(2):
            sql_function += tuple(op.strip() for op in m.group(2).split(','))

        self.val = qlast.OperatorCode(
            language=lang, from_function=sql_function)

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
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
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
            kind=kids[1].val,
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
            kind=kids[2].val,
            name=kids[4].val,
            params=kids[5].val,
            returning_typemod=kids[7].val,
            returning=kids[8].val,
            abstract=True,
            **self._process_operator_body(kids[9], abstract=True)
        )

    def _process_operator_body(self, block, abstract: bool=False):
        props: typing.Dict[str, typing.Any] = {}

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
    ResetFieldStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
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
            kind=kids[1].val,
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
            kind=kids[1].val,
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
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
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
    ResetFieldStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
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

#
# CREATE GLOBAL
#


commands_block(
    'CreateGlobal',
    UsingStmt,
    SetFieldStmt,
    CreateAnnotationValueStmt,
)


class CreateGlobalStmt(Nonterm):
    def reduce_CreateRegularGlobal(self, *kids):
        """%reduce
            CREATE OptPtrQuals GLOBAL NodeName
            ARROW FullTypeExpr
            OptCreateGlobalCommandsBlock
        """
        self.val = qlast.CreateGlobal(
            name=kids[3].val,
            is_required=kids[1].val.required,
            cardinality=kids[1].val.cardinality,
            target=kids[5].val,
            commands=kids[6].val,
        )

    def reduce_CreateComputableGlobal(self, *kids):
        """%reduce
            CREATE OptPtrQuals GLOBAL NodeName ASSIGN Expr
        """
        self.val = qlast.CreateGlobal(
            name=kids[3].val,
            is_required=kids[1].val.required,
            cardinality=kids[1].val.cardinality,
            target=kids[5].val,
        )

    def reduce_CreateComputableGlobalWithUsing(self, *kids):
        """%reduce
            CREATE OptPtrQuals GLOBAL NodeName
            OptCreateConcretePropertyCommandsBlock
        """
        cmds = kids[4].val
        target = None

        for cmd in cmds:
            if isinstance(cmd, qlast.SetField) and cmd.name == 'expr':
                if target is not None:
                    raise EdgeQLSyntaxError(
                        f'computed global with more than one expression',
                        context=kids[3].context)
                target = cmd.value

        if target is None:
            raise EdgeQLSyntaxError(
                f'computed global without expression',
                context=kids[3].context)

        self.val = qlast.CreateGlobal(
            name=kids[3].val,
            is_required=kids[1].val.required,
            cardinality=kids[1].val.cardinality,
            target=target,
            commands=cmds,
        )


class SetGlobalTypeStmt(Nonterm):

    def reduce_SETTYPE_FullTypeExpr_OptAlterUsingClause(self, *kids):
        self.val = qlast.SetGlobalType(
            value=kids[1].val,
            cast_expr=kids[2].val,
        )

    def reduce_SETTYPE_FullTypeExpr_RESET_TO_DEFAULT(self, *kids):
        self.val = qlast.SetGlobalType(
            value=kids[1].val,
            reset_value=True,
        )

    def reduce_RESET_TYPE(self, *kids):
        self.val = qlast.SetGlobalType(
            value=None,
        )


commands_block(
    'AlterGlobal',
    UsingStmt,
    RenameStmt,
    SetFieldStmt,
    ResetFieldStmt,
    CreateAnnotationValueStmt,
    AlterAnnotationValueStmt,
    DropAnnotationValueStmt,
    SetGlobalTypeStmt,
    SetCardinalityStmt,
    SetRequiredStmt,
    opt=False
)


class AlterGlobalStmt(Nonterm):
    def reduce_AlterGlobal(self, *kids):
        r"""%reduce \
            ALTER GLOBAL NodeName \
            AlterGlobalCommandsBlock \
        """
        self.val = qlast.AlterGlobal(
            name=kids[2].val,
            commands=kids[3].val
        )


class DropGlobalStmt(Nonterm):
    def reduce_DropGlobal(self, *kids):
        r"""%reduce DROP GLOBAL NodeName"""
        self.val = qlast.DropGlobal(
            name=kids[2].val
        )

#
# MIGRATIONS
#


class MigrationStmt(Nonterm):

    def reduce_CreateMigrationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterMigrationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterCurrentMigrationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_StartMigrationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AbortMigrationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_PopulateMigrationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CommitMigrationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropMigrationStmt(self, *kids):
        self.val = kids[0].val

    def reduce_ResetSchemaStmt(self, *kids):
        self.val = kids[0].val


class MigrationBody(typing.NamedTuple):

    body: qlast.NestedQLBlock
    fields: typing.List[qlast.SetField]


class CreateMigrationBodyBlock(NestedQLBlock):

    @property
    def allowed_fields(self) -> typing.FrozenSet[str]:
        return frozenset({'message', 'generated_by'})

    @property
    def result(self) -> typing.Any:
        return MigrationBody


nested_ql_block(
    'CreateMigration',
    production_tpl=CreateMigrationBodyBlock,
)


class MigrationNameAndParent(typing.NamedTuple):

    name: typing.Optional[qlast.ObjectRef]
    parent: typing.Optional[qlast.ObjectRef]


class OptMigrationNameParentName(Nonterm):

    def reduce_ShortNodeName_ONTO_ShortNodeName(self, *kids):
        self.val = MigrationNameAndParent(
            name=kids[0].val,
            parent=kids[2].val,
        )

    def reduce_ShortNodeName(self, *kids):
        self.val = MigrationNameAndParent(
            name=kids[0].val,
            parent=None,
        )

    def reduce_empty(self):
        self.val = MigrationNameAndParent(
            name=None,
            parent=None,
        )


class CreateMigrationStmt(Nonterm):

    def reduce_CreateMigration(self, *kids):
        r"""%reduce
            CREATE MIGRATION OptMigrationNameParentName
            OptCreateMigrationCommandsBlock
        """
        self.val = qlast.CreateMigration(
            name=kids[2].val.name,
            parent=kids[2].val.parent,
            body=kids[3].val.body,
            commands=kids[3].val.fields,
        )

    def reduce_CreateAppliedMigration(self, *kids):
        r"""%reduce
            CREATE APPLIED MIGRATION OptMigrationNameParentName
            OptCreateMigrationCommandsBlock
        """
        self.val = qlast.CreateMigration(
            name=kids[3].val.name,
            parent=kids[3].val.parent,
            body=kids[4].val.body,
            metadata_only=True,
            commands=kids[4].val.fields,
        )


class StartMigrationStmt(Nonterm):

    def reduce_StartMigration(self, *kids):
        r"""%reduce START MIGRATION TO SDLCommandBlock"""

        declarations = kids[3].val
        commondl._validate_declarations(declarations)
        self.val = qlast.StartMigration(
            target=qlast.Schema(declarations=declarations),
        )

    def reduce_StartMigrationToCommitted(self, *kids):
        r"""%reduce START MIGRATION TO COMMITTED SCHEMA"""
        self.val = qlast.StartMigration(
            target=qlast.CommittedSchema()
        )

    def reduce_StartMigrationRewrite(self, *kids):
        r"""%reduce START MIGRATION REWRITE"""
        self.val = qlast.StartMigrationRewrite()


class PopulateMigrationStmt(Nonterm):

    def reduce_POPULATE_MIGRATION(self, *kids):
        self.val = qlast.PopulateMigration()


class AlterCurrentMigrationStmt(Nonterm):

    def reduce_ALTER_CURRENT_MIGRATION_REJECT_PROPOSED(self, *kids):
        self.val = qlast.AlterCurrentMigrationRejectProposed()


class AbortMigrationStmt(Nonterm):

    def reduce_ABORT_MIGRATION(self, *kids):
        self.val = qlast.AbortMigration()

    def reduce_ABORT_MIGRATION_REWRITE(self, *kids):
        self.val = qlast.AbortMigrationRewrite()


class CommitMigrationStmt(Nonterm):

    def reduce_COMMIT_MIGRATION(self, *kids):
        self.val = qlast.CommitMigration()

    def reduce_COMMIT_MIGRATION_REWRITE(self, *kids):
        self.val = qlast.CommitMigrationRewrite()


commands_block(
    'AlterMigration',
    SetFieldStmt,
    ResetFieldStmt,
    opt=False,
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


class DropMigrationStmt(Nonterm):
    def reduce_DROP_MIGRATION_NodeName(self, *kids):
        self.val = qlast.DropMigration(
            name=kids[2].val,
        )


class ResetSchemaStmt(Nonterm):
    def reduce_ResetSchemaTo(self, *kids):
        r"""%reduce RESET SCHEMA TO NodeName"""
        self.val = qlast.ResetSchema(
            target=kids[3].val,
        )
