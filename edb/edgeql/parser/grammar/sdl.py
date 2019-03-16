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


import sys
import types

from edb.errors import EdgeQLSyntaxError

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.eschema import ast as esast

from edb.common import parsing

from .expressions import Nonterm
from . import tokens

from .precedence import *  # NOQA
from .tokens import *  # NOQA
from .expressions import *  # NOQA


ListNonterm = parsing.ListNonterm


def _process_commands(block):
    props = {}

    attributes = []
    fields = []
    properties = []
    links = []
    constraints = []
    indexes = []
    on_target_delete = None
    code = None
    language = qlast.Language.SQL
    from_expr = False
    from_function = None

    for node in block:
        if isinstance(node, esast.FunctionCode):
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
                language = node.language

            else:
                # FROM SQL EXPRESSION
                from_expr = True
        elif isinstance(node, esast.Attribute):
            attributes.append(node)
        elif isinstance(node, esast.Field):
            fields.append(node)
        elif isinstance(node, esast.Property):
            properties.append(node)
        elif isinstance(node, esast.Link):
            links.append(node)
        elif isinstance(node, esast.Constraint):
            constraints.append(node)
        elif isinstance(node, esast.Index):
            indexes.append(node)
        elif isinstance(node, esast.OnTargetDelete):
            if on_target_delete:
                raise EdgeQLSyntaxError(
                    f"more than one 'on target delete' specification",
                    context=node.context)
            else:
                on_target_delete = node

    if from_expr or from_function or code:
        props['function_code'] = esast.FunctionCode(
            language=language,
            from_function=from_function,
            from_expr=from_expr,
            code=code,
        )

    if attributes:
        props['attributes'] = attributes
    if fields:
        props['fields'] = fields
    if properties:
        props['properties'] = properties
    if links:
        props['links'] = links
    if constraints:
        props['constraints'] = constraints
    if indexes:
        props['indexes'] = indexes
    if on_target_delete:
        props['on_target_delete'] = on_target_delete

    return props


class SDLDocument(Nonterm):
    "%start"

    def reduce_OptSemicolons_EOF(self, *kids):
        self.val = esast.Schema(declarations=[])

    def reduce_statement_without_semicolons(self, *kids):
        r"""%reduce \
            OptSemicolons SDLShortStatement \
            EOF
        """
        self.val = esast.Schema(declarations=[kids[1].val])

    def reduce_statements_without_optional_trailing_semicolons(self, *kids):
        r"""%reduce \
            OptSemicolons SDLStatements \
            OptSemicolons SDLShortStatement \
            EOF
        """
        self.val = esast.Schema(declarations=kids[1].val + [kids[3].val])

    def reduce_OptSemicolons_SDLStatements_EOF(self, *kids):
        self.val = esast.Schema(declarations=kids[1].val)

    def reduce_OptSemicolons_SDLStatements_Semicolons_EOF(self, *kids):
        self.val = esast.Schema(declarations=kids[1].val)


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


# # top-level SDL statements
class SDLStatement(Nonterm):
    def reduce_SDLBlockStatement(self, *kids):
        self.val = kids[0].val

    def reduce_SDLShortStatement_SEMICOLON(self, *kids):
        self.val = kids[0].val


# These statements have a block
class SDLBlockStatement(Nonterm):
    def reduce_ScalarTypeDeclaration(self, *kids):
        self.val = kids[0].val

    def reduce_AttributeDeclaration(self, *kids):
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
        self.val = esast.Import(modules=kids[1].val)

    def reduce_IMPORT_LPAREN_ImportModuleList_RPAREN(self, *kids):
        self.val = esast.Import(modules=kids[2].val)

    def reduce_ScalarTypeDeclarationShort(self, *kids):
        self.val = kids[0].val

    def reduce_AttributeDeclarationShort(self, *kids):
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

    def reduce_Index(self, *kids):
        self.val = kids[0].val


class SDLStatements(ListNonterm, element=SDLStatement,
                    separator=OptSemicolons):
    pass


class DotName(Nonterm):
    def reduce_ModuleName(self, *kids):
        self.val = '.'.join(part for part in kids[0].val)


class ImportModule(Nonterm):
    def reduce_DotName(self, *kids):
        self.val = esast.ImportModule(module=kids[0].val)

    def reduce_DotName_AS_AnyIdentifier(self, *kids):
        self.val = esast.ImportModule(module=kids[0].val,
                                      alias=kids[2].val)


class ImportModuleList(ListNonterm, element=ImportModule,
                       separator=tokens.T_COMMA):
    pass


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


def commands_block(parent, *commands, opt=True):
    if parent is None:
        parent = ''

    # Command := Command1 | Command2 ...
    #
    # All the "short" commands, ones that need a ";" are gathered as
    # CommandShort.
    #
    # All the "block" commands, ones that have a "{...}" and don't
    # need a ";" are gathered as CommandBlock.
    clsdict_b = {}
    clsdict_s = {}

    for command in commands:
        if command.__name__.endswith('Block'):
            clsdict_b[f'reduce_{command.__name__}'] = \
                ProductionHelper._passthrough
        else:
            clsdict_s[f'reduce_{command.__name__}'] = \
                ProductionHelper._passthrough

    cmd_s = _new_nonterm(f'{parent}CommandShort', clsdict=clsdict_s)
    cmd_b = _new_nonterm(f'{parent}CommandBlock', clsdict=clsdict_b)

    # Merged command which has minimal ";"
    #
    # CommandFull := CommandShort ; | CommandBlock
    clsdict = {}
    clsdict[f'reduce_{cmd_s.__name__}_SEMICOLON'] = \
        ProductionHelper._passthrough
    clsdict[f'reduce_{cmd_b.__name__}'] = \
        ProductionHelper._passthrough
    cmd = _new_nonterm(f'{parent}CommandFull', clsdict=clsdict)

    # CommandsList := CommandFull [; CommandFull ...]
    cmdlist = _new_nonterm(f'{parent}CommandsList', clsbases=(ListNonterm,),
                           clskwds=dict(element=cmd, separator=OptSemicolons))

    # Command block is tricky, but the inner commands must terminate
    # without a ";", is possible.
    #
    # CommandsBlock :=
    #
    #   { [ ; ] CommandFull }
    #   { [ ; ] CommandsList [ ; ]} |
    #   { [ ; ] CommandsList [ ; ] CommandFull }
    clsdict = {}
    clsdict[f'reduce_LBRACE_OptSemicolons_{cmd_s.__name__}_RBRACE'] = \
        ProductionHelper._block
    clsdict[f'reduce_LBRACE_OptSemicolons_{cmdlist.__name__}_' +
            f'OptSemicolons_RBRACE'] = \
        ProductionHelper._block2
    clsdict[f'reduce_LBRACE_OptSemicolons_{cmdlist.__name__}_OptSemicolons_' +
            f'{cmd_s.__name__}_RBRACE'] = \
        ProductionHelper._block3
    _new_nonterm(f'{parent}CommandsBlock', clsdict=clsdict)

    if opt is False:
        #   | Command
        clsdict = {}
        clsdict[f'reduce_{cmd_s.__name__}'] = \
            ProductionHelper._singleton_list
        clsdict[f'reduce_{cmd_b.__name__}'] = \
            ProductionHelper._singleton_list
        _new_nonterm(parent + 'SingleCommandBlock', clsdict=clsdict)


class SetField(Nonterm):
    # field := <expr>
    def reduce_ShortNodeName_ASSIGN_Expr(self, *kids):
        self.val = esast.Field(name=kids[0].val, value=kids[2].val)


class SetAttribute(Nonterm):
    def reduce_ATTRIBUTE_ShortNodeName_ASSIGN_Expr(self, *kids):
        self.val = esast.Attribute(name=kids[1].val, value=kids[3].val)


commands_block(
    'Create',
    SetField,
    SetAttribute)


class OptExtending(Nonterm):
    def reduce_EXTENDING_SimpleTypeNameList(self, *kids):
        self.val = kids[1].val

    def reduce_empty(self, *kids):
        self.val = []


#
# CREATE CONSTRAINT
#
class ConstraintDeclaration(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT ShortNodeName OptOnExpr \
                    OptExtending CreateCommandsBlock"""
        self.val = esast.ConstraintDeclaration(
            abstract=True,
            name=kids[2].val.name,
            subject=kids[3].val,
            extends=kids[4].val,
            **_process_commands(kids[5].val)
        )

    def reduce_CreateConstraint_CreateFunctionArgs(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT ShortNodeName CreateFunctionArgs \
                    OptOnExpr OptExtending CreateCommandsBlock"""
        self.val = esast.ConstraintDeclaration(
            abstract=True,
            name=kids[2].val.name,
            params=kids[3].val,
            subject=kids[4].val,
            extends=kids[5].val,
            **_process_commands(kids[6].val)
        )


class ConstraintDeclarationShort(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT ShortNodeName OptOnExpr \
                    OptExtending"""
        self.val = esast.ConstraintDeclaration(
            abstract=True,
            name=kids[2].val.name,
            subject=kids[3].val,
            extends=kids[4].val,
        )

    def reduce_CreateConstraint_CreateFunctionArgs(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT ShortNodeName CreateFunctionArgs \
                    OptOnExpr OptExtending"""
        self.val = esast.ConstraintDeclaration(
            abstract=True,
            name=kids[2].val.name,
            params=kids[3].val,
            subject=kids[4].val,
            extends=kids[5].val,
        )


class OnExpr(Nonterm):
    def reduce_ON_LPAREN_Expr_RPAREN(self, *kids):
        self.val = kids[2].val


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


class ConcreteConstraintBlock(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr \
                    CreateCommandsBlock"""
        self.val = esast.Constraint(
            delegated=False,
            name=kids[1].val,
            args=kids[2].val,
            subject=kids[3].val,
            **_process_commands(kids[4].val)
        )

    def reduce_CreateDelegatedConstraint(self, *kids):
        r"""%reduce DELEGATED CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr \
                    CreateCommandsBlock"""
        self.val = esast.Constraint(
            delegated=True,
            name=kids[2].val,
            args=kids[3].val,
            subject=kids[4].val,
            **_process_commands(kids[5].val)
        )


class ConcreteConstraintShort(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr"""
        self.val = esast.Constraint(
            delegated=False,
            name=kids[1].val,
            args=kids[2].val,
            subject=kids[3].val,
        )

    def reduce_CreateDelegatedConstraint(self, *kids):
        r"""%reduce DELEGATED CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr"""
        self.val = esast.Constraint(
            delegated=True,
            name=kids[2].val,
            args=kids[3].val,
            subject=kids[4].val,
        )


#
# CREATE SCALAR TYPE
#

commands_block(
    'CreateScalarType',
    SetField,
    SetAttribute,
    ConcreteConstraintBlock,
    ConcreteConstraintShort,
)


class ScalarTypeDeclaration(Nonterm):
    def reduce_CreateAbstractScalarTypeStmt(self, *kids):
        r"""%reduce \
            ABSTRACT SCALAR TYPE ShortNodeName \
            OptExtending CreateScalarTypeCommandsBlock \
        """
        self.val = esast.ScalarTypeDeclaration(
            abstract=True,
            name=kids[3].val.name,
            extends=kids[4].val,
            **_process_commands(kids[5].val)
        )

    def reduce_CreateFinalScalarTypeStmt(self, *kids):
        r"""%reduce \
            FINAL SCALAR TYPE ShortNodeName \
            OptExtending CreateScalarTypeCommandsBlock \
        """
        self.val = esast.ScalarTypeDeclaration(
            final=True,
            name=kids[3].val.name,
            extends=kids[4].val,
            **_process_commands(kids[5].val)
        )

    def reduce_ScalarTypeDeclaration(self, *kids):
        r"""%reduce \
            SCALAR TYPE ShortNodeName \
            OptExtending CreateScalarTypeCommandsBlock \
        """
        self.val = esast.ScalarTypeDeclaration(
            name=kids[2].val.name,
            extends=kids[3].val,
            **_process_commands(kids[4].val)
        )


class ScalarTypeDeclarationShort(Nonterm):
    def reduce_CreateAbstractScalarTypeStmt(self, *kids):
        r"""%reduce \
            ABSTRACT SCALAR TYPE ShortNodeName \
            OptExtending \
        """
        self.val = esast.ScalarTypeDeclaration(
            abstract=True,
            name=kids[3].val.name,
            extends=kids[4].val,
        )

    def reduce_CreateFinalScalarTypeStmt(self, *kids):
        r"""%reduce \
            FINAL SCALAR TYPE ShortNodeName \
            OptExtending \
        """
        self.val = esast.ScalarTypeDeclaration(
            final=True,
            name=kids[3].val.name,
            extends=kids[4].val,
        )

    def reduce_ScalarTypeDeclaration(self, *kids):
        r"""%reduce \
            SCALAR TYPE ShortNodeName \
            OptExtending \
        """
        self.val = esast.ScalarTypeDeclaration(
            name=kids[2].val.name,
            extends=kids[3].val,
        )


#
# CREATE ATTRIBUTE
#
class AttributeDeclaration(Nonterm):
    def reduce_CreateAttribute(self, *kids):
        r"""%reduce ABSTRACT ATTRIBUTE ShortNodeName OptExtending \
                    CreateCommandsBlock"""
        self.val = esast.AttributeDeclaration(
            abstract=True,
            name=kids[2].val.name,
            extends=kids[3].val,
            inheritable=False,
            **_process_commands(kids[4].val)
        )

    def reduce_CreateInheritableAttribute(self, *kids):
        r"""%reduce ABSTRACT INHERITABLE ATTRIBUTE
                    ShortNodeName OptExtending CreateCommandsBlock"""
        self.val = esast.AttributeDeclaration(
            abstract=True,
            name=kids[3].val.name,
            extends=kids[4].val,
            inheritable=True,
            **_process_commands(kids[4].val)
        )


class AttributeDeclarationShort(Nonterm):
    def reduce_CreateAttribute(self, *kids):
        r"""%reduce ABSTRACT ATTRIBUTE ShortNodeName OptExtending"""
        self.val = esast.AttributeDeclaration(
            abstract=True,
            name=kids[2].val.name,
            extends=kids[3].val,
            inheritable=False,
        )

    def reduce_CreateInheritableAttribute(self, *kids):
        r"""%reduce ABSTRACT INHERITABLE ATTRIBUTE
                    ShortNodeName OptExtending"""
        self.val = esast.AttributeDeclaration(
            abstract=True,
            name=kids[3].val.name,
            extends=kids[4].val,
            inheritable=True,
        )


#
# CREATE INDEX
#
class Index(Nonterm):
    def reduce_INDEX_ShortNodeName_ON_Expr(self, *kids):
        self.val = esast.Index(
            name=kids[1].val,
            expression=kids[3].val
        )


#
# CREATE PROPERTY
#
class PropertyDeclaration(Nonterm):
    def reduce_CreateProperty(self, *kids):
        r"""%reduce ABSTRACT PROPERTY ShortNodeName OptExtending \
                    CreateCommandsBlock \
        """
        self.val = esast.PropertyDeclaration(
            abstract=True,
            name=kids[2].val.name,
            extends=kids[3].val,
            **_process_commands(kids[4].val)
        )


class PropertyDeclarationShort(Nonterm):
    def reduce_CreateProperty(self, *kids):
        r"""%reduce ABSTRACT PROPERTY ShortNodeName OptExtending"""
        self.val = esast.PropertyDeclaration(
            abstract=True,
            name=kids[2].val.name,
            extends=kids[3].val,
        )


#
# CREATE LINK ... { CREATE PROPERTY
#

commands_block(
    'CreateConcreteProperty',
    SetField,
    SetAttribute,
    ConcreteConstraintBlock,
    ConcreteConstraintShort,
)


class ConcretePropertyBlock(Nonterm):
    def reduce_CreateRegularProperty(self, *kids):
        """%reduce
            PROPERTY ShortNodeName OptExtending
            ARROW FullTypeExpr CreateConcretePropertyCommandsBlock
        """
        self.val = esast.Property(
            name=kids[1].val.name,
            extends=kids[2].val,
            target=qlast.get_targets(kids[4].val),
            **_process_commands(kids[5].val)
        )

    def reduce_CreateQualifiedRegularProperty(self, *kids):
        """%reduce
            PtrQuals PROPERTY ShortNodeName OptExtending
            ARROW FullTypeExpr CreateConcretePropertyCommandsBlock
        """
        self.val = esast.Property(
            name=kids[2].val.name,
            extends=kids[3].val,
            inherited=kids[0].val.inherited,
            required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            target=qlast.get_targets(kids[5].val),
            **_process_commands(kids[6].val)
        )


class ConcretePropertyShort(Nonterm):
    def reduce_CreateRegularProperty(self, *kids):
        """%reduce
            PROPERTY ShortNodeName OptExtending
            ARROW FullTypeExpr
        """
        self.val = esast.Property(
            name=kids[1].val.name,
            extends=kids[2].val,
            target=qlast.get_targets(kids[4].val),
        )

    def reduce_CreateQualifiedRegularProperty(self, *kids):
        """%reduce
            PtrQuals PROPERTY ShortNodeName OptExtending
            ARROW FullTypeExpr
        """
        self.val = esast.Property(
            name=kids[2].val.name,
            extends=kids[3].val,
            inherited=kids[0].val.inherited,
            required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            target=qlast.get_targets(kids[5].val),
        )

    def reduce_CreateComputableProperty(self, *kids):
        """%reduce
            PROPERTY ShortNodeName ASSIGN Expr
        """
        self.val = esast.Property(
            name=kids[1].val.name,
            expr=kids[3].val,
        )

    def reduce_CreateQualifiedComputableProperty(self, *kids):
        """%reduce
            PtrQuals PROPERTY ShortNodeName ASSIGN Expr
        """
        self.val = esast.Property(
            name=kids[2].val.name,
            inherited=kids[0].val.inherited,
            required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            expr=kids[4].val,
        )


#
# CREATE LINK
#

commands_block(
    'CreateLink',
    SetField,
    SetAttribute,
    ConcreteConstraintBlock,
    ConcreteConstraintShort,
    ConcretePropertyBlock,
    ConcretePropertyShort,
    Index,
)


class LinkDeclaration(Nonterm):
    def reduce_CreateLink(self, *kids):
        r"""%reduce \
            ABSTRACT LINK ShortNodeName OptExtending \
            CreateLinkCommandsBlock \
        """
        self.val = esast.LinkDeclaration(
            abstract=True,
            name=kids[2].val.name,
            extends=kids[3].val,
            **_process_commands(kids[4].val)
        )


class LinkDeclarationShort(Nonterm):
    def reduce_CreateLink(self, *kids):
        r"""%reduce \
            ABSTRACT LINK ShortNodeName OptExtending"""
        self.val = esast.LinkDeclaration(
            abstract=True,
            name=kids[2].val.name,
            extends=kids[3].val,
        )


#
# CREATE TYPE ... { CREATE LINK ... { ON TARGET DELETE ...
#
class OnTargetDelete(Nonterm):
    def reduce_ON_TARGET_DELETE_RESTRICT(self, *kids):
        self.val = esast.OnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.RESTRICT)

    def reduce_ON_TARGET_DELETE_DELETE_SOURCE(self, *kids):
        self.val = esast.OnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.DELETE_SOURCE)

    def reduce_ON_TARGET_DELETE_ALLOW(self, *kids):
        self.val = esast.OnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.ALLOW)

    def reduce_ON_TARGET_DELETE_DEFERRED_RESTRICT(self, *kids):
        self.val = esast.OnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.DEFERRED_RESTRICT)


#
# CREATE TYPE ... { CREATE LINK
#

commands_block(
    'CreateConcreteLink',
    SetField,
    SetAttribute,
    ConcreteConstraintBlock,
    ConcreteConstraintShort,
    ConcretePropertyBlock,
    ConcretePropertyShort,
    OnTargetDelete,
)


class ConcreteLinkBlock(Nonterm):
    def reduce_CreateRegularLink(self, *kids):
        """%reduce
            LINK ShortNodeName OptExtending
            ARROW FullTypeExpr CreateConcreteLinkCommandsBlock
        """
        self.val = esast.Link(
            name=kids[1].val.name,
            extends=kids[2].val,
            target=qlast.get_targets(kids[4].val),
            **_process_commands(kids[5].val)
        )

    def reduce_CreateQualifiedRegularLink(self, *kids):
        """%reduce
            PtrQuals LINK ShortNodeName OptExtending
            ARROW FullTypeExpr CreateConcreteLinkCommandsBlock
        """
        self.val = esast.Link(
            inherited=kids[0].val.inherited,
            required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            name=kids[2].val.name,
            extends=kids[3].val,
            target=qlast.get_targets(kids[5].val),
            **_process_commands(kids[6].val)
        )


class ConcreteLinkShort(Nonterm):
    def reduce_CreateRegularLink(self, *kids):
        """%reduce
            LINK ShortNodeName OptExtending
            ARROW FullTypeExpr
        """
        self.val = esast.Link(
            name=kids[1].val.name,
            extends=kids[2].val,
            target=qlast.get_targets(kids[4].val),
        )

    def reduce_CreateQualifiedRegularLink(self, *kids):
        """%reduce
            PtrQuals LINK ShortNodeName OptExtending
            ARROW FullTypeExpr
        """
        self.val = esast.Link(
            inherited=kids[0].val.inherited,
            required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            name=kids[2].val.name,
            extends=kids[3].val,
            target=qlast.get_targets(kids[5].val),
        )

    def reduce_CreateComputableLink(self, *kids):
        """%reduce
            LINK ShortNodeName ASSIGN Expr
        """
        self.val = esast.Link(
            name=kids[1].val.name,
            expr=kids[3].val,
        )

    def reduce_CreateQualifiedComputableLink(self, *kids):
        """%reduce
            PtrQuals LINK ShortNodeName ASSIGN Expr
        """
        self.val = esast.Link(
            inherited=kids[0].val.inherited,
            required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            name=kids[2].val.name,
            expr=kids[4].val,
        )


#
# CREATE TYPE
#

commands_block(
    'CreateObjectType',
    SetField,
    SetAttribute,
    ConcretePropertyBlock,
    ConcretePropertyShort,
    ConcreteLinkBlock,
    ConcreteLinkShort,
    Index,
)


class ObjectTypeDeclaration(Nonterm):
    def reduce_CreateAbstractObjectTypeStmt(self, *kids):
        r"""%reduce \
            ABSTRACT TYPE ShortNodeName OptExtending \
            CreateObjectTypeCommandsBlock \
        """
        self.val = esast.ObjectTypeDeclaration(
            abstract=True,
            name=kids[2].val.name,
            extends=kids[3].val,
            **_process_commands(kids[4].val)
        )

    def reduce_CreateRegularObjectTypeStmt(self, *kids):
        r"""%reduce \
            TYPE ShortNodeName OptExtending \
            CreateObjectTypeCommandsBlock \
        """
        self.val = esast.ObjectTypeDeclaration(
            name=kids[1].val.name,
            extends=kids[2].val,
            **_process_commands(kids[3].val)
        )


class ObjectTypeDeclarationShort(Nonterm):
    def reduce_CreateAbstractObjectTypeStmt(self, *kids):
        r"""%reduce \
            ABSTRACT TYPE ShortNodeName OptExtending"""
        self.val = esast.ObjectTypeDeclaration(
            abstract=True,
            name=kids[2].val.name,
            extends=kids[3].val,
        )

    def reduce_CreateRegularObjectTypeStmt(self, *kids):
        r"""%reduce \
            TYPE ShortNodeName OptExtending"""
        self.val = esast.ObjectTypeDeclaration(
            name=kids[1].val.name,
            extends=kids[2].val,
        )


#
# CREATE VIEW
#

commands_block(
    'CreateView',
    SetField,
    SetAttribute,
    opt=False
)


class ViewDeclaration(Nonterm):
    def reduce_CreateViewRegularStmt(self, *kids):
        r"""%reduce \
            VIEW ShortNodeName CreateViewCommandsBlock \
        """
        self.val = esast.ViewDeclaration(
            name=kids[1].val.name,
            **_process_commands(kids[2].val),
        )


class ViewDeclarationShort(Nonterm):
    def reduce_CreateViewShortStmt(self, *kids):
        r"""%reduce \
            VIEW ShortNodeName ASSIGN Expr \
        """
        self.val = esast.ViewDeclaration(
            name=kids[1].val.name,
            fields=[
                esast.Field(
                    name=qlast.ObjectRef(name='expr'),
                    value=kids[3].val,
                )
            ]
        )

    def reduce_CreateViewRegularStmt(self, *kids):
        r"""%reduce \
            VIEW ShortNodeName CreateViewSingleCommandBlock \
        """
        self.val = esast.ViewDeclaration(
            name=kids[1].val.name,
            **_process_commands(kids[2].val),
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
        self.val = qltypes.ParameterKind.POSITIONAL

    def reduce_VARIADIC(self, kid):
        self.val = qltypes.ParameterKind.VARIADIC

    def reduce_NAMEDONLY(self, *kids):
        self.val = qltypes.ParameterKind.NAMED_ONLY


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
        code = kids[2].val
        self.val = esast.FunctionCode(language=lang, code=code)

    def reduce_FROM_Identifier_FUNCTION_BaseStringConstant(self, *kids):
        lang = _parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in FROM FUNCTION clause',
                context=kids[1].context) from None

        self.val = esast.FunctionCode(language=lang,
                                      from_function=kids[3].val.value)

    def reduce_FROM_Identifier_EXPRESSION(self, *kids):
        lang = _parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in FROM clause',
                context=kids[1].context) from None

        self.val = esast.FunctionCode(language=lang)


#
# CREATE FUNCTION
#


class _ProcessFunctionBlockMixin:
    def _process_function_body(self, block):
        props = _process_commands(block.val)
        function_code = props.get('function_code')

        if (not function_code
                or (function_code.code is None
                    and function_code.from_function is None
                    and not function_code.from_expr)):
            raise EdgeQLSyntaxError(
                'CREATE FUNCTION requires at least one FROM clause',
                context=block.context)

        else:
            if function_code.from_expr and (function_code.from_function
                                            or function_code.code):
                raise EdgeQLSyntaxError(
                    'FROM SQL EXPRESSION is mutually exclusive with other '
                    'FROM variants',
                    context=block.context)

        return props


commands_block(
    'CreateFunction',
    FromFunction,
    SetField,
    SetAttribute,
    opt=False
)


class OptTypeQualifier(Nonterm):
    def reduce_SET_OF(self, *kids):
        self.val = qltypes.TypeModifier.SET_OF

    def reduce_OPTIONAL(self, *kids):
        self.val = qltypes.TypeModifier.OPTIONAL

    def reduce_empty(self):
        self.val = qltypes.TypeModifier.SINGLETON


class FunctionDeclaration(Nonterm, _ProcessFunctionBlockMixin):
    def reduce_CreateFunction(self, *kids):
        r"""%reduce FUNCTION ShortNodeName CreateFunctionArgs \
                ARROW OptTypeQualifier FunctionType \
                CreateFunctionCommandsBlock
        """
        self.val = esast.FunctionDeclaration(
            name=kids[1].val.name,
            params=kids[2].val,
            returning=kids[5].val,
            returning_typemod=kids[4].val,
            **self._process_function_body(kids[6]),
        )


class FunctionDeclarationShort(Nonterm, _ProcessFunctionBlockMixin):
    def reduce_CreateFunction(self, *kids):
        r"""%reduce FUNCTION ShortNodeName CreateFunctionArgs \
                ARROW OptTypeQualifier FunctionType \
                CreateFunctionSingleCommandBlock
        """
        self.val = esast.FunctionDeclaration(
            name=kids[1].val.name,
            params=kids[2].val,
            returning=kids[5].val,
            returning_typemod=kids[4].val,
            **self._process_function_body(kids[6]),
        )


class FunctionType(Nonterm):
    def reduce_FullTypeExpr(self, *kids):
        self.val = kids[0].val
