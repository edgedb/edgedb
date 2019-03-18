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


from edb.errors import EdgeQLSyntaxError

from edb.edgeql import ast as qlast

from edb.common import parsing

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
        if isinstance(node, qlast.SDLFunctionCode):
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
        elif isinstance(node, qlast.Attribute):
            attributes.append(node)
        elif isinstance(node, qlast.Field):
            fields.append(node)
        elif isinstance(node, qlast.Property):
            properties.append(node)
        elif isinstance(node, qlast.Link):
            links.append(node)
        elif isinstance(node, qlast.Constraint):
            constraints.append(node)
        elif isinstance(node, qlast.IndexDeclaration):
            indexes.append(node)
        elif isinstance(node, qlast.SDLOnTargetDelete):
            if on_target_delete:
                raise EdgeQLSyntaxError(
                    f"more than one 'on target delete' specification",
                    context=node.context)
            else:
                on_target_delete = node

    if from_expr or from_function or code:
        props['function_code'] = qlast.SDLFunctionCode(
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
        self.val = qlast.Import(modules=kids[1].val)

    def reduce_IMPORT_LPAREN_ImportModuleList_RPAREN(self, *kids):
        self.val = qlast.Import(modules=kids[2].val)

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

    def reduce_IndexDeclaration(self, *kids):
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
        self.val = qlast.Field(name=kids[0].val, value=kids[2].val)


class SetAttribute(Nonterm):
    def reduce_ATTRIBUTE_ShortNodeName_ASSIGN_Expr(self, *kids):
        self.val = qlast.Attribute(name=kids[1].val, value=kids[3].val)


sdl_commands_block(
    'Create',
    SetField,
    SetAttribute)


#
# CREATE CONSTRAINT
#
class ConstraintDeclaration(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT ShortNodeName OptOnExpr \
                    OptExtending CreateSDLCommandsBlock"""
        self.val = qlast.ConstraintDeclaration(
            abstract=True,
            name=kids[2].val.name,
            subject=kids[3].val,
            extends=kids[4].val,
            **_process_commands(kids[5].val)
        )

    def reduce_CreateConstraint_CreateFunctionArgs(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT ShortNodeName CreateFunctionArgs \
                    OptOnExpr OptExtending CreateSDLCommandsBlock"""
        self.val = qlast.ConstraintDeclaration(
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
        self.val = qlast.ConstraintDeclaration(
            abstract=True,
            name=kids[2].val.name,
            subject=kids[3].val,
            extends=kids[4].val,
        )

    def reduce_CreateConstraint_CreateFunctionArgs(self, *kids):
        r"""%reduce ABSTRACT CONSTRAINT ShortNodeName CreateFunctionArgs \
                    OptOnExpr OptExtending"""
        self.val = qlast.ConstraintDeclaration(
            abstract=True,
            name=kids[2].val.name,
            params=kids[3].val,
            subject=kids[4].val,
            extends=kids[5].val,
        )


class ConcreteConstraintBlock(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr \
                    CreateSDLCommandsBlock"""
        self.val = qlast.Constraint(
            delegated=False,
            name=kids[1].val,
            args=kids[2].val,
            subject=kids[3].val,
            **_process_commands(kids[4].val)
        )

    def reduce_CreateDelegatedConstraint(self, *kids):
        r"""%reduce DELEGATED CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr \
                    CreateSDLCommandsBlock"""
        self.val = qlast.Constraint(
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
        self.val = qlast.Constraint(
            delegated=False,
            name=kids[1].val,
            args=kids[2].val,
            subject=kids[3].val,
        )

    def reduce_CreateDelegatedConstraint(self, *kids):
        r"""%reduce DELEGATED CONSTRAINT \
                    NodeName OptConcreteConstraintArgList OptOnExpr"""
        self.val = qlast.Constraint(
            delegated=True,
            name=kids[2].val,
            args=kids[3].val,
            subject=kids[4].val,
        )


#
# CREATE SCALAR TYPE
#

sdl_commands_block(
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
            OptExtending CreateScalarTypeSDLCommandsBlock \
        """
        self.val = qlast.ScalarTypeDeclaration(
            abstract=True,
            name=kids[3].val.name,
            extends=kids[4].val,
            **_process_commands(kids[5].val)
        )

    def reduce_CreateFinalScalarTypeStmt(self, *kids):
        r"""%reduce \
            FINAL SCALAR TYPE ShortNodeName \
            OptExtending CreateScalarTypeSDLCommandsBlock \
        """
        self.val = qlast.ScalarTypeDeclaration(
            final=True,
            name=kids[3].val.name,
            extends=kids[4].val,
            **_process_commands(kids[5].val)
        )

    def reduce_ScalarTypeDeclaration(self, *kids):
        r"""%reduce \
            SCALAR TYPE ShortNodeName \
            OptExtending CreateScalarTypeSDLCommandsBlock \
        """
        self.val = qlast.ScalarTypeDeclaration(
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
        self.val = qlast.ScalarTypeDeclaration(
            abstract=True,
            name=kids[3].val.name,
            extends=kids[4].val,
        )

    def reduce_CreateFinalScalarTypeStmt(self, *kids):
        r"""%reduce \
            FINAL SCALAR TYPE ShortNodeName \
            OptExtending \
        """
        self.val = qlast.ScalarTypeDeclaration(
            final=True,
            name=kids[3].val.name,
            extends=kids[4].val,
        )

    def reduce_ScalarTypeDeclaration(self, *kids):
        r"""%reduce \
            SCALAR TYPE ShortNodeName \
            OptExtending \
        """
        self.val = qlast.ScalarTypeDeclaration(
            name=kids[2].val.name,
            extends=kids[3].val,
        )


#
# CREATE ATTRIBUTE
#
class AttributeDeclaration(Nonterm):
    def reduce_CreateAttribute(self, *kids):
        r"""%reduce ABSTRACT ATTRIBUTE ShortNodeName OptExtending \
                    CreateSDLCommandsBlock"""
        self.val = qlast.AttributeDeclaration(
            abstract=True,
            name=kids[2].val.name,
            extends=kids[3].val,
            inheritable=False,
            **_process_commands(kids[4].val)
        )

    def reduce_CreateInheritableAttribute(self, *kids):
        r"""%reduce ABSTRACT INHERITABLE ATTRIBUTE
                    ShortNodeName OptExtending CreateSDLCommandsBlock"""
        self.val = qlast.AttributeDeclaration(
            abstract=True,
            name=kids[3].val.name,
            extends=kids[4].val,
            inheritable=True,
            **_process_commands(kids[4].val)
        )


class AttributeDeclarationShort(Nonterm):
    def reduce_CreateAttribute(self, *kids):
        r"""%reduce ABSTRACT ATTRIBUTE ShortNodeName OptExtending"""
        self.val = qlast.AttributeDeclaration(
            abstract=True,
            name=kids[2].val.name,
            extends=kids[3].val,
            inheritable=False,
        )

    def reduce_CreateInheritableAttribute(self, *kids):
        r"""%reduce ABSTRACT INHERITABLE ATTRIBUTE
                    ShortNodeName OptExtending"""
        self.val = qlast.AttributeDeclaration(
            abstract=True,
            name=kids[3].val.name,
            extends=kids[4].val,
            inheritable=True,
        )


#
# CREATE INDEX
#
class IndexDeclaration(Nonterm):
    def reduce_INDEX_ShortNodeName_ON_Expr(self, *kids):
        self.val = qlast.IndexDeclaration(
            name=kids[1].val,
            expression=kids[3].val
        )


#
# CREATE PROPERTY
#
class PropertyDeclaration(Nonterm):
    def reduce_CreateProperty(self, *kids):
        r"""%reduce ABSTRACT PROPERTY ShortNodeName OptExtending \
                    CreateSDLCommandsBlock \
        """
        self.val = qlast.PropertyDeclaration(
            abstract=True,
            name=kids[2].val.name,
            extends=kids[3].val,
            **_process_commands(kids[4].val)
        )


class PropertyDeclarationShort(Nonterm):
    def reduce_CreateProperty(self, *kids):
        r"""%reduce ABSTRACT PROPERTY ShortNodeName OptExtending"""
        self.val = qlast.PropertyDeclaration(
            abstract=True,
            name=kids[2].val.name,
            extends=kids[3].val,
        )


#
# CREATE LINK ... { CREATE PROPERTY
#

sdl_commands_block(
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
            ARROW FullTypeExpr CreateConcretePropertySDLCommandsBlock
        """
        self.val = qlast.Property(
            name=kids[1].val.name,
            extends=kids[2].val,
            target=qlast.get_targets(kids[4].val),
            **_process_commands(kids[5].val)
        )

    def reduce_CreateQualifiedRegularProperty(self, *kids):
        """%reduce
            PtrQuals PROPERTY ShortNodeName OptExtending
            ARROW FullTypeExpr CreateConcretePropertySDLCommandsBlock
        """
        self.val = qlast.Property(
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
        self.val = qlast.Property(
            name=kids[1].val.name,
            extends=kids[2].val,
            target=qlast.get_targets(kids[4].val),
        )

    def reduce_CreateQualifiedRegularProperty(self, *kids):
        """%reduce
            PtrQuals PROPERTY ShortNodeName OptExtending
            ARROW FullTypeExpr
        """
        self.val = qlast.Property(
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
        self.val = qlast.Property(
            name=kids[1].val.name,
            expr=kids[3].val,
        )

    def reduce_CreateQualifiedComputableProperty(self, *kids):
        """%reduce
            PtrQuals PROPERTY ShortNodeName ASSIGN Expr
        """
        self.val = qlast.Property(
            name=kids[2].val.name,
            inherited=kids[0].val.inherited,
            required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            expr=kids[4].val,
        )


#
# CREATE LINK
#

sdl_commands_block(
    'CreateLink',
    SetField,
    SetAttribute,
    ConcreteConstraintBlock,
    ConcreteConstraintShort,
    ConcretePropertyBlock,
    ConcretePropertyShort,
    IndexDeclaration,
)


class LinkDeclaration(Nonterm):
    def reduce_CreateLink(self, *kids):
        r"""%reduce \
            ABSTRACT LINK ShortNodeName OptExtending \
            CreateLinkSDLCommandsBlock \
        """
        self.val = qlast.LinkDeclaration(
            abstract=True,
            name=kids[2].val.name,
            extends=kids[3].val,
            **_process_commands(kids[4].val)
        )


class LinkDeclarationShort(Nonterm):
    def reduce_CreateLink(self, *kids):
        r"""%reduce \
            ABSTRACT LINK ShortNodeName OptExtending"""
        self.val = qlast.LinkDeclaration(
            abstract=True,
            name=kids[2].val.name,
            extends=kids[3].val,
        )


#
# CREATE TYPE ... { CREATE LINK ... { ON TARGET DELETE ...
#
class SDLOnTargetDelete(Nonterm):
    def reduce_ON_TARGET_DELETE_RESTRICT(self, *kids):
        self.val = qlast.SDLOnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.RESTRICT)

    def reduce_ON_TARGET_DELETE_DELETE_SOURCE(self, *kids):
        self.val = qlast.SDLOnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.DELETE_SOURCE)

    def reduce_ON_TARGET_DELETE_ALLOW(self, *kids):
        self.val = qlast.SDLOnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.ALLOW)

    def reduce_ON_TARGET_DELETE_DEFERRED_RESTRICT(self, *kids):
        self.val = qlast.SDLOnTargetDelete(
            cascade=qlast.LinkTargetDeleteAction.DEFERRED_RESTRICT)


#
# CREATE TYPE ... { CREATE LINK
#

sdl_commands_block(
    'CreateConcreteLink',
    SetField,
    SetAttribute,
    ConcreteConstraintBlock,
    ConcreteConstraintShort,
    ConcretePropertyBlock,
    ConcretePropertyShort,
    SDLOnTargetDelete,
)


class ConcreteLinkBlock(Nonterm):
    def reduce_CreateRegularLink(self, *kids):
        """%reduce
            LINK ShortNodeName OptExtending
            ARROW FullTypeExpr CreateConcreteLinkSDLCommandsBlock
        """
        self.val = qlast.Link(
            name=kids[1].val.name,
            extends=kids[2].val,
            target=qlast.get_targets(kids[4].val),
            **_process_commands(kids[5].val)
        )

    def reduce_CreateQualifiedRegularLink(self, *kids):
        """%reduce
            PtrQuals LINK ShortNodeName OptExtending
            ARROW FullTypeExpr CreateConcreteLinkSDLCommandsBlock
        """
        self.val = qlast.Link(
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
        self.val = qlast.Link(
            name=kids[1].val.name,
            extends=kids[2].val,
            target=qlast.get_targets(kids[4].val),
        )

    def reduce_CreateQualifiedRegularLink(self, *kids):
        """%reduce
            PtrQuals LINK ShortNodeName OptExtending
            ARROW FullTypeExpr
        """
        self.val = qlast.Link(
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
        self.val = qlast.Link(
            name=kids[1].val.name,
            expr=kids[3].val,
        )

    def reduce_CreateQualifiedComputableLink(self, *kids):
        """%reduce
            PtrQuals LINK ShortNodeName ASSIGN Expr
        """
        self.val = qlast.Link(
            inherited=kids[0].val.inherited,
            required=kids[0].val.required,
            cardinality=kids[0].val.cardinality,
            name=kids[2].val.name,
            expr=kids[4].val,
        )


#
# CREATE TYPE
#

sdl_commands_block(
    'CreateObjectType',
    SetField,
    SetAttribute,
    ConcretePropertyBlock,
    ConcretePropertyShort,
    ConcreteLinkBlock,
    ConcreteLinkShort,
    IndexDeclaration,
)


class ObjectTypeDeclaration(Nonterm):
    def reduce_CreateAbstractObjectTypeStmt(self, *kids):
        r"""%reduce \
            ABSTRACT TYPE ShortNodeName OptExtending \
            CreateObjectTypeSDLCommandsBlock \
        """
        self.val = qlast.ObjectTypeDeclaration(
            abstract=True,
            name=kids[2].val.name,
            extends=kids[3].val,
            **_process_commands(kids[4].val)
        )

    def reduce_CreateRegularObjectTypeStmt(self, *kids):
        r"""%reduce \
            TYPE ShortNodeName OptExtending \
            CreateObjectTypeSDLCommandsBlock \
        """
        self.val = qlast.ObjectTypeDeclaration(
            name=kids[1].val.name,
            extends=kids[2].val,
            **_process_commands(kids[3].val)
        )


class ObjectTypeDeclarationShort(Nonterm):
    def reduce_CreateAbstractObjectTypeStmt(self, *kids):
        r"""%reduce \
            ABSTRACT TYPE ShortNodeName OptExtending"""
        self.val = qlast.ObjectTypeDeclaration(
            abstract=True,
            name=kids[2].val.name,
            extends=kids[3].val,
        )

    def reduce_CreateRegularObjectTypeStmt(self, *kids):
        r"""%reduce \
            TYPE ShortNodeName OptExtending"""
        self.val = qlast.ObjectTypeDeclaration(
            name=kids[1].val.name,
            extends=kids[2].val,
        )


#
# CREATE VIEW
#

sdl_commands_block(
    'CreateView',
    SetField,
    SetAttribute,
    opt=False
)


class ViewDeclaration(Nonterm):
    def reduce_CreateViewRegularStmt(self, *kids):
        r"""%reduce \
            VIEW ShortNodeName CreateViewSDLCommandsBlock \
        """
        self.val = qlast.ViewDeclaration(
            name=kids[1].val.name,
            **_process_commands(kids[2].val),
        )


class ViewDeclarationShort(Nonterm):
    def reduce_CreateViewShortStmt(self, *kids):
        r"""%reduce \
            VIEW ShortNodeName ASSIGN Expr \
        """
        self.val = qlast.ViewDeclaration(
            name=kids[1].val.name,
            fields=[
                qlast.Field(
                    name=qlast.ObjectRef(name='expr'),
                    value=kids[3].val,
                )
            ]
        )

    def reduce_CreateViewRegularStmt(self, *kids):
        r"""%reduce \
            VIEW ShortNodeName CreateViewSingleSDLCommandBlock \
        """
        self.val = qlast.ViewDeclaration(
            name=kids[1].val.name,
            **_process_commands(kids[2].val),
        )


#
# FUNCTIONS
#


# FIXME: this is identical to DDL except for the AST type
class SDLFromFunction(Nonterm):
    def reduce_FROM_Identifier_BaseStringConstant(self, *kids):
        lang = commondl._parse_language(kids[1])
        code = kids[2].val
        self.val = qlast.SDLFunctionCode(language=lang, code=code)

    def reduce_FROM_Identifier_FUNCTION_BaseStringConstant(self, *kids):
        lang = commondl._parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in FROM FUNCTION clause',
                context=kids[1].context) from None

        self.val = qlast.SDLFunctionCode(language=lang,
                                         from_function=kids[3].val.value)

    def reduce_FROM_Identifier_EXPRESSION(self, *kids):
        lang = commondl._parse_language(kids[1])
        if lang != qlast.Language.SQL:
            raise EdgeQLSyntaxError(
                f'{lang} language is not supported in FROM clause',
                context=kids[1].context) from None

        self.val = qlast.SDLFunctionCode(language=lang)


#
# CREATE FUNCTION
#


class _SDLProcessFunctionBlockMixin:
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


sdl_commands_block(
    'CreateFunction',
    SDLFromFunction,
    SetField,
    SetAttribute,
    opt=False
)


class FunctionDeclaration(Nonterm, _SDLProcessFunctionBlockMixin):
    def reduce_CreateFunction(self, *kids):
        r"""%reduce FUNCTION ShortNodeName CreateFunctionArgs \
                ARROW OptTypeQualifier FunctionType \
                CreateFunctionSDLCommandsBlock
        """
        self.val = qlast.FunctionDeclaration(
            name=kids[1].val.name,
            params=kids[2].val,
            returning=kids[5].val,
            returning_typemod=kids[4].val,
            **self._process_function_body(kids[6]),
        )


class FunctionDeclarationShort(Nonterm, _SDLProcessFunctionBlockMixin):
    def reduce_CreateFunction(self, *kids):
        r"""%reduce FUNCTION ShortNodeName CreateFunctionArgs \
                ARROW OptTypeQualifier FunctionType \
                CreateFunctionSingleSDLCommandBlock
        """
        self.val = qlast.FunctionDeclaration(
            name=kids[1].val.name,
            params=kids[2].val,
            returning=kids[5].val,
            returning_typemod=kids[4].val,
            **self._process_function_body(kids[6]),
        )
