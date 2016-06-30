##
# Copyright (c) 2015 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import types

from edgedb.lang.caosql import ast as qlast
from edgedb.lang.common.parsing import ListNonterm

from .precedence import *
from .tokens import *
from .expressions import *


class DDLStmt(Nonterm):
    def reduce_CreateDatabaseStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropDatabaseStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateActionStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterActionStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropActionStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateAtomStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterAtomStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropAtomStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateAttributeStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropAttributeStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateConceptStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterConceptStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropConceptStmt(self, *kids):
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

    def reduce_CreateLinkPropertyStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterLinkPropertyStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropLinkPropertyStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateEventStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CreateModuleStmt(self, *kids):
        self.val = kids[0].val

    def reduce_AlterModuleStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DropModuleStmt(self, *kids):
        self.val = kids[0].val


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
        self.val = None

    def _block(self, lbrace, cmdlist, rbrace):
        self.val = cmdlist.val


def commands_block(parent, *commands, opt=True):
    if parent is None:
        parent = ''

    clsname = parent + 'Command'
    clsdict = collections.OrderedDict()

    # Command := Command1 | Command2 ...
    #
    for command in commands:
        clsdict['reduce_{}'.format(command.__name__)] = \
            ProductionHelper._passthrough

    cmd = _new_nonterm(parent + 'Command', clsdict=clsdict)

    # CommandsList := Command [Command ...]
    cmdlist = _new_nonterm(parent + 'CommandsList', clsbases=(ListNonterm,),
                           clskwds=dict(element=cmd))

    # CommandsBlock :=
    #
    #   { CommandsList }
    clsdict = collections.OrderedDict()
    clsdict['reduce_LBRACE_' + cmdlist.__name__ + '_RBRACE'] = \
        ProductionHelper._block
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
    # field = <constant>
    def reduce_SET_NodeName_EQUALS_Expr(self, *kids):
        self.val = qlast.CreateAttributeValueNode(
            name=kids[1].val,
            value=kids[3].val
        )

    # field := <expr>
    def reduce_SET_NodeName_COLONEQUALS_Expr(self, *kids):
        self.val = qlast.CreateAttributeValueNode(
            name=kids[1].val,
            value=kids[3].val,
            as_expr=True
        )

commands_block('Create', SetFieldStmt)


class DropFieldStmt(Nonterm):
    def reduce_DROP_ATTRIBUTE_NodeName(self, *kids):
        self.val = qlast.DropAttributeValueNode(
            name=kids[2].val,
        )


class RenameStmt(Nonterm):
    def reduce_RENAME_TO_NodeName(self, *kids):
        self.val = qlast.RenameNode(new_name=kids[2].val)


commands_block('Alter', RenameStmt, SetFieldStmt, DropFieldStmt, opt=False)


class OptInheriting(Nonterm):
    def reduce_INHERITING_NodeName(self, *kids):
        self.val = [kids[1].val]

    def reduce_INHERITING_LPAREN_NodeNameList_RPAREN(self, *kids):
        self.val = kids[2].val

    def reduce_empty(self, *kids):
        self.val = []


class AlterAbstract(Nonterm):
    def reduce_DROP_ABSTRACT(self, *kids):
        self.val = qlast.SetSpecialFieldNode(name='is_abstract', value=False)

    def reduce_SET_ABSTRACT(self, *kids):
        self.val = qlast.SetSpecialFieldNode(name='is_abstract', value=True)


class AlterFinal(Nonterm):
    def reduce_DROP_FINAL(self, *kids):
        self.val = qlast.SetSpecialFieldNode(name='is_final', value=False)

    def reduce_SET_FINAL(self, *kids):
        self.val = qlast.SetSpecialFieldNode(name='is_final', value=True)


class OptInheritPosition(Nonterm):
    def reduce_BEFORE_NodeName(self, *kids):
        self.val = qlast.PositionNode(ref=kids[1].val, position='BEFORE')

    def reduce_AFTER_NodeName(self, *kids):
        self.val = qlast.PositionNode(ref=kids[1].val, position='AFTER')

    def reduce_FIRST(self, *kids):
        self.val = qlast.PositionNode(position='FIRST')

    def reduce_LAST(self, *kids):
        self.val = qlast.PositionNode(position='LAST')

    def reduce_empty(self, *kids):
        self.val = None


class AlterInheriting(Nonterm):
    def reduce_INHERIT_NodeNameList_OptInheritPosition(self, *kids):
        self.val = qlast.AlterAddInheritNode(bases=kids[1].val,
                                             position=kids[2].val)

    def reduce_DROP_INHERIT_NodeNameList(self, *kids):
        self.val = qlast.AlterDropInheritNode(bases=kids[2].val)

    def reduce_AlterAbstract(self, *kids):
        self.val = kids[0].val

    def reduce_AlterFinal(self, *kids):
        self.val = kids[0].val


#
# CREATE DATABASE
#
class CreateDatabaseStmt(Nonterm):
    def reduce_OptAliasBlock_CREATE_DATABASE_IDENT_OptCreateCommandsBlock(self, *kids):
        self.val = qlast.CreateDatabaseNode(
            name=qlast.PrototypeRefNode(name=kids[3].val),
            commands=kids[4].val
        )


#
# DROP DATABASE
#
class DropDatabaseStmt(Nonterm):
    def reduce_OptAliasBlock_DROP_DATABASE_IDENT(self, *kids):
        self.val = qlast.DropDatabaseNode(
            name=qlast.PrototypeRefNode(name=kids[3].val),
        )


#
# CREATE ACTION
#
class CreateActionStmt(Nonterm):
    def reduce_OptAliasBlock_CREATE_ACTION_NodeName_OptCreateCommandsBlock(self, *kids):
        self.val = qlast.CreateActionNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[3].val,
            commands = kids[4].val
        )


#
# ALTER ACTION
#
class AlterActionStmt(Nonterm):
    def reduce_OptAliasBlock_ALTER_ACTION_NodeName_AlterCommandsBlock(self, *kids):
        self.val = qlast.AlterActionNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[3].val,
            commands = kids[4].val
        )


#
# DROP ACTION
#
class DropActionStmt(Nonterm):
    def reduce_OptAliasBlock_DROP_ACTION_NodeName(self, *kids):
        self.val = qlast.DropActionNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[3].val,
        )



#
# CREATE POLICY
#
class CreateLocalPolicyStmt(Nonterm):
    def reduce_CREATE_POLICY_FOR_NodeName_TO_NodeNameList(
                                                self, *kids):
        self.val = qlast.CreateLocalPolicyNode(
            event = kids[3].val,
            actions = kids[5].val
        )


#
# ALTER POLICY
#
class AlterLocalPolicyStmt(Nonterm):
    def reduce_ALTER_POLICY_FOR_NodeName_TO_NodeNameList(
                                                self, *kids):
        self.val = qlast.AlterLocalPolicyNode(
            event = kids[3].val,
            actions = kids[5].val
        )


#
# DROP POLICY
#
class DropLocalPolicyStmt(Nonterm):
    def reduce_DROP_POLICY_FOR_NodeName(self, *kids):
        self.val = qlast.DropLocalPolicyNode(
            event = kids[3].val
        )


#
# CREATE CONSTRAINT
#
class CreateConstraintStmt(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce OptAliasBlock \
                    CREATE CONSTRAINT NodeName OptInheriting \
                    OptCreateCommandsBlock"""
        self.val = qlast.CreateConstraintNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[3].val,
            bases = kids[4].val,
            commands = kids[5].val,
        )


class AlterConstraintStmt(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce OptAliasBlock \
                    ALTER CONSTRAINT NodeName \
                    AlterCommandsBlock"""
        self.val = qlast.AlterConstraintNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[2].val,
            commands = kids[3].val,
        )


class DropConstraintStmt(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce OptAliasBlock \
                    DROP CONSTRAINT NodeName"""
        self.val = qlast.DropConstraintNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[2].val
        )


class CreateConcreteConstraintStmt(Nonterm):
    def reduce_CreateConstraint(self, *kids):
        r"""%reduce CREATE CONSTRAINT NodeName \
                    OptCreateCommandsBlock"""
        self.val = qlast.CreateConcreteConstraintNode(
            name = kids[2].val,
            commands = kids[3].val,
        )

    def reduce_CreateAbstractConstraint(self, *kids):
        r"""%reduce CREATE ABSTRACT CONSTRAINT NodeName \
                    OptCreateCommandsBlock"""
        self.val = qlast.CreateConcreteConstraintNode(
            is_abstract = True,
            name = kids[3].val,
            commands = kids[4].val,
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
        self.val = qlast.AlterConcreteConstraintNode(
            name = kids[2].val,
            commands = kids[3].val,
        )


class DropConcreteConstraintStmt(Nonterm):
    def reduce_DropConstraint(self, *kids):
        r"""%reduce DROP CONSTRAINT NodeName"""
        self.val = qlast.DropConcreteConstraintNode(
            name = kids[2].val
        )


#
# CREATE ATOM
#

commands_block('CreateAtom', SetFieldStmt, CreateConcreteConstraintStmt)

class CreateAtomStmt(Nonterm):
    def reduce_CreateAbstractAtomStmt(self, *kids):
        r"""%reduce \
            OptAliasBlock CREATE ABSTRACT ATOM NodeName \
            OptInheriting OptCreateAtomCommandsBlock \
        """
        self.val = qlast.CreateAtomNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[4].val,
            is_abstract = True,
            bases = kids[5].val,
            commands = kids[6].val
        )

    def reduce_CreateFinalAtomStmt(self, *kids):
        r"""%reduce \
            OptAliasBlock CREATE FINAL ATOM NodeName \
            OptInheriting OptCreateAtomCommandsBlock \
        """
        self.val = qlast.CreateAtomNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[4].val,
            is_final = True,
            bases = kids[5].val,
            commands = kids[6].val
        )

    def reduce_CreateAtomStmt(self, *kids):
        r"""%reduce \
            OptAliasBlock CREATE ATOM NodeName \
            OptInheriting OptCreateAtomCommandsBlock \
        """
        self.val = qlast.CreateAtomNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[3].val,
            bases = kids[4].val,
            commands = kids[5].val
        )


#
# ALTER ATOM
#

commands_block(
    'AlterAtom',
    RenameStmt,
    SetFieldStmt,
    DropFieldStmt,
    AlterInheriting,
    CreateConcreteConstraintStmt,
    AlterConcreteConstraintStmt,
    DropConcreteConstraintStmt,
    opt=False
)

class AlterAtomStmt(Nonterm):
    def reduce_AlterAtomStmt(self, *kids):
        r"""%reduce \
            OptAliasBlock ALTER ATOM NodeName AlterAtomCommandsBlock \
        """
        self.val = qlast.AlterAtomNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[3].val,
            commands = kids[4].val
        )


class DropAtomStmt(Nonterm):
    def reduce_OptAliasBlock_DROP_ATOM_NodeName(self, *kids):
        self.val = qlast.DropAtomNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[3].val
        )



#
# CREATE ATTRIBUTE
#
class DropAttributeStmt(Nonterm):
    def reduce_DropAttribute(self, *kids):
        r"""%reduce OptAliasBlock \
                    DROP ATTRIBUTE NodeName \
        """
        self.val = qlast.DropAttributeNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[3].val,
        )


#
# DROP ATTRIBUTE
#
class CreateAttributeStmt(Nonterm):
    def reduce_CreateAttribute(self, *kids):
        r"""%reduce OptAliasBlock \
                    CREATE ATTRIBUTE NodeName TypeName \
                    OptAttributeConstraint OptCreateCommandsBlock"""
        self.val = qlast.CreateAttributeNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[3].val,
            type = kids[4].val,
            constraint = kids[5].val,
            commands = kids[6].val,
        )


class OptAttributeConstraint(Nonterm):
    def reduce_WHERE_Expr(self, *kids):
        self.val = kids[1].val

    def reduce_empty(self):
        self.val = None


#
# CREATE INDEX
#
class CreateIndexStmt(Nonterm):
    def reduce_CREATE_INDEX_NodeName_Expr(self, *kids):
        self.val = qlast.CreateIndexNode(
            name=kids[2].val,
            expr=kids[3].val
        )


#
# DROP INDEX
#
class DropIndexStmt(Nonterm):
    def reduce_DROP_INDEX_NodeName(self, *kids):
        self.val = qlast.DropIndexNode(
            name=kids[2].val
        )


class AlterTargetStmt(Nonterm):
    def reduce_ALTER_TARGET_NodeNameList(self, *kids):
        self.val = qlast.AlterTargetNode(targets=kids[2].val)


#
# CREATE LINK PROPERTY
#
class CreateLinkPropertyStmt(Nonterm):
    def reduce_CreateLinkProperty(self, *kids):
        r"""%reduce \
            OptAliasBlock \
            CREATE LINKPROPERTY NodeName OptInheriting \
            OptCreateCommandsBlock \
        """
        self.val = qlast.CreateLinkPropertyNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name=kids[3].val,
            bases=kids[4].val,
            commands=kids[5].val
        )


#
# ALTER LINK PROPERTY
#

commands_block(
    'AlterLinkProperty',
    RenameStmt,
    SetFieldStmt,
    DropFieldStmt,
    opt=False
)


class AlterLinkPropertyStmt(Nonterm):
    def reduce_AlterLinkProperty(self, *kids):
        r"""%reduce \
            OptAliasBlock \
            ALTER LINKPROPERTY NodeName \
            AlterLinkPropertyCommandsBlock \
        """
        self.val = qlast.AlterLinkPropertyNode(
            namespaces=kids[0].val[0],
            aliases=kids[0].val[1],
            name=kids[3].val,
            commands=kids[4].val
        )


#
# DROP LINK PROPERTY
#
class DropLinkPropertyStmt(Nonterm):
    def reduce_DropLinkProperty(self, *kids):
        r"""%reduce \
            OptAliasBlock \
            DROP LINKPROPERTY NodeName \
        """
        self.val = qlast.DropLinkPropertyNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name=kids[3].val
        )


#
# CREATE LINK ... { CREATE LINK PROPERTY
#

commands_block(
    'CreateConcreteLinkProperty',
    SetFieldStmt,
    CreateConcreteConstraintStmt
)

class CreateConcreteLinkPropertyStmt(Nonterm):
    def reduce_CreateRegularRequiredLinkProperty(self, *kids):
        r"""%reduce \
            CREATE REQUIRED LINKPROPERTY NodeName \
            TO NodeName OptCreateConcreteLinkPropertyCommandsBlock \
        """
        self.val = qlast.CreateConcreteLinkPropertyNode(
            name=kids[3].val,
            is_required=True,
            target=kids[5].val,
            commands=kids[6].val
        )

    def reduce_CreateRegularLinkProperty(self, *kids):
        r"""%reduce \
            CREATE LINKPROPERTY NodeName \
            TO NodeName OptCreateConcreteLinkPropertyCommandsBlock \
        """
        self.val = qlast.CreateConcreteLinkPropertyNode(
            name=kids[2].val,
            is_required=False,
            target=kids[4].val,
            commands=kids[5].val
        )

    def reduce_CREATE_LINKPROPERTY_NodeName_AS_Expr(self, *kids):
        self.val = qlast.CreateConcreteLinkPropertyNode(
            name=kids[2].val,
            target=kids[4].val
        )


#
# ALTER LINK ... { ALTER LINK PROPERTY
#

commands_block(
    'AlterConcreteLinkProperty',
    RenameStmt,
    SetFieldStmt,
    DropFieldStmt,
    AlterTargetStmt,
    CreateConcreteConstraintStmt,
    AlterConcreteConstraintStmt,
    DropConcreteConstraintStmt,
    opt=False
)

class AlterConcreteLinkPropertyStmt(Nonterm):
    def reduce_AlterLinkProperty(self, *kids):
        r"""%reduce \
            ALTER LINKPROPERTY NodeName \
            AlterConcreteLinkPropertyCommandsBlock \
        """
        self.val = qlast.AlterConcreteLinkPropertyNode(
            name=kids[2].val,
            commands=kids[3].val
        )


#
# ALTER LINK ... { DROP LINK PROPERTY
#

class DropConcreteLinkPropertyStmt(Nonterm):
    def reduce_DropLinkProperty(self, *kids):
        r"""%reduce \
            DROP LINKPROPERTY NodeName \
        """
        self.val = qlast.DropConcreteLinkPropertyNode(
            name=kids[2].val
        )


#
# CREATE LINK
#

commands_block(
    'CreateLink',
    SetFieldStmt,
    CreateConcreteConstraintStmt,
    CreateConcreteLinkPropertyStmt,
    CreateIndexStmt,
    CreateLocalPolicyStmt
)


class CreateLinkStmt(Nonterm):
    def reduce_CreateLink(self, *kids):
        r"""%reduce \
            OptAliasBlock \
            CREATE LINK NodeName OptInheriting \
            OptCreateLinkCommandsBlock \
        """
        self.val = qlast.CreateLinkNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
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
    AlterInheriting,
    CreateConcreteConstraintStmt,
    AlterConcreteConstraintStmt,
    DropConcreteConstraintStmt,
    CreateConcreteLinkPropertyStmt,
    AlterConcreteLinkPropertyStmt,
    DropConcreteLinkPropertyStmt,
    CreateIndexStmt,
    DropIndexStmt,
    CreateLocalPolicyStmt,
    AlterLocalPolicyStmt,
    DropLocalPolicyStmt,
    opt=False
)


class AlterLinkStmt(Nonterm):
    def reduce_CreateLink(self, *kids):
        r"""%reduce \
            OptAliasBlock \
            ALTER LINK NodeName \
            AlterLinkCommandsBlock \
        """
        self.val = qlast.AlterLinkNode(
            namespaces=kids[0].val[0],
            aliases=kids[0].val[1],
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
    DropConcreteLinkPropertyStmt,
    DropIndexStmt,
    DropLocalPolicyStmt,
)


class DropLinkStmt(Nonterm):
    def reduce_DropLink(self, *kids):
        r"""%reduce \
            OptAliasBlock DROP LINK NodeName OptDropLinkCommandsBlock \
        """
        self.val = qlast.DropLinkNode(
            namespaces=kids[0].val[0],
            aliases=kids[0].val[1],
            name=kids[3].val,
            commands=kids[4].val
        )


#
# CREATE CONCEPT ... { CREATE LINK
#

commands_block(
    'CreateConcreteLink',
    SetFieldStmt,
    CreateConcreteConstraintStmt,
    CreateConcreteLinkPropertyStmt,
    CreateLocalPolicyStmt
)


class CreateConcreteLinkStmt(Nonterm):
    def reduce_CreateRegularRequiredLink(self, *kids):
        r"""%reduce \
            CREATE REQUIRED LINK NodeName \
            TO NodeNameList OptCreateConcreteLinkCommandsBlock \
        """
        self.val = qlast.CreateConcreteLinkNode(
            name=kids[3].val,
            is_required=True,
            targets=kids[5].val,
            commands=kids[6].val
        )

    def reduce_CreateRegularLink(self, *kids):
        r"""%reduce \
            CREATE LINK NodeName \
            TO NodeNameList OptCreateConcreteLinkCommandsBlock \
        """
        self.val = qlast.CreateConcreteLinkNode(
            name=kids[2].val,
            is_required=False,
            targets=kids[4].val,
            commands=kids[5].val
        )

    def reduce_CREATE_LINK_NodeName_AS_Expr(self, *kids):
        self.val = qlast.CreateConcreteLinkNode(
            name=kids[2].val,
            targets=[kids[4].val]
        )


commands_block(
    'AlterConcreteLink',
    RenameStmt,
    SetFieldStmt,
    DropFieldStmt,
    AlterTargetStmt,
    AlterInheriting,
    CreateConcreteConstraintStmt,
    AlterConcreteConstraintStmt,
    DropConcreteConstraintStmt,
    CreateConcreteLinkPropertyStmt,
    AlterConcreteLinkPropertyStmt,
    DropConcreteLinkPropertyStmt,
    CreateLocalPolicyStmt,
    AlterLocalPolicyStmt,
    DropLocalPolicyStmt,
    opt=False
)


class AlterConcreteLinkStmt(Nonterm):
    def reduce_AlterLink(self, *kids):
        r"""%reduce \
            ALTER LINK NodeName AlterConcreteLinkCommandsBlock \
        """
        self.val = qlast.AlterConcreteLinkNode(
            name=kids[2].val,
            commands=kids[3].val
        )


commands_block(
    'DropConcreteLink',
    DropConcreteConstraintStmt,
    DropConcreteLinkPropertyStmt,
    DropLocalPolicyStmt,
)


class DropConcreteLinkStmt(Nonterm):
    def reduce_DropLink(self, *kids):
        r"""%reduce \
            DROP LINK NodeName OptDropConcreteLinkCommandsBlock \
        """
        self.val = qlast.DropConcreteLinkNode(
            name=kids[2].val,
            commands=kids[3].val
        )


#
# CREATE CONCEPT
#

commands_block(
    'CreateConcept',
    SetFieldStmt,
    CreateConcreteLinkStmt,
    CreateIndexStmt
)


class CreateConceptStmt(Nonterm):
    def reduce_CreateAbstractConceptStmt(self, *kids):
        r"""%reduce \
            OptAliasBlock CREATE ABSTRACT CONCEPT NodeName \
            OptInheriting OptCreateConceptCommandsBlock \
        """
        self.val = qlast.CreateConceptNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[4].val,
            bases = kids[5].val,
            is_abstract = True,
            commands = kids[6].val
        )

    def reduce_CreateRegularConceptStmt(self, *kids):
        r"""%reduce \
            OptAliasBlock CREATE CONCEPT NodeName \
            OptInheriting OptCreateConceptCommandsBlock \
        """
        self.val = qlast.CreateConceptNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[3].val,
            bases = kids[4].val,
            is_abstract = False,
            commands = kids[5].val
        )


#
# ALTER CONCEPT
#

commands_block(
    'AlterConcept',
    RenameStmt,
    SetFieldStmt,
    DropFieldStmt,
    AlterInheriting,
    CreateConcreteLinkStmt,
    AlterConcreteLinkStmt,
    DropConcreteLinkStmt,
    CreateIndexStmt,
    DropIndexStmt,
    opt=False
)


class AlterConceptStmt(Nonterm):
    def reduce_AlterConceptStmt(self, *kids):
        r"""%reduce \
            OptAliasBlock ALTER CONCEPT NodeName \
            AlterConceptCommandsBlock \
        """
        self.val = qlast.AlterConceptNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[3].val,
            commands = kids[4].val
        )


#
# DROP CONCEPT
#

commands_block(
    'DropConcept',
    DropConcreteLinkStmt,
    DropConcreteConstraintStmt,
    DropIndexStmt
)


class DropConceptStmt(Nonterm):
    def reduce_DropConcept(self, *kids):
        r"""%reduce \
            OptAliasBlock DROP CONCEPT NodeName OptDropConceptCommandsBlock \
        """
        self.val = qlast.DropConceptNode(
            namespaces=kids[0].val[0],
            aliases=kids[0].val[1],
            name=kids[3].val,
            commands=kids[4].val
        )


#
# CREATE EVENT
#
class CreateEventStmt(Nonterm):
    def reduce_OptAliasBlock_CREATE_EVENT_NodeName_OptInheriting_OptCreateCommandsBlock(
                                                        self, *kids):
        self.val = qlast.CreateEventNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[3].val,
            bases=kids[4].val,
            commands=kids[5].val
        )


#
# CREATE MODULE
#
class CreateModuleStmt(Nonterm):
    def reduce_OptAliasBlock_CREATE_MODULE_NodeName_OptCreateCommandsBlock(
                                                self, *kids):
        self.val = qlast.CreateModuleNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[3].val,
            commands = kids[4].val
        )


#
# ALTER MODULE
#
class AlterModuleStmt(Nonterm):
    def reduce_OptAliasBlock_ALTER_MODULE_NodeName_AlterCommandsBlock(
                                                self, *kids):
        self.val = qlast.AlterModuleNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[3].val,
            commands = kids[4].val
        )


#
# DROP MODULE
#
class DropModuleStmt(Nonterm):
    def reduce_OptAliasBlock_DROP_MODULE_NodeName(self, *kids):
        self.val = qlast.DropModuleNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            name = kids[3].val
        )
