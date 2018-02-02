##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.edgeql import ast as qlast

from edgedb.lang.ir import utils as irutils

from . import atoms
from . import attributes
from . import concepts
from . import delta as sd
from . import nodes


class ViewCommandContext(sd.ClassCommandContext,
                         attributes.AttributeSubjectCommandContext,
                         nodes.NodeCommandContext):
    pass


class ViewCommand(nodes.NodeCommand, context_class=ViewCommandContext):

    _atom_cmd_map = {
        qlast.CreateView: atoms.CreateAtom,
        qlast.AlterView: atoms.AlterAtom,
        qlast.DropView: atoms.DeleteAtom,
    }

    _concept_cmd_map = {
        qlast.CreateView: concepts.CreateConcept,
        qlast.AlterView: concepts.AlterConcept,
        qlast.DropView: concepts.DeleteConcept,
    }

    @classmethod
    def _command_for_ast_node(cls, astnode, schema, context):
        classname = cls._classname_from_ast(astnode, context, schema)

        if isinstance(astnode, qlast.CreateView):
            expr = cls._get_view_expr(astnode)
            ir = cls._compile_view_expr(expr, classname, schema, context)
            scls = irutils.infer_type(ir, schema)
        else:
            scls = schema.get(classname)

        if isinstance(scls, atoms.Atom):
            mapping = cls._atom_cmd_map
        else:
            mapping = cls._concept_cmd_map

        return mapping[type(astnode)]


class CreateView(ViewCommand):
    astnode = qlast.CreateView


class RenameView(ViewCommand):
    pass


class AlterView(ViewCommand):
    astnode = qlast.AlterView


class DeleteView(ViewCommand):
    astnode = qlast.DropView
