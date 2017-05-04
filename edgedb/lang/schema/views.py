##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import compiler as qlcompiler

from edgedb.lang.ir import utils as irutils

from . import attributes
from . import concepts
from . import constraints
from . import delta as sd
from . import expr
from . import links
from . import modules
from . import named
from . import nodes
from . import objects as so
from . import schema as s_schema
from . import sources


class ViewCommandContext(sd.ClassCommandContext,
                         attributes.AttributeSubjectCommandContext,
                         nodes.NodeCommandContext):
    pass


class ViewCommand(constraints.ConsistencySubjectCommand,
                  attributes.AttributeSubjectCommand,
                  nodes.NodeCommand):
    context_class = ViewCommandContext

    @classmethod
    def _get_metaclass(cls):
        return View

    def _create_innards(self, schema, context):
        super()._create_innards(schema, context)

        for op in self.get_subcommands():
            op.apply(schema, context=context)

    def _alter_innards(self, schema, context, scls):
        super()._alter_innards(schema, context, scls)

        for op in self.get_subcommands():
            op.apply(schema, context=context)

    def _delete_innards(self, schema, context, scls):
        super()._delete_innards(schema, context, scls)

        for op in self.get_subcommands():
            op.apply(schema, context=context)

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        for subcmd in astnode.commands:
            if (isinstance(subcmd, qlast.CreateAttributeValue) and
                    subcmd.name.name == 'expr'):
                # compile the expression for validation
                ir = qlcompiler.compile_ast_to_ir(
                    subcmd.value, schema,
                    derived_target_module=cmd.classname.module)
                view_schema = _view_schema_from_ir(cmd.classname, ir, schema)

                if isinstance(astnode, qlast.AlterView):
                    prev = schema.get(cmd.classname)
                    prev_ir = qlcompiler.compile_to_ir(
                        prev.expr, schema,
                        derived_target_module=cmd.classname.module)
                    prev_view_schema = _view_schema_from_ir(
                        cmd.classname, prev_ir, schema)

                else:
                    prev_view_schema = _view_schema_from_ir(
                        cmd.classname, None, schema)

                derived_delta = sd.delta_schemas(
                    view_schema, prev_view_schema, include_derived=True)

                cmd.update(derived_delta.get_subcommands())
                break

        return cmd


class CreateView(ViewCommand, named.CreateNamedClass):
    astnode = qlast.CreateView


class RenameView(ViewCommand, named.RenameNamedClass):
    pass


class AlterView(ViewCommand, named.AlterNamedClass):
    astnode = qlast.AlterView


class DeleteView(ViewCommand, named.DeleteNamedClass):
    astnode = qlast.DropView

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        view = schema.get(cmd.classname)
        view_ir = qlcompiler.compile_to_ir(
            view.expr, schema,
            derived_target_module=cmd.classname.module)
        view_schema = _view_schema_from_ir(
            cmd.classname, view_ir, schema)

        new_view_schema = _view_schema_from_ir(
            cmd.classname, None, schema)

        derived_delta = sd.delta_schemas(
            new_view_schema, view_schema, include_derived=True)

        cmd.update(derived_delta.get_subcommands())

        return cmd


class View(sources.Source, nodes.Node, attributes.AttributeSubject):
    _type = 'view'

    expr = so.Field(expr.ExpressionText, default=None,
                    coerce=True, compcoef=0.909)

    result_type = so.Field(so.NodeClass, None, compcoef=0.833)

    delta_driver = sd.DeltaDriver(
        create=CreateView,
        alter=AlterView,
        rename=RenameView,
        delete=DeleteView
    )

    def copy(self):
        result = super().copy()
        result.expr = self.expr
        return result

    @classmethod
    def get_pointer_class(cls):
        return links.Link


def _view_schema_from_ir(view_name, ir, schema):
    vschema = s_schema.Schema()
    module_shell = modules.Module(name=view_name.module)
    vschema.add_module(module_shell)

    if ir is not None:
        ir_set = ir.result

        computables = _get_set_computables(ir_set)
        if computables:
            view = View(name=view_name)
            source = ir_set.scls.derive(vschema, view, add_to_schema=True,
                                        mark_derived=True)
            _add_derived_to_schema(source, computables, vschema)

    return vschema


def _get_set_computables(ir_set):
    computables = []

    if irutils.is_subquery_set(ir_set):
        ir_set = ir_set.expr.result

    if isinstance(ir_set.scls, concepts.Concept) and ir_set.shape:
        for el in ir_set.shape:
            if el.expr is not None:
                computables.append(el)

    return computables


def _add_derived_to_schema(source, computables, schema):
    for ir_set in computables:
        ptr = ir_set.rptr.ptrcls

        target_computables = _get_set_computables(ir_set.expr.result)
        if target_computables:
            target = ptr.target.derive(schema, source, ptr.shortname,
                                       add_to_schema=True, mark_derived=True)
            _add_derived_to_schema(target, target_computables, schema)
        else:
            target = ptr.target

        derived = ptr.derive_copy(schema, source, target,
                                  add_to_schema=True, mark_derived=True)
        source.add_pointer(derived)
