#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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


from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import errors as ql_errors

from . import delta as sd
from . import inheriting
from . import modules
from . import named
from . import schema as s_schema
from . import types as s_types


class Node(inheriting.InheritingObject, s_types.Type):
    def material_type(self):
        t = self
        while t.is_view():
            t = t.bases[0]
        return t

    def derive_subtype(self, schema, *, name: str) -> s_types.Type:
        return type(self).create_with_inheritance(
            schema, name=name, bases=[self])

    def peel_view(self):
        if self.is_view():
            return self.bases[0]
        else:
            return self


class NodeCommandContext:
    # context mixin
    pass


class NodeCommand(named.NamedObjectCommand):
    @classmethod
    def _maybe_get_view_expr(cls, astnode):
        for subcmd in astnode.commands:
            if (isinstance(subcmd, qlast.CreateAttributeValue) and
                    subcmd.name.name == 'expr'):
                return subcmd.value

    @classmethod
    def _get_view_expr(cls, astnode):
        expr = cls._maybe_get_view_expr(astnode)
        if expr is None:
            raise ql_errors.EdgeQLError(
                f'Missing required view expression', context=astnode.context)
        return expr

    @classmethod
    def _compile_view_expr(cls, expr, classname, schema, context):
        from edb.lang.edgeql import compiler as qlcompiler

        ir = context.get_cached((expr, classname))
        if ir is None:
            if not isinstance(expr, qlast.Statement):
                expr = qlast.SelectQuery(result=expr)
            ir = qlcompiler.compile_ast_to_ir(
                expr, schema, derived_target_module=classname.module,
                result_view_name=classname, modaliases=context.modaliases)
            context.cache_value((expr, classname), ir)

        return ir

    @classmethod
    def _view_schema_from_ir(cls, view_name, ir, schema):
        vschema = s_schema.Schema()
        module_shell = modules.Module(name=view_name.module)
        vschema = vschema.add_module(module_shell)

        if ir is not None:
            for view in ir.views.values():
                vschema = vschema.add(view)

            for view in ir.views.values():
                if not hasattr(view, 'own_pointers'):
                    continue

                for vptr in view.own_pointers.values():
                    vptr.target = vptr.target.material_type()
                    vschema = vschema.add(vptr)
                    if not hasattr(vptr, 'own_pointers'):
                        continue
                    for vlprop in vptr.own_pointers.values():
                        vlprop.target = vlprop.target.material_type()
                        vschema = vschema.add(vlprop)

        return vschema

    @classmethod
    def _handle_view_op(cls, cmd, astnode, context, schema):
        from edb.lang.ir import utils as irutils

        view_expr = cls._maybe_get_view_expr(astnode)
        if view_expr is not None:
            ir = cls._compile_view_expr(view_expr, cmd.classname,
                                        schema, context)
            rt = irutils.infer_type(ir, schema)

            if rt.is_view():
                # The expression itself declares a view, use it.
                rt.name = cmd.classname

            view_schema = cls._view_schema_from_ir(cmd.classname, ir, schema)
            if isinstance(astnode, qlast.AlterObjectType):
                prev = schema.get(cmd.classname)
                prev_ir = cls._compile_view_expr(
                    prev.expr, cmd.classname, schema, context)
                prev_view_schema = cls._view_schema_from_ir(
                    cmd.classname, prev_ir, schema)
            else:
                prev_view_schema = cls._view_schema_from_ir(
                    cmd.classname, None, schema)

            derived_delta = sd.delta_schemas(
                view_schema, prev_view_schema, include_derived=True)

            if rt.is_view():
                for op in list(derived_delta.get_subcommands()):
                    if op.classname == cmd.classname:
                        for subop in op.get_subcommands():
                            if isinstance(subop, sd.AlterObjectProperty):
                                cmd.discard_attribute(subop.property)
                            cmd.add(subop)

                        derived_delta.discard(op)

            cmd.update(derived_delta.get_subcommands())
            cmd.discard_attribute('view_type')
            cmd.add(sd.AlterObjectProperty(
                property='view_type', new_value=s_types.ViewType.Select))

        return cmd
