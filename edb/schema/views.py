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


from __future__ import annotations

from edb.edgeql import ast as qlast

from . import scalars as s_scalars
from . import annos as s_anno
from . import objtypes as s_objtypes
from . import delta as sd
from . import types_delta as s_types_d


class ViewCommandContext(sd.ObjectCommandContext,
                         s_anno.AnnotationSubjectCommandContext):
    pass


class ViewCommand(s_types_d.TypeCommand, context_class=ViewCommandContext):

    _scalar_cmd_map = {
        qlast.CreateView: s_scalars.CreateScalarType,
        qlast.AlterView: s_scalars.AlterScalarType,
        qlast.DropView: s_scalars.DeleteScalarType,
    }

    _objtype_cmd_map = {
        qlast.CreateView: s_objtypes.CreateObjectType,
        qlast.AlterView: s_objtypes.AlterObjectType,
        qlast.DropView: s_objtypes.DeleteObjectType,
    }

    @classmethod
    def _command_for_ast_node(cls, astnode, schema, context):
        modaliases = cls._modaliases_from_ast(schema, astnode, context)

        with context(ViewCommandContext(schema,
                                        op=None, modaliases=modaliases)):

            classname = cls._classname_from_ast(schema, astnode, context)

            if isinstance(astnode, qlast.CreateView):
                expr = cls._get_view_expr(astnode)
                ir = cls._compile_view_expr(expr, classname, schema, context)
                scls = ir.stype
            else:
                scls = schema.get(classname)

            if isinstance(scls, s_scalars.ScalarType):
                mapping = cls._scalar_cmd_map
            else:
                mapping = cls._objtype_cmd_map

            return mapping[type(astnode)]


class CreateView(ViewCommand):
    astnode = qlast.CreateView


class RenameView(ViewCommand):
    pass


class AlterView(ViewCommand):
    astnode = qlast.AlterView


class DeleteView(ViewCommand):
    astnode = qlast.DropView
