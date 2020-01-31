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

"""Mypy plugin to provide support for schema objects."""

from __future__ import annotations

from typing import *  # NoQA

from mypy import exprtotype
import mypy.plugin as mypy_plugin
from mypy import nodes
from mypy import types
from mypy.plugins import common as mypy_helpers
from mypy.server import trigger as mypy_trigger


METADATA_KEY = 'edbplugin'
STRUCT_BASE_METACLASSES = {
    'edb.common.struct.StructMeta',
}
STRUCT_FIELD_MAKERS = {'edb.common.struct.Field'}

SCHEMA_BASE_METACLASSES = {
    'edb.schema.objects.ObjectMeta',
    'edb.schema.types.SchemaCollectionMeta',
}
SCHEMA_FIELD_MAKERS = {'edb.schema.objects.SchemaField'}


def plugin(version: str):
    return EDBPlugin


class EDBPlugin(mypy_plugin.Plugin):

    def get_base_class_hook(self, fullname: str):
        if fullname.startswith('edb.'):
            return self.handle_schema_class

    def handle_schema_class(self, ctx: mypy_plugin.ClassDefContext):
        mcls = ctx.cls.info.metaclass_type
        if not mcls:
            return

        if any(c.fullname in SCHEMA_BASE_METACLASSES for c in mcls.type.mro):
            transformer = SchemaClassTransformer(
                ctx,
                field_makers=SCHEMA_FIELD_MAKERS,
            )
        elif any(c.fullname in STRUCT_BASE_METACLASSES for c in mcls.type.mro):
            transformer = StructTransformer(
                ctx,
                field_makers=STRUCT_FIELD_MAKERS,
            )
        else:
            return

        transformer.transform()


class DeferException(Exception):
    pass


class Field(NamedTuple):

    name: str
    is_optional: bool
    line: int
    column: int

    def serialize(self) -> nodes.JsonDict:
        return {
            'name': self.name,
            'is_optional': self.is_optional,
            'line': self.line,
            'column': self.column,
        }

    @classmethod
    def deserialize(
        cls,
        semanal,
        data: nodes.JsonDict,
    ) -> Field:
        return cls(
            name=data['name'],
            is_optional=data['is_optional'],
            line=data['line'],
            column=data['column'],
        )


class BaseStructTransformer:

    def __init__(
        self,
        ctx: mypy_plugin.ClassDefContext,
        field_makers: FrozenSet[str],
    ) -> None:
        self._ctx = ctx
        self._field_makers = field_makers

    def _lookup_type(self, fullname: str) -> types.Type:
        ctx = self._ctx

        type_sym = ctx.api.lookup_fully_qualified_or_none(fullname)

        if type_sym is None:
            raise DeferException

        t: types.Type

        if isinstance(type_sym.node, nodes.TypeInfo):
            from mypy.typevars import fill_typevars
            t = fill_typevars(type_sym.node)
        elif type_sym.type:
            t = type_sym.type
        else:
            ctx.api.fail(f'cannot find {fullname}', ctx.cls)

        return t

    def _collect_fields(self) -> List[Field]:
        """Collect all fields declared in a schema class and its ancestors."""

        ctx = self._ctx
        cls = self._ctx.cls

        fields: List[Field] = []

        known_fields: Set[str] = set()

        for stmt in cls.defs.body:
            if not isinstance(stmt, nodes.AssignmentStmt):
                continue

            lhs = stmt.lvalues[0]
            rhs = stmt.rvalue

            if not isinstance(rhs, nodes.CallExpr):
                continue

            if (isinstance(rhs.callee, nodes.RefExpr)
                    and rhs.callee.fullname in self._field_makers):
                field = self._field_from_field_def(stmt, lhs, rhs)
                fields.append(field)

        all_fields = fields.copy()
        for ancestor_info in cls.info.mro[1:-1]:
            metadata = ancestor_info.metadata.get(self._get_metadata_key())
            if metadata is None:
                continue
            elif not metadata.get('processed'):
                raise DeferException

            ancestor_fields = []

            ctx.api.add_plugin_dependency(
                mypy_trigger.make_wildcard_trigger(ancestor_info.fullname))

            for name, data in metadata['fields'].items():
                if name not in known_fields:
                    field = Field.deserialize(ctx.api, data)

                    known_fields.add(name)
                    ancestor_fields.append(field)
            all_fields = ancestor_fields + all_fields

        return all_fields

    def _field_from_field_def(self, stmt, lhs, call) -> Field:
        ctx = self._ctx
        type_arg = call.args[0]

        try:
            un_type = exprtotype.expr_to_unanalyzed_type(type_arg)
        except exprtotype.TypeTranslationError:
            ctx.api.fail('Cannot resolve schema field type', type_arg)
        else:
            ftype = ctx.api.anal_type(un_type)
            if ftype is None:
                raise DeferException

        is_optional = self._is_optional(call)
        if is_optional:
            ftype = types.UnionType.make_union(
                [ftype, types.NoneType()],
                line=ftype.line,
                column=ftype.column,
            )

        lhs.node.type = ftype

        return Field(
            name=lhs.name,
            is_optional=is_optional,
            line=stmt.line,
            column=stmt.column,
        )

    def _is_optional(self, call) -> bool:
        for (n, v) in zip(call.arg_names, call.args):
            if (n == 'default'
                    and (isinstance(v, nodes.NameExpr)
                         and v.fullname == 'builtins.None')):
                return True
        else:
            return False

    def _get_metadata_key(self) -> str:
        return f'{METADATA_KEY}%%{type(self).__name__}'


class StructTransformer(BaseStructTransformer):

    def transform(self):
        ctx = self._ctx
        metadata_key = self._get_metadata_key()
        metadata = ctx.cls.info.metadata.get(metadata_key)
        if not metadata:
            ctx.cls.info.metadata[metadata_key] = metadata = {}

        metadata['processing'] = True

        if metadata.get('processed'):
            return

        try:
            fields = self._collect_fields()
        except DeferException:
            ctx.api.defer()
            return None

        cls_info = ctx.cls.info

        for f in fields:
            finfo = cls_info.names.get(f.name)
            if finfo is None:
                continue

            node = finfo.node
            node.is_initialized_in_class = False

        metadata['fields'] = {f.name: f.serialize() for f in fields}
        metadata['processed'] = True

    def _field_from_field_def(self, stmt, lhs, rhs):
        field = super()._field_from_field_def(stmt, lhs, rhs)

        lhs.is_inferred_def = False
        stmt.rvalue = nodes.CastExpr(
            typ=lhs.node.type,
            expr=rhs,
        )
        stmt.rvalue.line = rhs.line
        stmt.rvalue.column = rhs.column

        return field


class SchemaClassTransformer(BaseStructTransformer):

    def transform(self):
        ctx = self._ctx
        metadata_key = self._get_metadata_key()
        metadata = ctx.cls.info.metadata.get(metadata_key)
        if not metadata:
            ctx.cls.info.metadata[metadata_key] = metadata = {}

        metadata['processing'] = True

        if metadata.get('processed'):
            return

        try:
            fields = self._collect_fields()
            schema_t = self._lookup_type('edb.schema.schema.Schema')
        except DeferException:
            ctx.api.defer()
            return None

        cls_info = ctx.cls.info

        for f in fields:
            ftype = cls_info.get(f.name).type
            if ftype is None or cls_info.get(f'get_{f.name}') is not None:
                # The class is already doing something funny with the
                # field or the accessor, so ignore it.
                continue

            if f.is_optional:
                ftype = types.UnionType.make_union(
                    [ftype, types.NoneType()],
                    line=ftype.line, column=ftype.column,
                )

            mypy_helpers.add_method(
                ctx,
                name=f'get_{f.name}',
                args=[
                    nodes.Argument(
                        variable=nodes.Var(
                            name='schema',
                            type=schema_t,
                        ),
                        type_annotation=schema_t,
                        initializer=None,
                        kind=nodes.ARG_POS,
                    ),
                ],
                return_type=ftype,
            )

        metadata['fields'] = {f.name: f.serialize() for f in fields}
        metadata['processed'] = True
