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

from typing import Optional, AbstractSet, List, Set, NamedTuple

from mypy import exprtotype
import mypy.plugin as mypy_plugin
from mypy import mro
from mypy import nodes
from mypy import options as mypy_options
from mypy import types
from mypy import typevars as mypy_typevars
from mypy import semanal_shared as mypy_semanal
from mypy.plugins import common as mypy_helpers
from mypy.server import trigger as mypy_trigger


METADATA_KEY = 'edbplugin'

AST_BASE_CLASSES = {
    'edb.common.ast.base.AST',
}

STRUCT_BASE_METACLASSES = {
    'edb.common.struct.StructMeta',
}

SCHEMA_BASE_METACLASSES = {
    'edb.schema.objects.ObjectMeta',
    'edb.schema.types.SchemaCollectionMeta',
}

ADAPTER_METACLASS = 'edb.common.adapter.Adapter'


def plugin(version: str):
    return EDBPlugin


class EDBPlugin(mypy_plugin.Plugin):

    def get_base_class_hook(self, fullname: str):
        if fullname.startswith('edb.'):
            return self.handle_schema_class

    def handle_schema_class(self, ctx: mypy_plugin.ClassDefContext):
        mro = ctx.cls.info.mro
        mcls = ctx.cls.info.metaclass_type
        mcls_mro = mcls.type.mro if mcls else []

        transformers: List[BaseTransformer] = []

        if any(c.fullname in SCHEMA_BASE_METACLASSES for c in mcls_mro):
            transformers.append(
                SchemaClassTransformer(
                    ctx,
                    self.options,
                    field_makers={'edb.schema.objects.SchemaField'},
                )
            )
            transformers.append(
                StructTransformer(
                    ctx,
                    self.options,
                    field_makers={'edb.schema.objects.Field'},
                )
            )

        elif any(c.fullname in STRUCT_BASE_METACLASSES for c in mcls_mro):
            transformers.append(
                StructTransformer(
                    ctx,
                    self.options,
                    field_makers={'edb.common.struct.Field'},
                )
            )

        elif any(c.fullname in AST_BASE_CLASSES for c in mro):
            transformers.append(
                ASTClassTransformer(
                    ctx,
                    self.options,
                )
            )

        for transformer in transformers:
            transformer.transform()

    def get_customize_class_mro_hook(self, fullname: str):
        if fullname.startswith('edb.'):
            return self.maybe_update_mro

    def maybe_update_mro(self, ctx: mypy_plugin.ClassDefContext):
        info = ctx.cls.info
        mcls = info.metaclass_type
        if not mcls:
            # This is a deep hack. The MRO gets computed *before* we
            # know the metaclass, which is kind of weird, since I
            # think metaclass shenanigans are the whole point of
            # get_customize_class_mro_hook. If we defer it, though,
            # then when we get called again it will still be sitting
            # there.
            if ctx.cls.metaclass:
                ctx.api.defer()
            return

        # If the adapter class is in our metaclass MRO and we have an
        # adapts argument, add it to our bases and recompute the MRO.
        # This mirrors what the actual metaclass does.
        if (
            any(c.fullname == ADAPTER_METACLASS for c in mcls.type.mro)
            and (adapts := ctx.cls.keywords.get('adapts'))
            and isinstance(adapts, nodes.RefExpr)
        ):
            if not (
                isinstance(adapts, nodes.RefExpr)
                and isinstance(adapts.node, nodes.TypeInfo)
            ):
                ctx.api.fail('Invalid argument to adapts', ctx.cls)
                return
            typ = types.Instance(adapts.node, ())
            if typ not in info.bases:
                info.bases.append(typ)

            old_mro = info.mro
            info.mro = []
            try:
                mro.calculate_mro(
                    info, lambda: ctx.api.named_type('builtins.object', []))
            except mro.MroError:
                ctx.api.fail(
                    "Cannot determine consistent method resolution "
                    'order (MRO) for "%s"' % ctx.cls.name,
                    ctx.cls,
                )
                info.mro = old_mro


class DeferException(Exception):
    pass


class Field(NamedTuple):

    name: str
    has_explicit_accessor: bool
    has_default: bool
    line: int
    column: int
    type: types.Type

    def to_argument(self) -> nodes.Argument:
        result = nodes.Argument(
            variable=self.to_var(),
            type_annotation=self.type,
            initializer=None,
            kind=nodes.ARG_NAMED_OPT if self.has_default else nodes.ARG_NAMED,
        )

        return result

    def to_var(self) -> nodes.Var:
        return nodes.Var(self.name, self.type)

    def serialize(self) -> nodes.JsonDict:
        return {
            'name': self.name,
            'has_explicit_accessor': self.has_explicit_accessor,
            'has_default': self.has_default,
            'line': self.line,
            'column': self.column,
            'type': self.type.serialize(),
        }

    @classmethod
    def deserialize(
        cls,
        api,
        data: nodes.JsonDict,
    ) -> Field:
        return cls(
            name=data['name'],
            has_explicit_accessor=data['has_explicit_accessor'],
            has_default=data['has_default'],
            line=data['line'],
            column=data['column'],
            type=mypy_helpers.deserialize_and_fixup_type(data['type'], api),
        )


class BaseTransformer:

    def __init__(
        self,
        ctx: mypy_plugin.ClassDefContext,
        options: mypy_options.Options,
    ) -> None:
        self._ctx = ctx
        self._options = options

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
            fields = self._transform()
        except DeferException:
            ctx.api.defer()
            return None

        metadata['fields'] = {f.name: f.serialize() for f in fields}
        metadata['processed'] = True

    def _transform(self) -> List[Field]:
        raise NotImplementedError

    def _field_from_field_def(
        self,
        stmt: nodes.AssignmentStmt,
        name: nodes.NameExpr,
        sym: nodes.SymbolTableNode,
    ) -> Optional[Field]:
        raise NotImplementedError

    def _collect_fields(self) -> List[Field]:
        """Collect all fields declared in a class and its ancestors."""

        cls = self._ctx.cls

        fields: List[Field] = []

        known_fields: Set[str] = set()

        for stmt in cls.defs.body:
            if not isinstance(stmt, nodes.AssignmentStmt):
                continue

            lhs = stmt.lvalues[0]
            if not isinstance(lhs, nodes.NameExpr):
                continue

            sym = cls.info.names.get(lhs.name)
            if sym is None or isinstance(sym.node, nodes.PlaceholderNode):
                # Not resolved yet?
                continue

            node = sym.node
            assert isinstance(node, nodes.Var)

            if node.is_classvar:
                # Disregard ClassVar stuff
                continue

            field = self._field_from_field_def(stmt, lhs, sym)
            if field is not None:
                fields.append(field)
                known_fields.add(field.name)

        return self._get_inherited_fields(known_fields) + fields

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

    def _get_metadata_key(self) -> str:
        return f'{METADATA_KEY}%%{type(self).__name__}'

    def _has_explicit_field_accessor(self, fieldname: str) -> bool:
        cls = self._ctx.cls
        accessor = cls.info.names.get(f'get_{fieldname}')
        return accessor is not None and not accessor.plugin_generated

    def _get_inherited_fields(self, self_fields: Set[str]) -> List[Field]:
        ctx = self._ctx
        cls = ctx.cls
        all_fields: List[Field] = []
        known_fields = set(self_fields)

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
                    if self._has_explicit_field_accessor(name):
                        data = dict(data)
                        data['has_explicit_accessor'] = True
                    field = Field.deserialize(ctx.api, data)

                    known_fields.add(name)
                    ancestor_fields.append(field)
            all_fields = ancestor_fields + all_fields

        return all_fields

    def _synthesize_init(self, fields: List[Field]) -> None:
        ctx = self._ctx
        cls_info = ctx.cls.info

        # If our self type has placeholders (probably because of type
        # var bounds), defer. If we skip deferring and stick something
        # in our symbol table anyway, we'll get in trouble.  (Arguably
        # plugins.common ought to help us with this, but oh well.)
        self_type = mypy_typevars.fill_typevars(cls_info)
        if mypy_semanal.has_placeholder(self_type):
            raise DeferException

        if (
            (
                '__init__' not in cls_info.names
                or cls_info.names['__init__'].plugin_generated
            ) and fields
        ):
            mypy_helpers.add_method(
                ctx,
                '__init__',
                self_type=self_type,
                args=[field.to_argument() for field in fields],
                return_type=types.NoneType(),
            )


class BaseStructTransformer(BaseTransformer):

    def __init__(
        self,
        ctx: mypy_plugin.ClassDefContext,
        options: mypy_options.Options,
        field_makers: AbstractSet[str],
    ) -> None:
        super().__init__(ctx, options)
        self._field_makers = field_makers

    def _field_from_field_def(
        self,
        stmt: nodes.AssignmentStmt,
        name: nodes.NameExpr,
        sym: nodes.SymbolTableNode,
    ) -> Optional[Field]:
        ctx = self._ctx

        rhs = stmt.rvalue

        if isinstance(rhs, nodes.CastExpr):
            rhs = rhs.expr

        if not isinstance(rhs, nodes.CallExpr):
            return None

        fdef = rhs.callee

        ftype = None
        if (
            isinstance(fdef, nodes.IndexExpr)
            and isinstance(fdef.analyzed, nodes.TypeApplication)
        ):
            # Explicitly typed Field declaration
            ctor = fdef.analyzed.expr
            if len(fdef.analyzed.types) > 1:
                ctx.api.fail('too many type arguments to Field', fdef)
            ftype = fdef.analyzed.types[0]
        else:
            ctor = fdef
            ftype = None

        if (
            not isinstance(ctor, nodes.RefExpr)
            or ctor.fullname not in self._field_makers
        ):
            return None

        type_arg = rhs.args[0]

        deflt = self._get_default(rhs)

        if ftype is None:
            try:
                un_type = exprtotype.expr_to_unanalyzed_type(
                    type_arg,
                    options=self._options,
                )
            except exprtotype.TypeTranslationError:
                ctx.api.fail('Cannot resolve schema field type', type_arg)
            else:
                ftype = ctx.api.anal_type(un_type)
            if ftype is None:
                raise DeferException

            is_optional = (
                isinstance(deflt, nodes.NameExpr)
                and deflt.fullname == 'builtins.None'
            )
            if is_optional:
                ftype = types.UnionType.make_union(
                    [ftype, types.NoneType()],
                    line=ftype.line,
                    column=ftype.column,
                )

        assert isinstance(name.node, nodes.Var)
        name.node.type = ftype

        return Field(
            name=name.name,
            has_explicit_accessor=self._has_explicit_field_accessor(name.name),
            has_default=deflt is not None,
            line=stmt.line,
            column=stmt.column,
            type=ftype,
        )

    def _get_default(self, call) -> Optional[nodes.Expression]:
        for (n, v) in zip(call.arg_names, call.args):
            if n == 'default':
                return v
        else:
            return None


class StructTransformer(BaseStructTransformer):

    def _transform(self) -> List[Field]:
        fields = self._collect_fields()
        self._synthesize_init(fields)
        return fields

    def _field_from_field_def(
        self,
        stmt: nodes.AssignmentStmt,
        name: nodes.NameExpr,
        sym: nodes.SymbolTableNode,
    ):
        field = super()._field_from_field_def(stmt, name, sym)
        if field is None:
            return None
        else:
            assert isinstance(sym.node, nodes.Var)
            sym.node.is_initialized_in_class = False

            name.is_inferred_def = False

            rhs = stmt.rvalue
            if not isinstance(rhs, nodes.CastExpr):
                stmt.rvalue = nodes.CastExpr(
                    typ=field.type,
                    expr=rhs,
                )
                stmt.rvalue.line = rhs.line
                stmt.rvalue.column = rhs.column

            return field


class SchemaClassTransformer(BaseStructTransformer):

    def _transform(self) -> List[Field]:
        ctx = self._ctx
        fields = self._collect_fields()
        schema_t = self._lookup_type('edb.schema.schema.Schema')

        for f in fields:
            if f.has_explicit_accessor:
                continue

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
                return_type=f.type,
            )

        return fields


class ASTClassTransformer(BaseTransformer):

    def _transform(self) -> List[Field]:
        fields = self._collect_fields()
        self._synthesize_init(fields)
        return fields

    def _field_from_field_def(
        self,
        stmt: nodes.AssignmentStmt,
        name: nodes.NameExpr,
        sym: nodes.SymbolTableNode,
    ) -> Optional[Field]:

        if sym.type is None:
            # If the assignment has a type annotation but the symbol
            # doesn't yet, we need to defer
            if stmt.type:
                raise DeferException
            # No type annotation?
            return None
        else:
            has_default = not isinstance(stmt.rvalue, nodes.TempNode)

            if not has_default:
                sym.implicit = True

            return Field(
                name=name.name,
                has_default=has_default,
                line=stmt.line,
                column=stmt.column,
                type=sym.type,
                has_explicit_accessor=False,
            )
