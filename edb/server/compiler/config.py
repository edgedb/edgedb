#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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
from typing import Iterable, Mapping, Sequence, Type

import dataclasses
import datetime
import functools

import immutables

from edb import errors
from edb.common import typeutils
from edb.edgeql import ast as qlast
from edb.edgeql import parser as qlparser
from edb.edgeql import qltypes
from edb.edgeql import compiler as qlcompiler
from edb.ir import ast as irast
from edb.ir import staeval as ireval
from edb.server import config
from edb.schema import name as sn
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import schema as s_schema
from edb.schema import types as s_types
from edb.schema import utils as s_utils

ConfigInput = (
    str
    | int
    | float
    | bool
    | datetime.datetime
    | datetime.date
    | datetime.time
    | Sequence["ConfigInput"]
    | Mapping[str, "ConfigInput"]
    | None
)
ConfigObject = Mapping[str, ConfigInput]


@dataclasses.dataclass
class Context:
    schema: s_schema.Schema
    obj_type: s_objtypes.ObjectType
    qual_name: str
    options: qlcompiler.CompilerOptions

    def get_ptr(self, name: str) -> s_pointers.Pointer:
        un = sn.UnqualName(name)
        schema = self.schema
        ty = self.obj_type
        ancestors = ty.get_ancestors(schema).objects(schema)
        for t in (ty,) + ancestors:
            if (rv := t.maybe_get_ptr(schema, un)) is not None:
                return rv
        raise errors.ConfigurationError(
            f"{ty.get_shortname(schema)!s} does not have field: {name!r}"
        )

    def get_full_name(self, ptr: s_pointers.Pointer) -> str:
        return f"{self.qual_name}::{ptr.get_local_name(self.schema)}"

    def is_multi(self, ptr: s_pointers.Pointer) -> bool:
        return ptr.get_cardinality(self.schema).is_multi()

    def get_type(
        self, ptr: s_pointers.Pointer, *, type: Type[s_types.TypeT]
    ) -> s_types.TypeT:
        rv = ptr.get_target(self.schema)
        if not isinstance(rv, type):
            raise TypeError(f"{ptr!r}.target is not {type:r}")
        return rv

    def get_ref(self, ptr: s_pointers.Pointer) -> qlast.ObjectRef:
        ty = self.get_type(ptr, type=s_types.QualifiedType)
        ty_name = ty.get_shortname(self.schema)
        return qlast.ObjectRef(name=ty_name.name, module=ty_name.module)

    def cast(
        self, expr: qlast.Expr, *, ptr: s_pointers.Pointer
    ) -> qlast.TypeCast:
        return qlast.TypeCast(
            expr=expr,
            type=qlast.TypeName(maintype=self.get_ref(ptr)),
        )


@functools.singledispatch
def compile_input_to_ast(
    value: ConfigInput, *, ptr: s_pointers.Pointer, ctx: Context
) -> qlast.Expr:
    raise errors.ConfigurationError(
        f"unsupported input type {type(value)!r} for {ctx.get_full_name(ptr)}"
    )


@compile_input_to_ast.register
def compile_input_str(
    value: str, *, ptr: s_pointers.Pointer, ctx: Context
) -> qlast.Expr:
    if value.startswith("{{") and value.endswith("}}"):
        return qlparser.parse_fragment(value[2:-2])
    ty = ctx.get_type(ptr, type=s_types.QualifiedType)
    if ty.is_enum(ctx.schema):
        ty_name = ty.get_shortname(ctx.schema)
        return qlast.Path(
            steps=[
                qlast.ObjectRef(name=ty_name.name, module=ty_name.module),
                qlast.Ptr(name=value),
            ]
        )
    else:
        return ctx.cast(qlast.Constant.string(value), ptr=ptr)


@compile_input_to_ast.register
def compile_input_scalar(
    value: int | float | bool, *, ptr: s_pointers.Pointer, ctx: Context
) -> qlast.Expr:
    return ctx.cast(s_utils.const_ast_from_python(value), ptr=ptr)


@compile_input_to_ast.register(dict)
@compile_input_to_ast.register(immutables.Map)
def compile_input_mapping(
    value: Mapping[str, ConfigInput],
    *,
    ptr: s_pointers.Pointer,
    ctx: Context,
) -> qlast.Expr:
    if "_tname" in value:
        tname = value["_tname"]
        if not isinstance(tname, str):
            raise errors.ConfigurationError(
                f"type of `_tname` must be str, got: {type(tname)!r}"
            )
        obj_type = ctx.schema.get(tname, type=s_objtypes.ObjectType)
    else:
        try:
            obj_type = ctx.get_type(ptr, type=s_objtypes.ObjectType)
        except TypeError:
            raise errors.ConfigurationError(
                f"unsupported input type {type(value)!r} "
                f"for {ctx.get_full_name(ptr)}"
            )
    obj_name = obj_type.get_shortname(ctx.schema)
    new_ctx = Context(
        schema=ctx.schema,
        obj_type=obj_type,
        qual_name=ctx.get_full_name(ptr),
        options=ctx.options,
    )
    return qlast.InsertQuery(
        subject=qlast.ObjectRef(name=obj_name.name, module=obj_name.module),
        shape=list(compile_dict_to_shape(value, ctx=new_ctx).values()),
    )


def compile_dict_to_shape(
    values: Mapping[str, ConfigInput], *, ctx: Context
) -> dict[str, qlast.ShapeElement]:
    rv = {}
    for name, value in values.items():
        if name == "_tname":
            continue
        ptr = ctx.get_ptr(name)
        expr: qlast.Expr
        if ctx.is_multi(ptr) and not isinstance(value, str):
            if not typeutils.is_container(value) or isinstance(value, Mapping):
                raise errors.ConfigurationError(
                    f"{ctx.get_full_name(ptr)} must be a sequence, "
                    f"got type: {type(value)!r}"
                )
            assert isinstance(value, Iterable)
            expr = qlast.Set(
                elements=[
                    compile_input_to_ast(v, ptr=ptr, ctx=ctx) for v in value
                ]
            )
        else:
            expr = compile_input_to_ast(value, ptr=ptr, ctx=ctx)
        rv[name] = qlast.ShapeElement(
            expr=qlast.Path(steps=[qlast.Ptr(name=name)]), compexpr=expr
        )
    return rv


def compile_ast_to_operation(
    obj_name: str,
    field_name: str,
    expr: qlast.Expr,
    *,
    schema: s_schema.Schema,
    options: qlcompiler.CompilerOptions,
    allow_nested: bool = True,
) -> config.Operation:
    cmd: qlast.ConfigOp
    if isinstance(expr, qlast.InsertQuery):
        if not allow_nested:
            raise errors.ConfigurationError(
                "nested config object is not allowed"
            )
        cmd = qlast.ConfigInsert(
            name=expr.subject,
            scope=qltypes.ConfigScope.INSTANCE,
            shape=expr.shape,
        )
    else:
        field_name_ref = qlast.ObjectRef(name=field_name)
        if obj_name != "cfg::Config":
            field_name_ref.module = obj_name
        cmd = qlast.ConfigSet(
            name=field_name_ref,
            scope=qltypes.ConfigScope.INSTANCE,
            expr=expr,
        )
    ir = qlcompiler.compile_ast_to_ir(cmd, schema=schema, options=options)
    if (
        isinstance(ir, irast.ConfigSet)
        or isinstance(ir, irast.Statement)
        and isinstance((ir := ir.expr.expr), irast.ConfigInsert)
    ):
        return ireval.evaluate_to_config_op(ir, schema=schema)

    raise errors.InternalServerError(f"unrecognized IR: {type(ir)!r}")


def compile_structured_config(
    objects: Mapping[str, ConfigObject],
    *,
    spec: config.Spec,
    schema: s_schema.Schema,
    source: str | None = None,
    allow_nested: bool = True,
) -> dict[str, immutables.Map[str, config.SettingValue]]:
    options = qlcompiler.CompilerOptions(
        modaliases={None: "cfg"},
        in_server_config_op=True,
    )
    rv = {}
    for obj_name, input_values in objects.items():
        storage: immutables.Map[str, config.SettingValue] = immutables.Map()
        ctx = Context(
            schema=schema,
            obj_type=schema.get(obj_name, type=s_objtypes.ObjectType),
            qual_name=obj_name,
            options=options,
        )
        shape = compile_dict_to_shape(input_values, ctx=ctx)
        for field_name, shape_el in shape.items():
            if isinstance(shape_el.compexpr, qlast.Set):
                elements = shape_el.compexpr.elements
                if not elements:
                    continue

                if isinstance(elements[0], qlast.InsertQuery):
                    for ast in shape_el.compexpr.elements:
                        op = compile_ast_to_operation(
                            obj_name,
                            field_name,
                            ast,
                            schema=schema,
                            options=options,
                            allow_nested=allow_nested,
                        )
                        storage = op.apply(spec, storage, source=source)
                    continue

            assert shape_el.compexpr is not None
            op = compile_ast_to_operation(
                obj_name,
                field_name,
                shape_el.compexpr,
                schema=schema,
                options=options,
                allow_nested=allow_nested,
            )
            storage = op.apply(spec, storage, source=source)

        rv[obj_name] = storage

    return rv
