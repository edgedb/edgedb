#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2015-present MagicStack Inc. and the EdgeDB authors.
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


import functools
import typing

from edb.schema import name as sn

from edb.edgeql import ast as qlast


class Type:

    def __init__(self, name):
        self.name = name

    def get_name(self, schema):
        return self.name


class ObjectType(Type):

    def __init__(self, name):
        super().__init__(name)
        self.pointers = {}

    def is_pointer(self):
        return False

    def getptr(self, schema, name):
        return self.pointers.get(name)


class UnionType:

    def __init__(self, types):
        self.types = types


class Pointer:

    def __init__(self, name, *, source=None, target=None):
        self.name = name
        self.source = source
        self.target = target
        self.pointers = {}

    def is_pointer(self):
        return True

    def getptr(self, schema, name):
        return self.pointers.get(name)

    def get_target(self, schema):
        return self.target

    def get_name(self, schema):
        return self.name


def trace_refs(
    qltree: qlast.Base,
    *,
    schema,
    source: typing.Optional[sn.Name] = None,
    subject: typing.Optional[sn.Name] = None,
    path_prefix: typing.Optional[sn.Name] = None,
    module: typing.Optional[str] = None,
    objects: typing.Dict[str, object],
) -> typing.FrozenSet[sn.Name]:

    """Return a list of schema item names used in an expression."""

    ctx = TracerContext(schema, module, objects,
                        source, subject, path_prefix)
    trace(qltree, ctx=ctx)
    return frozenset(ctx.refs)


class TracerContext:
    def __init__(self, schema, module, objects, source, subject, path_prefix):
        self.schema = schema
        self.refs = set()
        self.module = module
        self.objects = objects
        self.source = source
        self.subject = subject
        self.path_prefix = path_prefix

    def get_ref_name(self, ref: qlast.ObjectRef) -> sn.Name:
        if ref.module:
            return sn.Name(module=ref.module, name=ref.name)
        elif f'{self.module}::{ref.name}' in self.objects:
            return sn.Name(module=self.module, name=ref.name)
        else:
            return sn.Name(module="std", name=ref.name)


@functools.singledispatch
def trace(node: qlast.Base, *, ctx: TracerContext):
    raise NotImplementedError(f"do not know how to trace {node!r}")


@trace.register
def trace_none(node: type(None), *, ctx: TracerContext):
    pass


@trace.register
def trace_Constant(node: qlast.BaseConstant, *, ctx: TracerContext):
    pass


@trace.register
def trace_Array(node: qlast.Array, *, ctx: TracerContext):
    for el in node.elements:
        trace(el, ctx=ctx)


@trace.register
def trace_Set(node: qlast.Set, *, ctx: TracerContext):
    for el in node.elements:
        trace(el, ctx=ctx)


@trace.register
def trace_Tuple(node: qlast.Tuple, *, ctx: TracerContext):
    for el in node.elements:
        trace(el, ctx=ctx)


@trace.register
def trace_NamedTuple(node: qlast.NamedTuple, *, ctx: TracerContext):
    for el in node.elements:
        trace(el.val, ctx=ctx)


@trace.register
def trace_BinOp(node: qlast.BinOp, *, ctx: TracerContext):
    trace(node.left, ctx=ctx)
    trace(node.right, ctx=ctx)


@trace.register
def trace_UnaryOp(node: qlast.UnaryOp, *, ctx: TracerContext):
    trace(node.operand, ctx=ctx)


@trace.register
def trace_TypeCast(node: qlast.TypeCast, *, ctx: TracerContext):
    trace(node.expr, ctx=ctx)
    if not node.type.subtypes:
        ctx.refs.add(ctx.get_ref_name(node.type.maintype))


@trace.register
def trace_IsOp(node: qlast.IsOp, *, ctx: TracerContext):
    trace(node.left, ctx=ctx)
    if not node.right.subtypes:
        ctx.refs.add(ctx.get_ref_name(node.right.maintype))


@trace.register
def trace_FunctionCall(node: qlast.FunctionCall, *, ctx: TracerContext):
    for arg in node.args:
        trace(arg, ctx=ctx)
    for arg in node.kwargs.values():
        trace(arg, ctx=ctx)


@trace.register
def trace_Indirection(node: qlast.Indirection, *, ctx: TracerContext):
    for indirection in node.indirection:
        trace(indirection, ctx=ctx)
    trace(node.arg, ctx=ctx)


@trace.register
def trace_Index(node: qlast.Index, *, ctx: TracerContext):
    trace(node.index, ctx=ctx)


@trace.register
def trace_Slice(node: qlast.Slice, *, ctx: TracerContext):
    trace(node.start, ctx=ctx)
    trace(node.stop, ctx=ctx)


@trace.register
def trace_Path(node: qlast.Path, *, ctx: TracerContext):
    tip = None
    ptr = None
    plen = len(node.steps)

    for i, step in enumerate(node.steps):
        if isinstance(step, qlast.Source):
            if plen > 1:
                tip = ctx.objects[ctx.source]

        elif isinstance(step, qlast.Subject):
            if plen > 1:
                tip = ctx.objects[ctx.subject]
                if isinstance(tip, Pointer):
                    ptr = tip

        elif isinstance(step, qlast.ObjectRef):
            refname = ctx.get_ref_name(step)
            if refname in ctx.objects:
                ctx.refs.add(refname)
                tip = ctx.objects[refname]
            else:
                tip = ctx.schema.get(refname)

        elif isinstance(step, qlast.Ptr):
            if i == 0:
                # Abbreviated path.
                if ctx.path_prefix in ctx.objects:
                    tip = ctx.objects[ctx.path_prefix]
                else:
                    # We can't reason about this path.
                    return

            if step.type == 'property':
                lprop = ptr.getptr(ctx.schema, step.ptr.name)
                if lprop is None:
                    # Invalid link property reference, bail.
                    return

                if isinstance(lprop, Pointer):
                    ctx.refs.add(f'{lprop.source}@{step.ptr.name}')
            else:
                if step.direction == '<':
                    if plen > i + 1 and isinstance(node.steps[i + 1],
                                                   qlast.TypeIndirection):
                        # A reverse link traversal with a type filter,
                        # process it on the next step.
                        pass
                    else:
                        # otherwise we cannot say anything about the target,
                        # so bail.
                        return
                else:
                    ptr = tip.getptr(ctx.schema, step.ptr.name)
                    if ptr is None:
                        # Invalid pointer reference, bail.
                        return

                    if ptr.source == tip:
                        tip_name = tip.get_name(ctx.schema)
                        ctx.refs.add(f'{tip_name}@{step.ptr.name}')

                    tip = ptr.get_target(ctx.schema)

        elif isinstance(step, qlast.TypeIndirection):
            tip = _resolve_type_expr(step.type, ctx=ctx)
            prev_step = node.steps[i - 1]
            if prev_step.direction == '<':
                ptr = tip.getptr(ctx.schema, prev_step.ptr.name)
                if ptr is None:
                    # Invalid pointer reference, bail.
                    return

                if isinstance(tip, Type):
                    tip_name = tip.get_name(ctx.schema)
                    ctx.refs.add(f'{tip_name}@{prev_step.ptr.name}')

                tip = ptr.get_target(ctx.schema)

        else:
            trace(step, ctx=ctx)

    return tip


def _resolve_type_expr(texpr: qlast.TypeExpr, *, ctx: TracerContext):

    if isinstance(texpr, qlast.TypeName):
        if texpr.subtypes:
            return Type(name=texpr.maintype.name)
        else:
            refname = ctx.get_ref_name(texpr.maintype)
            obj = ctx.objects.get(refname)
            if obj is None:
                obj = ctx.schema.get(refname)
            else:
                ctx.refs.add(refname)

            return obj

    elif isinstance(texpr, qlast.TypeOp):

        if texpr.op == '|':
            return UnionType([
                _resolve_type_expr(texpr.left, ctx=ctx),
                _resolve_type_expr(texpr.right, ctx=ctx),
            ])

        else:
            raise NotImplementedError(
                f'unsupported type operation: {texpr.op}')

    else:
        raise NotImplementedError(
            f'unsupported type expression: {texpr!r}'
        )


@trace.register
def trace_TypeIndirection(node: qlast.TypeIndirection, *, ctx: TracerContext):
    trace(node.type, ctx=ctx)


@trace.register
def trace_TypeOf(node: qlast.TypeOf, *, ctx: TracerContext):
    trace(node.expr, ctx=ctx)


@trace.register
def trace_TypeName(node: qlast.TypeName, *, ctx: TracerContext):
    if node.subtypes:
        for st in node.subtypes:
            trace(st, ctx=ctx)
    else:
        fq_name = node.maintype.name
        if node.maintype.module:
            fq_name = f'{node.maintype.module}::{fq_name}'
        ctx.refs.add(fq_name)


@trace.register
def trace_TypeOp(node: qlast.TypeOp, *, ctx: TracerContext):
    trace(node.left, ctx=ctx)
    trace(node.right, ctx=ctx)


@trace.register
def trace_IfElse(node: qlast.IfElse, *, ctx: TracerContext):
    trace(node.if_expr, ctx=ctx)
    trace(node.else_expr, ctx=ctx)
    trace(node.condition, ctx=ctx)


@trace.register
def trace_Shape(node: qlast.Shape, *, ctx: TracerContext):
    if isinstance(node.expr, qlast.Path):
        tip = trace(node.expr, ctx=ctx)
        orig_prefix = ctx.path_prefix
        if tip is not None:
            ctx.path_prefix = tip.get_name(ctx.schema)
        else:
            ctx.path_prefix = None
    else:
        trace(node.expr, ctx=ctx)
    for element in node.elements:
        trace(element, ctx=ctx)
    if isinstance(node.expr, qlast.Path):
        ctx.path_prefix = orig_prefix


@trace.register
def trace_ShapeElement(node: qlast.ShapeElement, *, ctx: TracerContext):
    trace(node.expr, ctx=ctx)
    for element in node.elements:
        trace(element, ctx=ctx)
    trace(node.where, ctx=ctx)
    for element in node.orderby:
        trace(element, ctx=ctx)
    trace(node.offset, ctx=ctx)
    trace(node.limit, ctx=ctx)
    trace(node.compexpr, ctx=ctx)


@trace.register
def trace_Select(node: qlast.SelectQuery, *, ctx: TracerContext):
    for alias in node.aliases:
        if isinstance(alias, qlast.AliasedExpr):
            trace(alias.expr, ctx=ctx)

    trace(node.result, ctx=ctx)
    if node.where is not None:
        trace(node.where, ctx=ctx)
    if node.orderby:
        for expr in node.orderby:
            trace(expr, ctx=ctx)
    if node.offset is not None:
        trace(node.offset, ctx=ctx)
    if node.limit is not None:
        trace(node.limit, ctx=ctx)


@trace.register
def trace_SortExpr(node: qlast.SortExpr, *, ctx: TracerContext):
    trace(node.path, ctx=ctx)


@trace.register
def trace_InsertQuery(node: qlast.InsertQuery, *, ctx: TracerContext):

    for alias in node.aliases:
        if isinstance(alias, qlast.AliasedExpr):
            trace(alias.expr, ctx=ctx)

    trace(node.subject, ctx=ctx)

    for element in node.shape:
        trace(element, ctx=ctx)


@trace.register
def trace_UpdateQuery(node: qlast.UpdateQuery, *, ctx: TracerContext):

    for alias in node.aliases:
        if isinstance(alias, qlast.AliasedExpr):
            trace(alias.expr, ctx=ctx)

    trace(node.subject, ctx=ctx)

    for element in node.shape:
        trace(element, ctx=ctx)

    trace(node.where, ctx=ctx)
