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


from __future__ import annotations

# Import specific things to avoid name clashes
from typing import (Dict, FrozenSet, Generator, List, Mapping, Optional,
                    Union, Set)

import functools

from contextlib import contextmanager
from edb.schema import links as s_links
from edb.schema import name as sn
from edb.schema import objects as so
from edb.schema import pointers as s_pointers
from edb.schema import schema as s_schema
from edb.schema import sources as s_sources
from edb.schema import types as s_types

from edb.edgeql import ast as qlast


class NamedObject:
    '''Generic tracing object with an explicit name.'''

    def __init__(self, name: str) -> None:
        self.name = name

    def get_name(self, schema: s_schema.Schema) -> str:
        return self.name


class Function(NamedObject):
    pass


class Constraint(NamedObject):
    pass


class ConcreteConstraint(NamedObject):
    pass


class Annotation(NamedObject):
    pass


class Type(NamedObject):
    pass


class Source:
    '''Abstract type that mocks the s_sources.Source for tracing purposes.'''
    def _init_pointers(self) -> None:
        self.pointers: Dict[
            str,
            Union[s_pointers.Pointer, Pointer]
        ] = {}

    def getptr(
        self,
        schema: s_schema.Schema,
        name: str,
    ) -> Optional[Union[s_pointers.Pointer, Pointer]]:
        return self.pointers.get(name)


class ObjectType(Type, Source):

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self._init_pointers()

    def is_pointer(self) -> bool:
        return False


class UnionType:

    def __init__(
        self,
        types: List[Union[Type, UnionType, so.Object]]
    ) -> None:
        self.types = types


class Pointer(Source):

    def __init__(
        self,
        name: str,
        *,
        source: Optional[so.Object] = None,
        target: Optional[s_types.Type] = None,
    ) -> None:
        self.name = name
        self.source = source
        self.target = target
        self._init_pointers()

    def is_pointer(self) -> bool:
        return True

    def get_target(
        self,
        schema: s_schema.Schema,
    ) -> Optional[s_types.Type]:
        return self.target

    def get_source(
        self,
        schema: s_schema.Schema,
    ) -> Optional[so.Object]:
        return self.source

    def get_name(self, schema: s_schema.Schema) -> str:
        return self.name


def trace_refs(
    qltree: qlast.Base,
    *,
    schema: s_schema.Schema,
    source: Optional[sn.Name] = None,
    subject: Optional[sn.Name] = None,
    path_prefix: Optional[str] = None,
    module: Optional[str] = None,
    objects: Dict[str, so.Object],
    params: Dict[str, sn.Name],
) -> FrozenSet[str]:

    """Return a list of schema item names used in an expression."""

    ctx = TracerContext(
        schema=schema,
        module=module,
        objects=objects,
        source=source,
        subject=subject,
        path_prefix=path_prefix,
        modaliases={},
        params=params,
    )
    trace(qltree, ctx=ctx)
    return frozenset(ctx.refs)


class TracerContext:
    def __init__(
        self,
        *,
        schema: s_schema.Schema,
        module: Optional[str],
        objects: Dict[str, so.Object],
        source: Optional[sn.Name],
        subject: Optional[sn.Name],
        path_prefix: Optional[str],
        modaliases: Mapping[Optional[str], str],
        params: Dict[str, sn.Name],
    ) -> None:
        self.schema = schema
        self.refs: Set[str] = set()
        self.module = module
        self.objects = objects
        self.source = source
        self.subject = subject
        self.path_prefix = path_prefix
        self.modaliases = modaliases
        self.params = params

    def get_ref_name(self, ref: qlast.BaseObjectRef) -> sn.Name:
        # We don't actually expect to handle anything other than
        # ObjectRef here.
        assert isinstance(ref, qlast.ObjectRef)

        if ref.module:
            # replace the module alias with the real name
            module = self.modaliases.get(ref.module, ref.module)
            return sn.Name(module=module, name=ref.name)
        elif ref.name in self.params:
            return self.params[ref.name]
        elif f'{self.module}::{ref.name}' in self.objects:
            return sn.Name(module=self.module, name=ref.name)
        else:
            return sn.Name(module="std", name=ref.name)

    def get_ref_name_starstwith(
        self, ref: qlast.ObjectRef
    ) -> Set[sn.Name]:
        refs = set()
        prefixes = []

        if ref.module:
            # replace the module alias with the real name
            module = self.modaliases.get(ref.module, ref.module)
            prefixes.append(f'{module}::{ref.name}')
        else:
            prefixes.append(f'{self.module}::{ref.name}')
            prefixes.append(f'std::{ref.name}')

        for objname in self.objects.keys():
            for prefix in prefixes:
                if objname.startswith(prefix):
                    parts = objname.split('::', 1)
                    name = sn.Name(module=parts[0], name=parts[1])
                    refs.add(name)

        return refs


@contextmanager
def alias_context(
    ctx: TracerContext,
    aliases: List[Union[qlast.AliasedExpr, qlast.ModuleAliasDecl]],
) -> Generator[TracerContext, None, None]:
    module = None
    modaliases: Dict[Optional[str], str] = {}

    for alias in aliases:
        # module and modalias in ctx needs to be amended
        if isinstance(alias, qlast.ModuleAliasDecl):
            if alias.alias:
                modaliases[alias.alias] = alias.module
            else:
                # default module
                module = alias.module

        elif isinstance(alias, qlast.AliasedExpr):
            trace(alias.expr, ctx=ctx)

    if module or modaliases:
        nctx = TracerContext(
            schema=ctx.schema,
            module=module or ctx.module,
            objects=ctx.objects,
            source=ctx.source,
            subject=ctx.subject,
            path_prefix=ctx.path_prefix,
            modaliases=modaliases or ctx.modaliases,
            params=ctx.params,
        )
        # use the same refs set
        nctx.refs = ctx.refs
    else:
        nctx = ctx

    try:
        yield nctx
    finally:
        # refs are already updated
        pass


@functools.singledispatch
def trace(node: qlast.Base, *,
          ctx: TracerContext) -> Optional[so.Object]:
    raise NotImplementedError(f"do not know how to trace {node!r}")


@trace.register
def trace_none(node: Union[None], *, ctx: TracerContext) -> None:
    pass


@trace.register
def trace_Constant(node: qlast.BaseConstant, *, ctx: TracerContext) -> None:
    pass


@trace.register
def trace_Array(node: qlast.Array, *, ctx: TracerContext) -> None:
    for el in node.elements:
        trace(el, ctx=ctx)


@trace.register
def trace_Set(node: qlast.Set, *, ctx: TracerContext) -> None:
    for el in node.elements:
        trace(el, ctx=ctx)


@trace.register
def trace_Tuple(node: qlast.Tuple, *, ctx: TracerContext) -> None:
    for el in node.elements:
        trace(el, ctx=ctx)


@trace.register
def trace_NamedTuple(node: qlast.NamedTuple, *, ctx: TracerContext) -> None:
    for el in node.elements:
        trace(el.val, ctx=ctx)


@trace.register
def trace_BinOp(node: qlast.BinOp, *, ctx: TracerContext) -> None:
    trace(node.left, ctx=ctx)
    trace(node.right, ctx=ctx)


@trace.register
def trace_UnaryOp(node: qlast.UnaryOp, *, ctx: TracerContext) -> None:
    trace(node.operand, ctx=ctx)


@trace.register
def trace_Detached(
    node: qlast.DetachedExpr,
    *,
    ctx: TracerContext
) -> Optional[so.Object]:
    # DETACHED works with partial paths same as its inner expression.
    return trace(node.expr, ctx=ctx)


@trace.register
def trace_TypeCast(node: qlast.TypeCast, *, ctx: TracerContext) -> None:
    trace(node.expr, ctx=ctx)
    if isinstance(node.type, qlast.TypeName):
        if not node.type.subtypes:
            ctx.refs.add(ctx.get_ref_name(node.type.maintype))


@trace.register
def trace_IsOp(node: qlast.IsOp, *, ctx: TracerContext) -> None:
    trace(node.left, ctx=ctx)
    if isinstance(node.right, qlast.TypeName):
        if not node.right.subtypes:
            ctx.refs.add(ctx.get_ref_name(node.right.maintype))


@trace.register
def trace_Introspect(node: qlast.Introspect, *, ctx: TracerContext) -> None:
    if isinstance(node.type, qlast.TypeName):
        if not node.type.subtypes:
            ctx.refs.add(ctx.get_ref_name(node.type.maintype))


@trace.register
def trace_FunctionCall(node: qlast.FunctionCall, *,
                       ctx: TracerContext) -> None:

    if isinstance(node.func, tuple):
        fname = qlast.ObjectRef(module=node.func[0], name=node.func[1])
    else:
        fname = qlast.ObjectRef(name=node.func)
    # The function call is dependent on the function actually being
    # present, so we add all variations of that function name to the
    # dependency list.

    names = ctx.get_ref_name_starstwith(fname)
    ctx.refs.update(names)

    for arg in node.args:
        trace(arg, ctx=ctx)
    for arg in node.kwargs.values():
        trace(arg, ctx=ctx)


@trace.register
def trace_Indirection(node: qlast.Indirection, *, ctx: TracerContext) -> None:
    for indirection in node.indirection:
        trace(indirection, ctx=ctx)
    trace(node.arg, ctx=ctx)


@trace.register
def trace_Index(node: qlast.Index, *, ctx: TracerContext) -> None:
    trace(node.index, ctx=ctx)


@trace.register
def trace_Slice(node: qlast.Slice, *, ctx: TracerContext) -> None:
    trace(node.start, ctx=ctx)
    trace(node.stop, ctx=ctx)


@trace.register
def trace_Path(
    node: qlast.Path, *,
    ctx: TracerContext,
) -> Optional[Union[Type, UnionType, so.Object]]:
    tip: Optional[Union[Type, UnionType, so.Object]] = None
    ptr: Optional[Union[Pointer, s_pointers.Pointer]] = None
    plen = len(node.steps)

    for i, step in enumerate(node.steps):
        if isinstance(step, qlast.ObjectRef):
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
                    return None

            if step.type == 'property':
                if ptr is None:
                    # This is either a computable def, or
                    # unknown link, bail.
                    return None

                elif isinstance(ptr, (s_links.Link, Pointer)):
                    lprop = ptr.getptr(ctx.schema, step.ptr.name)
                    if lprop is None:
                        # Invalid link property reference, bail.
                        return None

                    if (isinstance(lprop, Pointer) and
                            lprop.source is not None):
                        src = lprop.source
                        src_name = src.get_name(ctx.schema)
                        if (isinstance(src, Pointer) and
                                src.source is not None):
                            src_src_name = src.source.get_name(ctx.schema)
                            source_name = f'{src_src_name}@{src_name}'
                        else:
                            source_name = src_name
                        ctx.refs.add(f'{source_name}@{step.ptr.name}')
            else:
                if step.direction == '<':
                    if plen > i + 1 and isinstance(node.steps[i + 1],
                                                   qlast.TypeIntersection):
                        # A reverse link traversal with a type intersection,
                        # process it on the next step.
                        pass
                    else:
                        # otherwise we cannot say anything about the target,
                        # so bail.
                        return None
                else:
                    if isinstance(tip, (Source, s_sources.Source)):
                        ptr = tip.getptr(ctx.schema, step.ptr.name)
                        if ptr is None:
                            # Invalid pointer reference, bail.
                            return None
                        else:
                            ptr_source = ptr.source

                        if ptr_source is not None:
                            source_name = ptr_source.get_name(ctx.schema)
                            ctx.refs.add(f'{source_name}@{step.ptr.name}')
                            tip = ptr.get_target(ctx.schema)
                        else:
                            # Can't figure out the new tip, so we bail.
                            return None

                    else:
                        # We can't reason about this path.
                        return None

        elif isinstance(step, qlast.TypeIntersection):
            # This tip is determined from the type in the type
            # intersection, which is valid in the general case, but
            # there's a special case that needs to be potentially
            # handled for backward links.
            tip = _resolve_type_expr(step.type, ctx=ctx)
            prev_step = node.steps[i - 1]
            if isinstance(prev_step, qlast.Ptr):
                if prev_step.direction == '<':
                    if isinstance(tip, (s_sources.Source, ObjectType)):
                        ptr = tip.getptr(ctx.schema, prev_step.ptr.name)
                        if ptr is None:
                            # Invalid pointer reference, bail.
                            return None

                        if isinstance(tip, Type):
                            tip_name = tip.get_name(ctx.schema)
                            ctx.refs.add(f'{tip_name}@{prev_step.ptr.name}')

                        # This is a backwards link, so we need the source.
                        tip = ptr.get_source(ctx.schema)

        else:
            tr = trace(step, ctx=ctx)
            if tr is not None:
                tip = tr
                if isinstance(tip, Pointer):
                    ptr = tip

    return tip


@trace.register
def trace_Source(node: qlast.Source, *, ctx: TracerContext) -> so.Object:
    assert ctx.source is not None
    return ctx.objects[ctx.source]


@trace.register
def trace_Subject(node: qlast.Subject, *,
                  ctx: TracerContext) -> Optional[so.Object]:
    # Apparently for some paths (of length 1) ctx.subject may be None.
    if ctx.subject is not None:
        return ctx.objects[ctx.subject]
    return None


def _resolve_type_expr(
    texpr: qlast.TypeExpr, *,
    ctx: TracerContext
) -> Union[Type, UnionType, so.Object]:

    if isinstance(texpr, qlast.TypeName):
        if texpr.subtypes and isinstance(texpr.maintype, qlast.ObjectRef):
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
def trace_TypeIntersection(node: qlast.TypeIntersection, *,
                           ctx: TracerContext) -> None:
    trace(node.type, ctx=ctx)


@trace.register
def trace_TypeOf(node: qlast.TypeOf, *, ctx: TracerContext) -> None:
    trace(node.expr, ctx=ctx)


@trace.register
def trace_TypeName(node: qlast.TypeName, *, ctx: TracerContext) -> None:
    if node.subtypes:
        for st in node.subtypes:
            trace(st, ctx=ctx)
    elif isinstance(node.maintype, qlast.ObjectRef):
        fq_name = node.maintype.name
        if node.maintype.module:
            fq_name = f'{node.maintype.module}::{fq_name}'
        ctx.refs.add(fq_name)


@trace.register
def trace_TypeOp(node: qlast.TypeOp, *, ctx: TracerContext) -> None:
    trace(node.left, ctx=ctx)
    trace(node.right, ctx=ctx)


@trace.register
def trace_IfElse(node: qlast.IfElse, *, ctx: TracerContext) -> None:
    trace(node.if_expr, ctx=ctx)
    trace(node.else_expr, ctx=ctx)
    trace(node.condition, ctx=ctx)


@trace.register
def trace_Shape(
    node: qlast.Shape,
    *,
    ctx: TracerContext
) -> Optional[so.Object]:
    tip = trace(node.expr, ctx=ctx)
    if isinstance(node.expr, qlast.Path):
        orig_prefix = ctx.path_prefix
        if tip is not None:
            ctx.path_prefix = tip.get_name(ctx.schema)
        else:
            ctx.path_prefix = None

    for element in node.elements:
        trace(element, ctx=ctx)

    if isinstance(node.expr, qlast.Path):
        ctx.path_prefix = orig_prefix

    return tip


@trace.register
def trace_ShapeElement(node: qlast.ShapeElement, *,
                       ctx: TracerContext) -> None:
    trace(node.expr, ctx=ctx)
    for element in node.elements:
        trace(element, ctx=ctx)
    trace(node.where, ctx=ctx)
    for sortexpr in node.orderby:
        trace(sortexpr, ctx=ctx)
    trace(node.offset, ctx=ctx)
    trace(node.limit, ctx=ctx)
    trace(node.compexpr, ctx=ctx)


@trace.register
def trace_Select(
    node: qlast.SelectQuery,
    *,
    ctx: TracerContext
) -> Optional[so.Object]:
    with alias_context(ctx, node.aliases) as ctx:
        tip = trace(node.result, ctx=ctx)
        if tip is not None:
            ctx.path_prefix = tip.get_name(ctx.schema)

        if node.where is not None:
            trace(node.where, ctx=ctx)
        if node.orderby:
            for expr in node.orderby:
                trace(expr, ctx=ctx)
        if node.offset is not None:
            trace(node.offset, ctx=ctx)
        if node.limit is not None:
            trace(node.limit, ctx=ctx)

        return tip


@trace.register
def trace_SortExpr(node: qlast.SortExpr, *, ctx: TracerContext) -> None:
    trace(node.path, ctx=ctx)


@trace.register
def trace_InsertQuery(node: qlast.InsertQuery, *, ctx: TracerContext) -> None:
    with alias_context(ctx, node.aliases) as ctx:
        trace(node.subject, ctx=ctx)

        for element in node.shape:
            trace(element, ctx=ctx)


@trace.register
def trace_UpdateQuery(
    node: qlast.UpdateQuery,
    *,
    ctx: TracerContext
) -> Optional[so.Object]:
    with alias_context(ctx, node.aliases) as ctx:
        tip = trace(node.subject, ctx=ctx)

        if tip is None:
            return None

        ctx.path_prefix = tip.get_name(ctx.schema)

        for element in node.shape:
            trace(element, ctx=ctx)

        trace(node.where, ctx=ctx)

        return tip


@trace.register
def trace_DeleteQuery(
    node: qlast.DeleteQuery,
    *,
    ctx: TracerContext
) -> Optional[so.Object]:
    with alias_context(ctx, node.aliases) as ctx:
        tip = trace(node.subject, ctx=ctx)
        if tip is not None:
            ctx.path_prefix = tip.get_name(ctx.schema)

        if node.where is not None:
            trace(node.where, ctx=ctx)
        if node.orderby:
            for expr in node.orderby:
                trace(expr, ctx=ctx)
        if node.offset is not None:
            trace(node.offset, ctx=ctx)
        if node.limit is not None:
            trace(node.limit, ctx=ctx)

        return tip


@trace.register
def trace_For(
    node: qlast.ForQuery,
    *,
    ctx: TracerContext
) -> Optional[so.Object]:
    with alias_context(ctx, node.aliases) as ctx:
        tip = trace(node.result, ctx=ctx)

        return tip


@trace.register
def trace_DescribeStmt(
    node: qlast.DescribeStmt, *,
    ctx: TracerContext,
) -> None:

    if isinstance(node.object, qlast.ObjectRef):
        fq_name = node.object.name
        if node.object.module:
            fq_name = f'{node.object.module}::{fq_name}'
        ctx.refs.add(fq_name)
