#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


"""SDL loader.

The purpose of this module is to take a set of SDL documents and
transform them into schema modules.  The crux of the task is to
break the SDL declarations into a correct sequence of DDL commands,
considering all possible cyclic references.  The dependency tracking
is complicated by the presence of expressions in schema definitions.
In those cases we make a best-effort tracing using a rudimentary
EdgeQL AST visitor.
"""

from __future__ import annotations
from typing import *

import copy
import functools
from collections import defaultdict

from edb import errors

from edb.common import parsing
from edb.common import topological
from edb.common import english
from edb.common.ordered import OrderedSet

from edb.edgeql import ast as qlast
from edb.edgeql import codegen as qlcodegen
from edb.edgeql import parser as qlparser
from edb.edgeql import tracer as qltracer

from edb.schema import annos as s_anno
from edb.schema import constraints as s_constr
from edb.schema import links as s_links
from edb.schema import name as s_name
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import properties as s_props
from edb.schema import pseudo as s_pseudo
from edb.schema import scalars as s_scalars
from edb.schema import schema as s_schema
from edb.schema import sources as s_sources
from edb.schema import types as s_types
from edb.schema import utils as s_utils


class TraceContextBase:

    schema: s_schema.Schema
    module: str
    depstack: List[Tuple[qlast.DDLOperation, s_name.QualName]]
    modaliases: Dict[Optional[str], str]
    objects: Dict[s_name.QualName, Optional[qltracer.ObjectLike]]
    parents: Dict[s_name.QualName, Set[s_name.QualName]]
    ancestors: Dict[s_name.QualName, Set[s_name.QualName]]
    defdeps: Dict[s_name.QualName, Set[s_name.QualName]]
    constraints: Dict[s_name.QualName, Set[s_name.QualName]]
    local_modules: AbstractSet[str]

    def __init__(
        self,
        schema: s_schema.Schema,
        local_modules: AbstractSet[str],
    ) -> None:
        self.schema = schema
        self.module = '__not_set__'
        self.depstack = []
        self.modaliases = {}
        self.objects = {}
        self.parents = {}
        self.ancestors = {}
        self.defdeps = defaultdict(set)
        self.constraints = defaultdict(set)
        self.local_modules = local_modules

    def set_module(self, module: str) -> None:
        self.module = module
        self.modaliases = {None: module}

    def get_local_name(
        self,
        ref: qlast.ObjectRef,
    ) -> s_name.QualName:
        return qltracer.get_name(
            ref,
            current_module=self.module,
            schema=self.schema,
            objects=self.objects,
            modaliases=None,
            local_modules=self.local_modules,
        )

    def get_ref_name(self, ref: qlast.BaseObjectRef) -> s_name.QualName:
        if isinstance(ref, qlast.ObjectRef):
            return self.get_local_name(ref)
        elif isinstance(ref, qlast.AnyType):
            # We pretend `anytype` has a fully-qualified name here, because
            # the tracing machinery really wants to work with fully-qualified
            # names and wants to distinguish between objects from the standard
            # library and the user-defines ones.  Ditto for `anytuple` below.
            return s_name.QualName('std', 'anytype')
        elif isinstance(ref, qlast.AnyTuple):
            return s_name.QualName('std', 'anytuple')
        else:
            raise TypeError(
                "ObjectRef expected "
                "(got type {!r})".format(type(ref).__name__)
            )

    def get_fq_name(
        self,
        decl: qlast.DDLOperation,
    ) -> Tuple[str, s_name.QualName]:
        # Get the basic name form.
        if isinstance(decl, qlast.CreateConcretePointer):
            name = decl.name.name
            parent_expected = True
        elif isinstance(decl, qlast.SetField):
            name = decl.name
            parent_expected = True
        elif isinstance(decl, qlast.ObjectDDL):
            fq_name = self.get_local_name(decl.name)
            name = str(fq_name)
            parent_expected = False
        else:
            raise AssertionError(f'unexpected DDL node: {decl!r}')

        if self.depstack:
            parent_name = self.depstack[-1][1]
            fq_name = s_name.QualName(
                module=parent_name.module,
                name=f'{parent_name.name}@{name}'
            )
        elif parent_expected:
            raise AssertionError(
                f'missing expected parent context for {decl!r}')

        # Additionally, functions and concrete constraints may need an
        # extra name piece.
        extra_name = None
        if isinstance(decl, qlast.CreateFunction):
            # Functions are defined by their name + call signature, so we
            # need to add that to the "extra_name".
            extra_name = f'({qlcodegen.generate_source(decl.params)})'

        elif isinstance(decl, qlast.CreateConcreteConstraint):
            # Concrete constraints are defined by their expr, so we need
            # to add that to the "extra_name".
            exprs = list(decl.args)
            if decl.subjectexpr:
                exprs.append(decl.subjectexpr)
            if decl.except_expr:
                # Add an extra dummy argument to distinguish between
                # ON and EXCEPT, when only one is present
                exprs.append(qlast.Set(elements=[]))
                exprs.append(decl.except_expr)

            for cmd in decl.commands:
                if isinstance(cmd, qlast.SetField) and cmd.name == "expr":
                    assert cmd.value, "sdl SetField should always have value"
                    assert isinstance(cmd.value, qlast.Expr)
                    exprs.append(cmd.value)

            extra_name = '|'.join(qlcodegen.generate_source(e) for e in exprs)

        elif isinstance(decl, qlast.CreateConcreteIndex):
            # Indexes are defined by what they are an index over, so we need
            # to add that to the "extra_name".
            extra_name = f'({qlcodegen.generate_source(decl.expr)})'
            if decl.except_expr:
                except_bit = f'({qlcodegen.generate_source(decl.except_expr)})'
                extra_name = f'{extra_name}/{except_bit}'

        if extra_name:
            fq_name = s_name.QualName(
                module=fq_name.module,
                name=f'{fq_name.name}@@{extra_name}',
            )

        return name, fq_name


def get_verbosename_from_fqname(
    fq_name: s_name.QualName,
    ctx: DepTraceContext,
) -> str:
    traceobj = ctx.objects[fq_name]
    assert traceobj is not None

    name = str(fq_name)
    clsname = traceobj.__class__.__name__.lower()
    ofobj = ''

    if isinstance(traceobj, qltracer.Alias):
        clsname = 'alias'
    elif isinstance(traceobj, qltracer.ObjectType):
        clsname = 'object'
    elif isinstance(traceobj, qltracer.ScalarType):
        clsname = 'scalar'
    elif isinstance(traceobj, qltracer.Function):
        node = ctx.ddlgraph[fq_name].item
        assert isinstance(node, qlast.FunctionCommand)
        params = ','.join(
            qlcodegen.generate_source(param, sdlmode=True)
            for param in node.params
        )
        name = f"{str(fq_name).split('@@', 1)[0]}({params})"
    elif isinstance(traceobj, qltracer.Pointer):
        ofobj, name = str(fq_name).split('@', 1)
        ofobj = f" of object type '{ofobj}'"
    elif isinstance(traceobj, qltracer.AccessPolicy):
        clsname = 'access policy'
        ofobj, name = str(fq_name).split('@', 1)
        _, name = name.split('::')
        ofobj = f" of object type '{ofobj}'"

    return f"{clsname} '{name}'{ofobj}"


class InheritanceGraphEntry(TypedDict):

    item: qltracer.NamedObject
    deps: AbstractSet[s_name.Name]
    merge: AbstractSet[s_name.Name]


class LayoutTraceContext(TraceContextBase):

    inh_graph: Dict[
        s_name.QualName,
        topological.DepGraphEntry[
            s_name.QualName,
            qltracer.NamedObject,
            bool,
        ],
    ]

    def __init__(
        self,
        schema: s_schema.Schema,
        local_modules: AbstractSet[str],
    ) -> None:
        super().__init__(schema, local_modules)
        self.inh_graph = {}


DDLGraph = Dict[
    s_name.QualName,
    topological.DepGraphEntry[s_name.QualName, qlast.DDLCommand, bool],
]


class DepTraceContext(TraceContextBase):

    def __init__(
        self,
        schema: s_schema.Schema,
        ddlgraph: DDLGraph,
        objects: Dict[s_name.QualName, Optional[qltracer.ObjectLike]],
        parents: Dict[s_name.QualName, Set[s_name.QualName]],
        ancestors: Dict[s_name.QualName, Set[s_name.QualName]],
        defdeps: Dict[s_name.QualName, Set[s_name.QualName]],
        constraints: Dict[s_name.QualName, Set[s_name.QualName]],
        local_modules: AbstractSet[str],
    ) -> None:
        super().__init__(schema, local_modules)
        self.ddlgraph = ddlgraph
        self.objects = objects
        self.parents = parents
        self.ancestors = ancestors
        self.defdeps = defdeps
        self.constraints = constraints


class Dependency:
    pass


class TypeDependency(Dependency):

    texpr: qlast.TypeExpr

    def __init__(self, texpr: qlast.TypeExpr) -> None:
        self.texpr = texpr


class ExprDependency(Dependency):

    expr: qlast.Expr

    def __init__(self, expr: qlast.Expr) -> None:
        self.expr = expr


class FunctionDependency(ExprDependency):

    params: Mapping[str, s_name.QualName]

    def __init__(
        self,
        expr: qlast.Expr,
        params: Mapping[str, s_name.QualName],
    ) -> None:
        super().__init__(expr=expr)
        self.params = params


def sdl_to_ddl(
    schema: s_schema.Schema,
    documents: Mapping[str, List[qlast.DDL]],
) -> Tuple[qlast.DDLCommand, ...]:

    ddlgraph: DDLGraph = {}
    mods: List[qlast.DDLCommand] = []

    ctx = LayoutTraceContext(schema, frozenset(mod for mod in documents))

    ctx.objects[s_name.QualName('std', 'anytype')] = (
        schema.get_global(s_pseudo.PseudoType, 'anytype'))
    ctx.objects[s_name.QualName('std', 'anytuple')] = (
        schema.get_global(s_pseudo.PseudoType, 'anytuple'))

    for module_name, declarations in documents.items():
        ctx.set_module(module_name)
        for decl_ast in declarations:
            if isinstance(decl_ast, qlast.CreateObject):
                _, fq_name = ctx.get_fq_name(decl_ast)

                if isinstance(decl_ast, qlast.CreateObjectType):
                    ctx.objects[fq_name] = qltracer.ObjectType(fq_name)
                elif isinstance(decl_ast, qlast.CreateAlias):
                    ctx.objects[fq_name] = qltracer.Alias(fq_name)
                elif isinstance(decl_ast, qlast.CreateScalarType):
                    ctx.objects[fq_name] = qltracer.ScalarType(fq_name)
                elif isinstance(decl_ast, qlast.CreateLink):
                    ctx.objects[fq_name] = qltracer.Link(
                        fq_name, source=None, target=None)
                elif isinstance(decl_ast, qlast.CreateProperty):
                    ctx.objects[fq_name] = qltracer.Property(
                        fq_name, source=None, target=None)
                elif isinstance(decl_ast, qlast.CreateFunction):
                    ctx.objects[fq_name] = qltracer.Function(fq_name)
                elif isinstance(decl_ast, qlast.CreateConstraint):
                    ctx.objects[fq_name] = qltracer.Constraint(fq_name)
                elif isinstance(decl_ast, qlast.CreateAnnotation):
                    ctx.objects[fq_name] = qltracer.Annotation(fq_name)
                elif isinstance(decl_ast, qlast.CreateGlobal):
                    ctx.objects[fq_name] = qltracer.Global(fq_name)
                else:
                    raise AssertionError(
                        f'unexpected SDL declaration: {decl_ast}')

    for module_name, declarations in documents.items():
        ctx.set_module(module_name)
        for decl_ast in declarations:
            trace_layout(decl_ast, ctx=ctx)

    # compute the ancestors graph
    for obj_name in ctx.parents.keys():
        ctx.ancestors[obj_name] = get_ancestors(
            obj_name, ctx.ancestors, ctx.parents)

    topological.normalize(
        ctx.inh_graph,
        merger=_graph_merge_cb,  # type: ignore
        schema=schema,
    )

    tracectx = DepTraceContext(
        schema, ddlgraph, ctx.objects, ctx.parents, ctx.ancestors,
        ctx.defdeps, ctx.constraints, ctx.local_modules,
    )

    created_modules = set()
    for module_name, declarations in documents.items():
        tracectx.set_module(module_name)
        # module (and any encosing modules) needs to be created
        # regardless of whether its contents are empty or not
        parts = module_name.split('::')
        for i in range(len(parts)):
            n = '::'.join(parts[:i + 1])
            if n not in created_modules:
                created_modules.add(n)
                mods.append(qlast.CreateModule(name=qlast.ObjectRef(name=n)))
        for decl_ast in declarations:
            trace_dependencies(decl_ast, ctx=tracectx)

    # Before sorting normalize all ordering, to make sure that errors
    # are consistent.
    for ddlentry in ddlgraph.values():
        ddlentry.deps = OrderedSet(sorted(ddlentry.deps))
        ddlentry.weak_deps = OrderedSet(sorted(ddlentry.weak_deps))

    try:
        ordered = topological.sort(ddlgraph, allow_unresolved=False)
    except topological.CycleError as e:
        assert isinstance(e.item, s_name.QualName)
        node = tracectx.ddlgraph[e.item].item
        item_vn = get_verbosename_from_fqname(e.item, tracectx)

        if e.path is not None and len(e.path):
            # Recursion involving more than one schema object.
            rec_vn = get_verbosename_from_fqname(e.path[-1], tracectx)
            msg = (
                f'definition dependency cycle between {rec_vn} '
                f'and {item_vn}'
            )
        else:
            # A single schema object with a recursive definition.
            msg = f'{item_vn} is defined recursively'

        raise errors.InvalidDefinitionError(msg, context=node.context) from e

    return tuple(mods) + tuple(ordered)


def _graph_merge_cb(
    item: qltracer.NamedObject,
    parent: qltracer.NamedObject,
    *,
    schema: s_schema.Schema,
) -> qltracer.NamedObject:
    if (
        isinstance(item, (qltracer.Source, s_sources.Source))
        and isinstance(parent, (qltracer.Source, s_sources.Source))
    ):
        return _merge_items(item, parent, schema=schema)
    else:
        return item


def _merge_items(
    item: qltracer.Source_T,
    parent: qltracer.SourceLike_T,
    *,
    schema: s_schema.Schema,
) -> qltracer.Source_T:

    item_ptrs = dict(item.get_pointers(schema).items(schema))

    for pn, ptr in parent.get_pointers(schema).items(schema):
        if not isinstance(ptr, (qltracer.Pointer, s_sources.Source)):
            continue

        if pn not in item_ptrs:
            PointerType = (qltracer.Property if ptr.is_property(schema)
                           else qltracer.Link)
            ptr_copy = PointerType(
                s_name.QualName('__', pn.name),
                source=ptr.get_source(schema),
                target=ptr.get_target(schema),
            )
            ptr_copy.pointers = dict(
                ptr.get_pointers(schema).items(schema))
            item.pointers[pn] = ptr_copy
        else:
            item_ptr = item.getptr(schema, pn)
            assert isinstance(item_ptr, (qltracer.Pointer, s_sources.Source))
            PointerType = (qltracer.Property if item_ptr.is_property(schema)
                           else qltracer.Link)
            ptr_copy = PointerType(
                s_name.QualName('__', pn.name),
                source=item,
                target=item_ptr.get_target(schema),
            )
            ptr_copy.pointers = dict(
                item_ptr.get_pointers(schema).items(schema))
            item.pointers[pn] = _merge_items(ptr_copy, ptr, schema=schema)

    return item


@functools.singledispatch
def trace_layout(
    node: qlast.Base,
    *,
    ctx: LayoutTraceContext,
) -> None:
    pass


@trace_layout.register
def trace_layout_Schema(
    node: qlast.Schema,
    *,
    ctx: LayoutTraceContext,
) -> None:
    for decl in node.declarations:
        trace_layout(decl, ctx=ctx)


@trace_layout.register
def trace_layout_CreateObjectType(
    node: qlast.CreateObjectType,
    *,
    ctx: LayoutTraceContext,
) -> None:
    _trace_item_layout(node, ctx=ctx)


@trace_layout.register
def trace_layout_CreateLink(
    node: qlast.CreateLink,
    *,
    ctx: LayoutTraceContext,
) -> None:
    _trace_item_layout(node, ctx=ctx)


@trace_layout.register
def trace_layout_CreateProperty(
    node: qlast.CreateProperty,
    *,
    ctx: LayoutTraceContext,
) -> None:
    _trace_item_layout(node, ctx=ctx)


def _trace_item_layout(
    node: qlast.CreateObject,
    *,
    obj: Optional[qltracer.NamedObject] = None,
    fq_name: Optional[s_name.QualName] = None,
    ctx: LayoutTraceContext,
) -> None:
    if obj is None:
        fq_name = ctx.get_local_name(node.name)
        local_obj = ctx.objects[fq_name]
        assert isinstance(local_obj, qltracer.NamedObject)
        obj = local_obj

    assert fq_name is not None

    if isinstance(node, qlast.BasesMixin):
        bases = []
        # construct the parents set, used later in ancestors graph
        parents = set()

        for ref in _get_bases(node, ctx=ctx):
            bases.append(ref)

            # ignore std modules dependencies
            if ref.get_module_name() not in s_schema.STD_MODULES:
                parents.add(ref)

            if (
                ref.module not in ctx.local_modules
                and ref not in ctx.inh_graph
            ):
                base_obj = type(obj)(name=ref)
                ctx.inh_graph[ref] = topological.DepGraphEntry(item=base_obj)

                base = ctx.schema.get(ref)
                if isinstance(base, s_sources.Source):
                    assert isinstance(base_obj, qltracer.Source)
                    base_pointers = base.get_pointers(ctx.schema)
                    for pn, p in base_pointers.items(ctx.schema):
                        PointerType = (
                            qltracer.Property
                            if p.is_property(ctx.schema) else
                            qltracer.Link
                        )
                        base_obj.pointers[pn] = PointerType(
                            s_name.QualName('__', pn.name),
                            source=base,
                            target=p.get_target(ctx.schema),
                        )

        ctx.parents[fq_name] = parents
        ctx.inh_graph[fq_name] = topological.DepGraphEntry(
            item=obj,
            deps=set(bases),
            merge=set(bases),
        )

    for decl in node.commands:
        if isinstance(decl, qlast.CreateConcretePointer):
            assert isinstance(obj, qltracer.Source)
            target: Optional[qltracer.TypeLike]
            target_expr: Optional[qlast.Expr]
            if isinstance(decl.target, qlast.TypeExpr):
                target = _resolve_type_expr(decl.target, ctx=ctx)
                target_expr = None
            else:
                target = None
                target_expr = decl.target

            pn = s_utils.ast_ref_to_unqualname(decl.name)

            PointerType = (
                qltracer.Property
                if isinstance(decl, qlast.CreateConcreteProperty) else
                qltracer.Link
            )
            ptr = PointerType(
                s_name.QualName('__', pn.name),
                source=obj,
                target=target,
                target_expr=target_expr,
            )
            obj.pointers[pn] = ptr
            ptr_name = s_name.QualName(
                module=fq_name.module,
                name=f'{fq_name.name}@{decl.name.name}',
            )
            ctx.objects[ptr_name] = ptr
            ctx.defdeps[fq_name].add(ptr_name)

            _trace_item_layout(
                decl, obj=ptr, fq_name=ptr_name, ctx=ctx)

        elif isinstance(decl, qlast.CreateConcreteConstraint):
            # Validate that the constraint exists at all.
            _validate_schema_ref(decl, ctx=ctx)
            _, con_fq_name = ctx.get_fq_name(decl)

            con_name = s_name.QualName(
                module=fq_name.module,
                name=f'{fq_name.name}@{con_fq_name}',
            )
            ctx.objects[con_name] = qltracer.ConcreteConstraint(con_name)
            ctx.constraints[fq_name].add(con_name)

        elif isinstance(decl, qlast.CreateAnnotationValue):
            # Validate that the annotation exists at all.
            _validate_schema_ref(decl, ctx=ctx)

        elif isinstance(decl, qlast.CreateAccessPolicy):
            _, pol_fq_name = ctx.get_fq_name(decl)

            pol_name = s_name.QualName(
                module=fq_name.module,
                name=f'{fq_name.name}@{pol_fq_name}',
            )
            assert isinstance(obj, qltracer.Source)
            ctx.objects[pol_name] = qltracer.AccessPolicy(pol_name, source=obj)


RECURSION_GUARD: Set[s_name.QualName] = set()


def get_ancestors(
    fq_name: s_name.QualName,
    ancestors: Dict[s_name.QualName, Set[s_name.QualName]],
    parents: Mapping[s_name.QualName, AbstractSet[s_name.QualName]],
) -> Set[s_name.QualName]:
    """Recursively compute ancestors (in place) from the parents graph."""

    # value already computed
    result = ancestors.get(fq_name, set())
    if result is RECURSION_GUARD:
        raise errors.InvalidDefinitionError(
            f'{str(fq_name)!r} is defined recursively')
    elif result:
        return result

    ancestors[fq_name] = RECURSION_GUARD

    parent_set = parents.get(fq_name, set())
    # base case: include the parents
    result = set(parent_set)
    for fq_parent in parent_set:
        # recursive step: include parents' ancestors
        result |= get_ancestors(fq_parent, ancestors, parents)

    ancestors[fq_name] = result

    return result


@functools.singledispatch
def trace_dependencies(
    node: qlast.Base,
    *,
    ctx: DepTraceContext,
) -> None:
    raise NotImplementedError(
        f"no SDL dep tracer handler for {node.__class__}")


@trace_dependencies.register
def trace_SetField(
    node: qlast.SetField,
    *,
    ctx: DepTraceContext,
) -> None:
    deps = set()

    assert node.value, "sdl SetField should always have value"
    for dep in qltracer.trace_refs(
        node.value,
        schema=ctx.schema,
        module=ctx.module,
        objects=ctx.objects,
        local_modules=ctx.local_modules,
        params={},
    ):
        # ignore std module dependencies
        if dep.get_module_name() not in s_schema.STD_MODULES:
            deps.add(dep)

    _register_item(node, deps=deps, ctx=ctx)


@trace_dependencies.register
def trace_ConcreteConstraint(
    node: qlast.CreateConcreteConstraint,
    *,
    ctx: DepTraceContext,
) -> None:
    deps = set()

    base_name = ctx.get_ref_name(node.name)
    if base_name.get_module_name() not in s_schema.STD_MODULES:
        deps.add(base_name)

    exprs = [ExprDependency(expr=arg) for arg in node.args]
    if node.subjectexpr:
        exprs.append(ExprDependency(expr=node.subjectexpr))
    if node.except_expr:
        exprs.append(ExprDependency(expr=node.except_expr))

    for cmd in node.commands:
        if isinstance(cmd, qlast.SetField) and cmd.name == "expr":
            assert cmd.value, "sdl SetField should always have value"
            assert isinstance(cmd.value, qlast.Expr)
            exprs.append(ExprDependency(expr=cmd.value))

    loop_control: Optional[s_name.QualName]
    if isinstance(ctx.depstack[-1][0], qlast.AlterScalarType):
        # Scalars are tightly bound to their constraints, so
        # we must prohibit any possible reference to this scalar
        # type from within the constraint.
        loop_control = ctx.depstack[-1][1]
    else:
        loop_control = None

    _register_item(
        node,
        deps=deps,
        hard_dep_exprs=exprs,
        loop_control=loop_control,
        source=ctx.depstack[-1][1],
        subject=ctx.depstack[-1][1],
        ctx=ctx,
    )


@trace_dependencies.register
def trace_AccessPolicy(
    node: qlast.CreateAccessPolicy,
    *,
    ctx: DepTraceContext,
) -> None:
    exprs = []
    if node.expr:
        exprs.append(ExprDependency(expr=node.expr))
    if node.condition:
        exprs.append(ExprDependency(expr=node.condition))

    _register_item(
        node,
        deps=set(),
        hard_dep_exprs=exprs,
        source=ctx.depstack[-1][1],
        subject=ctx.depstack[-1][1],
        ctx=ctx,
    )


@trace_dependencies.register
def trace_Index(
    node: qlast.CreateConcreteIndex,
    *,
    ctx: DepTraceContext,
) -> None:
    exprs = [ExprDependency(expr=node.expr)]
    if node.except_expr:
        exprs.append(ExprDependency(expr=node.except_expr))
    _register_item(
        node,
        hard_dep_exprs=exprs,
        source=ctx.depstack[-1][1],
        subject=ctx.depstack[-1][1],
        ctx=ctx,
    )


@trace_dependencies.register
def trace_ConcretePointer(
    node: qlast.CreateConcretePointer,
    *,
    ctx: DepTraceContext,
) -> None:
    deps: List[Dependency] = []
    if isinstance(node.target, qlast.TypeExpr):
        deps.append(TypeDependency(texpr=node.target))
    elif isinstance(node.target, qlast.Expr):
        deps.append(ExprDependency(expr=node.target))
    elif node.target is None:
        pass
    else:
        raise AssertionError(
            f'unexpected CreateConcretePointer.target: {node.target!r}')

    _register_item(
        node,
        hard_dep_exprs=deps,
        source=ctx.depstack[-1][1],
        ctx=ctx,
    )


@trace_dependencies.register
def trace_Alias(
    node: qlast.CreateAlias,
    *,
    ctx: DepTraceContext,
) -> None:
    hard_dep_exprs = []

    for cmd in node.commands:
        if isinstance(cmd, qlast.SetField) and cmd.name == "expr":
            assert cmd.value, "sdl SetField should always have value"
            assert isinstance(cmd.value, qlast.Expr)
            hard_dep_exprs.append(ExprDependency(expr=cmd.value))
            break

    _register_item(node, hard_dep_exprs=hard_dep_exprs, ctx=ctx)


@trace_dependencies.register
def trace_Global(
    node: qlast.CreateGlobal,
    *,
    ctx: DepTraceContext,
) -> None:
    hard_dep_exprs = []

    if isinstance(node.target, qlast.Expr):
        hard_dep_exprs.append(ExprDependency(expr=node.target))

    _register_item(node, hard_dep_exprs=hard_dep_exprs, ctx=ctx)


@trace_dependencies.register
def trace_Function(
    node: qlast.CreateFunction,
    *,
    ctx: DepTraceContext,
) -> None:
    # We also need to add all the signature types as dependencies
    # to make sure that DDL linearization of SDL will define the types
    # before the function.
    deps: List[Dependency] = []

    deps.extend(TypeDependency(texpr=param.type) for param in node.params)
    deps.append(TypeDependency(texpr=node.returning))

    params = {}
    for param in node.params:
        assert isinstance(param.type, qlast.TypeName)
        if not param.type.subtypes:
            param_t = ctx.get_ref_name(param.type.maintype)
            params[param.name] = param_t
        else:
            params[param.name] = s_name.QualName('std', 'BaseObject')

    if node.nativecode is not None:
        deps.append(FunctionDependency(expr=node.nativecode, params=params))
    elif (
        node.code is not None
        and node.code.language is qlast.Language.EdgeQL
        and node.code.code
    ):
        # Need to parse the actual code string and use that as the dependency.
        fcode = qlparser.parse(node.code.code)
        assert isinstance(fcode, qlast.Expr)
        deps.append(FunctionDependency(expr=fcode, params=params))

    # XXX: hard_dep_expr is used because it ultimately calls the
    # _get_hard_deps helper that extracts the proper dependency list
    # from types.
    _register_item(node, ctx=ctx, hard_dep_exprs=deps)


@trace_dependencies.register
def trace_default(
    node: qlast.CreateObject,
    *,
    ctx: DepTraceContext,
) -> None:
    # Generic DDL catchall
    _register_item(node, ctx=ctx)


def _clear_nonessential_subcommands(node: qlast.DDLOperation) -> None:
    node.commands = [
        cmd for cmd in node.commands
        if isinstance(cmd, qlast.SetField) and cmd.name.startswith('orig_')
    ]


def _register_item(
    decl: qlast.DDLOperation,
    *,
    deps: Optional[AbstractSet[s_name.QualName]] = None,
    hard_dep_exprs: Optional[Iterable[Dependency]] = None,
    loop_control: Optional[s_name.QualName] = None,
    source: Optional[s_name.QualName] = None,
    subject: Optional[s_name.QualName] = None,
    ctx: DepTraceContext,
) -> None:

    name, fq_name = ctx.get_fq_name(decl)

    if fq_name in ctx.ddlgraph:
        vn = get_verbosename_from_fqname(fq_name, ctx)
        msg = f'{vn} was already declared'

        raise errors.InvalidDefinitionError(
            msg, context=decl.context)

    if deps:
        deps = set(deps)
    else:
        deps = set()

    op = orig_op = copy.copy(decl)

    if ctx.depstack:
        if isinstance(op, qlast.CreateObject):
            op.sdl_alter_if_exists = True
        top_parent = parent = copy.copy(ctx.depstack[0][0])
        _clear_nonessential_subcommands(parent)
        for entry, _ in ctx.depstack[1:]:
            entry_op = copy.copy(entry)
            parent.commands.append(entry_op)
            parent = entry_op
            _clear_nonessential_subcommands(parent)

        parent.commands.append(op)
        op = top_parent
    else:
        assert isinstance(op, qlast.Command)
        op.aliases = [qlast.ModuleAliasDecl(alias=None, module=ctx.module)]

    assert isinstance(op, qlast.DDLCommand)
    node = topological.DepGraphEntry(
        item=op,
        deps={n for _, n in ctx.depstack if n != loop_control},
        extra=False,
    )
    ctx.ddlgraph[fq_name] = node

    if hasattr(decl, "bases"):
        # add parents to dependencies
        parents = ctx.parents.get(fq_name)
        if parents is not None:
            deps.update(parents)

    if ctx.depstack:
        # all ancestors should be seen as dependencies
        ancestor_bases = ctx.ancestors.get(ctx.depstack[-1][1])
        if ancestor_bases:
            for ancestor_base in ancestor_bases:
                base_item = qltracer.qualify_name(ancestor_base, name)
                if base_item in ctx.objects:
                    deps.add(base_item)

    ast_subcommands = getattr(decl, 'commands', [])
    commands = []
    if ast_subcommands:
        subcmds: List[qlast.DDLOperation] = []
        for cmd in ast_subcommands:
            # include dependency on constraints or annotations if present
            if isinstance(cmd, qlast.CreateConcreteConstraint):
                cmd_name = ctx.get_local_name(cmd.name)
                if cmd_name.get_module_name() not in s_schema.STD_MODULES:
                    deps.add(cmd_name)
            elif isinstance(cmd, qlast.CreateAnnotationValue):
                cmd_name = ctx.get_local_name(cmd.name)
                if cmd_name.get_module_name() not in s_schema.STD_MODULES:
                    deps.add(cmd_name)

            if (isinstance(cmd, qlast.ObjectDDL)
                    # HACK: functions don't have alters at the moment
                    and not isinstance(decl, qlast.CreateFunction)):
                subcmds.append(cmd)
            elif (isinstance(cmd, qlast.SetField)
                  and not cmd.special_syntax
                  and not isinstance(cmd.value, qlast.BaseConstant)
                  and not isinstance(
                      op, (qlast.CreateAlias, qlast.CreateGlobal))):
                subcmds.append(cmd)
            else:
                commands.append(cmd)

        if subcmds:
            assert isinstance(decl, qlast.ObjectDDL)
            alter_name = f"Alter{decl.__class__.__name__[len('Create'):]}"
            alter_cls = getattr(qlast, alter_name)
            alter_cmd: qlast.ObjectDDL = alter_cls(name=decl.name)

            # indexes need to preserve their "on" expression
            if isinstance(decl, qlast.CreateConcreteIndex):
                assert isinstance(alter_cmd, qlast.ConcreteIndexCommand)
                alter_cmd.expr = decl.expr

            # constraints need to preserve their "on" expression
            if isinstance(decl, qlast.CreateConcreteConstraint):
                assert isinstance(alter_cmd, qlast.ConcreteConstraintOp)
                alter_cmd.subjectexpr = decl.subjectexpr
                alter_cmd.args = decl.args

            if not ctx.depstack:
                alter_cmd.aliases = [
                    qlast.ModuleAliasDecl(alias=None, module=ctx.module)
                ]

            ctx.depstack.append((alter_cmd, fq_name))

            for cmd in subcmds:
                trace_dependencies(cmd, ctx=ctx)

            ctx.depstack.pop()

    if hard_dep_exprs:
        for expr in hard_dep_exprs:
            if isinstance(expr, TypeDependency):
                deps |= _get_hard_deps(expr.texpr, ctx=ctx)
            elif isinstance(expr, ExprDependency):
                qlexpr = expr.expr
                if isinstance(expr, FunctionDependency):
                    params = expr.params
                else:
                    params = {}

                tdeps = qltracer.trace_refs(
                    qlexpr,
                    schema=ctx.schema,
                    module=ctx.module,
                    source=source,
                    path_prefix=source,
                    subject=subject or fq_name,
                    objects=ctx.objects,
                    local_modules=ctx.local_modules,
                    params=params,
                )

                pdeps: MutableSet[s_name.QualName] = set()
                for dep in tdeps:
                    # ignore std module dependencies
                    if dep.get_module_name() not in s_schema.STD_MODULES:
                        # First check if the dep is a pointer that's
                        # defined explicitly. If it's not explicitly
                        # defined, check for ancestors and use them
                        # instead.
                        #
                        # FIXME: Ideally we should use the closest
                        # ancestor, instead of all of them, but
                        # including all is still correct.
                        if '@' in dep.name:
                            pdeps |= _get_pointer_deps(dep, ctx=ctx)
                        else:
                            pdeps.add(dep)

                # Handle the pre-processed deps now.
                for dep in pdeps:
                    deps.add(dep)

                    if isinstance(
                            decl, (qlast.CreateAlias, qlast.CreateGlobal)):
                        # If the declaration is a view, we need to be
                        # dependent on all the types and their props
                        # used in the view.
                        vdeps = {dep} | ctx.ancestors.get(dep, set())
                        for vdep in vdeps:
                            deps |= ctx.defdeps.get(vdep, set())

                    if (
                        isinstance(decl, (
                            qlast.CreateConcretePointer, qlast.CreateGlobal))
                        and isinstance(decl.target, qlast.Expr)
                    ) or isinstance(decl, qlast.CreateAccessPolicy):
                        # If the declaration is a computable pointer/global
                        # or access policy, we need to include the
                        # possible constraints for every dependency
                        # that it lists. This is so that any other
                        # links/props that this computable uses has
                        # all of their constraints defined before the
                        # computable and the cardinality can be
                        # inferred correctly.
                        cdeps = {dep} | ctx.ancestors.get(dep, set())
                        for cdep in cdeps:
                            deps |= ctx.constraints.get(cdep, set())
            else:
                raise AssertionError(f'unexpected dependency type: {expr!r}')

    orig_op.commands = commands

    if loop_control:
        parent_node = ctx.ddlgraph[loop_control]
        parent_node.loop_control.add(fq_name)

    node.deps |= deps


def _get_pointer_deps(
    pointer: s_name.QualName,
    *,
    ctx: DepTraceContext,
) -> MutableSet[s_name.QualName]:
    result: MutableSet[s_name.QualName] = set()
    owner_name, ptr_name = pointer.name.split('@', 1)
    # For every ancestor of the type, where
    # the pointer is defined, see if there are
    # ancestors of the pointer itself defined.
    for tansc in ctx.ancestors.get(
            s_name.QualName(
                module=pointer.module, name=owner_name
            ), set()):
        ptr_ansc = s_name.QualName(
            module=tansc.module,
            name=f'{tansc.name}@{ptr_name}',
        )

        # Only add the pointer's ancestor if
        # it is explicitly defined.
        if ptr_ansc in ctx.objects:
            result.add(ptr_ansc)

    # Only add the pointer if it is explicitly defined.
    if pointer in ctx.objects:
        result.add(pointer)

    # HACK: Add all pointers that have this pointer (link, actually)
    # as their prefix. As a rule, the assumption is that depending on
    # a link typically comes as a package of depending on the link's
    # property.
    for propname in ctx.objects:
        if str(propname).startswith(str(pointer) + '@'):
            result.add(propname)

    return result


def _get_hard_deps(
    expr: qlast.TypeExpr,
    *,
    ctx: DepTraceContext
) -> MutableSet[s_name.QualName]:
    deps: MutableSet[s_name.QualName] = set()

    if isinstance(expr, qlast.TypeName):
        # We care about subtypes dependencies, because
        # they can either be custom scalars or illegal
        # ObjectTypes (then error message will depend on
        # dependency tracing)
        if expr.subtypes:
            for subtype in expr.subtypes:
                deps |= _get_hard_deps(subtype, ctx=ctx)

        else:
            # Base case.
            name = ctx.get_ref_name(expr.maintype)
            if name.get_module_name() not in s_schema.STD_MODULES:
                deps.add(name)

    elif isinstance(expr, qlast.TypeExprLiteral):
        pass

    elif isinstance(expr, qlast.TypeOf):
        # TODO: maybe we should also recurse into the inner expr?
        pass

    elif isinstance(expr, qlast.TypeOp):
        deps |= _get_hard_deps(expr.left, ctx=ctx)
        deps |= _get_hard_deps(expr.right, ctx=ctx)

    return deps


def _get_bases(
    decl: qlast.CreateObject,
    *,
    ctx: LayoutTraceContext
) -> List[s_name.QualName]:
    """Resolve object bases from the "extends" declaration."""
    if not isinstance(decl, qlast.BasesMixin):
        return []

    bases = []

    if decl.bases:
        # Explicit inheritance
        has_enums = any(
            (
                isinstance(br.maintype, qlast.TypeName)
                and br.maintype.name == "enum"
                and br.subtypes
            )
            for br in decl.bases
        )

        if has_enums:
            if len(decl.bases) > 1:
                raise errors.SchemaError(
                    f"invalid scalar type definition, enumeration must "
                    f"be the only supertype specified",
                    context=decl.bases[0].context,
                )

            bases = [s_name.QualName("std", "anyenum")]

        else:
            for base_ref in decl.bases:
                # Validate that the base actually exists.
                tracer_type = _get_tracer_type(decl)
                assert tracer_type is not None
                obj = _resolve_type_name(
                    base_ref.maintype,
                    tracer_type=tracer_type,
                    ctx=ctx
                )
                name = obj.get_name(ctx.schema)
                if not isinstance(name, s_name.QualName):
                    qname = s_name.QualName.from_string(name.name)
                else:
                    qname = name
                bases.append(qname)

    return bases


def _resolve_type_expr(
    texpr: qlast.TypeExpr,
    *,
    ctx: LayoutTraceContext,
) -> qltracer.TypeLike:

    if isinstance(texpr, qlast.TypeName):
        if texpr.subtypes:
            return qltracer.Type(
                name=s_name.QualName(module='__coll__', name=texpr.name or ''),
            )
        else:
            return cast(
                qltracer.TypeLike,
                _resolve_type_name(
                    texpr.maintype,
                    tracer_type=qltracer.Type,
                    ctx=ctx,
                )
            )

    elif isinstance(texpr, qlast.TypeOp):

        if texpr.op == '|':
            return qltracer.UnionType([
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


TRACER_TO_REAL_TYPE_MAP = {
    qltracer.Type: s_types.Type,
    qltracer.ObjectType: s_objtypes.ObjectType,
    qltracer.ScalarType: s_scalars.ScalarType,
    qltracer.Constraint: s_constr.Constraint,
    qltracer.Annotation: s_anno.Annotation,
    qltracer.Property: s_props.Property,
    qltracer.Link: s_links.Link,
}


def _get_local_obj(
    refname: s_name.QualName,
    tracer_type: Type[qltracer.NamedObject],
    sourcectx: Optional[parsing.ParserContext],
    *,
    ctx: LayoutTraceContext,
) -> Optional[qltracer.NamedObject]:

    obj = ctx.objects.get(refname)

    if isinstance(obj, s_pseudo.PseudoType):
        raise errors.SchemaError(
            f'invalid type: {obj.get_verbosename(ctx.schema)} is a generic '
            f'type and they are not supported in user-defined schema',
            context=sourcectx,
        )

    elif obj is not None and not isinstance(obj, tracer_type):
        obj_type = TRACER_TO_REAL_TYPE_MAP[type(obj)]
        real_type = TRACER_TO_REAL_TYPE_MAP[tracer_type]
        raise errors.InvalidReferenceError(
            f'{str(refname)!r} exists, but is '
            f'{english.add_a(obj_type.get_schema_class_displayname())}, '
            f'not {english.add_a(real_type.get_schema_class_displayname())}',
            context=sourcectx,
        )

    return obj


def _resolve_type_name(
    ref: qlast.BaseObjectRef,
    *,
    tracer_type: Type[qltracer.NamedObject],
    ctx: LayoutTraceContext,
) -> qltracer.ObjectLike:

    refname = ctx.get_ref_name(ref)
    local_obj = _get_local_obj(refname, tracer_type, ref.context, ctx=ctx)
    obj: qltracer.ObjectLike
    if local_obj is not None:
        obj = local_obj
    else:
        obj = _resolve_schema_ref(
            refname,
            type=tracer_type,
            sourcectx=ref.context,
            ctx=ctx,
        )

    return obj


def _get_tracer_type(
    decl: qlast.CreateObject,
) -> Optional[Type[qltracer.NamedObject]]:

    tracer_type: Optional[Type[qltracer.NamedObject]] = None

    if isinstance(decl, qlast.CreateObjectType):
        tracer_type = qltracer.ObjectType
    elif isinstance(decl, qlast.CreateScalarType):
        tracer_type = qltracer.ScalarType
    elif isinstance(decl, (qlast.CreateConstraint,
                           qlast.CreateConcreteConstraint)):
        tracer_type = qltracer.Constraint
    elif isinstance(decl, (qlast.CreateAnnotation,
                           qlast.CreateAnnotationValue)):
        tracer_type = qltracer.Annotation
    elif isinstance(decl, (qlast.CreateProperty,
                           qlast.CreateConcreteProperty)):
        tracer_type = qltracer.Property
    elif isinstance(decl, (qlast.CreateLink,
                           qlast.CreateConcreteLink)):
        tracer_type = qltracer.Link

    return tracer_type


def _validate_schema_ref(
    decl: qlast.CreateObject,
    *,
    ctx: LayoutTraceContext,
) -> None:
    refname = ctx.get_ref_name(decl.name)
    tracer_type = _get_tracer_type(decl)
    if tracer_type is None:
        # Bail out and rely on some other validation mechanism
        return

    local_obj = _get_local_obj(refname, tracer_type, decl.context, ctx=ctx)

    if local_obj is None:
        _resolve_schema_ref(
            refname,
            type=tracer_type,
            sourcectx=decl.context,
            ctx=ctx,
        )


def _resolve_schema_ref(
    name: s_name.Name,
    type: Type[qltracer.NamedObject],
    sourcectx: Optional[parsing.ParserContext],
    *,
    ctx: LayoutTraceContext,
) -> s_obj.SubclassableObject:
    real_type = TRACER_TO_REAL_TYPE_MAP[type]
    try:
        return ctx.schema.get(name, type=real_type, sourcectx=sourcectx)
    except errors.InvalidReferenceError as e:
        s_utils.enrich_schema_lookup_error(
            e,
            name,
            schema=ctx.schema,
            modaliases=ctx.modaliases,
            item_type=real_type,
            context=sourcectx,
        )
        raise
