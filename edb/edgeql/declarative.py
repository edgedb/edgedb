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

import copy
import functools
import re
from typing import *  # NoQA
from collections import defaultdict

from edb import errors

from edb.common import topological

from edb.edgeql import ast as qlast
from edb.edgeql import codegen as qlcodegen
from edb.edgeql import tracer as qltracer

from edb.schema import name as s_name
from edb.schema import schema as s_schema


STD_PREFIX_RE = re.compile(rf'^({"|".join(s_schema.STD_MODULES)})::')


class TraceContextBase:
    def __init__(self, schema):
        self.schema = schema
        self.module = None
        self.depstack = []
        self.modaliases = {}
        self.objects = {}
        self.parents = {}
        self.ancestors = {}
        self.defdeps = defaultdict(set)

    def set_module(self, module):
        self.module = module
        self.modaliases = {None: module}

    def get_local_name(
        self,
        ref: qlast.ObjectRef,
        *,
        type: Optional[Type[qltracer.NamedObject]] = None
    ) -> s_name.Name:
        if isinstance(ref, qlast.ObjectRef):
            if ref.module:
                return s_name.Name(module=ref.module, name=ref.name)
            else:
                if type is None:
                    return s_name.Name(module=self.module, name=ref.name)
                else:
                    # check if there's a name in default module
                    # actually registered to the right type
                    name = f'{self.module}::{ref.name}'
                    if isinstance(self.objects.get(name), type):
                        return s_name.Name(module=self.module, name=ref.name)
                    else:
                        return s_name.Name(module='std', name=ref.name)
        else:
            raise TypeError(
                "ObjectRef expected "
                "(got type {!r})".format(type(ref).__name__)
            )

    def get_ref_name(self, ref: qlast.ObjectRef) -> s_name.Name:
        if isinstance(ref, qlast.ObjectRef):
            if ref.module:
                return s_name.Name(module=ref.module, name=ref.name)
            elif f'{self.module}::{ref.name}' in self.objects:
                return s_name.Name(module=self.module, name=ref.name)
            else:
                return s_name.Name(module="std", name=ref.name)
        else:
            raise TypeError(
                "ObjectRef expected "
                "(got type {!r})".format(type(ref).__name__)
            )

    def get_fq_name(self, decl: qlast.CreateObject) -> Tuple[str, str]:
        # Get the basic name form.
        if isinstance(decl, qlast.CreateConcretePointer):
            name = decl.name.name
        elif isinstance(decl, qlast.BaseSetField):
            name = decl.name
        else:
            name = self.get_local_name(decl.name)

        # The name may need to be augmented if it's a pointer.
        if self.depstack:
            fq_name = self.depstack[-1][1] + "@" + name
        else:
            fq_name = name

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

            for cmd in decl.commands:
                if isinstance(cmd, qlast.SetField) and cmd.name == "expr":
                    exprs.append(decl.value)

            extra_name = '|'.join(qlcodegen.generate_source(e) for e in exprs)

        if extra_name is not None:
            fq_name = f'{fq_name}:{extra_name}'

        return name, fq_name


class LayoutTraceContext(TraceContextBase):
    def __init__(self, schema, local_modules):
        super().__init__(schema)
        self.local_modules = local_modules
        self.inh_graph = {}


class DepTraceContext(TraceContextBase):
    def __init__(self, schema, ddlgraph, objects, parents, ancestors,
                 defdeps):
        super().__init__(schema)
        self.ddlgraph = ddlgraph
        self.objects = objects
        self.parents = parents
        self.ancestors = ancestors
        self.defdeps = defdeps


def sdl_to_ddl(schema, documents):
    ddlgraph = {}
    mods = []

    ctx = LayoutTraceContext(
        schema,
        local_modules=frozenset(mod for mod, schema_decl in documents),
    )

    for module_name, declarations in documents:
        ctx.set_module(module_name)
        for decl_ast in declarations:
            if isinstance(decl_ast, qlast.CreateObject):
                _, fq_name = ctx.get_fq_name(decl_ast)
                if isinstance(decl_ast, (qlast.CreateObjectType,
                                         qlast.CreateAlias)):
                    ctx.objects[fq_name] = qltracer.ObjectType(fq_name)

                elif isinstance(decl_ast, qlast.CreateScalarType):
                    ctx.objects[fq_name] = qltracer.Type(fq_name)

                elif isinstance(decl_ast, (qlast.CreateLink,
                                           qlast.CreateProperty)):
                    ctx.objects[fq_name] = qltracer.Pointer(
                        fq_name, source=None, target=None)
                elif isinstance(decl_ast, qlast.CreateFunction):
                    ctx.objects[fq_name] = qltracer.Function(fq_name)
                elif isinstance(decl_ast, qlast.CreateConstraint):
                    ctx.objects[fq_name] = qltracer.Constraint(fq_name)
                elif isinstance(decl_ast, qlast.CreateAnnotation):
                    ctx.objects[fq_name] = qltracer.Annotation(fq_name)
                else:
                    raise AssertionError(
                        f'unexpected SDL declaration: {decl_ast}')

    for module_name, declarations in documents:
        ctx.set_module(module_name)
        for decl_ast in declarations:
            trace_layout(decl_ast, ctx=ctx)

    # compute the ancestors graph
    for fq_name in ctx.parents.keys():
        ctx.ancestors[fq_name] = get_ancestors(
            fq_name, ctx.ancestors, ctx.parents)

    topological.normalize(ctx.inh_graph, _merge_items)

    ctx = DepTraceContext(
        schema, ddlgraph, ctx.objects, ctx.parents, ctx.ancestors,
        ctx.defdeps
    )
    for module_name, declarations in documents:
        ctx.set_module(module_name)
        # module needs to be created regardless of whether its
        # contents are empty or not
        mods.append(qlast.CreateModule(
            name=qlast.ObjectRef(name=module_name)))
        for decl_ast in declarations:
            trace_dependencies(decl_ast, ctx=ctx)

    return mods + list(topological.sort(ddlgraph, allow_unresolved=False))


def _merge_items(item, parent):

    for pn, ptr in parent.pointers.items():
        if pn not in item.pointers:
            ptr_copy = qltracer.Pointer(
                pn, source=ptr.source, target=ptr.target)
            ptr_copy.pointers = dict(ptr.pointers)
            item.pointers[pn] = ptr_copy
        else:
            ptr_copy = qltracer.Pointer(
                pn, source=item, target=item.pointers[pn].target)
            ptr_copy.pointers = dict(item.pointers[pn].pointers)
            item.pointers[pn] = _merge_items(ptr_copy, ptr)

    return item


@functools.singledispatch
def trace_layout(node: qlast.Base, *, ctx: LayoutTraceContext):
    pass


@trace_layout.register
def trace_layout_Schema(node: qlast.Schema, *, ctx: LayoutTraceContext):
    for decl in node.declarations:
        trace_layout(decl, ctx=ctx)


@trace_layout.register
def trace_layout_CreateObjectType(
        node: qlast.CreateObjectType, *, ctx: LayoutTraceContext):

    _trace_item_layout(node, ctx=ctx)


@trace_layout.register
def trace_layout_CreateLink(
        node: qlast.CreateLink, *, ctx: LayoutTraceContext):

    _trace_item_layout(node, ctx=ctx)


@trace_layout.register
def trace_layout_CreateProperty(
        node: qlast.CreateProperty, *, ctx: LayoutTraceContext):

    _trace_item_layout(node, ctx=ctx)


def _trace_item_layout(node: qlast.CreateObject, *,
                       obj=None, fq_name=None, ctx: LayoutTraceContext):
    if obj is None:
        fq_name = ctx.get_local_name(node.name)
        obj = ctx.objects[fq_name]

    if hasattr(node, "bases"):
        bases = []
        # construct the parents set, used later in ancestors graph
        parents = set()

        for ref in _get_bases(node, ctx=ctx):
            bases.append(ref)

            # ignore std modules dependencies
            if ref.module not in s_schema.STD_MODULES:
                parents.add(ref)

            if (ref.module not in ctx.local_modules
                    and ref not in ctx.inh_graph):
                base = ctx.schema.get(ref)
                base_obj = type(obj)(name=ref)
                for pn, p in base.get_pointers(ctx.schema).items(ctx.schema):
                    base_obj.pointers[pn] = qltracer.Pointer(
                        pn,
                        source=base,
                        target=p.get_target(ctx.schema),
                    )
                ctx.inh_graph[ref] = {
                    "item": base_obj,
                }

        ctx.parents[fq_name] = parents
        ctx.inh_graph[fq_name] = {
            "item": obj,
            "deps": bases,
            "merge": bases,
        }

    for decl in node.commands:
        if isinstance(decl, qlast.CreateConcretePointer):
            if isinstance(decl.target, qlast.TypeExpr):
                target = _resolve_type_expr(decl.target, ctx=ctx)
            else:
                target = None

            ptr = qltracer.Pointer(decl.name.name, source=obj, target=target)
            obj.pointers[decl.name.name] = ptr
            ptr_name = f'{fq_name}@{decl.name.name}'
            ctx.objects[ptr_name] = ptr
            ctx.defdeps[fq_name].add(ptr_name)

            _trace_item_layout(
                decl, obj=ptr, fq_name=ptr_name, ctx=ctx)


def get_ancestors(fq_name, ancestors, parents):
    '''Recursively compute ancestors (in place) from the parents graph.'''

    # value already computed
    result = ancestors.get('fq_name', set())
    if result:
        return result

    parent_set = parents.get(fq_name, set())
    # base case: include the parents
    result = set(parent_set)
    for fq_parent in parent_set:
        # recursive step: include parents' ancestors
        result |= get_ancestors(fq_parent, ancestors, parents)

    ancestors[fq_name] = result

    return result


@functools.singledispatch
def trace_dependencies(node: qlast.Base, *, ctx: DepTraceContext):
    raise NotImplementedError(
        f"no SDL dep tracer handler for {node.__class__}")


@trace_dependencies.register
def trace_SetField(node: qlast.SetField, *, ctx: DepTraceContext):
    deps = set()

    for dep in qltracer.trace_refs(
        node.value,
        schema=ctx.schema,
        module=ctx.module,
        objects=ctx.objects,
    ):
        # ignore std module dependencies
        if not STD_PREFIX_RE.match(dep):
            deps.add(dep)

    _register_item(node, deps=deps, ctx=ctx)


@trace_dependencies.register
def trace_ConcreteConstraint(
    node: qlast.CreateConcreteConstraint, *, ctx: DepTraceContext
):
    deps = set()

    base_name = ctx.get_ref_name(node.name)
    if base_name.module not in s_schema.STD_MODULES:
        deps.add(base_name)

    exprs = list(node.args)
    if node.subjectexpr:
        exprs.append(node.subjectexpr)

    for cmd in node.commands:
        if isinstance(cmd, qlast.SetField) and cmd.name == "expr":
            exprs.append(node.value)

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
        subject=ctx.depstack[-1][1],
        ctx=ctx,
    )


@trace_dependencies.register
def trace_Index(
    node: qlast.CreateIndex, *, ctx: DepTraceContext
):
    _register_item(
        node,
        hard_dep_exprs=[node.expr],
        source=ctx.depstack[-1][1],
        subject=ctx.depstack[-1][1],
        ctx=ctx,
    )


@trace_dependencies.register
def trace_ConcretePointer(
    node: qlast.CreateConcretePointer, *, ctx: DepTraceContext
):
    _register_item(
        node,
        hard_dep_exprs=[node.target],
        source=ctx.depstack[-1][1],
        ctx=ctx,
    )


@trace_dependencies.register
def trace_Alias(
    node: qlast.CreateAlias, *, ctx: DepTraceContext
):
    hard_dep_exprs = []

    for cmd in node.commands:
        # SetField or SetSpecialField are equivalent here
        if isinstance(cmd, qlast.BaseSetField) and cmd.name == "expr":
            hard_dep_exprs.append(cmd.value)
            break

    _register_item(node, hard_dep_exprs=hard_dep_exprs, ctx=ctx)


@trace_dependencies.register
def trace_Function(node: qlast.CreateFunction, *, ctx: DepTraceContext):
    # We also need to add all the signature types as dependencies
    # to make sure that DDL linearization of SDL will define the types
    # before the function.
    deps = [param.type for param in node.params]
    deps.append(node.returning)

    # XXX: hard_dep_expr is used because it ultimately calls the
    # _get_hard_deps helper that extracts the proper dependency list
    # from types.
    _register_item(node, ctx=ctx, hard_dep_exprs=deps)


@trace_dependencies.register
def trace_default(node: qlast.CreateObject, *, ctx: DepTraceContext):
    # Generic DDL catchall
    _register_item(node, ctx=ctx)


def _register_item(
    decl, *, deps=None, hard_dep_exprs=None, loop_control=None,
    source=None, subject=None, ctx: DepTraceContext
):

    name, fq_name = ctx.get_fq_name(decl)

    if deps:
        deps = set(deps)
    else:
        deps = set()

    op = orig_op = copy.copy(decl)

    if ctx.depstack:
        op.sdl_alter_if_exists = True
        top_parent = parent = copy.copy(ctx.depstack[0][0])
        parent.commands = []
        for entry, _entry_name in ctx.depstack[1:]:
            entry_op = copy.copy(entry)
            entry_op.commands = []
            parent.commands.append(entry_op)
            parent = entry_op

        parent.commands.append(op)
        op = top_parent
    else:
        op.aliases = [qlast.ModuleAliasDecl(alias=None, module=ctx.module)]

    node = {
        "item": op,
        "deps": {n for _, n in ctx.depstack if n != loop_control},
    }
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
                base_item = f'{ancestor_base}@{name}'

                if base_item in ctx.objects:
                    deps.add(base_item)

    ast_subcommands = getattr(decl, 'commands', [])
    commands = []
    if ast_subcommands:
        subcmds = []
        for cmd in ast_subcommands:
            # include dependency on constraints or annotations if present
            if isinstance(cmd, qlast.CreateConcreteConstraint):
                cmd_name = ctx.get_local_name(
                    cmd.name, type=qltracer.Constraint)
                if cmd_name.module not in s_schema.STD_MODULES:
                    deps.add(cmd_name)
            elif isinstance(cmd, qlast.CreateAnnotationValue):
                cmd_name = ctx.get_local_name(
                    cmd.name, type=qltracer.Annotation)
                if cmd_name.module not in s_schema.STD_MODULES:
                    deps.add(cmd_name)

            if (isinstance(cmd, qlast.ObjectDDL)
                    # HACK: functions don't have alters at the moment
                    and not isinstance(decl, qlast.CreateFunction)):
                subcmds.append(cmd)
            elif (isinstance(cmd, qlast.SetField)
                  and not isinstance(cmd.value, qlast.BaseConstant)
                  and not isinstance(op, qlast.CreateAlias)):
                subcmds.append(cmd)
            else:
                commands.append(cmd)

        if subcmds:
            alter_name = f"Alter{decl.__class__.__name__[len('Create'):]}"
            alter_cls = getattr(qlast, alter_name)
            alter_cmd = alter_cls(name=decl.name)

            # indexes need to preserve their "on" expression
            if alter_name == 'AlterIndex':
                # find the original expr, which will be in non-normalized form
                for sub in op.commands:
                    if isinstance(sub, qlast.CreateIndex):
                        alter_cmd.expr = sub.expr
                        break
            # constraints need to preserve their "on" expression
            elif alter_name == 'AlterConcreteConstraint':
                # find the original expr, which will be in non-normalized form
                for sub in op.commands:
                    if isinstance(sub, qlast.CreateConcreteConstraint):
                        alter_cmd.subjectexpr = sub.subjectexpr
                        break

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
            if isinstance(expr, qlast.TypeExpr):
                deps |= _get_hard_deps(expr, ctx=ctx)
            else:
                tdeps = qltracer.trace_refs(
                    expr,
                    schema=ctx.schema,
                    module=ctx.module,
                    source=source,
                    path_prefix=source,
                    subject=subject or fq_name,
                    objects=ctx.objects,
                )

                for dep in tdeps:
                    # ignore std module dependencies
                    if not STD_PREFIX_RE.match(dep):
                        deps.add(dep)

                        # If the declaration is a view, we need to be
                        # dependent on all the types and their props
                        # used in the view.
                        if isinstance(decl, qlast.CreateAlias):
                            vdeps = {dep} | ctx.ancestors.get(dep, set())
                            for vdep in vdeps:
                                deps |= ctx.defdeps.get(vdep, set())

    orig_op.commands = commands

    if loop_control:
        parent_node = ctx.ddlgraph[loop_control]
        if 'loop-control' not in parent_node:
            parent_node['loop-control'] = {fq_name}
        else:
            parent_node['loop-control'].add(fq_name)

    node["deps"].update(deps)


def _get_hard_deps(
    expr: qlast.TypeExpr, *,
    ctx: DepTraceContext
) -> MutableSet[s_name.Name]:
    deps = set()

    # If we have any type ops, get a flat list of their operands.
    targets = qlast.get_targets(expr)
    for target in targets:
        # We care about subtypes dependencies, because
        # they can either be custom scalars or illegal
        # ObjectTypes (then error message will depend on
        # dependency tracing)
        if target.subtypes:
            for subtype in target.subtypes:
                # Recurse!
                deps |= _get_hard_deps(subtype, ctx=ctx)

        else:
            # Base case.
            name = ctx.get_ref_name(target.maintype)
            if name.module not in s_schema.STD_MODULES:
                deps.add(name)

    return deps


def _get_bases(
    decl: qlast.BasesMixin,
    *,
    ctx: LayoutTraceContext
) -> List[s_name.Name]:
    """Resolve object bases from the "extends" declaration."""
    bases = []

    if decl.bases:
        # Explicit inheritance
        has_enums = any(
            br.maintype.name == "enum" and br.subtypes for br in decl.bases
        )

        if has_enums:
            if len(decl.bases) > 1:
                raise errors.SchemaError(
                    f"invalid scalar type definition, enumeration must "
                    f"be the only supertype specified",
                    context=decl.bases[0].context,
                )

            bases = [s_name.Name("std::anyenum")]

        else:
            for base_ref in decl.bases:
                base_name = ctx.get_ref_name(base_ref.maintype)
                bases.append(base_name)

    return bases


def _resolve_type_expr(texpr: qlast.TypeExpr, *, ctx: LayoutTraceContext):

    if isinstance(texpr, qlast.TypeName):
        if texpr.subtypes:
            return qltracer.Type(name=texpr.maintype.name)
        else:
            refname = ctx.get_ref_name(texpr.maintype)
            obj = ctx.objects.get(refname)
            if obj is None:
                obj = ctx.schema.get(refname, default=None)

            return obj

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
