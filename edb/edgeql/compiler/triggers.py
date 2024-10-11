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


"""EdgeQL trigger compilation."""


from __future__ import annotations

from typing import Optional, Collection

from edb import errors

from edb.ir import ast as irast

from edb.schema import name as sn
from edb.schema import objtypes as s_objtypes
from edb.schema import triggers as s_triggers
from edb.schema import types as s_types
from edb.schema import expr as s_expr

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from . import context
from . import dispatch
from . import options
from . import schemactx
from . import setgen
from . import typegen


TRIGGER_KINDS = {
    irast.UpdateStmt: qltypes.TriggerKind.Update,
    irast.DeleteStmt: qltypes.TriggerKind.Delete,
    irast.InsertStmt: qltypes.TriggerKind.Insert,
}


def compile_trigger(
    trigger: s_triggers.Trigger,
    affected: set[tuple[s_objtypes.ObjectType, irast.MutatingStmt]],
    all_typs: set[s_objtypes.ObjectType],
    *,
    ctx: context.ContextLevel,
) -> irast.Trigger:
    schema = ctx.env.schema

    scope = trigger.get_scope(schema)
    kinds = set(trigger.get_kinds(schema))
    source = trigger.get_subject(schema)

    with ctx.detached() as tc, tc.newscope(fenced=True) as sctx:
        sctx.schema_factoring()
        sctx.anchors = sctx.anchors.copy()

        anchors = {}
        new_path = irast.PathId.from_type(
            schema,
            source,
            typename=sn.QualName(module='__derived__', name='__new__'),
            env=ctx.env,
        )
        new_set = setgen.class_set(
            source, path_id=new_path, ignore_rewrites=True, ctx=sctx)
        new_set.expr = irast.TriggerAnchor(typeref=new_set.typeref)

        old_set = None
        if qltypes.TriggerKind.Insert not in kinds:
            old_path = irast.PathId.from_type(
                schema,
                source,
                typename=sn.QualName(module='__derived__', name='__old__'),
                env=ctx.env,
            )
            old_set = setgen.class_set(
                source, path_id=old_path, ignore_rewrites=True, ctx=sctx)
            old_set.expr = irast.TriggerAnchor(typeref=old_set.typeref)
            anchors['__old__'] = old_set
        if qltypes.TriggerKind.Delete not in kinds:
            anchors['__new__'] = new_set

        for name, ir in anchors.items():
            if scope == qltypes.TriggerScope.Each:
                sctx.path_scope.attach_path(ir.path_id, span=None, ctx=sctx)
                sctx.iterator_path_ids |= {ir.path_id}
            sctx.anchors[name] = ir

        trigger_expr: Optional[s_expr.Expression] = trigger.get_expr(schema)
        assert trigger_expr
        trigger_ast = trigger_expr.parse()

        # A conditional trigger desugars to a FOR query that puts the
        # condition in the FILTER of a trivial SELECT.
        condition: Optional[s_expr.Expression] = trigger.get_condition(schema)
        if condition:
            trigger_ast = qlast.ForQuery(
                iterator_alias='__',
                iterator=qlast.SelectQuery(
                    result=qlast.Tuple(elements=[]),
                    where=condition.parse(),
                ),
                result=trigger_ast,
            )

        trigger_set = dispatch.compile(trigger_ast, ctx=sctx)

    typeref = typegen.type_to_typeref(source, env=ctx.env)
    taffected = {
        (typegen.type_to_typeref(t, env=ctx.env), ir) for t, ir in affected
    }
    tall = {
        typegen.type_to_typeref(t, env=ctx.env) for t in all_typs
    }

    return irast.Trigger(
        expr=trigger_set,
        kinds=kinds,
        scope=scope,
        source_type=typeref,
        affected=taffected,
        all_affected_types=tall,
        new_set=new_set,
        old_set=old_set,
    )


def compile_triggers_phase(
    dml_stmts: Collection[irast.MutatingStmt],
    defining_trigger_on: Optional[s_types.Type],
    defining_trigger_kinds: Optional[Collection[qltypes.TriggerKind]],
    *,
    ctx: context.ContextLevel,
) -> tuple[irast.Trigger, ...]:
    schema = ctx.env.schema

    trigger_map: dict[
        s_triggers.Trigger,
        tuple[
            set[tuple[s_objtypes.ObjectType, irast.MutatingStmt]],
            set[s_objtypes.ObjectType],
        ],
    ] = {}
    for stmt in dml_stmts:
        kind = TRIGGER_KINDS[type(stmt)]

        stype = schemactx.concretify(
            setgen.get_set_type(stmt.result, ctx=ctx), ctx=ctx)
        assert isinstance(stype, s_objtypes.ObjectType)
        # For updates and deletes, we need to look to see if any
        # descendant types have triggers.
        if isinstance(stmt, irast.InsertStmt):
            stypes = {stype}
        else:
            stypes = schemactx.get_all_concrete(stype, ctx=ctx)

        # Process all the types, starting with the base type
        for subtype in sorted(stypes, key=lambda t: t != stype):
            if (defining_trigger_on and defining_trigger_kinds
                and kind in defining_trigger_kinds
                and subtype.issubclass(ctx.env.schema, defining_trigger_on)
            ):
                name = str(defining_trigger_on.get_name(ctx.env.schema))
                raise errors.SchemaDefinitionError(
                    f"trigger on {name} after {kind.lower()} is recursive"
                )

            for trigger in subtype.get_relevant_triggers(kind, schema):
                mro = (trigger, *trigger.get_ancestors(schema).objects(schema))
                base = mro[-1]
                tmap, all_typs = trigger_map.setdefault(base, (set(), set()))
                # N.B: If the *base type* of the DML appears, that
                # suffices, because it covers everything, and we don't
                # need to duplicate.  This is a specific interaction
                # with how dml.compile_trigger is implemented, where
                # processing the base type of a DML naturally covers
                # all subtypes, but processing a child does not cover
                # a grandchild.
                if (stype, stmt) not in tmap:
                    tmap.add((subtype, stmt))
                all_typs.add(subtype)

    # sort these by name just to avoid weird nondeterminism
    return tuple(
        compile_trigger(trigger, affected, all_typs, ctx=ctx)
        for trigger, (affected, all_typs)
        in sorted(trigger_map.items(), key=lambda t: t[0].get_name(schema))
    )


def compile_triggers(
    *,
    ctx: context.ContextLevel,
) -> tuple[tuple[irast.Trigger, ...], ...]:
    defining_trigger = (
        ctx.env.options.schema_object_context == s_triggers.Trigger)
    defining_trigger_on = None
    defining_trigger_kinds = None
    if (
        defining_trigger and
        isinstance(ctx.env.options, options.CompilerOptions)
    ):
        defining_trigger_on = ctx.env.options.trigger_type
        defining_trigger_kinds = ctx.env.options.trigger_kinds

    ir_triggers: list[tuple[irast.Trigger, ...]] = []
    start = 0
    all_trigger_causes: set[tuple[irast.TypeRef, qltypes.TriggerKind]] = set()
    while start < len(ctx.env.dml_stmts):
        end = len(ctx.env.dml_stmts)
        compiled_triggers = compile_triggers_phase(
            ctx.env.dml_stmts[start:],
            defining_trigger_on,
            defining_trigger_kinds,
            ctx=ctx
        )
        new_causes: set[tuple[irast.TypeRef, qltypes.TriggerKind]] = {
            (affected_type, kind)
            for compiled_trigger in compiled_triggers
            for affected_type in compiled_trigger.all_affected_types
            for kind in compiled_trigger.kinds
        }

        # Any given type is allowed allowed to have its triggers fire
        # in *one* phase of trigger execution, since the semantics get
        # a little unclear otherwise. We might relax this later.
        overlap = new_causes & all_trigger_causes
        if overlap:
            names: Collection[str] = sorted(
                f"{str(cause[0].name_hint)} after {cause[1].lower()}"
                for cause in overlap
            )
            raise errors.QueryError(
                f"trigger would need to be executed in multiple stages on "
                f"{', '.join(names)}"
            )
        all_trigger_causes |= new_causes
        ir_triggers.append(compiled_triggers)
        start = end

    return tuple(ir_triggers)
