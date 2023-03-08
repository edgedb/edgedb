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

from typing import *

from edb.ir import ast as irast

from edb.schema import name as sn
from edb.schema import objtypes as s_objtypes
from edb.schema import triggers as s_triggers

from edb.edgeql import qltypes

from . import context
from . import dispatch
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
    *,
    ctx: context.ContextLevel,
) -> irast.Trigger:
    schema = ctx.env.schema

    scope = trigger.get_scope(schema)
    kinds = set(trigger.get_kinds(schema))
    source = trigger.get_subject(schema)

    with ctx.detached() as _, _.newscope(fenced=True) as sctx:
        sctx.anchors = sctx.anchors.copy()

        anchors = {}
        new_path = irast.PathId.from_type(
            schema, source, typename=sn.QualName(
                module='__derived__', name='__new__')
        )
        new_set = setgen.class_set(
            source, path_id=new_path, ignore_rewrites=True, ctx=sctx)
        new_set.expr = irast.TriggerAnchor(typeref=new_set.typeref)

        old_set = None
        if qltypes.TriggerKind.Insert not in kinds:
            old_path = irast.PathId.from_type(
                schema, source, typename=sn.QualName(
                    module='__derived__', name='__old__')
            )
            old_set = setgen.class_set(
                source, path_id=old_path, ignore_rewrites=True, ctx=sctx)
            old_set.expr = irast.TriggerAnchor(typeref=old_set.typeref)
            anchors['__old__'] = old_set
        if qltypes.TriggerKind.Delete not in kinds:
            anchors['__new__'] = new_set

        for name, ir in anchors.items():
            if scope == qltypes.TriggerScope.Each:
                sctx.path_scope.attach_path(ir.path_id, context=None)
                sctx.iterator_path_ids |= {ir.path_id}
            sctx.anchors[name] = ir

        trigger_set = dispatch.compile(
            trigger.get_expr(schema).qlast, ctx=sctx)

    typeref = typegen.type_to_typeref(source, env=ctx.env)
    taffected = {
        (typegen.type_to_typeref(t, env=ctx.env), ir) for t, ir in affected
    }

    return irast.Trigger(
        expr=trigger_set,
        kinds=kinds,
        scope=scope,
        source_type=typeref,
        affected=taffected,
        new_set=new_set,
        old_set=old_set,
    )


def compile_triggers(
    dml_stmts: Collection[irast.MutatingStmt],
    *,
    ctx: context.ContextLevel,
) -> tuple[irast.Trigger, ...]:
    schema = ctx.env.schema

    trigger_map: dict[
        s_triggers.Trigger,
        set[tuple[s_objtypes.ObjectType, irast.MutatingStmt]],
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
            for trigger in subtype.get_relevant_triggers(kind, schema):
                mro = (trigger, *trigger.get_ancestors(schema).objects(schema))
                base = mro[-1]
                tmap = trigger_map.setdefault(base, set())
                # N.B: If the *base type* of the DML appears, that
                # suffices, because it covers everything, and we don't
                # need to duplicate.  This is a specific interaction
                # with how dml.compile_trigger is implemented, where
                # processing the base type of a DML naturally covers
                # all subtypes, but processing a child does not cover
                # a grandchild.
                if (stype, stmt) not in tmap:
                    tmap.add((subtype, stmt))

    # sort these by name just to avoid weird nondeterminism
    return tuple(
        compile_trigger(trigger, affected, ctx=ctx)
        for trigger, affected
        in sorted(trigger_map.items(), key=lambda t: t[0].get_name(schema))
    )
