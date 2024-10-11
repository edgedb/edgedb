#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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
from typing import Callable, Type, cast

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from . import delta as sd
from . import objects as so
from . import name as sn
from . import schema as s_schema


class FutureBehavior(
    so.Object,
    qlkind=qltypes.SchemaObjectClass.FUTURE,
    data_safe=False,
):
    name = so.SchemaField(
        sn.Name,
        inheritable=False,
        compcoef=0.0,  # can't rename
    )


class FutureBehaviorCommandContext(
    sd.ObjectCommandContext[FutureBehavior],
):
    pass


# Unlike extensions, futures are *explicitly* built into the
# language. Enabling or disabling a futures might require making
# other changes (recompiling functions that depend on it, for
# example), so each future is mapped to a handler function that can
# generate a command.
_FutureBehaviorHandler = Callable[
    ['FutureBehaviorCommand', s_schema.Schema, sd.CommandContext, bool],
    tuple[s_schema.Schema, sd.Command],
]

FUTURE_HANDLERS: dict[str, _FutureBehaviorHandler] = {}


def register_handler(
    name: str,
) -> Callable[[_FutureBehaviorHandler], _FutureBehaviorHandler]:
    def func(f: _FutureBehaviorHandler) -> _FutureBehaviorHandler:
        FUTURE_HANDLERS[name] = f
        return f

    return func


def future_enabled(schema: s_schema.Schema, feat: str) -> bool:
    return bool(schema.get_global(FutureBehavior, feat, default=None))


class FutureBehaviorCommand(
    sd.ObjectCommand[FutureBehavior],
    context_class=FutureBehaviorCommandContext,
):
    # A command that gets run after adjusting the future value.
    # It needs to run *after* the delete, for a 'drop future',
    # and so it can't use any of the existing varieties of subcommands.
    #
    # If anything else ends up needing to do this, we can add another
    # variety of subcommand.
    future_cmd: sd.Command | None = None

    def copy(self: FutureBehaviorCommand) -> FutureBehaviorCommand:
        result = super().copy()
        if self.future_cmd:
            result.future_cmd = self.future_cmd.copy()
        return result

    @classmethod
    def adapt(
        cls: Type[FutureBehaviorCommand], obj: sd.Command
    ) -> FutureBehaviorCommand:
        result = super(FutureBehaviorCommand, cls).adapt(obj)
        assert isinstance(obj, FutureBehaviorCommand)
        mcls = cast(sd.CommandMeta, type(cls))
        if obj.future_cmd:
            result.future_cmd = mcls.adapt(obj.future_cmd)
        return result

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        if not context.canonical and not isinstance(self, sd.AlterObject):
            key = str(self.classname)
            if key not in FUTURE_HANDLERS:
                raise errors.QueryError(
                    f"Unknown future '{str(key)}'"
                )
            schema, cmd = FUTURE_HANDLERS[key](
                self, schema, context, isinstance(self, sd.CreateObject))
            self.future_cmd = cmd

        if self.future_cmd:
            schema = self.future_cmd.apply(schema, context)

        return schema


class CreateFutureBehavior(
    FutureBehaviorCommand,
    sd.CreateObject[FutureBehavior],
):
    astnode = qlast.CreateFuture


class DeleteFutureBehavior(
    FutureBehaviorCommand,
    sd.DeleteObject[FutureBehavior],
):
    astnode = qlast.DropFuture


class AlterFutureBehavior(
    FutureBehaviorCommand,
    sd.AlterObject[FutureBehavior],
):
    pass


# These are registered here because they aren't directly related to
# any schema elements.
@register_handler('simple_scoping')
@register_handler('warn_old_scoping')
def toggle_scoping_future(
    cmd: FutureBehaviorCommand,
    schema: s_schema.Schema,
    context: sd.CommandContext,
    on: bool,
) -> tuple[s_schema.Schema, sd.Command]:
    from . import types as s_types
    from . import pointers as s_pointers
    from . import constraints as s_constraints
    from . import indexes as s_indexes

    # We need a subcommand to apply the _propagate_if_expr_refs on, so
    # make an alter.
    dummy_object = cmd.scls
    alter_cmd = dummy_object.init_delta_command(
        schema, cmdtype=sd.AlterObject)

    all_expr_fields = _get_all_expr_fields()

    # Indexes and constraints use simple expressions that cannot
    # depend on path factoring!
    del all_expr_fields[s_constraints.Constraint]
    del all_expr_fields[s_indexes.Index]

    types = tuple(all_expr_fields)

    all_expr_objects: list[so.Object] = list(schema.get_objects(
        exclude_stdlib=True,
        extra_filters=[lambda _, x: isinstance(x, types)],
    ))
    extra_refs = {
        obj: fields
        for obj in all_expr_objects
        if (fields := all_expr_fields[type(obj)])
        and any(obj.get_field_value(schema, name) for name in fields)
        and not (
            isinstance(obj, (s_types.Type, s_pointers.Pointer))
            and obj.get_from_alias(schema)
        )
    }

    schema = alter_cmd._propagate_if_expr_refs(
        schema,
        context,
        action=f'toggle value of scoping future behavior',
        extra_refs=extra_refs,
        metadata_only=False,
    )

    return schema, alter_cmd


def _get_all_expr_fields() -> dict[type[so.Object], list[str]]:
    r = {}
    from . import expr as s_expr

    for schemacls in so.ObjectMeta.get_schema_metaclasses():
        if schemacls.is_abstract():
            continue
        fields = [
            name
            for name, field in schemacls.get_schema_fields().items()
            if issubclass(field.type, s_expr.EXPRESSION_TYPES)
        ]
        if fields:
            r[schemacls] = fields

    return r
