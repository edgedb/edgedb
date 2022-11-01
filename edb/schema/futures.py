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
from typing import *

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


def register_handler(name: str) -> Callable[
    [_FutureBehaviorHandler], _FutureBehaviorHandler
]:
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
        if not context.canonical:
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
