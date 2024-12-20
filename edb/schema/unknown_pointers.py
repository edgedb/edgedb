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

"""Machinery for handling pointers with an unspecified kind.

Most of the DDL/delta machinery really requires that we know whether
we are operating on a link or a property, but our SDL syntax allows
omitting the specifier. Because the pointer might be computed, it's
not possible to resolve this ahead of time, so we build just enough
machinery for compiling unknown pointer operations to make
ddl.apply_sdl work.

"""

from __future__ import annotations


from edb.common import struct

from edb.edgeql import ast as qlast
from edb.edgeql import parser as qlparser

from . import delta as sd
from . import objects as so
from . import objtypes as s_objtypes
from . import properties as s_props
from . import pointers
from . import sources
from . import schema as s_schema


class UnknownPointerSourceContext(
    sources.SourceCommandContext[sources.Source_T]
):
    pass


class UnknownPointerCommand(
    pointers.PointerCommand[pointers.Pointer],
    context_class=pointers.PointerCommandContext,
    referrer_context_class=UnknownPointerSourceContext,
):
    _schema_metaclass = pointers.Pointer

    def _propagate_ref_creation(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        referrer: so.InheritingObject,
    ) -> None:
        pass


class CreateUnknownPointer(
    UnknownPointerCommand,
    pointers.CreatePointer[pointers.Pointer],
):
    astnode = qlast.CreateConcreteUnknownPointer
    referenced_astnode = qlast.CreateConcreteUnknownPointer

    # We stash the original AST node here, so we can reuse it in apply
    # after we've figured out the type.
    node = struct.Field(qlast.CreateConcreteUnknownPointer, default=None)

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        assert isinstance(astnode, qlast.CreateConcreteUnknownPointer)

        # We don't need any of the subcommands in order to figure out
        # the kind, and we avoid needing to get the contexts right if
        # we skip them.
        fakenode = astnode.replace(commands=[])
        cmd = super()._cmd_tree_from_ast(schema, fakenode, context)
        assert isinstance(cmd, CreateUnknownPointer)
        cmd._process_create_or_alter_ast(schema, fakenode, context)

        if context.modaliases:
            astnode = astnode.replace()
            qlparser.append_module_aliases(astnode, context.modaliases)

        cmd.node = astnode

        return cmd

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        # We don't know what the real type of this pointer is, so this
        # is a two step process:
        # 1. Apply it using purely generic Pointer code. This doesn't produce
        #    a fully legitimate result, but will resolve the target.
        # 2. Check whether the target is an object, and construct a new
        #    create AST node specialized to pointer or link. Then compile
        #    that to a delta tree and apply it.

        nschema = super().apply(schema, context)
        source = self.scls.get_source(nschema)
        target = self.scls.get_target(nschema)
        assert source and target

        astnode = self.node
        assert astnode
        astcls = (
            qlast.CreateConcreteLink
            # It's a link if the target is an object and so is the source.
            # If the source isn't, it's a link property, which will fail.
            if target.is_object_type()
            and isinstance(source, s_objtypes.ObjectType)
            else qlast.CreateConcreteProperty
        )
        astnode = astnode.replace(__class__=astcls)

        ncmd = sd.compile_ddl(schema, astnode, context=context)
        assert isinstance(ncmd, pointers.CreatePointer)

        rschema = ncmd.apply(schema, context)
        return rschema


class AlterUnknownPointer(
    UnknownPointerCommand,
    pointers.AlterPointer[pointers.Pointer],
):
    astnode = qlast.AlterConcreteUnknownPointer
    referenced_astnode = qlast.AlterConcreteUnknownPointer

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> pointers.AlterPointer[pointers.Pointer]:
        # For alters that get run as part of apply_sdl, the relevant
        # object should exist in the schema when _cmd_tree_from_ast is
        # called, so we can resolve whether it is a link or a property
        # right away and never need to return an AlterUnknownPointer
        # object.

        # We don't need any of the subcommands in order to figure out
        # the kind, and we avoid needing to get the contexts right if
        # we skip them.
        fakenode = astnode.replace(commands=[])
        cmd = super()._cmd_tree_from_ast(schema, fakenode, context)

        obj = cmd.get_object(schema, context)
        source = obj.get_source(schema)
        is_prop = (
            isinstance(obj, s_props.Property)
            or not isinstance(source, s_objtypes.ObjectType)
        )

        astcls = (
            qlast.AlterConcreteProperty
            if is_prop
            else qlast.AlterConcreteLink
        ) if isinstance(astnode, qlast.AlterObject) else (
            qlast.CreateConcreteProperty
            if is_prop
            else qlast.CreateConcreteLink
        )
        astnode = astnode.replace(__class__=astcls)
        qlparser.append_module_aliases(astnode, context.modaliases)
        res = sd.compile_ddl(schema, astnode, context=context)
        assert isinstance(res, pointers.AlterPointer)
        return res
