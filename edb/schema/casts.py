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


from __future__ import annotations

import functools
from typing import *

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from . import abc as s_abc
from . import annos as s_anno
from . import delta as sd
from . import functions as s_func
from . import name as sn
from . import objects as so
from . import types as s_types
from . import utils

if TYPE_CHECKING:
    from edb.schema import schema as s_schema


_NOT_REACHABLE = 10000000


def _is_reachable(
    schema: s_schema.Schema,
    cast_kwargs: Mapping[str, bool],
    source: s_types.Type,
    target: s_types.Type,
    distance: int,
) -> int:

    if source == target:
        return distance

    casts = schema.get_casts_to_type(target, **cast_kwargs)
    if not casts:
        return _NOT_REACHABLE

    sources = {c.get_from_type(schema) for c in casts}

    distance += 1
    if source in sources:
        return distance
    else:
        return min(
            _is_reachable(schema, cast_kwargs, source, s, distance)
            for s in sources
        )


@functools.lru_cache()
def get_implicit_cast_distance(
    schema: s_schema.Schema,
    source: s_types.Type,
    target: s_types.Type,
) -> int:
    dist = _is_reachable(schema, {'implicit': True}, source, target, 0)
    if dist == _NOT_REACHABLE:
        return -1
    else:
        return dist


def is_implicitly_castable(
    schema: s_schema.Schema,
    source: s_types.Type,
    target: s_types.Type,
) -> bool:
    return get_implicit_cast_distance(schema, source, target) >= 0


@functools.lru_cache()
def find_common_castable_type(
    schema: s_schema.Schema,
    source: s_types.Type,
    target: s_types.Type,
) -> Optional[s_types.Type]:

    if get_implicit_cast_distance(schema, target, source) >= 0:
        return source
    if get_implicit_cast_distance(schema, source, target) >= 0:
        return target

    # Elevate target in the castability ladder, and check if
    # source is castable to it on each step.
    while True:
        casts = schema.get_casts_from_type(target, implicit=True)
        if not casts:
            return None

        targets = {c.get_to_type(schema) for c in casts}

        if len(targets) > 1:
            for t in targets:
                candidate = find_common_castable_type(schema, source, t)
                if candidate is not None:
                    return candidate
            else:
                return None
        else:
            target = next(iter(targets))
            if get_implicit_cast_distance(schema, source, target) >= 0:
                return target


@functools.lru_cache()
def is_assignment_castable(
    schema: s_schema.Schema,
    source: s_types.Type,
    target: s_types.Type,
) -> bool:

    # Implicitly castable implies assignment castable.
    if is_implicitly_castable(schema, source, target):
        return True

    # Assignment casts are valid only as one-hop casts.
    casts = schema.get_casts_to_type(target, assignment=True)
    if not casts:
        return False

    for c in casts:
        if c.get_from_type(schema) == source:
            return True
    return False


def get_cast_shortname(
    schema: s_schema.Schema,
    module: str,
    from_type: s_types.Type,
    to_type: s_types.Type,
) -> sn.Name:
    return sn.Name(f'{module}::cast')


def get_cast_fullname(
    schema: s_schema.Schema,
    module: str,
    from_type: s_types.Type,
    to_type: s_types.Type,
) -> sn.Name:
    quals = [from_type.get_name(schema), to_type.get_name(schema)]
    shortname = get_cast_shortname(schema, module, from_type, to_type)
    return sn.Name(
        module=shortname.module,
        name=sn.get_specialized_name(shortname, *quals))


class Cast(
    so.QualifiedObject,
    s_anno.AnnotationSubject,
    s_func.VolatilitySubject,
    s_abc.Cast,
    qlkind=qltypes.SchemaObjectClass.CAST,
):

    from_type = so.SchemaField(
        s_types.Type, compcoef=0.5)

    to_type = so.SchemaField(
        s_types.Type, compcoef=0.5)

    allow_implicit = so.SchemaField(
        bool, default=False, compcoef=0.4)

    allow_assignment = so.SchemaField(
        bool, default=False, compcoef=0.4)

    language = so.SchemaField(
        qlast.Language, default=None, compcoef=0.4, coerce=True)

    from_function = so.SchemaField(
        str, default=None, compcoef=0.4, introspectable=False)

    from_expr = so.SchemaField(
        bool, default=False, compcoef=0.4, introspectable=False)

    from_cast = so.SchemaField(
        bool, default=False, compcoef=0.4, introspectable=False)

    code = so.SchemaField(
        str, default=None, compcoef=0.4, introspectable=False)


class CastCommandContext(sd.ObjectCommandContext[Cast],
                         s_anno.AnnotationSubjectCommandContext):
    pass


class CastCommand(sd.QualifiedObjectCommand[Cast],
                  schema_metaclass=Cast,
                  context_class=CastCommandContext):

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        if not context.stdmode and not context.testmode:
            raise errors.UnsupportedFeatureError(
                'user-defined casts are not supported',
                context=astnode.context
            )

        return super()._cmd_tree_from_ast(schema, astnode, context)

    @classmethod
    def _classname_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        context: sd.CommandContext,
    ) -> sn.Name:
        assert isinstance(astnode, qlast.CastCommand)
        modaliases = context.modaliases

        from_type = utils.resolve_typeref(
            utils.ast_to_typeref(astnode.from_type, modaliases=modaliases,
                                 schema=schema),
            schema=schema
        )

        to_type = utils.resolve_typeref(
            utils.ast_to_typeref(astnode.to_type, modaliases=modaliases,
                                 schema=schema),
            schema=schema
        )
        assert isinstance(from_type, s_types.Type)
        assert isinstance(to_type, s_types.Type)
        return get_cast_fullname(schema, 'std', from_type, to_type)


class CreateCast(CastCommand, sd.CreateObject[Cast]):
    astnode = qlast.CreateCast

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        fullname = self.classname
        cast = schema.get(fullname, None)
        if cast:
            from_type = self.get_attribute_value('from_type')
            to_type = self.get_attribute_value('to_type')

            raise errors.DuplicateCastDefinitionError(
                f'a cast from {from_type.get_displayname(schema)!r}'
                f'to {to_type.get_displayname(schema)!r} is already defined',
                context=self.source_context)

        return super()._create_begin(schema, context)

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        assert isinstance(astnode, qlast.CreateCast)
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        modaliases = context.modaliases

        cmd.set_attribute_value(
            'from_type',
            utils.ast_to_typeref(
                astnode.from_type,
                modaliases=modaliases,
                schema=schema,
            ),
        )

        cmd.set_attribute_value(
            'to_type',
            utils.ast_to_typeref(
                astnode.to_type,
                modaliases=modaliases,
                schema=schema,
            ),
        )

        cmd.set_attribute_value(
            'allow_implicit',
            astnode.allow_implicit,
        )

        cmd.set_attribute_value(
            'allow_assignment',
            astnode.allow_assignment,
        )

        if astnode.code is not None:
            cmd.set_attribute_value(
                'language',
                astnode.code.language,
            )
            if astnode.code.from_function is not None:
                cmd.set_attribute_value(
                    'from_function',
                    astnode.code.from_function,
                )
            if astnode.code.code is not None:
                cmd.set_attribute_value(
                    'code',
                    astnode.code.code,
                )
            if astnode.code.from_expr is not None:
                cmd.set_attribute_value(
                    'from_expr',
                    astnode.code.from_expr,
                )
            if astnode.code.from_cast is not None:
                cmd.set_attribute_value(
                    'from_cast',
                    astnode.code.from_cast,
                )

        return cmd


class RenameCast(CastCommand, sd.RenameObject):
    pass


class AlterCast(CastCommand, sd.AlterObject[Cast]):
    astnode = qlast.AlterCast


class DeleteCast(CastCommand, sd.DeleteObject[Cast]):
    astnode = qlast.DropCast
