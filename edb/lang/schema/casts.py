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


import functools
import typing

from edb import errors

from edb.lang.edgeql import ast as qlast

from . import abc as s_abc
from . import attributes
from . import delta as sd
from . import name as sn
from . import objects as so
from . import types as s_types
from . import utils


_NOT_REACHABLE = 10000000


def _is_reachable(schema, cast_kwargs, source: s_types.Type,
                  target: s_types.Type, distance: int) -> int:

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
        schema, source: s_types.Type, target: s_types.Type) -> int:
    dist = _is_reachable(schema, {'implicit': True}, source, target, 0)
    if dist == _NOT_REACHABLE:
        return -1
    else:
        return dist


def is_implicitly_castable(
        schema, source: s_types.Type, target: s_types.Type) -> bool:
    return get_implicit_cast_distance(schema, source, target) >= 0


@functools.lru_cache()
def find_common_castable_type(
        schema, source: s_types.Type,
        target: s_types.Type) -> typing.Optional[s_types.Type]:

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
        schema, source: s_types.Type, target: s_types.Type) -> bool:

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
        schema, module: str,
        from_type: s_types.Type, to_type: s_types.Type) -> sn.Name:
    return sn.Name(f'{module}::cast')


def get_cast_fullname(
        schema, module: str, from_type: s_types.Type,
        to_type: s_types.Type) -> sn.Name:
    quals = [from_type.get_name(schema), to_type.get_name(schema)]
    shortname = get_cast_shortname(schema, module, from_type, to_type)
    return sn.Name(
        module=shortname.module,
        name=sn.get_specialized_name(shortname, *quals))


class Cast(attributes.AttributeSubject, s_abc.Cast):

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


class CastCommandContext(sd.ObjectCommandContext,
                         attributes.AttributeSubjectCommandContext):
    pass


class CastCommand(sd.ObjectCommand,
                  schema_metaclass=Cast,
                  context_class=CastCommandContext):

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        if not context.stdmode and not context.testmode:
            raise errors.UnsupportedFeatureError(
                'user-defined casts are not supported',
                context=astnode.context
            )

        return super()._cmd_tree_from_ast(schema, astnode, context)

    @classmethod
    def _classname_from_ast(cls, schema, astnode: qlast.OperatorCommand,
                            context) -> sn.Name:
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

        return get_cast_fullname(schema, 'std', from_type, to_type)


class CreateCast(CastCommand, sd.CreateObject):
    astnode = qlast.CreateCast

    def _create_begin(self, schema, context):

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
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        modaliases = context.modaliases

        cmd.add(sd.AlterObjectProperty(
            property='from_type',
            new_value=utils.ast_to_typeref(
                astnode.from_type, modaliases=modaliases, schema=schema),
        ))

        cmd.add(sd.AlterObjectProperty(
            property='to_type',
            new_value=utils.ast_to_typeref(
                astnode.to_type, modaliases=modaliases, schema=schema),
        ))

        cmd.add(sd.AlterObjectProperty(
            property='allow_implicit',
            new_value=astnode.allow_implicit,
        ))

        cmd.add(sd.AlterObjectProperty(
            property='allow_assignment',
            new_value=astnode.allow_assignment,
        ))

        if astnode.code is not None:
            cmd.add(sd.AlterObjectProperty(
                property='language',
                new_value=astnode.code.language
            ))
            if astnode.code.from_function is not None:
                cmd.add(sd.AlterObjectProperty(
                    property='from_function',
                    new_value=astnode.code.from_function
                ))
            if astnode.code.code is not None:
                cmd.add(sd.AlterObjectProperty(
                    property='code',
                    new_value=astnode.code.code
                ))
            if astnode.code.from_expr is not None:
                cmd.add(sd.AlterObjectProperty(
                    property='from_expr',
                    new_value=astnode.code.from_expr
                ))
            if astnode.code.from_cast is not None:
                cmd.add(sd.AlterObjectProperty(
                    property='from_cast',
                    new_value=astnode.code.from_cast
                ))

        return cmd


class RenameCast(CastCommand, sd.RenameObject):
    pass


class AlterCast(CastCommand, sd.AlterObject):
    astnode = qlast.AlterCast


class DeleteCast(CastCommand, sd.DeleteObject):
    astnode = qlast.DropCast
