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


from __future__ import annotations

import typing


if typing.TYPE_CHECKING:
    from . import schema as s_schema

    ObjectContainer_T = typing.TypeVar(
        'ObjectContainer_T', bound='ObjectContainer'
    )


class Schema:
    pass


class Object:
    pass


class ObjectContainer:
    def _reduce_to_ref(
        self: ObjectContainer_T, schema: s_schema.Schema
    ) -> typing.Tuple[ObjectContainer_T, typing.Any]:
        raise NotImplementedError


class Database(Object):
    pass


class Migration(Object):
    pass


class Constraint(Object):
    pass


class Callable(Object):
    pass


class Function(Callable):
    pass


class Operator(Callable):
    pass


class Cast(Callable):
    pass


class Parameter(Object):
    pass


class Type(Object):
    pass


class ScalarType(Type):
    pass


class ObjectType(Type):
    pass


class Collection(Type):
    pass


class Tuple(Collection):
    pass


class Array(Collection):
    pass


class Pointer(Object):
    pass


class Property(Pointer):
    pass


class Link(Pointer):
    pass
