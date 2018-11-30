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


class Object:
    pass


class Database(Object):
    pass


class Delta(Object):
    pass


class Constraint(Object):
    pass


class Function(Object):
    pass


class Operator(Object):
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
