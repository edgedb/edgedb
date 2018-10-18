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


from edb.lang.common import enum as s_enum


class ParameterKind(s_enum.StrEnum):
    VARIADIC = 'VARIADIC'
    NAMED_ONLY = 'NAMED ONLY'
    POSITIONAL = 'POSITIONAL'

    def to_edgeql(self):
        if self is ParameterKind.VARIADIC:
            return 'VARIADIC'
        elif self is ParameterKind.NAMED_ONLY:
            return 'NAMED ONLY'
        else:
            return ''


class TypeModifier(s_enum.StrEnum):
    SET_OF = 'SET OF'
    OPTIONAL = 'OPTIONAL'
    SINGLETON = 'SINGLETON'

    def to_edgeql(self):
        if self is TypeModifier.SET_OF:
            return 'SET OF'
        elif self is TypeModifier.OPTIONAL:
            return 'OPTIONAL'
        else:
            return ''
