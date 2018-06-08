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


from . import base as s_types


_add_impl = s_types.BaseTypeMeta.add_implementation
_add_map = s_types.BaseTypeMeta.add_mapping


class Str(str):
    pass


_add_impl('std::str', Str)
_add_map(Str, 'std::str')
_add_map(str, 'std::str')


class StdStr(s_types.SchemaObject, name='std::str'):
    pass


class StrTypeInfo(s_types.TypeInfo, type=Str):
    def strop(self, other: str) -> StdStr:
        pass

    __add__ = strop
    __radd__ = strop
