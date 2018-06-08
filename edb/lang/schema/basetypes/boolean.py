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


from edb.lang.common import ast

from . import base as s_types


class Bool(int):
    def __new__(cls, value=0):
        if value == 'False':
            value = 0
        elif value == 'True':
            value = 1
        elif value is None:
            value = 0

        return super().__new__(cls, value)

    def __repr__(self):
        return 'True' if self else 'False'

    __str__ = __repr__

    def __mm_serialize__(self):
        return bool(self)


s_types.BaseTypeMeta.add_implementation(
    'std::bool', Bool)
s_types.BaseTypeMeta.add_mapping(
    Bool, 'std::bool')
s_types.BaseTypeMeta.add_mapping(
    bool, 'std::bool')

s_types.TypeRules.add_rule(
    ast.ops.OR, (Bool, Bool), 'std::bool')
s_types.TypeRules.add_rule(
    ast.ops.AND, (Bool, Bool), 'std::bool')
s_types.TypeRules.add_rule(
    ast.ops.NOT, (Bool,), 'std::bool')
