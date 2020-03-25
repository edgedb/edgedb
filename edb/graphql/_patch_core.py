#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


def patch_graphql_core():
    import graphql
    import graphql.utilities.type_comparators as type_comparators

    old_is_type_sub_type_of = type_comparators.is_type_sub_type_of

    def is_type_sub_type_of(schema, maybe_subtype, super_type):
        # allow coercing ints to floats
        if super_type is graphql.GraphQLFloat:
            if maybe_subtype is graphql.GraphQLInt:
                return True
        return old_is_type_sub_type_of(schema, maybe_subtype, super_type)

    type_comparators.is_type_sub_type_of = is_type_sub_type_of
