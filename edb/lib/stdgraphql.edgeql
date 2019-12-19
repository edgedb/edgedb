#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
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


CREATE MODULE stdgraphql {
    SET builtin := true;
};


# these are just some placeholders for packaging GraphQL queries
CREATE TYPE stdgraphql::Query;
ALTER TYPE stdgraphql::Query {
    CREATE PROPERTY __typename := 'Query';
};


CREATE TYPE stdgraphql::Mutation;
ALTER TYPE stdgraphql::Mutation {
    CREATE PROPERTY __typename := 'Mutation';
};


CREATE FUNCTION stdgraphql::short_name(name: str) -> str {
    SET volatility := 'IMMUTABLE';
    USING (
        SELECT (
            name[5:] IF name LIKE 'std::%' ELSE
            name[9:] IF name LIKE 'default::%' ELSE
            re_replace(r'(.+?)::(.+$)', r'\1__\2', name)
        ) ++ 'Type'
    );
};
