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


## Base object type, link and property definitions.


CREATE ABSTRACT PROPERTY std::property;

CREATE ABSTRACT PROPERTY std::id;

CREATE ABSTRACT PROPERTY std::source;

CREATE ABSTRACT PROPERTY std::target;

CREATE ABSTRACT LINK std::link;

CREATE ABSTRACT TYPE std::BaseObject {
    CREATE REQUIRED PROPERTY id EXTENDING std::id -> std::uuid {
        SET default := std::uuid_generate_v1mc();
        SET readonly := True;
        CREATE CONSTRAINT std::exclusive;
    };
    CREATE ANNOTATION std::description := 'Root object type.'
};

CREATE ABSTRACT TYPE std::Object EXTENDING std::BaseObject {
    CREATE ANNOTATION std::description :=
        'Root object type for user-defined types';
};

# N.B: This does *not* derive from std::BaseObject!
CREATE TYPE std::FreeObject {
    CREATE ANNOTATION std::description :=
        'Object type for free shapes';
};

# 'USING SQL EXPRESSION' creates an EdgeDB Operator for purposes of
# introspection and validation by the EdgeQL compiler. However, no
# object is created in Postgres and the EdgeQL->SQL compiler is expected
# to produce some expression that will be valid.
#
# 'USING SQL OPERATOR' does all of the above and it also creates an
# actual Postgres operator. It is expected that the EdgeQL->SQL compiler
# will specifically use that operator.

# HACK: We use 'USING SQL EXPRESSION' instead of 'USING SQL OPERATOR'
# here because in actuality Objects will be resolved as their uuids
# and in the end it's the uuid operators that will be called in SQL.
# On the other hand, if we use "USING SQL OPERATOR", we will end up
# clashing with the operators for uuid in Postgres.
CREATE INFIX OPERATOR
std::`=` (l: std::BaseObject, r: std::BaseObject) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`?=` (
    l: OPTIONAL std::BaseObject,
    r: OPTIONAL std::BaseObject
) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::BaseObject, r: std::BaseObject) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`?!=` (
    l: OPTIONAL std::BaseObject,
    r: OPTIONAL std::BaseObject
) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>=` (l: std::BaseObject, r: std::BaseObject) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: std::BaseObject, r: std::BaseObject) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`<=` (l: std::BaseObject, r: std::BaseObject) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`<` (l: std::BaseObject, r: std::BaseObject) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


# The only possible Object cast is into json.
CREATE CAST FROM std::BaseObject TO std::json {
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};
CREATE CAST FROM std::FreeObject TO std::json {
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};
