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

CREATE ABSTRACT TYPE std::Object {
    CREATE REQUIRED PROPERTY id EXTENDING std::id -> std::uuid {
        SET default := (SELECT std::uuid_generate_v1mc());
        SET readonly := True;
        CREATE CONSTRAINT std::exclusive;
    };
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
std::`=` (l: std::Object, r: std::Object) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::Object, r: OPTIONAL std::Object) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::Object, r: std::Object) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::Object, r: OPTIONAL std::Object) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>=` (l: std::Object, r: std::Object) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: std::Object, r: std::Object) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`<=` (l: std::Object, r: std::Object) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`<` (l: std::Object, r: std::Object) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


# The only possible Object cast is into json.
CREATE CAST FROM std::Object TO std::json {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};
