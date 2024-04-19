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


## Byte string functions
## ---------------------

CREATE FUNCTION
std::bytes_get_bit(bytes: std::bytes, num: int64) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'Get the *nth* bit of the *bytes* value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT get_bit("bytes", "num"::int)::bigint
    $$;
};

CREATE FUNCTION
std::bit_count(bytes: std::bytes) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'Count the number of set bits the bytes value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT bit_count(bytes)
    $$;
};



## Byte string operators
## ---------------------

CREATE INFIX OPERATOR
std::`=` (l: std::bytes, r: std::bytes) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::bytes, r: OPTIONAL std::bytes) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::bytes, r: std::bytes) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::bytes, r: OPTIONAL std::bytes) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`++` (l: std::bytes, r: std::bytes) -> std::bytes {
    CREATE ANNOTATION std::identifier := 'concat';
    CREATE ANNOTATION std::description := 'Bytes concatenation.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'||';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::bytes, r: std::bytes) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR '>=';
};


CREATE INFIX OPERATOR
std::`>` (l: std::bytes, r: std::bytes) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR '>';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::bytes, r: std::bytes) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR '<=';
};


CREATE INFIX OPERATOR
std::`<` (l: std::bytes, r: std::bytes) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR '<';
};

CREATE INFIX OPERATOR
std::`[]` (l: std::bytes, r: std::int64) -> std::bytes {
    CREATE ANNOTATION std::identifier := 'index';
    CREATE ANNOTATION std::description := 'Bytes indexing.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};

CREATE INFIX OPERATOR
std::`[]` (l: std::bytes, r: tuple<std::int64, std::int64>) -> std::bytes {
    CREATE ANNOTATION std::identifier := 'slice';
    CREATE ANNOTATION std::description := 'Bytes slicing.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};
