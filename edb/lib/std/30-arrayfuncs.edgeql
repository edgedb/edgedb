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


## Array functions


CREATE FUNCTION
std::array_agg(s: SET OF anytype) -> array<anytype>
{
    CREATE ANNOTATION std::description :=
        'Return the array made from all of the input set elements.';
    SET volatility := 'Immutable';
    SET initial_value := [];
    USING SQL FUNCTION 'array_agg';
};


CREATE FUNCTION
std::array_unpack(array: array<anytype>) -> SET OF anytype
{
    CREATE ANNOTATION std::description := 'Return array elements as a set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'unnest';
};


CREATE FUNCTION
std::array_get(
    array: array<anytype>,
    idx: std::int64,
    NAMED ONLY default: OPTIONAL anytype={}
) -> OPTIONAL anytype
{
    CREATE ANNOTATION std::description :=
        'Return the element of *array* at the specified *index*.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT COALESCE(
        "array"[
            edgedb._normalize_array_index(
                "idx"::int, array_upper("array", 1))
        ],
        "default"
    )
    $$;
};


CREATE FUNCTION
std::array_join(array: array<std::str>, delimiter: std::str) -> std::str
{
    CREATE ANNOTATION std::description := 'Render an array to a string.';
    SET volatility := 'Stable';
    USING SQL $$
    SELECT array_to_string("array", "delimiter");
    $$;
};


## Array operators


CREATE INFIX OPERATOR
std::`=` (l: array<anytype>, r: array<anytype>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR '=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL array<anytype>,
           r: OPTIONAL array<anytype>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
    SET recursive := true;
};


CREATE INFIX OPERATOR
std::`!=` (l: array<anytype>, r: array<anytype>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR '<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL array<anytype>,
            r: OPTIONAL array<anytype>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
    SET recursive := true;
};

CREATE INFIX OPERATOR
std::`>=` (l: array<anytype>, r: array<anytype>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR '>=';
};

CREATE INFIX OPERATOR
std::`>` (l: array<anytype>, r: array<anytype>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR '>';
};

CREATE INFIX OPERATOR
std::`<=` (l: array<anytype>, r: array<anytype>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR '<=';
};

CREATE INFIX OPERATOR
std::`<` (l: array<anytype>, r: array<anytype>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR '<';
};

# Concatenation
CREATE INFIX OPERATOR
std::`++` (l: array<anytype>, r: array<anytype>) -> array<anytype> {
    CREATE ANNOTATION std::identifier := 'concat';
    CREATE ANNOTATION std::description := 'Array concatenation.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR '||';
};

CREATE INFIX OPERATOR
std::`[]` (l: array<anytype>, r: std::int64) -> anytype {
    CREATE ANNOTATION std::identifier := 'index';
    CREATE ANNOTATION std::description := 'Array indexing.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};

CREATE INFIX OPERATOR
std::`[]` (l: array<anytype>, r: tuple<std::int64, std::int64>) -> array<anytype> {
    CREATE ANNOTATION std::identifier := 'slice';
    CREATE ANNOTATION std::description := 'Array slicing.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};
