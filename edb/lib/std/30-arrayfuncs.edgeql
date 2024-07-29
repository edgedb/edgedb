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
    SET impl_is_strict := false;
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
std::array_fill(val: anytype, n: std::int64) -> array<anytype>
{
    CREATE ANNOTATION std::description :=
        'Return an array filled with the given value repeated \
         as many times as specified.';
    SET volatility := 'Immutable';
    # Postgres uses integer (int4) as the second argument. There is a maximum
    # array size, however. So when we get an `n` value greater than maximum
    # int4, we just truncate it to the maximum and let Postgres produce its
    # error.
    USING SQL $$
    SELECT array_fill(
        val,
        ARRAY[(CASE WHEN n > 2147483647 THEN 2147483647 ELSE n END)::int4]
    )
    $$;
};


CREATE FUNCTION
std::array_replace(
    array: array<anytype>,
    old: anytype,
    new: anytype
) -> array<anytype>
{
    CREATE ANNOTATION std::description :=
        'Replace each array element equal to the second argument \
         with the third argument.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'array_replace';
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
            edgedb_VER._normalize_array_index(
                "idx"::int, array_upper("array", 1))
        ],
        "default"
    )
    $$;
};


CREATE FUNCTION
std::array_set(
    array: array<anytype>,
    idx: std::int64,
    val: anytype
) -> array<anytype>
{
    CREATE ANNOTATION std::description :=
        'Set the element of *array* at the specified *index*.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT CASE
        WHEN cardinality("array") = 0 THEN
            edgedb.raise(
                "array",
                'invalid_parameter_value',
                msg => 'array index ' || idx::text || ' is out of bounds'
            )
        WHEN edgedb._normalize_array_index(
            "idx"::int, array_upper("array", 1)
        ) NOT BETWEEN 1 and array_upper("array", 1) THEN
            edgedb.raise(
                "array",
                'invalid_parameter_value',
                msg => 'array index ' || idx::text || ' is out of bounds'
            )
        WHEN edgedb._normalize_array_index(
            "idx"::int, array_upper("array", 1)
        ) = 1 THEN
            ARRAY[val] || "array"[2 :]
        WHEN edgedb._normalize_array_index(
            "idx"::int, array_upper("array", 1)
        ) = array_upper("array", 1) THEN
            "array"[: array_upper("array", 1) - 1] || ARRAY[val]
        ELSE
            "array"[
                : edgedb._normalize_array_index(
                    "idx"::int,
                    array_upper("array", 1)
                ) - 1
            ]
            || ARRAY[val]
            || "array"[
                edgedb._normalize_array_index(
                    "idx"::int,
                    array_upper("array", 1)
                ) + 1 :
            ]
    END
    $$;
};


CREATE FUNCTION
std::array_insert(
    array: array<anytype>,
    idx: std::int64,
    val: anytype
) -> array<anytype>
{
    CREATE ANNOTATION std::description :=
        'Insert *val* at the specified *index* of the *array*.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT CASE
        WHEN cardinality("array") = 0 AND "idx"::int != 0 THEN
            edgedb.raise(
                "array",
                'invalid_parameter_value',
                msg => 'array index ' || idx::text || ' is out of bounds'
            )
        WHEN cardinality("array") = 0 AND "idx"::int = 0 THEN
            ARRAY[val]

        WHEN edgedb._normalize_array_index(
            "idx"::int, array_upper("array", 1)
        ) NOT BETWEEN 1 and array_upper("array", 1) + 1 THEN
            edgedb.raise(
                "array",
                'invalid_parameter_value',
                msg => 'array index ' || idx::text || ' is out of bounds'
            )
        WHEN edgedb._normalize_array_index(
            "idx"::int, array_upper("array", 1)
        ) = 1 THEN
            ARRAY[val] || "array"
        WHEN edgedb._normalize_array_index(
            "idx"::int, array_upper("array", 1)
        ) = array_upper("array", 1) + 1 THEN
            "array" || ARRAY[val]
        ELSE
            "array"[
                : edgedb._normalize_array_index(
                    "idx"::int,
                    array_upper("array", 1)
                ) - 1
            ]
            || ARRAY[val]
            || "array"[
                edgedb._normalize_array_index(
                    "idx"::int,
                    array_upper("array", 1)
                ) :
            ]
    END
    $$;
};


CREATE FUNCTION
std::array_join(array: array<std::str>, delimiter: std::str) -> std::str
{
    CREATE ANNOTATION std::description := 'Render an array to a string.';
    # The Postgres function array_to_string works for any array type, but we
    # use it specifically for string arrays. For string arrays it should be
    # "immutable".
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT array_to_string("array", "delimiter");
    $$;
};


CREATE FUNCTION
std::array_join(array: array<std::bytes>, delimiter: std::bytes) -> std::bytes
{
    CREATE ANNOTATION std::description := 'Render an array to a byte-string.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT
        COALESCE (string_agg(el, "delimiter"), '\x')
    FROM
        (SELECT unnest("array") AS el) AS t
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
    SET impl_is_strict := false;
    USING SQL FUNCTION 'array_cat';
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
