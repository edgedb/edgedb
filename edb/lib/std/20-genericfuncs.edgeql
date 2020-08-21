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

## Fundamental polymorphic functions


# std::len
# --------

CREATE FUNCTION
std::len(str: std::str) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'A polymorphic function to calculate a "length" of its first argument.';
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT char_length("str")::bigint
    $$;
};


CREATE FUNCTION
std::len(bytes: std::bytes) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'A polymorphic function to calculate a "length" of its first argument.';
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT length("bytes")::bigint
    $$;
};


CREATE FUNCTION
std::len(array: array<anytype>) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'A polymorphic function to calculate a "length" of its first argument.';
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT cardinality("array")::bigint
    $$;
};


# std::sum
# --------

CREATE FUNCTION
std::sum(s: SET OF std::bigint) -> std::bigint
{
    CREATE ANNOTATION std::description :=
        'Return the sum of the set of numbers.';
    SET volatility := 'IMMUTABLE';
    SET initial_value := 0;
    SET force_return_cast := true;
    USING SQL FUNCTION 'sum';
};


CREATE FUNCTION
std::sum(s: SET OF std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the sum of the set of numbers.';
    SET volatility := 'IMMUTABLE';
    SET initial_value := 0;
    USING SQL FUNCTION 'sum';
};


CREATE FUNCTION
std::sum(s: SET OF std::int32) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'Return the sum of the set of numbers.';
    SET volatility := 'IMMUTABLE';
    SET initial_value := 0;
    SET force_return_cast := true;
    USING SQL FUNCTION 'sum';
};


CREATE FUNCTION
std::sum(s: SET OF std::int64) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'Return the sum of the set of numbers.';
    SET volatility := 'IMMUTABLE';
    SET initial_value := 0;
    SET force_return_cast := true;
    USING SQL FUNCTION 'sum';
};


CREATE FUNCTION
std::sum(s: SET OF std::float32) -> std::float32
{
    CREATE ANNOTATION std::description :=
        'Return the sum of the set of numbers.';
    SET volatility := 'IMMUTABLE';
    SET initial_value := 0;
    USING SQL FUNCTION 'sum';
};


CREATE FUNCTION
std::sum(s: SET OF std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the sum of the set of numbers.';
    SET volatility := 'IMMUTABLE';
    SET initial_value := 0;
    USING SQL FUNCTION 'sum';
};


# std::count
# ----------

CREATE FUNCTION
std::count(s: SET OF anytype) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'Return the number of elements in a set.';
    SET volatility := 'IMMUTABLE';
    SET initial_value := 0;
    USING SQL FUNCTION 'count';
};


# std::random
# -----------

CREATE FUNCTION
std::random() -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return a pseudo-random number in the range `0.0 <= x < 1.0`';
    SET volatility := 'VOLATILE';
    USING SQL FUNCTION 'random';
};


# std::min
# --------

CREATE FUNCTION
std::min(vals: SET OF anytype) -> OPTIONAL anytype
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'min';
};


# std::max
# --------

CREATE FUNCTION
std::max(vals: SET OF anytype) -> OPTIONAL anytype
{
    CREATE ANNOTATION std::description :=
        'Return the greatest value of the input set.';
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'max';
};


# std::all
# --------

CREATE FUNCTION
std::all(vals: SET OF std::bool) -> std::bool
{
    CREATE ANNOTATION std::description :=
        'Generalized boolean `AND` applied to the set of *values*.';
    SET volatility := 'IMMUTABLE';
    SET initial_value := True;
    USING SQL FUNCTION 'bool_and';
};


# std::any
# --------

CREATE FUNCTION
std::any(vals: SET OF std::bool) -> std::bool
{
    CREATE ANNOTATION std::description :=
        'Generalized boolean `OR` applied to the set of *values*.';
    SET volatility := 'IMMUTABLE';
    SET initial_value := False;
    USING SQL FUNCTION 'bool_or';
};


# std::enumerate
# --------------

CREATE FUNCTION
std::enumerate(
    vals: SET OF anytype
) -> SET OF tuple<std::int64, anytype>
{
    CREATE ANNOTATION std::description :=
        'Return a set of tuples of the form `(index, element)`.';
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


# std::round
# ----------

CREATE FUNCTION
std::round(val: std::int64) -> std::int64
{
    CREATE ANNOTATION std::description := 'Round to the nearest value.';
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT "val"
    $$;
};


CREATE FUNCTION
std::round(val: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description := 'Round to the nearest value.';
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT round("val")
    $$;
};


CREATE FUNCTION
std::round(val: std::bigint) -> std::bigint
{
    CREATE ANNOTATION std::description := 'Round to the nearest value.';
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT "val";
    $$;
};


CREATE FUNCTION
std::round(val: std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description := 'Round to the nearest value.';
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT round("val");
    $$;
};


CREATE FUNCTION
std::round(val: std::decimal, d: std::int64) -> std::decimal
{
    CREATE ANNOTATION std::description := 'Round to the nearest value.';
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT round("val", "d"::int4)
    $$;
};


# std::contains
# ---------

CREATE FUNCTION
std::contains(haystack: std::str, needle: std::str) -> std::bool
{
    CREATE ANNOTATION std::description :=
        'A polymorphic function to test if a sequence contains a certain element.';
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT (
        -- There was a regression in 12.0 (fixed in 12.1): strpos
        -- started to report 0 for empty search strings:
        -- https://postgr.es/m/CADT4RqAz7oN4vkPir86Kg1_mQBmBxCp-L_=9vRpgSNPJf0KRkw@mail.gmail.com
        --
        -- This CASE..WHEN fixes this edge case.
        CASE
            WHEN "needle" = '' THEN 1
            ELSE strpos("haystack", "needle")
        END
    ) != 0
    $$;
};


CREATE FUNCTION
std::contains(haystack: std::bytes, needle: std::bytes) -> std::bool
{
    CREATE ANNOTATION std::description :=
        'A polymorphic function to test if a sequence contains a certain element.';
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT position("needle" in "haystack") != 0
    $$;
};


CREATE FUNCTION
std::contains(haystack: array<anytype>, needle: anytype) -> std::bool
{
    CREATE ANNOTATION std::description :=
        'A polymorphic function to test if a sequence contains a certain element.';
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT
        CASE
            WHEN "needle" IS NULL THEN NULL
            ELSE array_position("haystack", "needle") IS NOT NULL
        END;
    $$;
};


# std::find
# ---------

CREATE FUNCTION
std::find(haystack: std::str, needle: std::str) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'A polymorphic function to find index of an element in a sequence.';
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT (
        -- There was a regression in 12.0 (fixed in 12.1): strpos
        -- started to report 0 for empty search strings:
        -- https://postgr.es/m/CADT4RqAz7oN4vkPir86Kg1_mQBmBxCp-L_=9vRpgSNPJf0KRkw@mail.gmail.com
        --
        -- This CASE..WHEN fixes this edge case.
        CASE
            WHEN "needle" = '' THEN 0
            ELSE strpos("haystack", "needle") - 1
        END
    )::int8
    $$;
};


CREATE FUNCTION
std::find(haystack: std::bytes, needle: std::bytes) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'A polymorphic function to find index of an element in a sequence.';
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT (position("needle" in "haystack") - 1)::int8
    $$;
};


CREATE FUNCTION
std::find(haystack: array<anytype>, needle: anytype,
          from_pos: std::int64=0) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'A polymorphic function to find index of an element in a sequence.';
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT COALESCE(
        array_position("haystack", "needle", ("from_pos"::int4 + 1)::int4) - 1,
        -1)::int8
    $$;
};


# Generic comparison operators
# ----------------------------

CREATE INFIX OPERATOR
std::`=` (l: anytuple, r: anytuple) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET recursive := true;
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR '=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL anytuple, r: OPTIONAL anytuple) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
    SET recursive := true;
};


CREATE INFIX OPERATOR
std::`!=` (l: anytuple, r: anytuple) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET recursive := true;
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR '<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL anytuple, r: OPTIONAL anytuple) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
    SET recursive := true;
};


CREATE INFIX OPERATOR
std::`>=` (l: anytuple, r: anytuple) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET recursive := true;
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR '>=';
};


CREATE INFIX OPERATOR
std::`>` (l: anytuple, r: anytuple) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET recursive := true;
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR '>';
};


CREATE INFIX OPERATOR
std::`<=` (l: anytuple, r: anytuple) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET recursive := true;
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR '<=';
};


CREATE INFIX OPERATOR
std::`<` (l: anytuple, r: anytuple) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET recursive := true;
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR '<';
};
