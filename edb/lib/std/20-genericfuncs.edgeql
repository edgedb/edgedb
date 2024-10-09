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


# std::assert_single -- runtime cardinality assertion (upper bound)
# -----------------------------------------------------------------

CREATE FUNCTION
std::assert_single(
    input: SET OF anytype,
    NAMED ONLY message: OPTIONAL str = <str>{},
) -> OPTIONAL anytype
{
    CREATE ANNOTATION std::description :=
        "Check that the input set contains at most one element, raise
         CardinalityViolationError otherwise.";
    SET volatility := 'Stable';
    SET preserves_optionality := true;
    USING SQL EXPRESSION;
};


# std::assert_exists -- runtime cardinality assertion (lower bound)
# -----------------------------------------------------------------

CREATE FUNCTION
std::assert_exists(
    input: SET OF anytype,
    NAMED ONLY message: OPTIONAL str = <str>{},
) -> SET OF anytype
{
    CREATE ANNOTATION std::description :=
        "Check that the input set contains at least one element, raise
         CardinalityViolationError otherwise.";
    SET volatility := 'Stable';
    SET preserves_upper_cardinality := true;
    USING SQL EXPRESSION;
};


# std::assert_distinct -- runtime multiplicity assertion
# ------------------------------------------------------

CREATE FUNCTION
std::assert_distinct(
    input: SET OF anytype,
    NAMED ONLY message: OPTIONAL str = <str>{},
) -> SET OF anytype
{
    CREATE ANNOTATION std::description :=
        "Check that the input set is a proper set, i.e. all elements
         are unique";
    SET volatility := 'Stable';
    SET preserves_optionality := true;
    SET preserves_upper_cardinality := true;
    USING SQL EXPRESSION;
};

# std::assert -- boolean assertion
# --------------------------------
CREATE FUNCTION
std::assert(
    input: bool,
    NAMED ONLY message: OPTIONAL str = <str>{},
) -> bool
{
    CREATE ANNOTATION std::description :=
        "Assert that a boolean value is true.";
    SET volatility := 'Stable';
    USING SQL $$
    SELECT (
        edgedb_VER.raise_on_null(
            nullif("input", false),
            'cardinality_violation',
            "constraint" => 'std::assert',
            msg => coalesce("message", 'assertion failed')
        )
    )
    $$;
};

# std::materialized_exists -- force materialization of a set
# ----------------------------------------------------------

CREATE FUNCTION
std::materialized(
    input: anytype,
) -> anytype
{
    CREATE ANNOTATION std::description :=
        "Force materialization of a set.";
    SET volatility := 'Volatile';
    USING SQL EXPRESSION;
};


# std::len
# --------

CREATE FUNCTION
std::len(str: std::str) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'A polymorphic function to calculate a "length" of its first argument.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT char_length("str")::bigint
    $$;
};


CREATE FUNCTION
std::len(bytes: std::bytes) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'A polymorphic function to calculate a "length" of its first argument.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT length("bytes")::bigint
    $$;
};


CREATE FUNCTION
std::len(array: array<anytype>) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'A polymorphic function to calculate a "length" of its first argument.';
    SET volatility := 'Immutable';
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
    SET volatility := 'Immutable';
    SET initial_value := 0;
    SET force_return_cast := true;
    USING SQL FUNCTION 'sum';
};


CREATE FUNCTION
std::sum(s: SET OF std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return the sum of the set of numbers.';
    SET volatility := 'Immutable';
    SET initial_value := 0;
    USING SQL FUNCTION 'sum';
};


CREATE FUNCTION
std::sum(s: SET OF std::int32) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'Return the sum of the set of numbers.';
    SET volatility := 'Immutable';
    SET initial_value := 0;
    SET force_return_cast := true;
    USING SQL FUNCTION 'sum';
};


CREATE FUNCTION
std::sum(s: SET OF std::int64) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'Return the sum of the set of numbers.';
    SET volatility := 'Immutable';
    SET initial_value := 0;
    SET force_return_cast := true;
    USING SQL FUNCTION 'sum';
};


CREATE FUNCTION
std::sum(s: SET OF std::float32) -> std::float32
{
    CREATE ANNOTATION std::description :=
        'Return the sum of the set of numbers.';
    SET volatility := 'Immutable';
    SET initial_value := 0;
    USING SQL FUNCTION 'sum';
};


CREATE FUNCTION
std::sum(s: SET OF std::float64) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return the sum of the set of numbers.';
    SET volatility := 'Immutable';
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
    SET volatility := 'Immutable';
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
    SET volatility := 'Volatile';
    USING SQL FUNCTION 'random';
};


# std::min
# --------

CREATE FUNCTION
std::min(vals: SET OF anytype) -> OPTIONAL anytype
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET fallback := true;
    SET preserves_optionality := true;
    USING SQL EXPRESSION;
};


# Postgres only implements min and max for specific scalars and their
# respective arrays, but in EdgeDB every type is orderable and so
# minimum and maximum value can be determined for all types. The
# general catch-all using `anytype` above is valid for all types, but
# it is somewhat slower than the specialized natively implemented min
# and max aggregates. So for the types that Postgres supports, we want
# to use the more specialized implementation.
#
# Turns out that the min/max implementation for arrays is not
# noticeably faster than the fallback we use, so there's no
# specialized version of it in the polymorphic implementations.
CREATE FUNCTION
std::min(vals: SET OF anyreal) -> OPTIONAL anyreal
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF anyenum) -> OPTIONAL anyenum
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF str) -> OPTIONAL str
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF datetime) -> OPTIONAL datetime
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF duration) -> OPTIONAL duration
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


# std::max
# --------

CREATE FUNCTION
std::max(vals: SET OF anytype) -> OPTIONAL anytype
{
    CREATE ANNOTATION std::description :=
        'Return the greatest value of the input set.';
    SET volatility := 'Immutable';
    SET fallback := true;
    SET preserves_optionality := true;
    USING SQL EXPRESSION;
};


# Postgres only implements min and max for specific scalars and their
# respective arrays, but in EdgeDB every type is orderable and so
# minimum and maximum value can be determined for all types. The
# general catch-all using `anytype` above is valid for all types, but
# it is somewhat slower than the specialized natively implemented min
# and max aggregates. So for the types that Postgres supports, we want
# to use the more specialized implementation.
#
# Turns out that the min/max implementation for arrays is not
# noticeably faster than the fallback we use, so there's no
# specialized version of it in the polymorphic implementations.
CREATE FUNCTION
std::max(vals: SET OF anyreal) -> OPTIONAL anyreal
{
    CREATE ANNOTATION std::description :=
        'Return the greatest value of the input set.';
    SET volatility := 'Immutable';
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF anyenum) -> OPTIONAL anyenum
{
    CREATE ANNOTATION std::description :=
        'Return the greatest value of the input set.';
    SET volatility := 'Immutable';
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF str) -> OPTIONAL str
{
    CREATE ANNOTATION std::description :=
        'Return the greatest value of the input set.';
    SET volatility := 'Immutable';
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF datetime) -> OPTIONAL datetime
{
    CREATE ANNOTATION std::description :=
        'Return the greatest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF duration) -> OPTIONAL duration
{
    CREATE ANNOTATION std::description :=
        'Return the greatest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


# std::all
# --------

CREATE FUNCTION
std::all(vals: SET OF std::bool) -> std::bool
{
    CREATE ANNOTATION std::description :=
        'Generalized boolean `AND` applied to the set of *values*.';
    SET volatility := 'Immutable';
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
    SET volatility := 'Immutable';
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
    SET volatility := 'Immutable';
    SET preserves_optionality := true;
    SET preserves_upper_cardinality := true;
    USING SQL EXPRESSION;
};


# std::round
# ----------

CREATE FUNCTION
std::round(val: std::int64) -> std::int64
{
    CREATE ANNOTATION std::description := 'Round to the nearest value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT "val"
    $$;
};


CREATE FUNCTION
std::round(val: std::float64) -> std::float64
{
    CREATE ANNOTATION std::description := 'Round to the nearest value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT round("val")
    $$;
};


CREATE FUNCTION
std::round(val: std::bigint) -> std::bigint
{
    CREATE ANNOTATION std::description := 'Round to the nearest value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT "val";
    $$;
};


CREATE FUNCTION
std::round(val: std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::description := 'Round to the nearest value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT round("val");
    $$;
};


CREATE FUNCTION
std::round(val: std::decimal, d: std::int64) -> std::decimal
{
    CREATE ANNOTATION std::description := 'Round to the nearest value.';
    SET volatility := 'Immutable';
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
    SET volatility := 'Immutable';
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
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT position("needle" in "haystack") != 0
    $$;
};


CREATE FUNCTION
std::contains(haystack: array<anytype>, needle: anytype) -> std::bool
{
    CREATE ANNOTATION std::description :=
        'A polymorphic function to test if a sequence contains a certain element.';
    SET volatility := 'Immutable';
    # Postgres only manages to inline this function if it isn't marked strict,
    # and we want it to be inlined so that std::pg::gin indexes work with it.
    SET impl_is_strict := false;
    USING SQL $$
    SELECT "haystack" @> ARRAY["needle"]
    $$;
};


CREATE FUNCTION
std::contains(haystack: json, needle: json) -> std::bool
{
    CREATE ANNOTATION std::description :=
        'A polymorphic function to test if one JSON value contains another JSON value.';
    SET volatility := 'Immutable';
    # Postgres only manages to inline this function if it isn't marked strict,
    # and we want it to be inlined so that std::pg::gin indexes work with it.
    SET impl_is_strict := false;
    USING SQL $$
    SELECT "haystack" @> "needle"
    $$;
};


# std::find
# ---------

CREATE FUNCTION
std::find(haystack: std::str, needle: std::str) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'A polymorphic function to find index of an element in a sequence.';
    SET volatility := 'Immutable';
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
    SET volatility := 'Immutable';
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
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT COALESCE(
        array_position("haystack", "needle", ("from_pos"::int4 + 1)::int4) - 1,
        -1)::int8
    $$;
};


# Generic comparison operators
# ----------------------------

CREATE INFIX OPERATOR
std::`=` (l: anyscalar, r: anyscalar) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL anyscalar, r: OPTIONAL anyscalar) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: anyscalar, r: anyscalar) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL anyscalar, r: OPTIONAL anyscalar) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>=` (l: anyscalar, r: anyscalar) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR '>=';
};


CREATE INFIX OPERATOR
std::`>` (l: anyscalar, r: anyscalar) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR '>';
};


CREATE INFIX OPERATOR
std::`<=` (l: anyscalar, r: anyscalar) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR '<=';
};


CREATE INFIX OPERATOR
std::`<` (l: anyscalar, r: anyscalar) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR '<';
};


CREATE INFIX OPERATOR
std::`=` (l: anytuple, r: anytuple) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR '=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL anytuple, r: OPTIONAL anytuple) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
    SET recursive := true;
};


CREATE INFIX OPERATOR
std::`!=` (l: anytuple, r: anytuple) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR '<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL anytuple, r: OPTIONAL anytuple) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
    SET recursive := true;
};


CREATE INFIX OPERATOR
std::`>=` (l: anytuple, r: anytuple) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR '>=';
};


CREATE INFIX OPERATOR
std::`>` (l: anytuple, r: anytuple) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR '>';
};


CREATE INFIX OPERATOR
std::`<=` (l: anytuple, r: anytuple) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR '<=';
};


CREATE INFIX OPERATOR
std::`<` (l: anytuple, r: anytuple) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR '<';
};
