#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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


## Range/multirange functions


CREATE FUNCTION
std::range(
    lower: optional std::anypoint = {},
    upper: optional std::anypoint = {},
    named only inc_lower: bool = true,
    named only inc_upper: bool = false,
    named only empty: bool = false,
) -> range<std::anypoint>
{
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


# TODO: maybe also add a constructor taking a set?
CREATE FUNCTION
std::multirange(
    ranges: array<range<std::anypoint>>,
) -> multirange<std::anypoint>
{
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE FUNCTION
std::range_is_empty(
    val: range<anypoint>
) -> bool
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'isempty';
};


CREATE FUNCTION
std::range_is_empty(
    val: multirange<anypoint>
) -> bool
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'isempty';
};


CREATE FUNCTION
std::range_unpack(
    val: range<int32>
) -> set of int32
{
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT
            generate_series(
                (
                    edgedb_VER.range_lower_validate(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE 1 END)
                )::int8,
                (
                    edgedb_VER.range_upper_validate(val) -
                    (CASE WHEN upper_inc(val) THEN 0 ELSE 1 END)
                )::int8
            )::int4
    $$;
};


CREATE FUNCTION
std::range_unpack(
    val: range<int32>,
    step: int32
) -> set of int32
{
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT
            generate_series(
                (
                    edgedb_VER.range_lower_validate(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE 1 END)
                )::int8,
                (
                    edgedb_VER.range_upper_validate(val) -
                    (CASE WHEN upper_inc(val) THEN 0 ELSE 1 END)
                )::int8,
                step::int8
            )::int4
    $$;
};


CREATE FUNCTION
std::range_unpack(
    val: range<int64>
) -> set of int64
{
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT
            generate_series(
                (
                    edgedb_VER.range_lower_validate(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE 1 END)
                )::int8,
                (
                    edgedb_VER.range_upper_validate(val) -
                    (CASE WHEN upper_inc(val) THEN 0 ELSE 1 END)
                )::int8
            )
    $$;
};


CREATE FUNCTION
std::range_unpack(
    val: range<int64>,
    step: int64
) -> set of int64
{
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT
            generate_series(
                (
                    edgedb_VER.range_lower_validate(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE 1 END)
                )::int8,
                (
                    edgedb_VER.range_upper_validate(val) -
                    (CASE WHEN upper_inc(val) THEN 0 ELSE 1 END)
                )::int8,
                step
            )
    $$;
};


CREATE FUNCTION
std::range_unpack(
    val: range<float32>,
    step: float32
) -> set of float32
{
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT num::float4
        FROM
            generate_series(
                (
                    edgedb_VER.range_lower_validate(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE step END)
                )::numeric,
                (
                    edgedb_VER.range_upper_validate(val)
                )::numeric,
                step::numeric
            ) AS num
        WHERE
            upper_inc(val) OR num::float4 < upper(val)
    $$;
};


CREATE FUNCTION
std::range_unpack(
    val: range<float64>,
    step: float64
) -> set of float64
{
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT num::float8
        FROM
            generate_series(
                (
                    edgedb_VER.range_lower_validate(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE step END)
                )::numeric,
                (
                    edgedb_VER.range_upper_validate(val)
                )::numeric,
                step::numeric
            ) AS num
        WHERE
            upper_inc(val) OR num::float8 < upper(val)
    $$;
};


CREATE FUNCTION
std::range_unpack(
    val: range<decimal>,
    step: decimal
) -> set of decimal
{
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT num
        FROM
            generate_series(
                edgedb_VER.range_lower_validate(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE step END),
                edgedb_VER.range_upper_validate(val),
                step
            ) AS num
        WHERE
            upper_inc(val) OR num < upper(val)
    $$;
};


CREATE FUNCTION
std::range_unpack(
    val: range<datetime>,
    step: duration
) -> set of datetime
{
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT d::edgedbt.timestamptz_t
        FROM
            generate_series(
                (
                    edgedb_VER.range_lower_validate(val) + (
                        CASE WHEN lower_inc(val)
                            THEN '0'::interval
                            ELSE step
                        END
                    )
                )::timestamptz,
                (
                    edgedb_VER.range_upper_validate(val)
                )::timestamptz,
                step::interval
            ) AS d
        WHERE
            upper_inc(val) OR d::edgedbt.timestamptz_t < upper(val)
    $$;
};


CREATE FUNCTION std::range_get_upper(r: range<anypoint>) -> optional anypoint
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'upper';
    SET force_return_cast := true;
};


CREATE FUNCTION std::range_get_lower(r: range<anypoint>) -> optional anypoint
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'lower';
    SET force_return_cast := true;
};


CREATE FUNCTION std::range_is_inclusive_upper(r: range<anypoint>) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'upper_inc';
};


CREATE FUNCTION std::range_is_inclusive_lower(r: range<anypoint>) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'lower_inc';
};


CREATE FUNCTION std::range_get_upper(
    r: multirange<anypoint>
) -> optional anypoint
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'upper';
    SET force_return_cast := true;
};


CREATE FUNCTION std::range_get_lower(
    r: multirange<anypoint>
) -> optional anypoint
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'lower';
    SET force_return_cast := true;
};


CREATE FUNCTION std::range_is_inclusive_upper(
    r: multirange<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'upper_inc';
};


CREATE FUNCTION std::range_is_inclusive_lower(
    r: multirange<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'lower_inc';
};


CREATE FUNCTION std::contains(
    haystack: range<anypoint>,
    needle: range<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "haystack" @> "needle"
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    # Postgres only manages to inline this function if it isn't marked strict,
    # and we want it to be inlined so that std::pg::gin indexes work with it.
    set impl_is_strict := false;
};


CREATE FUNCTION std::contains(
    haystack: range<anypoint>,
    needle: anypoint
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "haystack" @> "needle"
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};


CREATE FUNCTION std::contains(
    haystack: multirange<anypoint>,
    needle: multirange<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "haystack" @> "needle"
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};


CREATE FUNCTION std::contains(
    haystack: multirange<anypoint>,
    needle: range<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "haystack" @> "needle"
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};


CREATE FUNCTION std::contains(
    haystack: multirange<anypoint>,
    needle: anypoint
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "haystack" @> "needle"
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};


CREATE FUNCTION std::overlaps(
    l: range<anypoint>,
    r: range<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "l" && "r"
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};


CREATE FUNCTION std::overlaps(
    l: multirange<anypoint>,
    r: multirange<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "l" && "r"
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};


# FIXME: These functions introduce the concrete multirange types into the
# schema. That's why they exist for each concrete type explicitly and aren't
# defined generically for anytype.
CREATE FUNCTION std::multirange_unpack(
    val: multirange<std::int32>,
) -> set of range<std::int32>
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'unnest';
};


CREATE FUNCTION std::multirange_unpack(
    val: multirange<std::int64>,
) -> set of range<std::int64>
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'unnest';
};


CREATE FUNCTION std::multirange_unpack(
    val: multirange<std::float32>,
) -> set of range<std::float32>
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'unnest';
};


CREATE FUNCTION std::multirange_unpack(
    val: multirange<std::float64>,
) -> set of range<std::float64>
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'unnest';
};


CREATE FUNCTION std::multirange_unpack(
    val: multirange<std::decimal>,
) -> set of range<std::decimal>
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'unnest';
};


CREATE FUNCTION std::multirange_unpack(
    val: multirange<std::datetime>,
) -> set of range<std::datetime>
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'unnest';
};


CREATE FUNCTION std::strictly_below(
    l: range<anypoint>,
    r: range<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "l" << "r"
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};


CREATE FUNCTION std::strictly_below(
    l: multirange<anypoint>,
    r: multirange<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "l" << "r"
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};


CREATE FUNCTION std::strictly_above(
    l: range<anypoint>,
    r: range<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "l" >> "r"
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};


CREATE FUNCTION std::strictly_above(
    l: multirange<anypoint>,
    r: multirange<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "l" >> "r"
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};


CREATE FUNCTION std::bounded_above(
    l: range<anypoint>,
    r: range<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "l" &< "r"
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};


CREATE FUNCTION std::bounded_above(
    l: multirange<anypoint>,
    r: multirange<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "l" &< "r"
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};


CREATE FUNCTION std::bounded_below(
    l: range<anypoint>,
    r: range<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "l" &> "r"
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};


CREATE FUNCTION std::bounded_below(
    l: multirange<anypoint>,
    r: multirange<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "l" &> "r"
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};


CREATE FUNCTION std::adjacent(
    l: range<anypoint>,
    r: range<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "l" -|- "r"
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};


CREATE FUNCTION std::adjacent(
    l: multirange<anypoint>,
    r: multirange<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "l" -|- "r"
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};


## Range operators


CREATE INFIX OPERATOR
std::`=` (l: range<anypoint>, r: range<anypoint>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR '=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL range<anypoint>,
           r: OPTIONAL range<anypoint>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
    SET recursive := true;
};


CREATE INFIX OPERATOR
std::`!=` (l: range<anypoint>, r: range<anypoint>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR '<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL range<anypoint>,
            r: OPTIONAL range<anypoint>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
    SET recursive := true;
};


CREATE INFIX OPERATOR
std::`>=` (l: range<anypoint>, r: range<anypoint>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR '>=';
};


CREATE INFIX OPERATOR
std::`>` (l: range<anypoint>, r: range<anypoint>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR '>';
};


CREATE INFIX OPERATOR
std::`<=` (l: range<anypoint>, r: range<anypoint>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR '<=';
};


CREATE INFIX OPERATOR
std::`<` (l: range<anypoint>, r: range<anypoint>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR '<';
};


CREATE INFIX OPERATOR
std::`+` (l: range<anypoint>, r: range<anypoint>) -> range<anypoint> {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Range union.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`-` (l: range<anypoint>, r: range<anypoint>) -> range<anypoint> {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Range difference.';
    SET volatility := 'Immutable';
    SET recursive := true;
    USING SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`*` (l: range<anypoint>, r: range<anypoint>) -> range<anypoint> {
    CREATE ANNOTATION std::identifier := 'mult';
    CREATE ANNOTATION std::description := 'Range intersection.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::*';
    USING SQL OPERATOR r'*';
};


## MultiRange operators


CREATE INFIX OPERATOR
std::`=` (l: multirange<anypoint>, r: multirange<anypoint>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR '=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL multirange<anypoint>,
           r: OPTIONAL multirange<anypoint>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
    SET recursive := true;
};


CREATE INFIX OPERATOR
std::`!=` (l: multirange<anypoint>, r: multirange<anypoint>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR '<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL multirange<anypoint>,
            r: OPTIONAL multirange<anypoint>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
    SET recursive := true;
};


CREATE INFIX OPERATOR
std::`>=` (l: multirange<anypoint>, r: multirange<anypoint>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR '>=';
};


CREATE INFIX OPERATOR
std::`>` (l: multirange<anypoint>, r: multirange<anypoint>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR '>';
};


CREATE INFIX OPERATOR
std::`<=` (l: multirange<anypoint>, r: multirange<anypoint>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR '<=';
};


CREATE INFIX OPERATOR
std::`<` (l: multirange<anypoint>, r: multirange<anypoint>) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR '<';
};


CREATE INFIX OPERATOR
std::`+` (l: multirange<anypoint>, r: multirange<anypoint>) -> multirange<anypoint> {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Range union.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`-` (l: multirange<anypoint>, r: multirange<anypoint>) -> multirange<anypoint> {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Range difference.';
    SET volatility := 'Immutable';
    SET recursive := true;
    USING SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`*` (l: multirange<anypoint>, r: multirange<anypoint>) -> multirange<anypoint> {
    CREATE ANNOTATION std::identifier := 'mult';
    CREATE ANNOTATION std::description := 'Range intersection.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::*';
    USING SQL OPERATOR r'*';
};


## Range/multirange casts

CREATE CAST FROM range<anypoint> TO multirange<anypoint> {
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
    # Any range can be implicitly cast into a multirange.
    ALLOW IMPLICIT;
};
