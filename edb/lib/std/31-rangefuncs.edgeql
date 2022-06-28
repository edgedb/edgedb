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


## Range casts
## -----------
##
## Ranges over numeric types should be castable into each other in ways that
## mirror the casts between the underlying numeric types. Unlike array casts,
## that are not defined explicitly, only specific range types exist and their
## casts should be explicit.


## Implicit casts between numerics.


CREATE CAST FROM range<std::int32> TO range<std::int64> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT int8range(lower(val)::int8, upper(val)::int8);
    $$;
    ALLOW IMPLICIT;
};


CREATE CAST FROM range<std::int32> TO range<std::float32> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT edgedb.float32_range_t(
            lower(val)::float4, upper(val)::float4, '[)');
    $$;
    ALLOW IMPLICIT;
};


CREATE CAST FROM range<std::int32> TO range<std::float64> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT edgedb.float64_range_t(
            lower(val)::float8, upper(val)::float8, '[)');
    $$;
    ALLOW IMPLICIT;
};


CREATE CAST FROM range<std::int32> TO range<std::decimal> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT numrange(lower(val)::numeric, upper(val)::numeric, '[)');
    $$;
    ALLOW IMPLICIT;
};


CREATE CAST FROM range<std::int64> TO range<std::float64> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT edgedb.float64_range_t(
            lower(val)::float8, upper(val)::float8, '[)');
    $$;
    ALLOW IMPLICIT;
};


CREATE CAST FROM range<std::int64> TO range<std::decimal> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT numrange(lower(val)::numeric, upper(val)::numeric, '[)');
    $$;
    ALLOW IMPLICIT;
};


CREATE CAST FROM range<std::float32> TO range<std::float64> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN lower_inc(val)
            THEN
                CASE
                    WHEN upper_inc(val)
                    THEN
                        edgedb.float64_range_t(
                            lower(val)::float8, upper(val)::float8, '[]')
                    ELSE
                        edgedb.float64_range_t(
                            lower(val)::float8, upper(val)::float8, '[)')
                END
            ELSE
                CASE
                    WHEN upper_inc(val)
                    THEN
                        edgedb.float64_range_t(
                            lower(val)::float8, upper(val)::float8, '(]')
                    ELSE
                        edgedb.float64_range_t(
                            lower(val)::float8, upper(val)::float8, '()')
                END
        END
    $$;
    ALLOW IMPLICIT;
};


## Explicit and assignment casts.

CREATE CAST FROM range<std::int64> TO range<std::int32> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT int4range(lower(val)::int4, upper(val)::int4);
    $$;
    ALLOW ASSIGNMENT;
};


CREATE CAST FROM range<std::int64> TO range<std::float32> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT edgedb.float32_range_t(
            lower(val)::float4, upper(val)::float4, '[)');
    $$;
    ALLOW ASSIGNMENT;
};


CREATE CAST FROM range<std::float32> TO range<std::int32> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN lower_inc(val)
            THEN
                CASE
                    WHEN upper_inc(val)
                    THEN
                        int4range(lower(val)::int4, upper(val)::int4, '[]')
                    ELSE
                        int4range(lower(val)::int4, upper(val)::int4, '[)')
                END
            ELSE
                CASE
                    WHEN upper_inc(val)
                    THEN
                        int4range(lower(val)::int4, upper(val)::int4, '(]')
                    ELSE
                        int4range(lower(val)::int4, upper(val)::int4, '()')
                END
        END
    $$;
};


CREATE CAST FROM range<std::float32> TO range<std::int64> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN lower_inc(val)
            THEN
                CASE
                    WHEN upper_inc(val)
                    THEN
                        int8range(lower(val)::int8, upper(val)::int8, '[]')
                    ELSE
                        int8range(lower(val)::int8, upper(val)::int8, '[)')
                END
            ELSE
                CASE
                    WHEN upper_inc(val)
                    THEN
                        int8range(lower(val)::int8, upper(val)::int8, '(]')
                    ELSE
                        int8range(lower(val)::int8, upper(val)::int8, '()')
                END
        END
    $$;
};


CREATE CAST FROM range<std::float32> TO range<std::decimal> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN lower_inc(val)
            THEN
                CASE
                    WHEN upper_inc(val)
                    THEN
                        numrange(
                            lower(val)::numeric, upper(val)::numeric, '[]')
                    ELSE
                        numrange(
                            lower(val)::numeric, upper(val)::numeric, '[)')
                END
            ELSE
                CASE
                    WHEN upper_inc(val)
                    THEN
                        numrange(
                            lower(val)::numeric, upper(val)::numeric, '(]')
                    ELSE
                        numrange(
                            lower(val)::numeric, upper(val)::numeric, '()')
                END
        END
    $$;
};


CREATE CAST FROM range<std::float64> TO range<std::int32> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN lower_inc(val)
            THEN
                CASE
                    WHEN upper_inc(val)
                    THEN
                        int4range(lower(val)::int4, upper(val)::int4, '[]')
                    ELSE
                        int4range(lower(val)::int4, upper(val)::int4, '[)')
                END
            ELSE
                CASE
                    WHEN upper_inc(val)
                    THEN
                        int4range(lower(val)::int4, upper(val)::int4, '(]')
                    ELSE
                        int4range(lower(val)::int4, upper(val)::int4, '()')
                END
        END
    $$;
};


CREATE CAST FROM range<std::float64> TO range<std::int64> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN lower_inc(val)
            THEN
                CASE
                    WHEN upper_inc(val)
                    THEN
                        int8range(lower(val)::int8, upper(val)::int8, '[]')
                    ELSE
                        int8range(lower(val)::int8, upper(val)::int8, '[)')
                END
            ELSE
                CASE
                    WHEN upper_inc(val)
                    THEN
                        int8range(lower(val)::int8, upper(val)::int8, '(]')
                    ELSE
                        int8range(lower(val)::int8, upper(val)::int8, '()')
                END
        END
    $$;
};


CREATE CAST FROM range<std::float64> TO range<std::float32> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN lower_inc(val)
            THEN
                CASE
                    WHEN upper_inc(val)
                    THEN
                        edgedb.float32_range_t(
                            lower(val)::float4, upper(val)::float4, '[]')
                    ELSE
                        edgedb.float32_range_t(
                            lower(val)::float4, upper(val)::float4, '[)')
                END
            ELSE
                CASE
                    WHEN upper_inc(val)
                    THEN
                        edgedb.float32_range_t(
                            lower(val)::float4, upper(val)::float4, '(]')
                    ELSE
                        edgedb.float32_range_t(
                            lower(val)::float4, upper(val)::float4, '()')
                END
        END
    $$;
    ALLOW ASSIGNMENT;
};


CREATE CAST FROM range<std::float64> TO range<std::decimal> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN lower_inc(val)
            THEN
                CASE
                    WHEN upper_inc(val)
                    THEN
                        numrange(
                            lower(val)::numeric, upper(val)::numeric, '[]')
                    ELSE
                        numrange(
                            lower(val)::numeric, upper(val)::numeric, '[)')
                END
            ELSE
                CASE
                    WHEN upper_inc(val)
                    THEN
                        numrange(
                            lower(val)::numeric, upper(val)::numeric, '(]')
                    ELSE
                        numrange(
                            lower(val)::numeric, upper(val)::numeric, '()')
                END
        END
    $$;
};


CREATE CAST FROM range<std::decimal> TO range<std::int32> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN lower_inc(val)
            THEN
                CASE
                    WHEN upper_inc(val)
                    THEN
                        int4range(lower(val)::int4, upper(val)::int4, '[]')
                    ELSE
                        int4range(lower(val)::int4, upper(val)::int4, '[)')
                END
            ELSE
                CASE
                    WHEN upper_inc(val)
                    THEN
                        int4range(lower(val)::int4, upper(val)::int4, '(]')
                    ELSE
                        int4range(lower(val)::int4, upper(val)::int4, '()')
                END
        END
    $$;
};


CREATE CAST FROM range<std::decimal> TO range<std::int64> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN lower_inc(val)
            THEN
                CASE
                    WHEN upper_inc(val)
                    THEN
                        int8range(lower(val)::int8, upper(val)::int8, '[]')
                    ELSE
                        int8range(lower(val)::int8, upper(val)::int8, '[)')
                END
            ELSE
                CASE
                    WHEN upper_inc(val)
                    THEN
                        int8range(lower(val)::int8, upper(val)::int8, '(]')
                    ELSE
                        int8range(lower(val)::int8, upper(val)::int8, '()')
                END
        END
    $$;
};


CREATE CAST FROM range<std::decimal> TO range<std::float32> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN lower_inc(val)
            THEN
                CASE
                    WHEN upper_inc(val)
                    THEN
                        edgedb.float32_range_t(
                            lower(val)::float4, upper(val)::float4, '[]')
                    ELSE
                        edgedb.float32_range_t(
                            lower(val)::float4, upper(val)::float4, '[)')
                END
            ELSE
                CASE
                    WHEN upper_inc(val)
                    THEN
                        edgedb.float32_range_t(
                            lower(val)::float4, upper(val)::float4, '(]')
                    ELSE
                        edgedb.float32_range_t(
                            lower(val)::float4, upper(val)::float4, '()')
                END
        END
    $$;
};


CREATE CAST FROM range<std::decimal> TO range<std::float64> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN lower_inc(val)
            THEN
                CASE
                    WHEN upper_inc(val)
                    THEN
                        edgedb.float64_range_t(lower(val), upper(val), '[]')
                    ELSE
                        edgedb.float64_range_t(lower(val), upper(val), '[)')
                END
            ELSE
                CASE
                    WHEN upper_inc(val)
                    THEN
                        edgedb.float64_range_t(lower(val), upper(val), '(]')
                    ELSE
                        edgedb.float64_range_t(lower(val), upper(val), '()')
                END
        END
    $$;
};


## Range functions


CREATE FUNCTION
std::range(
    lower: optional int32 = {},
    upper: optional int32 = {},
    named only inc_lower: bool = true,
    named only inc_upper: bool = false
) -> range<int32>
{
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN "inc_lower" AND "inc_upper"
            THEN int4range("lower", "upper", '[]')
            WHEN NOT "inc_lower" AND "inc_upper"
            THEN int4range("lower", "upper", '(]')
            WHEN NOT "inc_lower" AND NOT "inc_upper"
            THEN int4range("lower", "upper", '()')
            WHEN "inc_lower" AND NOT "inc_upper"
            THEN int4range("lower", "upper", '[)')
        END
    $$;
};


CREATE FUNCTION
std::range(
    lower: optional int64 = {},
    upper: optional int64 = {},
    named only inc_lower: bool = true,
    named only inc_upper: bool = false
) -> range<int64>
{
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN "inc_lower" AND "inc_upper"
            THEN int8range("lower", "upper", '[]')
            WHEN NOT "inc_lower" AND "inc_upper"
            THEN int8range("lower", "upper", '(]')
            WHEN NOT "inc_lower" AND NOT "inc_upper"
            THEN int8range("lower", "upper", '()')
            WHEN "inc_lower" AND NOT "inc_upper"
            THEN int8range("lower", "upper", '[)')
        END
    $$;
};


CREATE FUNCTION
std::range(
    lower: optional float32 = {},
    upper: optional float32 = {},
    named only inc_lower: bool = true,
    named only inc_upper: bool = false
) -> range<float32>
{
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN "inc_lower" AND "inc_upper"
            THEN edgedb.float32_range_t("lower", "upper", '[]')
            WHEN NOT "inc_lower" AND "inc_upper"
            THEN edgedb.float32_range_t("lower", "upper", '(]')
            WHEN NOT "inc_lower" AND NOT "inc_upper"
            THEN edgedb.float32_range_t("lower", "upper", '()')
            WHEN "inc_lower" AND NOT "inc_upper"
            THEN edgedb.float32_range_t("lower", "upper", '[)')
        END
    $$;
};


CREATE FUNCTION
std::range(
    lower: optional float64 = {},
    upper: optional float64 = {},
    named only inc_lower: bool = true,
    named only inc_upper: bool = false
) -> range<float64>
{
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN "inc_lower" AND "inc_upper"
            THEN edgedb.float64_range_t("lower", "upper", '[]')
            WHEN NOT "inc_lower" AND "inc_upper"
            THEN edgedb.float64_range_t("lower", "upper", '(]')
            WHEN NOT "inc_lower" AND NOT "inc_upper"
            THEN edgedb.float64_range_t("lower", "upper", '()')
            WHEN "inc_lower" AND NOT "inc_upper"
            THEN edgedb.float64_range_t("lower", "upper", '[)')
        END
    $$;
};


CREATE FUNCTION
std::range(
    lower: optional decimal = {},
    upper: optional decimal = {},
    named only inc_lower: bool = true,
    named only inc_upper: bool = false
) -> range<decimal>
{
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN "inc_lower" AND "inc_upper"
            THEN numrange("lower", "upper", '[]')
            WHEN NOT "inc_lower" AND "inc_upper"
            THEN numrange("lower", "upper", '(]')
            WHEN NOT "inc_lower" AND NOT "inc_upper"
            THEN numrange("lower", "upper", '()')
            WHEN "inc_lower" AND NOT "inc_upper"
            THEN numrange("lower", "upper", '[)')
        END
    $$;
};


CREATE FUNCTION
std::range(
    lower: optional datetime = {},
    upper: optional datetime = {},
    named only inc_lower: bool = true,
    named only inc_upper: bool = false
) -> range<datetime>
{
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT CASE
            WHEN "inc_lower" AND "inc_upper"
            THEN edgedb.datetime_range_t("lower", "upper", '[]')
            WHEN NOT "inc_lower" AND "inc_upper"
            THEN edgedb.datetime_range_t("lower", "upper", '(]')
            WHEN NOT "inc_lower" AND NOT "inc_upper"
            THEN edgedb.datetime_range_t("lower", "upper", '()')
            WHEN "inc_lower" AND NOT "inc_upper"
            THEN edgedb.datetime_range_t("lower", "upper", '[)')
        END
    $$;
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
                    lower(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE 1 END)
                )::int8,
                (
                    upper(val) -
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
                    lower(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE step END)
                )::int8,
                (
                    upper(val) -
                    (CASE WHEN upper_inc(val) THEN 0 ELSE step END)
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
                    lower(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE 1 END)
                )::int8,
                (
                    upper(val) -
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
                    lower(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE step END)
                )::int8,
                (
                    upper(val) -
                    (CASE WHEN upper_inc(val) THEN 0 ELSE step END)
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
        SELECT
            generate_series(
                (
                    lower(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE step END)
                )::numeric,
                (
                    upper(val) -
                    (CASE WHEN upper_inc(val) THEN 0 ELSE step END)
                )::numeric,
                step::numeric
            )::float4
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
        SELECT
            generate_series(
                (
                    lower(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE step END)
                )::numeric,
                (
                    upper(val) -
                    (CASE WHEN upper_inc(val) THEN 0 ELSE step END)
                )::numeric,
                step::numeric
            )::float8
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
        SELECT
            generate_series(
                lower(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE step END),
                upper(val) -
                    (CASE WHEN upper_inc(val) THEN 0 ELSE step END),
                step
            )
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
        SELECT
            generate_series(
                (
                    lower(val) + (
                        CASE WHEN lower_inc(val)
                            THEN '0'::interval
                            ELSE step
                        END
                    )
                )::timestamptz,
                (
                    upper(val) - (
                        CASE WHEN upper_inc(val)
                            THEN '0'::interval
                            ELSE step
                        END
                    )
                )::timestamptz,
                step::interval
            )::edgedb.timestamptz_t
    $$;
};


CREATE FUNCTION std::range_get_upper(r: range<anypoint>) -> optional anypoint
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'upper';
};


CREATE FUNCTION std::range_get_lower(r: range<anypoint>) -> optional anypoint
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'lower';
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


CREATE FUNCTION std::contains(
    haystack: range<anypoint>,
    needle: range<anypoint>
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "haystack" @> "needle"
    $$;
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
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Range difference.';
    SET volatility := 'Immutable';
    SET recursive := true;
    USING SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`*` (l: range<anypoint>, r: range<anypoint>) -> range<anypoint> {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Range intersection.';
    SET volatility := 'Immutable';
    SET recursive := true;
    SET commutator := 'std::*';
    USING SQL OPERATOR r'*';
};
