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


## Range functions


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


CREATE FUNCTION
std::range_is_empty(
    val: range<anypoint>
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
