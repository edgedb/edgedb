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


"""Patches to apply to databases"""

from __future__ import annotations
from typing import *


PATCHES: list[tuple[str, str]] = [
    ('sql', '''
CREATE OR REPLACE FUNCTION
 edgedbstd."std|cast@std|json@array<std||json>_f"(val jsonb)
 RETURNS jsonb[]
 LANGUAGE sql
AS $function$
SELECT (
    CASE WHEN nullif(val, 'null'::jsonb) IS NULL THEN NULL
    ELSE
        (SELECT COALESCE(array_agg(j), ARRAY[]::jsonb[])
        FROM jsonb_array_elements(val) as j)
    END
)
$function$
    '''),
    ('edgeql', '''
ALTER FUNCTION
std::range_unpack(
    val: range<int32>,
    step: int32
)
{
    USING SQL $$
        SELECT
            generate_series(
                (
                    edgedb.range_lower_validate(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE 1 END)
                )::int8,
                (
                    edgedb.range_upper_validate(val) -
                    (CASE WHEN upper_inc(val) THEN 0 ELSE 1 END)
                )::int8,
                step::int8
            )::int4
    $$;
};
    '''),
    ('edgeql', '''
ALTER FUNCTION
std::range_unpack(
    val: range<int64>,
    step: int64
)
{
    USING SQL $$
        SELECT
            generate_series(
                (
                    edgedb.range_lower_validate(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE 1 END)
                )::int8,
                (
                    edgedb.range_upper_validate(val) -
                    (CASE WHEN upper_inc(val) THEN 0 ELSE 1 END)
                )::int8,
                step
            )
    $$;
};
    '''),
    ('edgeql', '''
ALTER FUNCTION
std::range_unpack(
    val: range<float32>,
    step: float32
)
{
    USING SQL $$
        SELECT num::float4
        FROM
            generate_series(
                (
                    edgedb.range_lower_validate(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE step END)
                )::numeric,
                (
                    edgedb.range_upper_validate(val)
                )::numeric,
                step::numeric
            ) AS num
        WHERE
            upper_inc(val) OR num::float4 < upper(val)
    $$;
};
    '''),
    ('edgeql', '''
ALTER FUNCTION
std::range_unpack(
    val: range<float64>,
    step: float64
)
{
    USING SQL $$
        SELECT num::float8
        FROM
            generate_series(
                (
                    edgedb.range_lower_validate(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE step END)
                )::numeric,
                (
                    edgedb.range_upper_validate(val)
                )::numeric,
                step::numeric
            ) AS num
        WHERE
            upper_inc(val) OR num::float8 < upper(val)
    $$;
};
    '''),
    ('edgeql', '''
ALTER FUNCTION
std::range_unpack(
    val: range<decimal>,
    step: decimal
)
{
    USING SQL $$
        SELECT num
        FROM
            generate_series(
                edgedb.range_lower_validate(val) +
                    (CASE WHEN lower_inc(val) THEN 0 ELSE step END),
                edgedb.range_upper_validate(val),
                step
            ) AS num
        WHERE
            upper_inc(val) OR num < upper(val)
    $$;
};
    '''),
    ('edgeql', '''
ALTER FUNCTION
std::range_unpack(
    val: range<datetime>,
    step: duration
)
{
    USING SQL $$
        SELECT d::edgedb.timestamptz_t
        FROM
            generate_series(
                (
                    edgedb.range_lower_validate(val) + (
                        CASE WHEN lower_inc(val)
                            THEN '0'::interval
                            ELSE step
                        END
                    )
                )::timestamptz,
                (
                    edgedb.range_upper_validate(val)
                )::timestamptz,
                step::interval
            ) AS d
        WHERE
            upper_inc(val) OR d::edgedb.timestamptz_t < upper(val)
    $$;
};
    '''),
    ('edgeql', '''
ALTER FUNCTION
std::range_unpack(
    val: range<cal::local_datetime>,
    step: cal::relative_duration
)
{
    USING SQL $$
        SELECT d::edgedb.timestamp_t
        FROM
            generate_series(
                (
                    edgedb.range_lower_validate(val) + (
                        CASE WHEN lower_inc(val)
                            THEN '0'::interval
                            ELSE step
                        END
                    )
                )::timestamptz,
                (
                    edgedb.range_upper_validate(val)
                )::timestamptz,
                step::interval
            ) AS d
        WHERE
            upper_inc(val) OR d::edgedb.timestamp_t < upper(val)
    $$;
};
    '''),
    ('edgeql', '''
ALTER FUNCTION
std::range_unpack(
    val: range<cal::local_date>,
    step: cal::date_duration
)
{
    USING SQL $$
        SELECT
            generate_series(
                (
                    edgedb.range_lower_validate(val) + (
                        CASE WHEN lower_inc(val)
                            THEN '0'::interval
                            ELSE 'P1D'::interval
                        END
                    )
                )::timestamp,
                (
                    edgedb.range_upper_validate(val) - (
                        CASE WHEN upper_inc(val)
                            THEN '0'::interval
                            ELSE 'P1D'::interval
                        END
                    )
                )::timestamp,
                step::interval
            )::edgedb.date_t
    $$;
};
    '''),
    ('sql', '''
        INSERT INTO edgedb._dml_dummy VALUES (0, false)
    '''),

    ('sql', '''
DROP FUNCTION edgedb._slice(anyarray, bigint, bigint);
    '''),
    ('sql', '''
CREATE FUNCTION edgedb._slice(val anyarray, start integer, stop integer)
 RETURNS anyarray
 LANGUAGE sql
 IMMUTABLE
AS $function$
SELECT
    val[edgedb._normalize_array_index(start, cardinality(val)):
        edgedb._normalize_array_index(stop, cardinality(val)) - 1]
$function$
    '''),

    ('sql', '''
DROP FUNCTION edgedb._substr(anyelement, bigint, int)
    '''),
    ('sql', '''
CREATE FUNCTION edgedb._substr(val anyelement, start integer, length integer)
 RETURNS anyelement
 LANGUAGE sql
 IMMUTABLE STRICT
AS $function$
SELECT
    CASE
        WHEN length < 0 THEN ''
        ELSE substr(val, start::int, length)
    END
$function$
    '''),

    ('sql', '''
DROP FUNCTION edgedb._str_slice(anyelement, bigint, bigint)
    '''),
    ('sql', '''
CREATE FUNCTION edgedb._str_slice(val anyelement, start integer, stop integer)
 RETURNS anyelement
 LANGUAGE sql
 IMMUTABLE
AS $function$
SELECT
    edgedb._substr(
        val,
        edgedb._normalize_array_index(
            start, edgedb._length(val)),
        edgedb._normalize_array_index(
            stop, edgedb._length(val)) -
        edgedb._normalize_array_index(
            start, edgedb._length(val))
    )
$function$
    '''),

    ('sql', '''
DROP FUNCTION edgedb._slice(text, bigint, bigint)
    '''),
    ('sql', '''
CREATE FUNCTION edgedb._slice(val text, start integer, stop integer)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE
AS $function$
SELECT edgedb._str_slice(val, start, stop)
$function$
    '''),

    ('sql', '''
DROP FUNCTION edgedb._slice(bytea, bigint, bigint)
    '''),
    ('sql', '''
CREATE FUNCTION edgedb._slice(val bytea, start integer, stop integer)
 RETURNS bytea
 LANGUAGE sql
 IMMUTABLE
AS $function$
SELECT edgedb._str_slice(val, start, stop)
$function$
    '''),

    ('sql', '''
DROP FUNCTION edgedb._slice(jsonb, bigint, bigint)
    '''),
    ('sql', '''
CREATE OR REPLACE FUNCTION edgedb._slice(
    val jsonb, start integer, stop integer
)
 RETURNS jsonb
 LANGUAGE sql
 STABLE
AS $function$
SELECT
    CASE
    WHEN val IS NULL THEN NULL
    WHEN jsonb_typeof(val) = 'array' THEN (
        to_jsonb(edgedb._slice(
            (
                SELECT coalesce(array_agg(value), '{}'::jsonb[])
                FROM jsonb_array_elements(val)
            ),
            start, stop
        ))
    )
    WHEN jsonb_typeof(val) = 'string' THEN (
        to_jsonb(edgedb._slice(val#>>'{}', start, stop))
    )
    ELSE
        edgedb.raise(
            NULL::jsonb,
            'wrong_object_type',
            msg => (
                'cannot slice JSON '
                || coalesce(jsonb_typeof(val), 'UNKNOWN')
            ),
            detail => (
                '{"hint":"Slicing is only available for JSON arrays'
                || ' and strings."}'
            )
        )
    END
$function$
    '''),
]
