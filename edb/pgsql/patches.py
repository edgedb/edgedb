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
]
