#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
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

## Function that construct various scalars from strings or other types.


# std::to_str
# --------

# Normalize [local] datetime to text conversion to have
# the same format as one would get by serializing to JSON.
# Otherwise Postgres doesn't follow the ISO8601 standard
# and uses ' ' instead of 'T' as a separator between date
# and time.
#
# EdgeQL: <text><datetime>'2010-10-10';
# To SQL: trim(to_json('2010-01-01'::timestamptz)::text, '"')
CREATE FUNCTION
std::to_str(dt: std::datetime, fmt: OPTIONAL str={}) -> std::str
{
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            trim(to_json("dt")::text, '"')
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_str(): "fmt" argument must be a non-empty string',
                '',
                NULL::text)
        ELSE
            edgedb._raise_exception_on_null(
                to_char("dt", "fmt"),
                'invalid_parameter_value',
                'to_str(): format ''' || "fmt" || ''' is invalid',
                ''
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_str(td: std::duration, fmt: OPTIONAL str={}) -> std::str
{
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            trim(to_json("td")::text, '"')
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_str(): "fmt" argument must be a non-empty string',
                '',
                NULL::text)
        ELSE
            edgedb._raise_exception_on_null(
                to_char("td", "fmt"),
                'invalid_parameter_value',
                'to_str(): format ''' || "fmt" || ''' is invalid',
                ''
            )
        END
    )
    $$;
};


# FIXME: There's no good safe default for all possible durations and some
# durations cannot be formatted without non-trivial conversions (e.g.
# 7,000 days).


CREATE FUNCTION
std::to_str(i: std::int64, fmt: OPTIONAL str={}) -> std::str
{
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            "i"::text
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_str(): "fmt" argument must be a non-empty string',
                '',
                NULL::text)
        ELSE
            edgedb._raise_exception_on_null(
                to_char("i", "fmt"),
                'invalid_parameter_value',
                'to_str(): format ''' || "fmt" || ''' is invalid',
                ''
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_str(f: std::float64, fmt: OPTIONAL str={}) -> std::str
{
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            "f"::text
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_str(): "fmt" argument must be a non-empty string',
                '',
                NULL::text)
        ELSE
            edgedb._raise_exception_on_null(
                to_char("f", "fmt"),
                'invalid_parameter_value',
                'to_str(): format ''' || "fmt" || ''' is invalid',
                ''
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_str(d: std::bigint, fmt: OPTIONAL str={}) -> std::str
{
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            "d"::text
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_str(): "fmt" argument must be a non-empty string',
                '',
                NULL::text)
        ELSE
            edgedb._raise_exception_on_null(
                to_char("d", "fmt"),
                'invalid_parameter_value',
                'to_str(): format ''' || "fmt" || ''' is invalid',
                ''
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_str(d: std::decimal, fmt: OPTIONAL str={}) -> std::str
{
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            "d"::text
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_str(): "fmt" argument must be a non-empty string',
                '',
                NULL::text)
        ELSE
            edgedb._raise_exception_on_null(
                to_char("d", "fmt"),
                'invalid_parameter_value',
                'to_str(): format ''' || "fmt" || ''' is invalid',
                ''
            )
        END
    )
    $$;
};




# JSON can be prettified by specifying 'pretty' as the format, any
# other value will result in an exception.
CREATE FUNCTION
std::to_str(json: std::json, fmt: OPTIONAL str={}) -> std::str
{
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            "json"::text
        WHEN "fmt" = 'pretty' THEN
            jsonb_pretty("json")
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_str(): "fmt" argument must be a non-empty string',
                '',
                NULL::text)
        ELSE
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_str(): format ''' || "fmt" || ''' is invalid',
                NULL,
                NULL::text
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_str(array: array<std::str>, delimiter: std::str) -> std::str
{
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT array_to_string("array", "delimiter");
    $$;
};


CREATE FUNCTION
std::to_json(str: std::str) -> std::json
{
    # Casting of jsonb to and from text in PostgreSQL is IMMUTABLE.
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT "str"::jsonb
    $$;
};


CREATE FUNCTION
std::to_datetime(s: std::str, fmt: OPTIONAL str={}) -> std::datetime
{
    # Helper function to_datetime is VOLATILE.
    SET volatility := 'VOLATILE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb.datetime_in("s")
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_datetime(): "fmt" argument must be a non-empty string',
                '',
                NULL::timestamptz)
        ELSE
            edgedb._raise_exception_on_null(
                edgedb.to_datetime("s", "fmt"),
                'invalid_parameter_value',
                'to_datetime(): format ''' || "fmt" || ''' is invalid',
                ''
            )
        END
    )
    $$;
};

CREATE FUNCTION
std::to_datetime(year: std::int64, month: std::int64, day: std::int64,
                 hour: std::int64, min: std::int64, sec: std::float64,
                 timezone: std::str)
    -> std::datetime
{
    # make_timestamptz is STABLE
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT make_timestamptz(
        "year"::int, "month"::int, "day"::int,
        "hour"::int, "min"::int, "sec", "timezone"
    )
    $$;
};


CREATE FUNCTION
std::to_duration(
        NAMED ONLY hours: std::int64=0,
        NAMED ONLY minutes: std::int64=0,
        NAMED ONLY seconds: std::float64=0,
        NAMED ONLY microseconds: std::int64=0
    ) -> std::duration
{
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT make_interval(
        0,
        0,
        0,
        0,
        "hours"::int,
        "minutes"::int,
        "seconds"
    ) + (microseconds::text || ' microseconds')::interval
    $$;
};


CREATE FUNCTION
std::to_bigint(s: std::str, fmt: OPTIONAL str={}) -> std::bigint
{
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb.str_to_bigint("s")
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_bigint(): "fmt" argument must be a non-empty string',
                '',
                NULL::edgedb.bigint_t)
        ELSE
            edgedb._raise_exception_on_null(
                to_number("s", "fmt")::edgedb.bigint_t,
                'invalid_parameter_value',
                'to_bigint(): format ''' || "fmt" || ''' is invalid',
                ''
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_decimal(s: std::str, fmt: OPTIONAL str={}) -> std::decimal
{
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb.str_to_decimal("s")
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_decimal(): "fmt" argument must be a non-empty string',
                '',
                NULL::numeric)
        ELSE
            edgedb._raise_exception_on_null(
                to_number("s", "fmt")::numeric,
                'invalid_parameter_value',
                'to_decimal(): format ''' || "fmt" || ''' is invalid',
                ''
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_int64(s: std::str, fmt: OPTIONAL str={}) -> std::int64
{
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            -- Must use the noninline version to prevent
            -- the overeager function inliner from crashing
            edgedb.str_to_int64_noinline("s")
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_int64(): "fmt" argument must be a non-empty string',
                '',
                NULL::bigint)
        ELSE
            edgedb._raise_exception_on_null(
                to_number("s", "fmt")::bigint,
                'invalid_parameter_value',
                'to_int64(): format ''' || "fmt" || ''' is invalid',
                ''
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_int32(s: std::str, fmt: OPTIONAL str={}) -> std::int32
{
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            -- Must use the noninline version to prevent
            -- the overeager function inliner from crashing
            edgedb.str_to_int32_noinline("s")
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_int32(): "fmt" argument must be a non-empty string',
                '',
                NULL::int)
        ELSE
            edgedb._raise_exception_on_null(
                to_number("s", "fmt")::int,
                'invalid_parameter_value',
                'to_int32(): format ''' || "fmt" || ''' is invalid',
                ''
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_int16(s: std::str, fmt: OPTIONAL str={}) -> std::int16
{
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            -- Must use the noninline version to prevent
            -- the overeager function inliner from crashing
            edgedb.str_to_int16_noinline("s")
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_int16(): "fmt" argument must be a non-empty string',
                '',
                NULL::smallint)
        ELSE
            edgedb._raise_exception_on_null(
                to_number("s", "fmt")::smallint,
                'invalid_parameter_value',
                'to_int16(): format ''' || "fmt" || ''' is invalid',
                ''
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_float64(s: std::str, fmt: OPTIONAL str={}) -> std::float64
{
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb.str_to_float64_noinline("s")
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_float64(): "fmt" argument must be a non-empty string',
                '',
                NULL::float8)
        ELSE
            edgedb._raise_exception_on_null(
                to_number("s", "fmt")::float8,
                'invalid_parameter_value',
                'to_float64(): format ''' || "fmt" || ''' is invalid',
                ''
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_float32(s: std::str, fmt: OPTIONAL str={}) -> std::float32
{
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb.str_to_float32_noinline("s")
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_float32(): "fmt" argument must be a non-empty string',
                '',
                NULL::float4)
        ELSE
            edgedb._raise_exception_on_null(
                to_number("s", "fmt")::float4,
                'invalid_parameter_value',
                'to_float32(): format ''' || "fmt" || ''' is invalid',
                ''
            )
        END
    )
    $$;
};
