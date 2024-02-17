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
    CREATE ANNOTATION std::description :=
        'Return string representation of the input value.';
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'Stable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            trim(to_json("dt")::text, '"')
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::text,
                'invalid_parameter_value',
                msg => 'to_str(): "fmt" argument must be a non-empty string'
            )
        ELSE
            edgedb.raise_on_null(
                to_char("dt", "fmt"),
                'invalid_parameter_value',
                msg => 'to_str(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_str(td: std::duration, fmt: OPTIONAL str={}) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Return string representation of the input value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            trim(to_json("td")::text, '"')
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::text,
                'invalid_parameter_value',
                msg => 'to_str(): "fmt" argument must be a non-empty string'
            )
        ELSE
            edgedb.raise_on_null(
                to_char("td", "fmt"),
                'invalid_parameter_value',
                msg => 'to_str(): format ''' || "fmt" || ''' is invalid'
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
    CREATE ANNOTATION std::description :=
        'Return string representation of the input value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            "i"::text
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::text,
                'invalid_parameter_value',
                msg => 'to_str(): "fmt" argument must be a non-empty string'
            )
        ELSE
            edgedb.raise_on_null(
                to_char("i", "fmt"),
                'invalid_parameter_value',
                msg => 'to_str(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_str(f: std::float64, fmt: OPTIONAL str={}) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Return string representation of the input value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            "f"::text
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::text,
                'invalid_parameter_value',
                msg => 'to_str(): "fmt" argument must be a non-empty string'
            )
        ELSE
            edgedb.raise_on_null(
                to_char("f", "fmt"),
                'invalid_parameter_value',
                msg => 'to_str(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_str(d: std::bigint, fmt: OPTIONAL str={}) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Return string representation of the input value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            "d"::text
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::text,
                'invalid_parameter_value',
                msg => 'to_str(): "fmt" argument must be a non-empty string'
            )
        ELSE
            edgedb.raise_on_null(
                to_char("d", "fmt"),
                'invalid_parameter_value',
                'to_str(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_str(d: std::decimal, fmt: OPTIONAL str={}) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Return string representation of the input value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            "d"::text
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::text,
                'invalid_parameter_value',
                msg => 'to_str(): "fmt" argument must be a non-empty string'
            )
        ELSE
            edgedb.raise_on_null(
                to_char("d", "fmt"),
                'invalid_parameter_value',
                msg => 'to_str(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_str(array: array<std::str>, delimiter: std::str) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Return string representation of the input value.';
    CREATE ANNOTATION std::deprecated :=
        'This converter function is deprecated and \
         is scheduled to be removed before 1.0.\n\
         Use std::array_join() instead.';
    SET volatility := 'Immutable';
    USING (
        SELECT std::array_join(array, delimiter)
    );
};


# JSON can be prettified by specifying 'pretty' as the format, any
# other value will result in an exception.
CREATE FUNCTION
std::to_str(json: std::json, fmt: OPTIONAL str={}) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Return string representation of the input value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            "json"::text
        WHEN "fmt" = 'pretty' THEN
            jsonb_pretty("json")
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::text,
                'invalid_parameter_value',
                msg => 'to_str(): "fmt" argument must be a non-empty string'
            )
        ELSE
            edgedb.raise(
                NULL::text,
                'invalid_parameter_value',
                msg => 'to_str(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_str(b: std::bytes) -> std::str {
    CREATE ANNOTATION std::description :=
        'Convert a binary UTF-8 string to a text value.';
    SET volatility := 'Immutable';
    USING SQL $$ SELECT pg_catalog.convert_from("b", 'UTF8') $$;
};


CREATE FUNCTION
std::to_bytes(s: std::str) -> std::bytes {
    CREATE ANNOTATION std::description :=
        'Convert a text string to a binary UTF-8 string.';
    SET volatility := 'Immutable';
    USING SQL $$ SELECT pg_catalog.convert_to("s", 'UTF8') $$;
};


CREATE FUNCTION
std::to_bytes(val: std::int16) -> std::bytes
{
    CREATE ANNOTATION std::description :=
        'Convert an int16 to big-endian binary format';
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT int2send(val);
    $$;
};


CREATE FUNCTION
std::to_bytes(val: std::int32) -> std::bytes
{
    CREATE ANNOTATION std::description :=
        'Convert an int32 to big-endian binary format';
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT int4send(val);
    $$;
};


CREATE FUNCTION
std::to_bytes(val: std::int64) -> std::bytes
{
    CREATE ANNOTATION std::description :=
        'Convert an int64 to big-endian binary format';
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT int8send(val);
    $$;
};


CREATE FUNCTION
std::to_bytes(val: std::uuid) -> std::bytes
{
    CREATE ANNOTATION std::description :=
        'Convert an UUID to binary format';
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT uuid_send(val);
    $$;
};


CREATE FUNCTION
std::to_json(str: std::str) -> std::json
{
    CREATE ANNOTATION std::description :=
        'Return JSON value represented by the input *string*.';
    # Casting of jsonb to and from text in PostgreSQL is IMMUTABLE.
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT "str"::jsonb
    $$;
};


CREATE FUNCTION
std::to_datetime(s: std::str, fmt: OPTIONAL str={}) -> std::datetime
{
    CREATE ANNOTATION std::description := 'Create a `datetime` value.';
    # Helper function to_datetime is VOLATILE.
    SET volatility := 'Volatile';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb.datetime_in("s")
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::edgedb.timestamptz_t,
                'invalid_parameter_value',
                msg => (
                    'to_datetime(): "fmt" argument must be a non-empty string'
                )
            )
        ELSE
            edgedb.raise_on_null(
                edgedb.to_datetime("s", "fmt"),
                'invalid_parameter_value',
                msg => 'to_datetime(): format ''' || "fmt" || ''' is invalid'
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
    CREATE ANNOTATION std::description := 'Create a `datetime` value.';
    # make_timestamptz is STABLE
    SET volatility := 'Stable';
    USING SQL $$
    SELECT make_timestamptz(
        "year"::int, "month"::int, "day"::int,
        "hour"::int, "min"::int, "sec", "timezone"
    )::edgedb.timestamptz_t
    $$;
};


CREATE FUNCTION
std::to_datetime(epochseconds: std::float64) -> std::datetime
{
    CREATE ANNOTATION std::description := 'Create a `datetime` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT to_timestamp("epochseconds")::edgedb.timestamptz_t
    $$;
};


CREATE FUNCTION
std::to_datetime(epochseconds: std::int64) -> std::datetime
{
    CREATE ANNOTATION std::description := 'Create a `datetime` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT to_timestamp("epochseconds")::edgedb.timestamptz_t
    $$;
};


CREATE FUNCTION
std::to_datetime(epochseconds: std::decimal) -> std::datetime
{
    CREATE ANNOTATION std::description := 'Create a `datetime` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT to_timestamp("epochseconds")::edgedb.timestamptz_t
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
    CREATE ANNOTATION std::description := 'Create a `duration` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        make_interval(
            0,
            0,
            0,
            0,
            "hours"::int,
            "minutes"::int,
            "seconds"
        ) +
        (microseconds::text || ' microseconds')::interval
    )::edgedb.duration_t
    $$;
};


CREATE FUNCTION
std::to_bigint(s: std::str, fmt: OPTIONAL str={}) -> std::bigint
{
    CREATE ANNOTATION std::description := 'Create a `bigint` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb.str_to_bigint("s")
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::edgedb.bigint_t,
                'invalid_parameter_value',
                msg => (
                    'to_bigint(): "fmt" argument must be a non-empty string'
                )
            )
        ELSE
            edgedb.raise_on_null(
                to_number("s", "fmt")::edgedb.bigint_t,
                'invalid_parameter_value',
                msg => 'to_bigint(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_decimal(s: std::str, fmt: OPTIONAL str={}) -> std::decimal
{
    CREATE ANNOTATION std::description := 'Create a `decimal` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb.str_to_decimal("s")
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::numeric,
                'invalid_parameter_value',
                msg => (
                    'to_decimal(): "fmt" argument must be a non-empty string'
                )
            )
        ELSE
            edgedb.raise_on_null(
                to_number("s", "fmt")::numeric,
                'invalid_parameter_value',
                msg => 'to_decimal(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_int64(s: std::str, fmt: OPTIONAL str={}) -> std::int64
{
    CREATE ANNOTATION std::description := 'Create a `int64` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            -- Must use the noninline version to prevent
            -- the overeager function inliner from crashing
            edgedb.str_to_int64_noinline("s")
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::bigint,
                'invalid_parameter_value',
                msg => 'to_int64(): "fmt" argument must be a non-empty string'
            )
        ELSE
            edgedb.raise_on_null(
                to_number("s", "fmt")::bigint,
                'invalid_parameter_value',
                msg => 'to_int64(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_int64(val: std::bytes) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'Convert bytes into `int64` value using big-endian format.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT
        CASE WHEN (length(val) = 8) THEN
            ('x' || right(val::bytea::text, 16))::bit(64)::bigint
        ELSE
            edgedb.raise(
                0::int8,
                'invalid_parameter_value',
                msg => (
                    'to_int64(): the argument must be exactly 8 bytes long'
                )
            )
        END
    $$;
};


CREATE FUNCTION
std::to_int32(s: std::str, fmt: OPTIONAL str={}) -> std::int32
{
    CREATE ANNOTATION std::description := 'Create a `int32` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            -- Must use the noninline version to prevent
            -- the overeager function inliner from crashing
            edgedb.str_to_int32_noinline("s")
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::int,
                'invalid_parameter_value',
                msg => 'to_int32(): "fmt" argument must be a non-empty string'
            )
        ELSE
            edgedb.raise_on_null(
                to_number("s", "fmt")::int,
                'invalid_parameter_value',
                msg => 'to_int32(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_int32(val: std::bytes) -> std::int32
{
    CREATE ANNOTATION std::description :=
        'Convert bytes into `int32` value using big-endian format.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT
        CASE WHEN (length(val) = 4) THEN
            ('x' || right(val::bytea::text, 8))::bit(32)::int
        ELSE
            edgedb.raise(
                0::int4,
                'invalid_parameter_value',
                msg => (
                    'to_int32(): the argument must be exactly 4 bytes long'
                )
            )
        END
    $$;
};


CREATE FUNCTION
std::to_int16(s: std::str, fmt: OPTIONAL str={}) -> std::int16
{
    CREATE ANNOTATION std::description := 'Create a `int16` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            -- Must use the noninline version to prevent
            -- the overeager function inliner from crashing
            edgedb.str_to_int16_noinline("s")
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::smallint,
                'invalid_parameter_value',
                msg => 'to_int16(): "fmt" argument must be a non-empty string'
            )
        ELSE
            edgedb.raise_on_null(
                to_number("s", "fmt")::smallint,
                'invalid_parameter_value',
                msg => 'to_int16(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_int16(val: std::bytes) -> std::int16
{
    CREATE ANNOTATION std::description :=
        'Convert bytes into `int16` value using big-endian format.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT
        CASE WHEN (length(val) = 2) THEN
            ('x' || right(val::bytea::text, 4))::bit(16)::int::smallint
        ELSE
            edgedb.raise(
                0::int2,
                'invalid_parameter_value',
                msg => (
                    'to_int16(): the argument must be exactly 2 bytes long'
                )
            )
        END
    $$;
};


CREATE FUNCTION
std::to_float64(s: std::str, fmt: OPTIONAL str={}) -> std::float64
{
    CREATE ANNOTATION std::description := 'Create a `float64` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb.str_to_float64_noinline("s")
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::float8,
                'invalid_parameter_value',
                msg => (
                    'to_float64(): "fmt" argument must be a non-empty string'
                )
            )
        ELSE
            edgedb.raise_on_null(
                to_number("s", "fmt")::float8,
                'invalid_parameter_value',
                msg => 'to_float64(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_float32(s: std::str, fmt: OPTIONAL str={}) -> std::float32
{
    CREATE ANNOTATION std::description := 'Create a `float32` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb.str_to_float32_noinline("s")
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::float4,
                'invalid_parameter_value',
                msg => (
                    'to_float32(): "fmt" argument must be a non-empty string'
                )
            )
        ELSE
            edgedb.raise_on_null(
                to_number("s", "fmt")::float4,
                'invalid_parameter_value',
                msg => 'to_float32(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_uuid(val: std::bytes) -> std::uuid
{
    CREATE ANNOTATION std::description :=
        'Convert binary representation into UUID value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT
        CASE WHEN (length(val) = 16) THEN
            ENCODE(val, 'hex')::uuid
        ELSE
            edgedb.raise(
                NULL::uuid,
                'invalid_parameter_value',
                msg => (
                    'to_uuid(): the argument must be exactly 16 bytes long'
                )
            )
        END
    $$;
};
