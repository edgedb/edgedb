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

CREATE MODULE std::cal;

CREATE SCALAR TYPE std::cal::local_datetime
    EXTENDING std::anycontiguous;

CREATE SCALAR TYPE std::cal::local_date
    EXTENDING std::anydiscrete;

CREATE SCALAR TYPE std::cal::local_time EXTENDING std::anyscalar;

CREATE SCALAR TYPE std::cal::relative_duration EXTENDING std::anyscalar;

CREATE SCALAR TYPE std::cal::date_duration EXTENDING std::anyscalar;


## Functions
## ---------

CREATE FUNCTION
std::cal::to_local_datetime(s: std::str, fmt: OPTIONAL str={})
    -> std::cal::local_datetime
{
    CREATE ANNOTATION std::description :=
        'Create a `std::cal::local_datetime` value.';
    # Helper function to_local_datetime is VOLATILE.
    SET volatility := 'Volatile';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb_VER.local_datetime_in("s")
        WHEN "fmt" = '' THEN
            edgedb_VER.raise(
                NULL::edgedbt.timestamp_t,
                'invalid_parameter_value',
                msg => (
                    'to_local_datetime(): '
                    || '"fmt" argument must be a non-empty string'
                )
            )
        ELSE
            edgedb_VER.raise_on_null(
                edgedb_VER.to_local_datetime("s", "fmt"),
                'invalid_parameter_value',
                msg => (
                    'to_local_datetime(): '
                    || 'format ''' || "fmt" || ''' is invalid'
                )
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::cal::to_local_datetime(year: std::int64, month: std::int64, day: std::int64,
                       hour: std::int64, min: std::int64, sec: std::float64)
    -> std::cal::local_datetime
{
    CREATE ANNOTATION std::description :=
        'Create a `std::cal::local_datetime` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT make_timestamp(
        "year"::int, "month"::int, "day"::int,
        "hour"::int, "min"::int, "sec"
    )::edgedbt.timestamp_t
    $$;
};


CREATE FUNCTION
std::cal::to_local_datetime(dt: std::datetime, zone: std::str)
    -> std::cal::local_datetime
{
    CREATE ANNOTATION std::description :=
        'Create a `std::cal::local_datetime` value.';
    # The version of timezone with these arguments is IMMUTABLE.
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT timezone("zone", "dt")::edgedbt.timestamp_t;
    $$;
};


CREATE FUNCTION
std::cal::to_local_date(s: std::str, fmt: OPTIONAL str={}) -> std::cal::local_date
{
    CREATE ANNOTATION std::description := 'Create a `std::cal::local_date` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb_VER.local_date_in("s")
        WHEN "fmt" = '' THEN
            edgedb_VER.raise(
                NULL::edgedbt.date_t,
                'invalid_parameter_value',
                msg => (
                    'to_local_date(): '
                    || '"fmt" argument must be a non-empty string'
                )
            )
        ELSE
            edgedb_VER.raise_on_null(
                edgedb_VER.to_local_datetime("s", "fmt")::edgedbt.date_t,
                'invalid_parameter_value',
                msg => (
                    'to_local_date(): format ''' || "fmt" || ''' is invalid'
                )
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::cal::to_local_date(dt: std::datetime, zone: std::str)
    -> std::cal::local_date
{
    CREATE ANNOTATION std::description := 'Create a `std::cal::local_date` value.';
    # The version of timezone with these arguments is IMMUTABLE.
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT timezone("zone", "dt")::edgedbt.date_t;
    $$;
};


CREATE FUNCTION
std::cal::to_local_date(year: std::int64, month: std::int64, day: std::int64)
    -> std::cal::local_date
{
    CREATE ANNOTATION std::description := 'Create a `std::cal::local_date` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT make_date("year"::int, "month"::int, "day"::int)::edgedbt.date_t
    $$;
};


CREATE FUNCTION
std::cal::to_local_time(s: std::str, fmt: OPTIONAL str={}) -> std::cal::local_time
{
    CREATE ANNOTATION std::description := 'Create a `std::cal::local_time` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb_VER.local_time_in("s")
        WHEN "fmt" = '' THEN
            edgedb_VER.raise(
                NULL::time,
                'invalid_parameter_value',
                msg => (
                    'to_local_time(): '
                    || '"fmt" argument must be a non-empty string'
                )
            )
        ELSE
            edgedb_VER.raise_on_null(
                edgedb_VER.to_local_datetime("s", "fmt")::time,
                'invalid_parameter_value',
                msg => (
                    'to_local_time(): '
                    || 'format ''' || "fmt" || ''' is invalid'
                )
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::cal::to_local_time(dt: std::datetime, zone: std::str)
    -> std::cal::local_time
{
    CREATE ANNOTATION std::description := 'Create a `std::cal::local_time` value.';
    # The version of timezone with these arguments is IMMUTABLE and so
    # is the cast.
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT timezone("zone", "dt")::time;
    $$;
};


CREATE FUNCTION
std::cal::to_local_time(hour: std::int64, min: std::int64, sec: std::float64)
    -> std::cal::local_time
{
    CREATE ANNOTATION std::description := 'Create a `std::cal::local_time` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT
        CASE WHEN date_part('hour', x.t) = 24
        THEN
            edgedb_VER.raise(
                NULL::time,
                'invalid_datetime_format',
                msg => (
                    'std::cal::local_time field value out of range: '
                    || quote_literal(x.t::text)
                )
            )
        ELSE
            x.t
        END
    FROM (
        SELECT make_time("hour"::int, "min"::int, "sec") as t
    ) as x
    $$;
};


CREATE FUNCTION
std::cal::to_relative_duration(
        NAMED ONLY years: std::int64=0,
        NAMED ONLY months: std::int64=0,
        NAMED ONLY days: std::int64=0,
        NAMED ONLY hours: std::int64=0,
        NAMED ONLY minutes: std::int64=0,
        NAMED ONLY seconds: std::float64=0,
        NAMED ONLY microseconds: std::int64=0
    ) -> std::cal::relative_duration
{
    CREATE ANNOTATION std::description := 'Create a `std::cal::relative_duration` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        make_interval(
            "years"::int,
            "months"::int,
            0,
            "days"::int,
            "hours"::int,
            "minutes"::int,
            "seconds"
        ) +
        (microseconds::text || ' microseconds')::interval
    )::edgedbt.relative_duration_t
    $$;
};


CREATE FUNCTION
std::cal::to_date_duration(
        NAMED ONLY years: std::int64=0,
        NAMED ONLY months: std::int64=0,
        NAMED ONLY days: std::int64=0
    ) -> std::cal::date_duration
{
    CREATE ANNOTATION std::description := 'Create a `std::cal::date_duration` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT make_interval(
        "years"::int,
        "months"::int,
        0,
        "days"::int
    )::edgedbt.date_duration_t
    $$;
};


CREATE FUNCTION
std::cal::time_get(dt: std::cal::local_time, el: std::str) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Extract a specific element of input time by name.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT CASE WHEN "el" IN ('hour', 'microseconds', 'milliseconds',
            'minutes', 'seconds')
        THEN date_part("el", "dt")
        WHEN "el" = 'midnightseconds'
        THEN date_part('epoch', "dt")
        ELSE
            edgedb_VER.raise(
                NULL::float,
                'invalid_datetime_format',
                msg => (
                    'invalid unit for std::time_get: ' || quote_literal("el")
                ),
                detail => (
                    '{"hint":"Supported units: hour, microseconds, ' ||
                    'midnightseconds, milliseconds, minutes, seconds."}'
                )
            )
        END
    $$;
};


CREATE FUNCTION
std::cal::date_get(dt: std::cal::local_date, el: std::str) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Extract a specific element of input date by name.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT CASE WHEN "el" IN (
            'century', 'day', 'decade', 'dow', 'doy',
            'isodow', 'isoyear', 'millennium',
            'month', 'quarter', 'week', 'year')
        THEN date_part("el", "dt")
        ELSE
            edgedb_VER.raise(
                NULL::float,
                'invalid_datetime_format',
                msg => (
                    'invalid unit for std::date_get: ' || quote_literal("el")
                ),
                detail => (
                    '{"hint":"Supported units: century, day, ' ||
                    'decade, dow, doy, isodow, isoyear, ' ||
                    'millennium, month, quarter, seconds, week, year."}'
                )
            )
        END
    $$;
};


CREATE FUNCTION
std::cal::duration_normalize_hours(dur: std::cal::relative_duration)
  -> std::cal::relative_duration
{
    CREATE ANNOTATION std::description :=
        'Convert 24-hour chunks into days.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL FUNCTION 'justify_hours';
};


CREATE FUNCTION
std::cal::duration_normalize_days(dur: std::cal::relative_duration)
  -> std::cal::relative_duration
{
    CREATE ANNOTATION std::description :=
        'Convert 30-day chunks into months.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL FUNCTION 'justify_days';
};


CREATE FUNCTION
std::cal::duration_normalize_days(dur: std::cal::date_duration)
  -> std::cal::date_duration
{
    CREATE ANNOTATION std::description :=
        'Convert 30-day chunks into months.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL FUNCTION 'justify_days';
};



## Operators on std::datetime
## --------------------------

CREATE INFIX OPERATOR
std::`+` (l: std::datetime, r: std::cal::relative_duration) -> std::datetime {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    # Immutable because datetime is guaranteed to be in UTC and no DST issues
    # should affect this.
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedbt.timestamptz_t
    $$
};


CREATE INFIX OPERATOR
std::`+` (l: std::cal::relative_duration, r: std::datetime) -> std::datetime {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    # Immutable because datetime is guaranteed to be in UTC and no DST issues
    # should affect this.
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedbt.timestamptz_t
    $$
};


CREATE INFIX OPERATOR
std::`-` (l: std::datetime, r: std::cal::relative_duration) -> std::datetime {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time subtraction.';
    # Immutable because datetime is guaranteed to be in UTC and no DST issues
    # should affect this.
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT ("l" - "r")::edgedbt.timestamptz_t
    $$
};


## Operators on std::cal::local_datetime
## --------------------------------

CREATE INFIX OPERATOR
std::`=` (l: std::cal::local_datetime, r: std::cal::local_datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::cal::local_datetime,
           r: OPTIONAL std::cal::local_datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::cal::local_datetime, r: std::cal::local_datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::cal::local_datetime,
            r: OPTIONAL std::cal::local_datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: std::cal::local_datetime, r: std::cal::local_datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::cal::local_datetime, r: std::cal::local_datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`<` (l: std::cal::local_datetime, r: std::cal::local_datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::cal::local_datetime, r: std::cal::local_datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`+` (l: std::cal::local_datetime, r: std::duration) -> std::cal::local_datetime {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedbt.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: std::cal::local_datetime) -> std::cal::local_datetime {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedbt.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: std::cal::local_datetime, r: std::duration) -> std::cal::local_datetime {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time subtraction.';
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT ("l" - "r")::edgedbt.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: std::cal::local_datetime, r: std::cal::relative_duration) -> std::cal::local_datetime {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedbt.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: std::cal::relative_duration, r: std::cal::local_datetime) -> std::cal::local_datetime {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedbt.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: std::cal::local_datetime, r: std::cal::relative_duration) -> std::cal::local_datetime {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time subtraction.';
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT ("l" - "r")::edgedbt.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: std::cal::local_datetime, r: std::cal::local_datetime) -> std::cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Date/time subtraction.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL OPERATOR r'-(timestamp, timestamp)';
};


## Operators on std::cal::local_date
## ----------------------------

CREATE INFIX OPERATOR
std::`=` (l: std::cal::local_date, r: std::cal::local_date) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=(date,date)';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::cal::local_date,
           r: OPTIONAL std::cal::local_date) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::cal::local_date, r: std::cal::local_date) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>(date,date)';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::cal::local_date,
            r: OPTIONAL std::cal::local_date) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: std::cal::local_date, r: std::cal::local_date) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>(date,date)';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::cal::local_date, r: std::cal::local_date) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=(date,date)';
};


CREATE INFIX OPERATOR
std::`<` (l: std::cal::local_date, r: std::cal::local_date) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<(date,date)';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::cal::local_date, r: std::cal::local_date) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=(date,date)';
};


CREATE INFIX OPERATOR
std::`+` (l: std::cal::local_date, r: std::duration) -> std::cal::local_datetime
{
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" + "r")::edgedbt.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: std::cal::local_date) -> std::cal::local_datetime
{
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" + "r")::edgedbt.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: std::cal::local_date, r: std::duration) -> std::cal::local_datetime
{
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time subtraction.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" - "r")::edgedbt.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: std::cal::local_date, r: std::cal::relative_duration) -> std::cal::local_datetime
{
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" + "r")::edgedbt.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: std::cal::relative_duration, r: std::cal::local_date) -> std::cal::local_datetime
{
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" + "r")::edgedbt.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: std::cal::local_date, r: std::cal::relative_duration) -> std::cal::local_datetime
{
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time subtraction.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" - "r")::edgedbt.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: std::cal::local_date, r: std::cal::date_duration) -> std::cal::local_date
{
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" + "r")::edgedbt.date_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: std::cal::date_duration, r: std::cal::local_date) -> std::cal::local_date
{
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" + "r")::edgedbt.date_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: std::cal::local_date, r: std::cal::date_duration) -> std::cal::local_date
{
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time subtraction.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" - "r")::edgedbt.date_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: std::cal::local_date, r: std::cal::local_date) -> std::cal::date_duration
{
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Date subtraction.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL $$
        SELECT make_interval(0, 0, 0, "l" - "r")::edgedbt.date_duration_t
    $$;
};


## Operators on std::cal::local_time
## ----------------------------

CREATE INFIX OPERATOR
std::`=` (l: std::cal::local_time, r: std::cal::local_time) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::cal::local_time,
           r: OPTIONAL std::cal::local_time) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::cal::local_time, r: std::cal::local_time) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::cal::local_time,
            r: OPTIONAL std::cal::local_time) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: std::cal::local_time, r: std::cal::local_time) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::cal::local_time, r: std::cal::local_time) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`<` (l: std::cal::local_time, r: std::cal::local_time) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::cal::local_time, r: std::cal::local_time) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`+` (l: std::cal::local_time, r: std::duration) -> std::cal::local_time {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+(time, interval)';
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: std::cal::local_time) -> std::cal::local_time {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+(interval, time)';
};


CREATE INFIX OPERATOR
std::`-` (l: std::cal::local_time, r: std::duration) -> std::cal::local_time {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time subtraction.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-(time, interval)';
};


CREATE INFIX OPERATOR
std::`+` (l: std::cal::local_time, r: std::cal::relative_duration) -> std::cal::local_time {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+(time, interval)';
};


CREATE INFIX OPERATOR
std::`+` (l: std::cal::relative_duration, r: std::cal::local_time) -> std::cal::local_time {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+(interval, time)';
};


CREATE INFIX OPERATOR
std::`-` (l: std::cal::local_time, r: std::cal::relative_duration) -> std::cal::local_time {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time subtraction.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-(time, interval)';
};


CREATE INFIX OPERATOR
std::`-` (l: std::cal::local_time, r: std::cal::local_time) -> std::cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Time subtraction.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL OPERATOR r'-(time, time)';
};


## Operators on std::cal::relative_duration
## ----------------------------

CREATE INFIX OPERATOR
std::`=` (l: std::cal::relative_duration, r: std::cal::relative_duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=(interval,interval)';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::cal::relative_duration,
           r: OPTIONAL std::cal::relative_duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::cal::relative_duration, r: std::cal::relative_duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>(interval,interval)';
};


CREATE INFIX OPERATOR
std::`?!=` (
        l: OPTIONAL std::cal::relative_duration,
        r: OPTIONAL std::cal::relative_duration
) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: std::cal::relative_duration, r: std::cal::relative_duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>(interval,interval)';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::cal::relative_duration, r: std::cal::relative_duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=(interval,interval)';
};


CREATE INFIX OPERATOR
std::`<` (l: std::cal::relative_duration, r: std::cal::relative_duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<(interval,interval)';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::cal::relative_duration, r: std::cal::relative_duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=(interval,interval)';
};


CREATE INFIX OPERATOR
std::`+` (l: std::cal::relative_duration, r: std::cal::relative_duration) -> std::cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
    SELECT ("l"::interval + "r"::interval)::edgedbt.relative_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: std::cal::relative_duration, r: std::cal::relative_duration) -> std::cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval subtraction.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT ("l"::interval - "r"::interval)::edgedbt.relative_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: std::cal::date_duration, r: std::cal::date_duration) -> std::cal::date_duration {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
    SELECT ("l" + "r")::edgedbt.date_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: std::cal::date_duration, r: std::cal::date_duration) -> std::cal::date_duration {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval subtraction.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT ("l" - "r")::edgedbt.date_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: std::cal::relative_duration) -> std::cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
    SELECT ("l"::interval + "r"::interval)::edgedbt.relative_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: std::cal::relative_duration, r: std::duration) -> std::cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
    SELECT ("l"::interval + "r"::interval)::edgedbt.relative_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: std::duration, r: std::cal::relative_duration) -> std::cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval subtraction.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT ("l"::interval - "r"::interval)::edgedbt.relative_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: std::cal::relative_duration, r: std::duration) -> std::cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval subtraction.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT ("l"::interval - "r"::interval)::edgedbt.relative_duration_t;
    $$;
};


CREATE PREFIX OPERATOR
std::`-` (v: std::cal::relative_duration) -> std::cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval negation.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (-"v"::interval)::edgedbt.relative_duration_t;
    $$;
};


## Date/time casts
## ---------------

CREATE CAST FROM std::cal::local_datetime TO std::cal::local_date {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::cal::local_datetime TO std::cal::local_time {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::cal::local_date TO std::cal::local_datetime {
    SET volatility := 'Immutable';
    USING SQL CAST;
    # Analogous to implicit cast from int64 to float64.
    ALLOW IMPLICIT;
};


CREATE CAST FROM std::str TO std::cal::local_datetime {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'edgedb.local_datetime_in';
};


CREATE CAST FROM std::str TO std::cal::local_date {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'edgedb.local_date_in';
};


CREATE CAST FROM std::str TO std::cal::local_time {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'edgedb.local_time_in';
};


CREATE CAST FROM std::str TO std::cal::relative_duration {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT val::edgedbt.relative_duration_t;
    $$;
};


CREATE CAST FROM std::str TO std::cal::date_duration {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'edgedb.date_duration_in';
};


CREATE CAST FROM std::cal::local_datetime TO std::str {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT trim(to_json(val)::text, '"');
    $$;
};

CREATE CAST FROM std::cal::local_date TO std::str {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::cal::local_time TO std::str {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::cal::relative_duration TO std::str {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::cal::date_duration TO std::str {
    SET volatility := 'Immutable';
    # We want the 0 date_duration canonically represented be in lowest
    # date_duration units, i.e. in days.
    USING SQL $$
    SELECT CASE WHEN (val::text = 'PT0S')
        THEN 'P0D'
        ELSE val::text
    END
    $$;
};


CREATE CAST FROM std::cal::local_datetime TO std::json {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::cal::local_date TO std::json {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::cal::local_time TO std::json {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::cal::relative_duration TO std::json {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::cal::date_duration TO std::json {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'to_jsonb';
    # We want the 0 date_duration canonically represented be in lowest
    # date_duration units, i.e. in days.
    USING SQL $$
    SELECT CASE WHEN (val::text = 'PT0S')
        THEN to_jsonb('P0D'::text)
        ELSE to_jsonb(val)
    END
    $$;
};


CREATE CAST FROM std::json TO std::cal::local_datetime {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT edgedb_VER.local_datetime_in(
        edgedb_VER.jsonb_extract_scalar(val, 'string', detail => detail)
    );
    $$;
};


CREATE CAST FROM std::json TO std::cal::local_date {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT edgedb_VER.local_date_in(
        edgedb_VER.jsonb_extract_scalar(val, 'string', detail => detail)
    );
    $$;
};


CREATE CAST FROM std::json TO std::cal::local_time {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT edgedb_VER.local_time_in(
        edgedb_VER.jsonb_extract_scalar(val, 'string', detail => detail)
    );
    $$;
};


CREATE CAST FROM std::json TO std::cal::relative_duration {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT edgedb_VER.jsonb_extract_scalar(
        val, 'string', detail => detail
    )::interval::edgedbt.relative_duration_t;
    $$;
};


CREATE CAST FROM std::json TO std::cal::date_duration {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT edgedb_VER.date_duration_in(
        edgedb_VER.jsonb_extract_scalar(val, 'string', detail => detail)
    );
    $$;
};


CREATE CAST FROM std::duration TO std::cal::relative_duration {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::cal::relative_duration TO std::duration {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::cal::date_duration TO std::cal::relative_duration {
    # Same underlying types that don't require any DST calculations to convert
    # into eachother.
    SET volatility := 'Immutable';
    USING SQL CAST;
    # Analogous to implicit cast from int64 to float64.
    ALLOW IMPLICIT;
};


CREATE CAST FROM std::cal::relative_duration TO std::cal::date_duration {
    # Same underlying types that don't require any DST calculations to convert
    # into eachother.
    SET volatility := 'Immutable';
    USING SQL CAST;
};


## Modified functions
## ------------------

CREATE FUNCTION
std::datetime_get(dt: std::cal::local_datetime, el: std::str) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Extract a specific element of input datetime by name.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT CASE WHEN "el" IN (
            'century', 'day', 'decade', 'dow', 'doy', 'hour',
            'isodow', 'isoyear', 'microseconds', 'millennium',
            'milliseconds', 'minutes', 'month', 'quarter',
            'seconds', 'week', 'year')
        THEN date_part("el", "dt")
        WHEN "el" = 'epochseconds'
        THEN date_part('epoch', "dt")
        ELSE
            edgedb_VER.raise(
                NULL::float,
                'invalid_datetime_format',
                msg => (
                    'invalid unit for std::datetime_get: '
                    || quote_literal("el")
                ),
                detail => (
                    '{"hint":"Supported units: epochseconds, century, '
                    || 'day, decade, dow, doy, hour, isodow, isoyear, '
                    || 'microseconds, millennium, milliseconds, minutes, '
                    || 'month, quarter, seconds, week, year."}'
                )
            )
        END
    $$;
};


CREATE FUNCTION
std::duration_get(dt: std::cal::date_duration, el: std::str) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Extract a specific element of input duration by name.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT CASE WHEN "el" IN (
            'millennium', 'century', 'decade', 'year', 'quarter', 'month',
            'day')
        THEN date_part("el", "dt")
        WHEN "el" = 'totalseconds'
        THEN date_part('epoch', "dt")
        ELSE
            edgedb_VER.raise(
                NULL::float,
                'invalid_datetime_format',
                msg => (
                    'invalid unit for std::duration_get: '
                    || quote_literal("el")
                ),
                detail => (
                    '{"hint":"Supported units: '
                    || 'millennium, century, decade, year, quarter, month, day, '
                    || 'hour, and totalseconds."}'
                )
            )
        END
    $$;
};


CREATE FUNCTION
std::duration_get(dt: std::cal::relative_duration, el: std::str) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Extract a specific element of input duration by name.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT CASE WHEN "el" IN (
            'millennium', 'century', 'decade', 'year', 'quarter', 'month',
            'day', 'hour', 'minutes', 'seconds', 'milliseconds',
            'microseconds')
        THEN date_part("el", "dt")
        WHEN "el" = 'totalseconds'
        THEN date_part('epoch', "dt")
        ELSE
            edgedb_VER.raise(
                NULL::float,
                'invalid_datetime_format',
                msg => (
                    'invalid unit for std::duration_get: '
                    || quote_literal("el")
                ),
                detail => (
                    '{"hint":"Supported units: '
                    || 'millennium, century, decade, year, quarter, month, day, '
                    || 'hour, minutes, seconds, milliseconds, microseconds, '
                    || 'and totalseconds."}'
                )
            )
        END
    $$;
};


CREATE FUNCTION
std::duration_truncate(
    dt: std::cal::date_duration,
    unit: std::str
) -> std::cal::date_duration
{
    CREATE ANNOTATION std::description :=
        'Truncate the input duration to a particular precision.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT CASE WHEN "unit" IN (
            'days', 'weeks', 'months', 'years', 'decades', 'centuries')
        THEN date_trunc("unit", "dt")::edgedbt.relative_duration_t
        WHEN "unit" = 'quarters'
        THEN date_trunc('quarter', "dt")::edgedbt.relative_duration_t
        ELSE
            edgedb_VER.raise(
                NULL::edgedbt.relative_duration_t,
                'invalid_datetime_format',
                msg => (
                    'invalid unit for std::duration_truncate: '
                    || quote_literal("unit")
                ),
                detail => (
                    '{"hint":"Supported units: days, weeks, months, '
                    || 'quarters, years, decades, centuries."}'
                )
            )
        END
    $$;
};


CREATE FUNCTION
std::duration_truncate(
    dt: std::cal::relative_duration,
    unit: std::str
) -> std::cal::relative_duration
{
    CREATE ANNOTATION std::description :=
        'Truncate the input duration to a particular precision.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT CASE WHEN "unit" IN (
            'microseconds', 'milliseconds', 'seconds',
            'minutes', 'hours', 'days', 'weeks', 'months',
            'years', 'decades', 'centuries')
        THEN date_trunc("unit", "dt")::edgedbt.relative_duration_t
        WHEN "unit" = 'quarters'
        THEN date_trunc('quarter', "dt")::edgedbt.relative_duration_t
        ELSE
            edgedb_VER.raise(
                NULL::edgedbt.relative_duration_t,
                'invalid_datetime_format',
                msg => (
                    'invalid unit for std::duration_truncate: '
                    || quote_literal("unit")
                ),
                detail => (
                    '{"hint":"Supported units: microseconds, milliseconds, '
                    || 'seconds, minutes, hours, days, weeks, months, '
                    || 'quarters, years, decades, centuries."}'
                )
            )
        END
    $$;
};


CREATE FUNCTION
std::to_str(dt: std::cal::local_datetime, fmt: OPTIONAL str={}) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Return string representation of the input value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            trim(to_json("dt")::text, '"')
        WHEN "fmt" = '' THEN
            edgedb_VER.raise(
                NULL::text,
                'invalid_parameter_value',
                msg => 'to_str(): "fmt" argument must be a non-empty string'
            )
        ELSE
            edgedb_VER.raise_on_null(
                to_char("dt", "fmt"),
                'invalid_parameter_value',
                msg => 'to_str(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_str(d: std::cal::local_date, fmt: OPTIONAL str={}) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Return string representation of the input value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            "d"::text
        WHEN "fmt" = '' THEN
            edgedb_VER.raise(
                NULL::text,
                'invalid_parameter_value',
                msg => 'to_str(): "fmt" argument must be a non-empty string'
            )
        ELSE
            edgedb_VER.raise_on_null(
                to_char("d", "fmt"),
                'invalid_parameter_value',
                msg => 'to_str(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


# Currently local time is formatted by composing it with the local
# current local date. This at least guarantees that the time
# formatting is accessible and consistent with full datetime
# formatting, but it exposes current date as well if it is included in
# the format.
# FIXME: date formatting should not have any special effect.
CREATE FUNCTION
std::to_str(nt: std::cal::local_time, fmt: OPTIONAL str={}) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Return string representation of the input value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            "nt"::text
        WHEN "fmt" = '' THEN
            edgedb_VER.raise(
                NULL::text,
                'invalid_parameter_value',
                msg => 'to_str(): "fmt" argument must be a non-empty string'
            )
        ELSE
            edgedb_VER.raise_on_null(
                to_char(date_trunc('day', localtimestamp) + "nt", "fmt"),
                'invalid_parameter_value',
                msg => 'to_str(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_str(rd: std::cal::relative_duration, fmt: OPTIONAL str={}) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Return string representation of the input value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            "rd"::text
        WHEN "fmt" = '' THEN
            edgedb_VER.raise(
                NULL::text,
                'invalid_parameter_value',
                msg => 'to_str(): "fmt" argument must be a non-empty string'
            )
        ELSE
            edgedb_VER.raise_on_null(
                to_char("rd", "fmt"),
                'invalid_parameter_value',
                msg => 'to_str(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_datetime(local: std::cal::local_datetime, zone: std::str)
    -> std::datetime
{
    CREATE ANNOTATION std::description := 'Create a `datetime` value.';
    # The version of timezone with these arguments is IMMUTABLE.
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT timezone("zone", "local")::edgedbt.timestamptz_t;
    $$;
};


CREATE FUNCTION
std::min(vals: SET OF std::cal::local_datetime) -> OPTIONAL std::cal::local_datetime
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF std::cal::local_date) -> OPTIONAL std::cal::local_date
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF std::cal::local_time) -> OPTIONAL std::cal::local_time
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF std::cal::relative_duration) -> OPTIONAL std::cal::relative_duration
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF std::cal::date_duration) -> OPTIONAL std::cal::date_duration
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF array<std::cal::local_datetime>) -> OPTIONAL array<std::cal::local_datetime>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF array<std::cal::local_date>) -> OPTIONAL array<std::cal::local_date>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF array<std::cal::local_time>) -> OPTIONAL array<std::cal::local_time>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF array<std::cal::relative_duration>) -> OPTIONAL array<std::cal::relative_duration>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF array<std::cal::date_duration>) -> OPTIONAL array<std::cal::date_duration>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::max(vals: SET OF std::cal::local_datetime) -> OPTIONAL std::cal::local_datetime
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF std::cal::local_date) -> OPTIONAL std::cal::local_date
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF std::cal::local_time) -> OPTIONAL std::cal::local_time
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF std::cal::relative_duration) -> OPTIONAL std::cal::relative_duration
{
    CREATE ANNOTATION std::description :=
        'Return the greatest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF std::cal::date_duration) -> OPTIONAL std::cal::date_duration
{
    CREATE ANNOTATION std::description :=
        'Return the greatest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF array<std::cal::local_datetime>) -> OPTIONAL array<std::cal::local_datetime>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF array<std::cal::local_date>) -> OPTIONAL array<std::cal::local_date>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF array<std::cal::local_time>) -> OPTIONAL array<std::cal::local_time>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF array<std::cal::relative_duration>) -> OPTIONAL array<std::cal::relative_duration>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF array<std::cal::date_duration>) -> OPTIONAL array<std::cal::date_duration>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


## Range functions


# FIXME: These functions introduce the concrete multirange types into the
# schema. That's why they exist for each concrete type explicitly and aren't
# defined generically for anytype.
CREATE FUNCTION std::multirange_unpack(
    val: multirange<std::cal::local_datetime>,
) -> set of range<std::cal::local_datetime>
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'unnest';
};


CREATE FUNCTION std::multirange_unpack(
    val: multirange<std::cal::local_date>,
) -> set of range<std::cal::local_date>
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'unnest';
};


CREATE FUNCTION
std::range_unpack(
    val: range<std::cal::local_datetime>,
    step: std::cal::relative_duration
) -> set of std::cal::local_datetime
{
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT d::edgedbt.timestamp_t
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
            upper_inc(val) OR d::edgedbt.timestamp_t < upper(val)
    $$;
};


CREATE FUNCTION
std::range_unpack(
    val: range<std::cal::local_date>
) -> set of std::cal::local_date
{
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT
            generate_series(
                (
                    edgedb_VER.range_lower_validate(val) + (
                        CASE WHEN lower_inc(val)
                            THEN '0'::interval
                            ELSE 'P1D'::interval
                        END
                    )
                )::timestamp,
                (
                    edgedb_VER.range_upper_validate(val) - (
                        CASE WHEN upper_inc(val)
                            THEN '0'::interval
                            ELSE 'P1D'::interval
                        END
                    )
                )::timestamp,
                'P1D'::interval
            )::edgedbt.date_t
    $$;
};


CREATE FUNCTION
std::range_unpack(
    val: range<std::cal::local_date>,
    step: std::cal::date_duration
) -> set of std::cal::local_date
{
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT
            generate_series(
                (
                    edgedb_VER.range_lower_validate(val) + (
                        CASE WHEN lower_inc(val)
                            THEN '0'::interval
                            ELSE 'P1D'::interval
                        END
                    )
                )::timestamp,
                (
                    edgedb_VER.range_upper_validate(val) - (
                        CASE WHEN upper_inc(val)
                            THEN '0'::interval
                            ELSE 'P1D'::interval
                        END
                    )
                )::timestamp,
                step::interval
            )::edgedbt.date_t
    $$;
};

# Need to cast edgedbt.date_t to date in order for the @> operator to work.
CREATE FUNCTION std::contains(
    haystack: range<std::cal::local_date>,
    needle: std::cal::local_date
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "haystack" @> ("needle"::date)
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};


CREATE FUNCTION std::contains(
    haystack: multirange<std::cal::local_date>,
    needle: std::cal::local_date
) -> std::bool
{
    SET volatility := 'Immutable';
    USING SQL $$
       SELECT "haystack" @> ("needle"::date)
    $$;
    # Needed to pick up the indexes when used in FILTER.
    set prefer_subquery_args := true;
    set impl_is_strict := false;
};