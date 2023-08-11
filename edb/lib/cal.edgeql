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

CREATE MODULE cal;

CREATE SCALAR TYPE cal::local_datetime
    EXTENDING std::anycontiguous;

CREATE SCALAR TYPE cal::local_date
    EXTENDING std::anydiscrete;

CREATE SCALAR TYPE cal::local_time EXTENDING std::anyscalar;

CREATE SCALAR TYPE cal::relative_duration EXTENDING std::anyscalar;

CREATE SCALAR TYPE cal::date_duration EXTENDING std::anyscalar;


## Functions
## ---------

CREATE FUNCTION
cal::to_local_datetime(s: std::str, fmt: OPTIONAL str={})
    -> cal::local_datetime
{
    CREATE ANNOTATION std::description :=
        'Create a `cal::local_datetime` value.';
    # Helper function to_local_datetime is VOLATILE.
    SET volatility := 'Volatile';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb.local_datetime_in("s")
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::edgedb.timestamp_t,
                'invalid_parameter_value',
                msg => (
                    'to_local_datetime(): '
                    || '"fmt" argument must be a non-empty string'
                )
            )
        ELSE
            edgedb.raise_on_null(
                edgedb.to_local_datetime("s", "fmt"),
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
cal::to_local_datetime(year: std::int64, month: std::int64, day: std::int64,
                       hour: std::int64, min: std::int64, sec: std::float64)
    -> cal::local_datetime
{
    CREATE ANNOTATION std::description :=
        'Create a `cal::local_datetime` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT make_timestamp(
        "year"::int, "month"::int, "day"::int,
        "hour"::int, "min"::int, "sec"
    )::edgedb.timestamp_t
    $$;
};


CREATE FUNCTION
cal::to_local_datetime(dt: std::datetime, zone: std::str)
    -> cal::local_datetime
{
    CREATE ANNOTATION std::description :=
        'Create a `cal::local_datetime` value.';
    # The version of timezone with these arguments is IMMUTABLE.
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT timezone("zone", "dt")::edgedb.timestamp_t;
    $$;
};


CREATE FUNCTION
cal::to_local_date(s: std::str, fmt: OPTIONAL str={}) -> cal::local_date
{
    CREATE ANNOTATION std::description := 'Create a `cal::local_date` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb.local_date_in("s")
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::edgedb.date_t,
                'invalid_parameter_value',
                msg => (
                    'to_local_date(): '
                    || '"fmt" argument must be a non-empty string'
                )
            )
        ELSE
            edgedb.raise_on_null(
                edgedb.to_local_datetime("s", "fmt")::edgedb.date_t,
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
cal::to_local_date(dt: std::datetime, zone: std::str)
    -> cal::local_date
{
    CREATE ANNOTATION std::description := 'Create a `cal::local_date` value.';
    # The version of timezone with these arguments is IMMUTABLE.
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT timezone("zone", "dt")::edgedb.date_t;
    $$;
};


CREATE FUNCTION
cal::to_local_date(year: std::int64, month: std::int64, day: std::int64)
    -> cal::local_date
{
    CREATE ANNOTATION std::description := 'Create a `cal::local_date` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT make_date("year"::int, "month"::int, "day"::int)::edgedb.date_t
    $$;
};


CREATE FUNCTION
cal::to_local_time(s: std::str, fmt: OPTIONAL str={}) -> cal::local_time
{
    CREATE ANNOTATION std::description := 'Create a `cal::local_time` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb.local_time_in("s")
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::time,
                'invalid_parameter_value',
                msg => (
                    'to_local_time(): '
                    || '"fmt" argument must be a non-empty string'
                )
            )
        ELSE
            edgedb.raise_on_null(
                edgedb.to_local_datetime("s", "fmt")::time,
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
cal::to_local_time(dt: std::datetime, zone: std::str)
    -> cal::local_time
{
    CREATE ANNOTATION std::description := 'Create a `cal::local_time` value.';
    # The version of timezone with these arguments is IMMUTABLE and so
    # is the cast.
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT timezone("zone", "dt")::time;
    $$;
};


CREATE FUNCTION
cal::to_local_time(hour: std::int64, min: std::int64, sec: std::float64)
    -> cal::local_time
{
    CREATE ANNOTATION std::description := 'Create a `cal::local_time` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT
        CASE WHEN date_part('hour', x.t) = 24
        THEN
            edgedb.raise(
                NULL::time,
                'invalid_datetime_format',
                msg => (
                    'cal::local_time field value out of range: '
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
cal::to_relative_duration(
        NAMED ONLY years: std::int64=0,
        NAMED ONLY months: std::int64=0,
        NAMED ONLY days: std::int64=0,
        NAMED ONLY hours: std::int64=0,
        NAMED ONLY minutes: std::int64=0,
        NAMED ONLY seconds: std::float64=0,
        NAMED ONLY microseconds: std::int64=0
    ) -> cal::relative_duration
{
    CREATE ANNOTATION std::description := 'Create a `cal::relative_duration` value.';
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
    )::edgedb.relative_duration_t
    $$;
};


CREATE FUNCTION
cal::to_date_duration(
        NAMED ONLY years: std::int64=0,
        NAMED ONLY months: std::int64=0,
        NAMED ONLY days: std::int64=0
    ) -> cal::date_duration
{
    CREATE ANNOTATION std::description := 'Create a `cal::date_duration` value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT make_interval(
        "years"::int,
        "months"::int,
        0,
        "days"::int
    )::edgedb.date_duration_t
    $$;
};


CREATE FUNCTION
cal::time_get(dt: cal::local_time, el: std::str) -> std::float64
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
            edgedb.raise(
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
cal::date_get(dt: cal::local_date, el: std::str) -> std::float64
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
            edgedb.raise(
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
cal::duration_normalize_hours(dur: cal::relative_duration)
  -> cal::relative_duration
{
    CREATE ANNOTATION std::description :=
        'Convert 24-hour chunks into days.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL FUNCTION 'justify_hours';
};


CREATE FUNCTION
cal::duration_normalize_days(dur: cal::relative_duration)
  -> cal::relative_duration
{
    CREATE ANNOTATION std::description :=
        'Convert 30-day chunks into months.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL FUNCTION 'justify_days';
};


CREATE FUNCTION
cal::duration_normalize_days(dur: cal::date_duration)
  -> cal::date_duration
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
std::`+` (l: std::datetime, r: cal::relative_duration) -> std::datetime {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    # Immutable because datetime is guaranteed to be in UTC and no DST issues
    # should affect this.
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedb.timestamptz_t
    $$
};


CREATE INFIX OPERATOR
std::`+` (l: cal::relative_duration, r: std::datetime) -> std::datetime {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    # Immutable because datetime is guaranteed to be in UTC and no DST issues
    # should affect this.
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedb.timestamptz_t
    $$
};


CREATE INFIX OPERATOR
std::`-` (l: std::datetime, r: cal::relative_duration) -> std::datetime {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time subtraction.';
    # Immutable because datetime is guaranteed to be in UTC and no DST issues
    # should affect this.
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT ("l" - "r")::edgedb.timestamptz_t
    $$
};


## Operators on cal::local_datetime
## --------------------------------

CREATE INFIX OPERATOR
std::`=` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL cal::local_datetime,
           r: OPTIONAL cal::local_datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL cal::local_datetime,
            r: OPTIONAL cal::local_datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`>=` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`<` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`<=` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`+` (l: cal::local_datetime, r: std::duration) -> cal::local_datetime {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedb.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: cal::local_datetime) -> cal::local_datetime {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedb.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_datetime, r: std::duration) -> cal::local_datetime {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time subtraction.';
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT ("l" - "r")::edgedb.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: cal::local_datetime, r: cal::relative_duration) -> cal::local_datetime {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedb.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: cal::relative_duration, r: cal::local_datetime) -> cal::local_datetime {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedb.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_datetime, r: cal::relative_duration) -> cal::local_datetime {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time subtraction.';
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT ("l" - "r")::edgedb.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_datetime, r: cal::local_datetime) -> cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Date/time subtraction.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL OPERATOR r'-(timestamp, timestamp)';
};


## Operators on cal::local_date
## ----------------------------

CREATE INFIX OPERATOR
std::`=` (l: cal::local_date, r: cal::local_date) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=(date,date)';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL cal::local_date,
           r: OPTIONAL cal::local_date) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: cal::local_date, r: cal::local_date) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>(date,date)';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL cal::local_date,
            r: OPTIONAL cal::local_date) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: cal::local_date, r: cal::local_date) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>(date,date)';
};


CREATE INFIX OPERATOR
std::`>=` (l: cal::local_date, r: cal::local_date) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=(date,date)';
};


CREATE INFIX OPERATOR
std::`<` (l: cal::local_date, r: cal::local_date) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<(date,date)';
};


CREATE INFIX OPERATOR
std::`<=` (l: cal::local_date, r: cal::local_date) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=(date,date)';
};


CREATE INFIX OPERATOR
std::`+` (l: cal::local_date, r: std::duration) -> cal::local_datetime
{
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" + "r")::edgedb.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: cal::local_date) -> cal::local_datetime
{
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" + "r")::edgedb.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_date, r: std::duration) -> cal::local_datetime
{
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time subtraction.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" - "r")::edgedb.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: cal::local_date, r: cal::relative_duration) -> cal::local_datetime
{
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" + "r")::edgedb.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: cal::relative_duration, r: cal::local_date) -> cal::local_datetime
{
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" + "r")::edgedb.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_date, r: cal::relative_duration) -> cal::local_datetime
{
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time subtraction.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" - "r")::edgedb.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: cal::local_date, r: cal::date_duration) -> cal::local_date
{
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" + "r")::edgedb.date_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: cal::date_duration, r: cal::local_date) -> cal::local_date
{
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" + "r")::edgedb.date_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_date, r: cal::date_duration) -> cal::local_date
{
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time subtraction.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" - "r")::edgedb.date_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_date, r: cal::local_date) -> cal::date_duration
{
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Date subtraction.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL $$
        SELECT make_interval(0, 0, 0, "l" - "r")::edgedb.date_duration_t
    $$;
};


## Operators on cal::local_time
## ----------------------------

CREATE INFIX OPERATOR
std::`=` (l: cal::local_time, r: cal::local_time) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL cal::local_time,
           r: OPTIONAL cal::local_time) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: cal::local_time, r: cal::local_time) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL cal::local_time,
            r: OPTIONAL cal::local_time) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: cal::local_time, r: cal::local_time) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>=` (l: cal::local_time, r: cal::local_time) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`<` (l: cal::local_time, r: cal::local_time) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<=` (l: cal::local_time, r: cal::local_time) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`+` (l: cal::local_time, r: std::duration) -> cal::local_time {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+(time, interval)';
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: cal::local_time) -> cal::local_time {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+(interval, time)';
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_time, r: std::duration) -> cal::local_time {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time subtraction.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-(time, interval)';
};


CREATE INFIX OPERATOR
std::`+` (l: cal::local_time, r: cal::relative_duration) -> cal::local_time {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+(time, interval)';
};


CREATE INFIX OPERATOR
std::`+` (l: cal::relative_duration, r: cal::local_time) -> cal::local_time {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+(interval, time)';
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_time, r: cal::relative_duration) -> cal::local_time {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval and date/time subtraction.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-(time, interval)';
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_time, r: cal::local_time) -> cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Time subtraction.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL OPERATOR r'-(time, time)';
};


## Operators on cal::relative_duration
## ----------------------------

CREATE INFIX OPERATOR
std::`=` (l: cal::relative_duration, r: cal::relative_duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=(interval,interval)';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL cal::relative_duration,
           r: OPTIONAL cal::relative_duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: cal::relative_duration, r: cal::relative_duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>(interval,interval)';
};


CREATE INFIX OPERATOR
std::`?!=` (
        l: OPTIONAL cal::relative_duration,
        r: OPTIONAL cal::relative_duration
) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: cal::relative_duration, r: cal::relative_duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>(interval,interval)';
};


CREATE INFIX OPERATOR
std::`>=` (l: cal::relative_duration, r: cal::relative_duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=(interval,interval)';
};


CREATE INFIX OPERATOR
std::`<` (l: cal::relative_duration, r: cal::relative_duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<(interval,interval)';
};


CREATE INFIX OPERATOR
std::`<=` (l: cal::relative_duration, r: cal::relative_duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=(interval,interval)';
};


CREATE INFIX OPERATOR
std::`+` (l: cal::relative_duration, r: cal::relative_duration) -> cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
    SELECT ("l"::interval + "r"::interval)::edgedb.relative_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::relative_duration, r: cal::relative_duration) -> cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval subtraction.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT ("l"::interval - "r"::interval)::edgedb.relative_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: cal::date_duration, r: cal::date_duration) -> cal::date_duration {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
    SELECT ("l" + "r")::edgedb.date_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::date_duration, r: cal::date_duration) -> cal::date_duration {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval subtraction.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT ("l" - "r")::edgedb.date_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: cal::relative_duration) -> cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
    SELECT ("l"::interval + "r"::interval)::edgedb.relative_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: cal::relative_duration, r: std::duration) -> cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
    SELECT ("l"::interval + "r"::interval)::edgedb.relative_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: std::duration, r: cal::relative_duration) -> cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval subtraction.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT ("l"::interval - "r"::interval)::edgedb.relative_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::relative_duration, r: std::duration) -> cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval subtraction.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT ("l"::interval - "r"::interval)::edgedb.relative_duration_t;
    $$;
};


CREATE PREFIX OPERATOR
std::`-` (v: cal::relative_duration) -> cal::relative_duration {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval negation.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (-"v"::interval)::edgedb.relative_duration_t;
    $$;
};


## Date/time casts
## ---------------

CREATE CAST FROM cal::local_datetime TO cal::local_date {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM cal::local_datetime TO cal::local_time {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM cal::local_date TO cal::local_datetime {
    SET volatility := 'Immutable';
    USING SQL CAST;
    # Analogous to implicit cast from int64 to float64.
    ALLOW IMPLICIT;
};


CREATE CAST FROM std::str TO cal::local_datetime {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'edgedb.local_datetime_in';
};


CREATE CAST FROM std::str TO cal::local_date {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'edgedb.local_date_in';
};


CREATE CAST FROM std::str TO cal::local_time {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'edgedb.local_time_in';
};


CREATE CAST FROM std::str TO cal::relative_duration {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT val::edgedb.relative_duration_t;
    $$;
};


CREATE CAST FROM std::str TO cal::date_duration {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'edgedb.date_duration_in';
};


CREATE CAST FROM cal::local_datetime TO std::str {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT trim(to_json(val)::text, '"');
    $$;
};

CREATE CAST FROM cal::local_date TO std::str {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM cal::local_time TO std::str {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM cal::relative_duration TO std::str {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM cal::date_duration TO std::str {
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


CREATE CAST FROM cal::local_datetime TO std::json {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM cal::local_date TO std::json {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM cal::local_time TO std::json {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM cal::relative_duration TO std::json {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM cal::date_duration TO std::json {
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


CREATE CAST FROM std::json TO cal::local_datetime {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT edgedb.local_datetime_in(
        edgedb.jsonb_extract_scalar(val, 'string'));
    $$;
};


CREATE CAST FROM std::json TO cal::local_date {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT edgedb.local_date_in(edgedb.jsonb_extract_scalar(val, 'string'));
    $$;
};


CREATE CAST FROM std::json TO cal::local_time {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT edgedb.local_time_in(edgedb.jsonb_extract_scalar(val, 'string'));
    $$;
};


CREATE CAST FROM std::json TO cal::relative_duration {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'string')::interval::edgedb.relative_duration_t;
    $$;
};


CREATE CAST FROM std::json TO cal::date_duration {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT edgedb.date_duration_in(edgedb.jsonb_extract_scalar(val, 'string'));
    $$;
};


CREATE CAST FROM std::duration TO cal::relative_duration {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM cal::relative_duration TO std::duration {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM cal::date_duration TO cal::relative_duration {
    # Same underlying types that don't require any DST calculations to convert
    # into eachother.
    SET volatility := 'Immutable';
    USING SQL CAST;
    # Analogous to implicit cast from int64 to float64.
    ALLOW IMPLICIT;
};


CREATE CAST FROM cal::relative_duration TO cal::date_duration {
    # Same underlying types that don't require any DST calculations to convert
    # into eachother.
    SET volatility := 'Immutable';
    USING SQL CAST;
};


## Modified functions
## ------------------

CREATE FUNCTION
std::datetime_get(dt: cal::local_datetime, el: std::str) -> std::float64
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
            edgedb.raise(
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
std::duration_get(dt: cal::date_duration, el: std::str) -> std::float64
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
            edgedb.raise(
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
std::duration_get(dt: cal::relative_duration, el: std::str) -> std::float64
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
            edgedb.raise(
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
    dt: cal::date_duration,
    unit: std::str
) -> cal::date_duration
{
    CREATE ANNOTATION std::description :=
        'Truncate the input duration to a particular precision.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT CASE WHEN "unit" IN (
            'days', 'weeks', 'months', 'years', 'decades', 'centuries')
        THEN date_trunc("unit", "dt")::edgedb.relative_duration_t
        WHEN "unit" = 'quarters'
        THEN date_trunc('quarter', "dt")::edgedb.relative_duration_t
        ELSE
            edgedb.raise(
                NULL::edgedb.relative_duration_t,
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
    dt: cal::relative_duration,
    unit: std::str
) -> cal::relative_duration
{
    CREATE ANNOTATION std::description :=
        'Truncate the input duration to a particular precision.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT CASE WHEN "unit" IN (
            'microseconds', 'milliseconds', 'seconds',
            'minutes', 'hours', 'days', 'weeks', 'months',
            'years', 'decades', 'centuries')
        THEN date_trunc("unit", "dt")::edgedb.relative_duration_t
        WHEN "unit" = 'quarters'
        THEN date_trunc('quarter', "dt")::edgedb.relative_duration_t
        ELSE
            edgedb.raise(
                NULL::edgedb.relative_duration_t,
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
std::to_str(dt: cal::local_datetime, fmt: OPTIONAL str={}) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Return string representation of the input value.';
    SET volatility := 'Immutable';
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
std::to_str(d: cal::local_date, fmt: OPTIONAL str={}) -> std::str
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


# Currently local time is formatted by composing it with the local
# current local date. This at least guarantees that the time
# formatting is accessible and consistent with full datetime
# formatting, but it exposes current date as well if it is included in
# the format.
# FIXME: date formatting should not have any special effect.
CREATE FUNCTION
std::to_str(nt: cal::local_time, fmt: OPTIONAL str={}) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Return string representation of the input value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            "nt"::text
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::text,
                'invalid_parameter_value',
                msg => 'to_str(): "fmt" argument must be a non-empty string'
            )
        ELSE
            edgedb.raise_on_null(
                to_char(date_trunc('day', localtimestamp) + "nt", "fmt"),
                'invalid_parameter_value',
                msg => 'to_str(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_str(rd: cal::relative_duration, fmt: OPTIONAL str={}) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Return string representation of the input value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            "rd"::text
        WHEN "fmt" = '' THEN
            edgedb.raise(
                NULL::text,
                'invalid_parameter_value',
                msg => 'to_str(): "fmt" argument must be a non-empty string'
            )
        ELSE
            edgedb.raise_on_null(
                to_char("rd", "fmt"),
                'invalid_parameter_value',
                msg => 'to_str(): format ''' || "fmt" || ''' is invalid'
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_datetime(local: cal::local_datetime, zone: std::str)
    -> std::datetime
{
    CREATE ANNOTATION std::description := 'Create a `datetime` value.';
    # The version of timezone with these arguments is IMMUTABLE.
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT timezone("zone", "local")::edgedb.timestamptz_t;
    $$;
};


CREATE FUNCTION
std::min(vals: SET OF cal::local_datetime) -> OPTIONAL cal::local_datetime
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF cal::local_date) -> OPTIONAL cal::local_date
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF cal::local_time) -> OPTIONAL cal::local_time
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF cal::relative_duration) -> OPTIONAL cal::relative_duration
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF cal::date_duration) -> OPTIONAL cal::date_duration
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF array<cal::local_datetime>) -> OPTIONAL array<cal::local_datetime>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF array<cal::local_date>) -> OPTIONAL array<cal::local_date>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF array<cal::local_time>) -> OPTIONAL array<cal::local_time>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF array<cal::relative_duration>) -> OPTIONAL array<cal::relative_duration>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF array<cal::date_duration>) -> OPTIONAL array<cal::date_duration>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::max(vals: SET OF cal::local_datetime) -> OPTIONAL cal::local_datetime
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF cal::local_date) -> OPTIONAL cal::local_date
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF cal::local_time) -> OPTIONAL cal::local_time
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF cal::relative_duration) -> OPTIONAL cal::relative_duration
{
    CREATE ANNOTATION std::description :=
        'Return the greatest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF cal::date_duration) -> OPTIONAL cal::date_duration
{
    CREATE ANNOTATION std::description :=
        'Return the greatest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF array<cal::local_datetime>) -> OPTIONAL array<cal::local_datetime>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF array<cal::local_date>) -> OPTIONAL array<cal::local_date>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF array<cal::local_time>) -> OPTIONAL array<cal::local_time>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF array<cal::relative_duration>) -> OPTIONAL array<cal::relative_duration>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    SET preserves_optionality := true;
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF array<cal::date_duration>) -> OPTIONAL array<cal::date_duration>
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
    val: multirange<cal::local_datetime>,
) -> set of range<cal::local_datetime>
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'unnest';
};


CREATE FUNCTION std::multirange_unpack(
    val: multirange<cal::local_date>,
) -> set of range<cal::local_date>
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'unnest';
};


CREATE FUNCTION
std::range_unpack(
    val: range<cal::local_datetime>,
    step: cal::relative_duration
) -> set of cal::local_datetime
{
    SET volatility := 'Immutable';
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


CREATE FUNCTION
std::range_unpack(
    val: range<cal::local_date>
) -> set of cal::local_date
{
    SET volatility := 'Immutable';
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
                'P1D'::interval
            )::edgedb.date_t
    $$;
};


CREATE FUNCTION
std::range_unpack(
    val: range<cal::local_date>,
    step: cal::date_duration
) -> set of cal::local_date
{
    SET volatility := 'Immutable';
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

# Need to cast edgedb.date_t to date in order for the @> operator to work.
CREATE FUNCTION std::contains(
    haystack: range<cal::local_date>,
    needle: cal::local_date
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
    haystack: multirange<cal::local_date>,
    needle: cal::local_date
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