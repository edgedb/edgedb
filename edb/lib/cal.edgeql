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

CREATE SCALAR TYPE cal::local_datetime EXTENDING std::anyscalar;

CREATE SCALAR TYPE cal::local_date EXTENDING std::anyscalar;

CREATE SCALAR TYPE cal::local_time EXTENDING std::anyscalar;

CREATE SCALAR TYPE cal::relative_duration EXTENDING std::anyscalar;


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
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'Stable';
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
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'Stable';
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
    SELECT make_time("hour"::int, "min"::int, "sec")
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
            'isodow', 'isoyear', 'millenium',
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
                    'millenium, month, quarter, seconds, week, year."}'
                )
            )
        END
    $$;
};


## Operators on std::datetime
## --------------------------

CREATE INFIX OPERATOR
std::`+` (l: std::datetime, r: cal::relative_duration) -> std::datetime {
    # operators on timestamptz are STABLE in PostgreSQL
    SET volatility := 'Stable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedb.timestamptz_t
    $$
};


CREATE INFIX OPERATOR
std::`+` (l: cal::relative_duration, r: std::datetime) -> std::datetime {
    # operators on timestamptz are STABLE in PostgreSQL
    SET volatility := 'Stable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedb.timestamptz_t
    $$
};


CREATE INFIX OPERATOR
std::`-` (l: std::datetime, r: cal::relative_duration) -> std::datetime {
    # operators on timestamptz are STABLE in PostgreSQL
    SET volatility := 'Stable';
    USING SQL $$
        SELECT ("l" - "r")::edgedb.timestamptz_t
    $$
};


## Operators on cal::local_datetime
## --------------------------------

CREATE INFIX OPERATOR
std::`=` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL cal::local_datetime,
           r: OPTIONAL cal::local_datetime) -> std::bool {
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL cal::local_datetime,
            r: OPTIONAL cal::local_datetime) -> std::bool {
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`>=` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`<` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`<=` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=(timestamp,timestamp)';
};


CREATE INFIX OPERATOR
std::`+` (l: cal::local_datetime, r: std::duration) -> cal::local_datetime {
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedb.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: cal::local_datetime) -> cal::local_datetime {
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedb.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_datetime, r: std::duration) -> cal::local_datetime {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT ("l" - "r")::edgedb.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: cal::local_datetime, r: cal::relative_duration) -> cal::local_datetime {
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedb.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: cal::relative_duration, r: cal::local_datetime) -> cal::local_datetime {
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
        SELECT ("l" + "r")::edgedb.timestamp_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_datetime, r: cal::relative_duration) -> cal::local_datetime {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT ("l" - "r")::edgedb.timestamp_t
    $$;
};


## Operators on cal::local_date
## ----------------------------

CREATE INFIX OPERATOR
std::`=` (l: cal::local_date, r: cal::local_date) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=(date,date)';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL cal::local_date,
           r: OPTIONAL cal::local_date) -> std::bool {
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: cal::local_date, r: cal::local_date) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>(date,date)';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL cal::local_date,
            r: OPTIONAL cal::local_date) -> std::bool {
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: cal::local_date, r: cal::local_date) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>(date,date)';
};


CREATE INFIX OPERATOR
std::`>=` (l: cal::local_date, r: cal::local_date) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=(date,date)';
};


CREATE INFIX OPERATOR
std::`<` (l: cal::local_date, r: cal::local_date) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<(date,date)';
};


CREATE INFIX OPERATOR
std::`<=` (l: cal::local_date, r: cal::local_date) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=(date,date)';
};


CREATE INFIX OPERATOR
std::`+` (l: cal::local_date, r: std::duration) -> cal::local_date
{
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" + "r")::edgedb.date_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: cal::local_date) -> cal::local_date
{
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" + "r")::edgedb.date_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_date, r: std::duration) -> cal::local_date
{
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" - "r")::edgedb.date_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: cal::local_date, r: cal::relative_duration) -> cal::local_date
{
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" + "r")::edgedb.date_t
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: cal::relative_duration, r: cal::local_date) -> cal::local_date
{
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" + "r")::edgedb.date_t
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_date, r: cal::relative_duration) -> cal::local_date
{
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL $$
        SELECT ("l" - "r")::edgedb.date_t
    $$;
};


## Operators on cal::local_time
## ----------------------------

CREATE INFIX OPERATOR
std::`=` (l: cal::local_time, r: cal::local_time) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL cal::local_time,
           r: OPTIONAL cal::local_time) -> std::bool {
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: cal::local_time, r: cal::local_time) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL cal::local_time,
            r: OPTIONAL cal::local_time) -> std::bool {
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: cal::local_time, r: cal::local_time) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>=` (l: cal::local_time, r: cal::local_time) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`<` (l: cal::local_time, r: cal::local_time) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<=` (l: cal::local_time, r: cal::local_time) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`+` (l: cal::local_time, r: std::duration) -> cal::local_time {
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+(time, interval)';
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: cal::local_time) -> cal::local_time {
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+(interval, time)';
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_time, r: std::duration) -> cal::local_time {
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-(time, interval)';
};


CREATE INFIX OPERATOR
std::`+` (l: cal::local_time, r: cal::relative_duration) -> cal::local_time {
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+(time, interval)';
};


CREATE INFIX OPERATOR
std::`+` (l: cal::relative_duration, r: cal::local_time) -> cal::local_time {
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+(interval, time)';
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_time, r: cal::relative_duration) -> cal::local_time {
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-(time, interval)';
};


## Operators on cal::relative_duration
## ----------------------------

CREATE INFIX OPERATOR
std::`=` (l: cal::relative_duration, r: cal::relative_duration) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=(interval,interval)';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL cal::relative_duration, r: OPTIONAL cal::relative_duration) -> std::bool {
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: cal::relative_duration, r: cal::relative_duration) -> std::bool {
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
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: cal::relative_duration, r: cal::relative_duration) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>(interval,interval)';
};


CREATE INFIX OPERATOR
std::`>=` (l: cal::relative_duration, r: cal::relative_duration) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=(interval,interval)';
};


CREATE INFIX OPERATOR
std::`<` (l: cal::relative_duration, r: cal::relative_duration) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<(interval,interval)';
};


CREATE INFIX OPERATOR
std::`<=` (l: cal::relative_duration, r: cal::relative_duration) -> std::bool {
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=(interval,interval)';
};


CREATE INFIX OPERATOR
std::`+` (l: cal::relative_duration, r: cal::relative_duration) -> cal::relative_duration {
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
    SELECT ("l"::interval + "r"::interval)::edgedb.relative_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::relative_duration, r: cal::relative_duration) -> cal::relative_duration {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT ("l"::interval - "r"::interval)::edgedb.relative_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: cal::relative_duration) -> cal::relative_duration {
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
    SELECT ("l"::interval + "r"::interval)::edgedb.relative_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`+` (l: cal::relative_duration, r: std::duration) -> cal::relative_duration {
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
    SELECT ("l"::interval + "r"::interval)::edgedb.relative_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: std::duration, r: cal::relative_duration) -> cal::relative_duration {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT ("l"::interval - "r"::interval)::edgedb.relative_duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::relative_duration, r: std::duration) -> cal::relative_duration {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT ("l"::interval - "r"::interval)::edgedb.relative_duration_t;
    $$;
};


CREATE PREFIX OPERATOR
std::`-` (v: cal::relative_duration) -> cal::relative_duration {
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
};


CREATE CAST FROM std::str TO cal::local_datetime {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'edgedb.local_datetime_in';
};


CREATE CAST FROM std::str TO cal::local_date {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'edgedb.local_date_in';
};


CREATE CAST FROM std::str TO cal::local_time {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'edgedb.local_time_in';
};

CREATE CAST FROM std::str TO cal::relative_duration {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT val::edgedb.relative_duration_t;
    $$;
};


CREATE CAST FROM cal::local_datetime TO std::str {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT trim(to_json(val)::text, '"');
    $$;
};

CREATE CAST FROM cal::local_date TO std::str {
    SET volatility := 'Stable';
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


CREATE CAST FROM cal::local_datetime TO std::json {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM cal::local_date TO std::json {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM cal::local_time TO std::json {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM cal::relative_duration TO std::json {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::json TO cal::local_datetime {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT edgedb.local_datetime_in(
        edgedb.jsonb_extract_scalar(val, 'string'));
    $$;
};


CREATE CAST FROM std::json TO cal::local_date {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT edgedb.local_date_in(edgedb.jsonb_extract_scalar(val, 'string'));
    $$;
};


CREATE CAST FROM std::json TO cal::local_time {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT edgedb.local_time_in(edgedb.jsonb_extract_scalar(val, 'string'));
    $$;
};


CREATE CAST FROM std::json TO cal::relative_duration {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'string')::interval::edgedb.relative_duration_t;
    $$;
};


CREATE CAST FROM std::duration TO cal::relative_duration {
    SET volatility := 'Stable';
    USING SQL CAST;
};


CREATE CAST FROM cal::relative_duration TO std::duration {
    SET volatility := 'Stable';
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
            'isodow', 'isoyear', 'microseconds', 'millenium',
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
                    || 'microseconds, millenium, milliseconds, minutes, '
                    || 'month, quarter, seconds, week, year."}'
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
std::to_str(d: cal::local_date, fmt: OPTIONAL str={}) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Return string representation of the input value.';
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'Stable';
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
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'Stable';
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
    SET volatility := 'Stable';
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
std::min(vals: SET OF cal::local_datetime) -> OPTIONAL cal::local_datetime
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF cal::local_date) -> OPTIONAL cal::local_date
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF cal::local_time) -> OPTIONAL cal::local_time
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF array<cal::local_datetime>) -> OPTIONAL array<cal::local_datetime>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF array<cal::local_date>) -> OPTIONAL array<cal::local_date>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF array<cal::local_time>) -> OPTIONAL array<cal::local_time>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::min(vals: SET OF array<cal::relative_duration>) -> OPTIONAL array<cal::relative_duration>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'min';
};


CREATE FUNCTION
std::max(vals: SET OF cal::local_datetime) -> OPTIONAL cal::local_datetime
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF cal::local_date) -> OPTIONAL cal::local_date
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF cal::local_time) -> OPTIONAL cal::local_time
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF array<cal::local_datetime>) -> OPTIONAL array<cal::local_datetime>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF array<cal::local_date>) -> OPTIONAL array<cal::local_date>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF array<cal::local_time>) -> OPTIONAL array<cal::local_time>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'max';
};


CREATE FUNCTION
std::max(vals: SET OF array<cal::relative_duration>) -> OPTIONAL array<cal::relative_duration>
{
    CREATE ANNOTATION std::description :=
        'Return the smallest value of the input set.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'max';
};
