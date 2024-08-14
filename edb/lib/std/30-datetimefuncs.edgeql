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


## Date/time functions
## -------------------

CREATE FUNCTION
std::datetime_current() -> std::datetime
{
    CREATE ANNOTATION std::description :=
        'Return the current server date and time.';
    SET volatility := 'Volatile';
    SET force_return_cast := true;
    USING SQL FUNCTION 'clock_timestamp';
};


CREATE FUNCTION
std::datetime_of_transaction() -> std::datetime
{
    CREATE ANNOTATION std::description :=
        'Return the date and time of the start of the current transaction.';
    SET volatility := 'Stable';
    SET force_return_cast := true;
    USING SQL FUNCTION 'transaction_timestamp';
};


CREATE FUNCTION
std::datetime_of_statement() -> std::datetime
{
    CREATE ANNOTATION std::description :=
        'Return the date and time of the start of the current statement.';
    SET volatility := 'Stable';
    SET force_return_cast := true;
    USING SQL FUNCTION 'statement_timestamp';
};


CREATE FUNCTION
std::datetime_get(dt: std::datetime, el: std::str) -> std::float64
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
                    '{"hint":"Supported units: epochseconds, century, day, '
                    || 'decade, dow, doy, hour, isodow, isoyear, '
                    || 'microseconds, millennium, milliseconds, minutes, '
                    || 'month, quarter, seconds, week, year."}'
                )
            )
        END
    $$;
};


CREATE FUNCTION
std::datetime_truncate(dt: std::datetime, unit: std::str) -> std::datetime
{
    CREATE ANNOTATION std::description :=
        'Truncate the input datetime to a particular precision.';
    # date_trunc of timestamptz is STABLE in PostgreSQL
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT CASE WHEN "unit" IN (
            'microseconds', 'milliseconds', 'seconds',
            'minutes', 'hours', 'days', 'weeks', 'months',
            'years', 'decades', 'centuries')
        THEN date_trunc("unit", "dt")::edgedbt.timestamptz_t
        WHEN "unit" = 'quarters'
        THEN date_trunc('quarter', "dt")::edgedbt.timestamptz_t
        ELSE
            edgedb_VER.raise(
                NULL::edgedbt.timestamptz_t,
                'invalid_datetime_format',
                msg => (
                    'invalid unit for std::datetime_truncate: '
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
std::duration_get(dt: std::duration, el: std::str) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Extract a specific element of input duration by name.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT CASE WHEN "el" IN (
            'hour', 'minutes', 'seconds', 'milliseconds', 'microseconds')
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
                    || 'hour, minutes, seconds, milliseconds, microseconds, '
                    || 'and totalseconds."}'
                )
            )
        END
    $$;
};


CREATE FUNCTION
std::duration_truncate(dt: std::duration, unit: std::str) -> std::duration
{
    CREATE ANNOTATION std::description :=
        'Truncate the input duration to a particular precision.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT CASE WHEN "unit" in ('microseconds', 'milliseconds',
                                'seconds', 'minutes', 'hours')
        THEN date_trunc("unit", "dt")::edgedbt.duration_t
        ELSE
            edgedb_VER.raise(
                NULL::edgedbt.duration_t,
                'invalid_datetime_format',
                msg => (
                    'invalid unit for std::duration_truncate: '
                    || quote_literal("unit")
                ),
                detail => (
                    '{"hint":"Supported units: microseconds, milliseconds, '
                    || 'seconds, minutes, hours."}'
                )
            )
        END
    $$;
};


CREATE FUNCTION
std::duration_to_seconds(dur: std::duration) -> std::decimal
{
    CREATE ANNOTATION std::description :=
        'Return duration as total number of seconds in interval.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT EXTRACT(epoch FROM date_trunc('minute', dur))::bigint::decimal +
           '0.000001'::decimal*EXTRACT(microsecond FROM dur)::decimal
    $$;
};


## Date/time operators
## -------------------

# std::datetime

CREATE INFIX OPERATOR
std::`=` (l: std::datetime, r: std::datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=(timestamptz,timestamptz)';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::datetime, r: OPTIONAL std::datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::datetime, r: std::datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>(timestamptz,timestamptz)';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::datetime, r: OPTIONAL std::datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: std::datetime, r: std::datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>(timestamptz,timestamptz)';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::datetime, r: std::datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=(timestamptz,timestamptz)';
};


CREATE INFIX OPERATOR
std::`<` (l: std::datetime, r: std::datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<(timestamptz,timestamptz)';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::datetime, r: std::datetime) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=(timestamptz,timestamptz)';
};


CREATE INFIX OPERATOR
std::`+` (l: std::datetime, r: std::duration) -> std::datetime {
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
std::`+` (l: std::duration, r: std::datetime) -> std::datetime {
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
std::`-` (l: std::datetime, r: std::duration) -> std::datetime {
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


CREATE INFIX OPERATOR
std::`-` (l: std::datetime, r: std::datetime) -> std::duration {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Date/time subtraction.';
    # Immutable because datetime is guaranteed to be in UTC and no DST issues
    # should affect this.
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT EXTRACT(epoch FROM "l" - "r")::text::edgedbt.duration_t
    $$
};


# std::duration

CREATE INFIX OPERATOR
std::`=` (l: std::duration, r: std::duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=(interval,interval)';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::duration, r: OPTIONAL std::duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::duration, r: std::duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>(interval,interval)';
};


CREATE INFIX OPERATOR
std::`?!=` (
        l: OPTIONAL std::duration,
        r: OPTIONAL std::duration
) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: std::duration, r: std::duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>(interval,interval)';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::duration, r: std::duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=(interval,interval)';
};


CREATE INFIX OPERATOR
std::`<` (l: std::duration, r: std::duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<(interval,interval)';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::duration, r: std::duration) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=(interval,interval)';
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: std::duration) -> std::duration {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description :=
        'Time interval addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL $$
    SELECT ("l"::interval + "r"::interval)::edgedbt.duration_t;
    $$;
};


CREATE INFIX OPERATOR
std::`-` (l: std::duration, r: std::duration) -> std::duration {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval subtraction.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT ("l"::interval - "r"::interval)::edgedbt.duration_t;
    $$;
};


CREATE PREFIX OPERATOR
std::`-` (v: std::duration) -> std::duration {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description :=
        'Time interval negation.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT (-"v"::interval)::edgedbt.duration_t;
    $$;
};


## String casts

CREATE CAST FROM std::str TO std::datetime {
    # Stable because the input string can contain an explicit time-zone. Time
    # zones are externally defined things that can change suddenly and
    # arbitrarily by human laws, thus potentially changing the interpretatio
    # of the input string.
    SET volatility := 'Stable';
    USING SQL FUNCTION 'edgedb.datetime_in';
};


CREATE CAST FROM std::str TO std::duration {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'edgedb.duration_in';
};


# Normalize [local] datetime to text conversion to have
# the same format as one would get by serializing to JSON.
# Otherwise Postgres doesn't follow the ISO8601 standard
# and uses ' ' instead of 'T' as a separator between date
# and time.
CREATE CAST FROM std::datetime TO std::str {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT trim(to_json(val)::text, '"');
    $$;
};


CREATE CAST FROM std::duration TO std::str {
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT regexp_replace(val::text, '[[:<:]]mon(?=s?[[:>:]])', 'month');
    $$;
};
