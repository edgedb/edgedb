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
    SET volatility := 'VOLATILE';
    USING SQL FUNCTION 'clock_timestamp';
};


CREATE FUNCTION
std::datetime_of_transaction() -> std::datetime
{
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'transaction_timestamp';
};


CREATE FUNCTION
std::datetime_of_statement() -> std::datetime
{
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'statement_timestamp';
};


CREATE FUNCTION
std::datetime_get(dt: std::datetime, el: std::str) -> std::float64
{
    # date_part of timestamptz is STABLE in PostgreSQL
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT date_part("el", "dt")
    $$;
};


CREATE FUNCTION
std::datetime_truncate(dt: std::datetime, unit: std::str) -> std::datetime
{
    # date_trunc of timestamptz is STABLE in PostgreSQL
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT date_trunc("unit", "dt")
    $$;
};


CREATE FUNCTION
std::duration_truncate(dt: std::duration, unit: std::str) -> std::duration
{
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT CASE WHEN "unit" in ('microseconds', 'milliseconds',
                                'seconds', 'minutes', 'hours')
        THEN date_trunc("unit", "dt")
        ELSE
            edgedb._raise_specific_exception(
                'invalid_datetime_format',
                'invalid input syntax for type std::duration_truncate: '
                    || quote_literal("dt"),
                '{"hint":"Supported units: microseconds, milliseconds, ' ||
                'seconds, minutes, hours."}',
                NULL::interval
            )
        END
    $$;
};


CREATE FUNCTION
std::duration_to_seconds(dur: std::duration) -> std::decimal
{
    SET volatility := 'IMMUTABLE';
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
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::datetime, r: OPTIONAL std::datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::datetime, r: std::datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::datetime, r: OPTIONAL std::datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: std::datetime, r: std::datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::datetime, r: std::datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`<` (l: std::datetime, r: std::datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::datetime, r: std::datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`+` (l: std::datetime, r: std::duration) -> std::datetime {
    # operators on timestamptz are STABLE in PostgreSQL
    SET volatility := 'STABLE';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: std::datetime) -> std::datetime {
    # operators on timestamptz are STABLE in PostgreSQL
    SET volatility := 'STABLE';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`-` (l: std::datetime, r: std::duration) -> std::datetime {
    # operators on timestamptz are STABLE in PostgreSQL
    SET volatility := 'STABLE';
    USING SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`-` (l: std::datetime, r: std::datetime) -> std::duration {
    SET volatility := 'IMMUTABLE';
    USING SQL $$
        SELECT EXTRACT(epoch FROM "l" - "r")::text::interval
    $$
};


# std::duration

CREATE INFIX OPERATOR
std::`=` (l: std::duration, r: std::duration) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::duration, r: OPTIONAL std::duration) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::duration, r: std::duration) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (
        l: OPTIONAL std::duration,
        r: OPTIONAL std::duration
) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: std::duration, r: std::duration) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::duration, r: std::duration) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`<` (l: std::duration, r: std::duration) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::duration, r: std::duration) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: std::duration) -> std::duration {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`-` (l: std::duration, r: std::duration) -> std::duration {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


CREATE PREFIX OPERATOR
std::`-` (v: std::duration) -> std::duration {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


## String casts

# Casts from text to any sort of date/time types are all STABLE in
# PostgreSQL, but it may be the case that our restricted versions that
# prohibit usage of timezone for non-timezone-aware types are actually
# IMMUTABLE (as the corresponding casts from those types to text).
CREATE CAST FROM std::str TO std::datetime {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'edgedb.datetime_in';
};


CREATE CAST FROM std::str TO std::duration {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'edgedb.duration_in';
};


# Normalize [local] datetime to text conversion to have
# the same format as one would get by serializing to JSON.
# Otherwise Postgres doesn't follow the ISO8601 standard
# and uses ' ' instead of 'T' as a separator between date
# and time.
CREATE CAST FROM std::datetime TO std::str {
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT trim(to_json(val)::text, '"');
    $$;
};


CREATE CAST FROM std::duration TO std::str {
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT regexp_replace(val::text, '[[:<:]]mon(?=s?[[:>:]])', 'month');
    $$;
};
