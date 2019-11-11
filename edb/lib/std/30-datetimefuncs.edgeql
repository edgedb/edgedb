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
    FROM SQL FUNCTION 'clock_timestamp';
};


CREATE FUNCTION
std::datetime_of_transaction() -> std::datetime
{
    SET volatility := 'STABLE';
    FROM SQL FUNCTION 'transaction_timestamp';
};


CREATE FUNCTION
std::datetime_of_statement() -> std::datetime
{
    SET volatility := 'STABLE';
    FROM SQL FUNCTION 'statement_timestamp';
};


CREATE FUNCTION
std::datetime_get(dt: std::datetime, el: std::str) -> std::float64
{
    # date_part of timestamptz is STABLE in PostgreSQL
    SET volatility := 'STABLE';
    FROM SQL $$
    SELECT date_part("el", "dt")
    $$;
};


CREATE FUNCTION
std::datetime_get(dt: std::local_datetime, el: std::str) -> std::float64
{
    SET volatility := 'IMMUTABLE';
    FROM SQL $$
    SELECT date_part("el", "dt")
    $$;
};


CREATE FUNCTION
std::time_get(dt: std::local_time, el: std::str) -> std::float64
{
    SET volatility := 'IMMUTABLE';
    FROM SQL $$
    SELECT date_part("el", "dt")
    $$;
};


CREATE FUNCTION
std::date_get(dt: std::local_date, el: std::str) -> std::float64
{
    SET volatility := 'IMMUTABLE';
    FROM SQL $$
    SELECT date_part("el", "dt")
    $$;
};


CREATE FUNCTION
std::duration_get(dt: std::duration, el: std::str) -> std::float64
{
    SET volatility := 'IMMUTABLE';
    FROM SQL $$
    SELECT date_part("el", "dt")
    $$;
};


CREATE FUNCTION
std::datetime_trunc(dt: std::datetime, unit: std::str) -> std::datetime
{
    # date_trunc of timestamptz is STABLE in PostgreSQL
    SET volatility := 'STABLE';
    FROM SQL $$
    SELECT date_trunc("unit", "dt")
    $$;
};


CREATE FUNCTION
std::duration_trunc(dt: std::duration, unit: std::str) -> std::duration
{
    SET volatility := 'IMMUTABLE';
    FROM SQL $$
    SELECT date_trunc("unit", "dt")
    $$;
};


## Date/time operators
## -------------------

# std::datetime

CREATE INFIX OPERATOR
std::`=` (l: std::datetime, r: std::datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::datetime, r: OPTIONAL std::datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::datetime, r: std::datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::datetime, r: OPTIONAL std::datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: std::datetime, r: std::datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::datetime, r: std::datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`<` (l: std::datetime, r: std::datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::datetime, r: std::datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`+` (l: std::datetime, r: std::duration) -> std::datetime {
    # operators on timestamptz are STABLE in PostgreSQL
    SET volatility := 'STABLE';
    FROM SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: std::datetime) -> std::datetime {
    # operators on timestamptz are STABLE in PostgreSQL
    SET volatility := 'STABLE';
    FROM SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`-` (l: std::datetime, r: std::duration) -> std::datetime {
    # operators on timestamptz are STABLE in PostgreSQL
    SET volatility := 'STABLE';
    FROM SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`-` (l: std::datetime, r: std::datetime) -> std::duration {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'-';
};


# std::local_datetime

CREATE INFIX OPERATOR
std::`=` (l: std::local_datetime, r: std::local_datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::local_datetime,
           r: OPTIONAL std::local_datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::local_datetime, r: std::local_datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::local_datetime,
            r: OPTIONAL std::local_datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: std::local_datetime, r: std::local_datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::local_datetime, r: std::local_datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`<` (l: std::local_datetime, r: std::local_datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::local_datetime, r: std::local_datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`+` (l: std::local_datetime, r: std::duration) -> std::local_datetime {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: std::local_datetime) -> std::local_datetime {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`-` (l: std::local_datetime, r: std::duration) -> std::local_datetime {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`-` (l: std::local_datetime, r: std::local_datetime) -> std::duration {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'-';
};


# std::local_date

CREATE INFIX OPERATOR
std::`=` (l: std::local_date, r: std::local_date) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::local_date,
           r: OPTIONAL std::local_date) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::local_date, r: std::local_date) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::local_date,
            r: OPTIONAL std::local_date) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: std::local_date, r: std::local_date) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::local_date, r: std::local_date) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`<` (l: std::local_date, r: std::local_date) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::local_date, r: std::local_date) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`+` (l: std::local_date, r: std::duration) -> std::local_date
{
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR '+';
    SET force_return_cast := true;
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: std::local_date) -> std::local_date
{
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR '+';
    SET force_return_cast := true;
};


CREATE INFIX OPERATOR
std::`-` (l: std::local_date, r: std::duration) -> std::local_date
{
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR '-';
    SET force_return_cast := true;
};


CREATE INFIX OPERATOR
std::`-` (l: std::local_date, r: std::local_date) -> std::duration {
    SET volatility := 'IMMUTABLE';
    FROM SQL $$
    SELECT make_interval(days => "l" - "r")
    $$;
};


# std::local_time

CREATE INFIX OPERATOR
std::`=` (l: std::local_time, r: std::local_time) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::local_time,
           r: OPTIONAL std::local_time) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::local_time, r: std::local_time) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::local_time,
            r: OPTIONAL std::local_time) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: std::local_time, r: std::local_time) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::local_time, r: std::local_time) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`<` (l: std::local_time, r: std::local_time) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::local_time, r: std::local_time) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`+` (l: std::local_time, r: std::duration) -> std::local_time {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: std::local_time) -> std::local_time {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`-` (l: std::local_time, r: std::duration) -> std::local_time {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`-` (l: std::local_time, r: std::local_time) -> std::duration {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'-';
};


# std::duration

CREATE INFIX OPERATOR
std::`=` (l: std::duration, r: std::duration) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::duration, r: OPTIONAL std::duration) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::duration, r: std::duration) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (
        l: OPTIONAL std::duration,
        r: OPTIONAL std::duration
) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: std::duration, r: std::duration) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::duration, r: std::duration) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`<` (l: std::duration, r: std::duration) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::duration, r: std::duration) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: std::duration) -> std::duration {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`-` (l: std::duration, r: std::duration) -> std::duration {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'-';
};


CREATE PREFIX OPERATOR
std::`-` (v: std::duration) -> std::duration {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'-';
};


## Date/time casts
## ---------------

CREATE CAST FROM std::local_datetime TO std::local_date {
    SET volatility := 'IMMUTABLE';
    FROM SQL CAST;
};


CREATE CAST FROM std::local_datetime TO std::local_time {
    SET volatility := 'IMMUTABLE';
    FROM SQL CAST;
};


CREATE CAST FROM std::local_date TO std::local_datetime {
    SET volatility := 'IMMUTABLE';
    FROM SQL CAST;
};


## String casts

# Casts from text to any sort of date/time types are all STABLE in
# PostgreSQL, but it may be the case that our restricted versions that
# prohibit usage of timezone for non-timezone-aware types are actually
# IMMUTABLE (as the corresponding casts from those types to text).
CREATE CAST FROM std::str TO std::datetime {
    SET volatility := 'STABLE';
    FROM SQL FUNCTION 'edgedb.datetime_in';
};


CREATE CAST FROM std::str TO std::local_datetime {
    SET volatility := 'STABLE';
    FROM SQL FUNCTION 'edgedb.local_datetime_in';
};


CREATE CAST FROM std::str TO std::local_date {
    SET volatility := 'STABLE';
    FROM SQL FUNCTION 'edgedb.local_date_in';
};


CREATE CAST FROM std::str TO std::local_time {
    SET volatility := 'STABLE';
    FROM SQL FUNCTION 'edgedb.local_time_in';
};


CREATE CAST FROM std::str TO std::duration {
    SET volatility := 'STABLE';
    FROM SQL CAST;
};


# Normalize [local] datetime to text conversion to have
# the same format as one would get by serializing to JSON.
# Otherwise Postgres doesn't follow the ISO8601 standard
# and uses ' ' instead of 'T' as a separator between date
# and time.
CREATE CAST FROM std::datetime TO std::str {
    SET volatility := 'STABLE';
    FROM SQL $$
    SELECT trim(to_json(val)::text, '"');
    $$;
};


CREATE CAST FROM std::local_datetime TO std::str {
    SET volatility := 'STABLE';
    FROM SQL $$
    SELECT trim(to_json(val)::text, '"');
    $$;
};


CREATE CAST FROM std::local_date TO std::str {
    SET volatility := 'STABLE';
    FROM SQL CAST;
};


CREATE CAST FROM std::local_time TO std::str {
    SET volatility := 'IMMUTABLE';
    FROM SQL CAST;
};


CREATE CAST FROM std::duration TO std::str {
    SET volatility := 'IMMUTABLE';
    FROM SQL $$
    SELECT regexp_replace(val::text, '[[:<:]]mon(?=s?[[:>:]])', 'month');
    $$;
};
