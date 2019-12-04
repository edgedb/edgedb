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


## Functions
## ---------

CREATE FUNCTION
cal::to_local_datetime(s: std::str, fmt: OPTIONAL str={})
    -> cal::local_datetime
{
    # Helper function to_local_datetime is VOLATILE.
    SET volatility := 'VOLATILE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb.local_datetime_in("s")
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_local_datetime(): "fmt" argument must be a non-empty string',
                '',
                NULL::timestamp)
        ELSE
            edgedb._raise_exception_on_null(
                edgedb.to_local_datetime("s", "fmt"),
                'invalid_parameter_value',
                'to_local_datetime(): format ''' || "fmt" || ''' is invalid',
                ''
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
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT make_timestamp(
        "year"::int, "month"::int, "day"::int,
        "hour"::int, "min"::int, "sec"
    )
    $$;
};


CREATE FUNCTION
cal::to_local_datetime(dt: std::datetime, zone: std::str)
    -> cal::local_datetime
{
    # The version of timezone with these arguments is IMMUTABLE.
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT timezone("zone", "dt");
    $$;
};


CREATE FUNCTION
cal::to_local_date(s: std::str, fmt: OPTIONAL str={}) -> cal::local_date
{
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb.local_date_in("s")
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_local_date(): "fmt" argument must be a non-empty string',
                '',
                NULL::date)
        ELSE
            edgedb._raise_exception_on_null(
                edgedb.to_local_datetime("s", "fmt")::date,
                'invalid_parameter_value',
                'to_local_date(): format ''' || "fmt" || ''' is invalid',
                ''
            )
        END
    )
    $$;
};


CREATE FUNCTION
cal::to_local_date(dt: std::datetime, zone: std::str)
    -> cal::local_date
{
    # The version of timezone with these arguments is IMMUTABLE.
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT timezone("zone", "dt")::date;
    $$;
};


CREATE FUNCTION
cal::to_local_date(year: std::int64, month: std::int64, day: std::int64)
    -> cal::local_date
{
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT make_date("year"::int, "month"::int, "day"::int)
    $$;
};


CREATE FUNCTION
cal::to_local_time(s: std::str, fmt: OPTIONAL str={}) -> cal::local_time
{
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            edgedb.local_time_in("s")
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_local_time(): "fmt" argument must be a non-empty string',
                '',
                NULL::time)
        ELSE
            edgedb._raise_exception_on_null(
                edgedb.to_local_datetime("s", "fmt")::time,
                'invalid_parameter_value',
                'to_local_time(): format ''' || "fmt" || ''' is invalid',
                ''
            )
        END
    )
    $$;
};


CREATE FUNCTION
cal::to_local_time(dt: std::datetime, zone: std::str)
    -> cal::local_time
{
    # The version of timezone with these arguments is IMMUTABLE and so
    # is the cast.
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT timezone("zone", "dt")::time;
    $$;
};


CREATE FUNCTION
cal::to_local_time(hour: std::int64, min: std::int64, sec: std::float64)
    -> cal::local_time
{
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT make_time("hour"::int, "min"::int, "sec")
    $$;
};


CREATE FUNCTION
cal::time_get(dt: cal::local_time, el: std::str) -> std::float64
{
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT date_part("el", "dt")
    $$;
};


CREATE FUNCTION
cal::date_get(dt: cal::local_date, el: std::str) -> std::float64
{
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT date_part("el", "dt")
    $$;
};


## Operators on cal::local_datetime
## --------------------------------

CREATE INFIX OPERATOR
std::`=` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL cal::local_datetime,
           r: OPTIONAL cal::local_datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL cal::local_datetime,
            r: OPTIONAL cal::local_datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>=` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`<` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<=` (l: cal::local_datetime, r: cal::local_datetime) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`+` (l: cal::local_datetime, r: std::duration) -> cal::local_datetime {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: cal::local_datetime) -> cal::local_datetime {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_datetime, r: std::duration) -> cal::local_datetime {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_datetime, r: cal::local_datetime) -> std::duration {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


## Operators on cal::local_date
## ----------------------------

CREATE INFIX OPERATOR
std::`=` (l: cal::local_date, r: cal::local_date) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL cal::local_date,
           r: OPTIONAL cal::local_date) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: cal::local_date, r: cal::local_date) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL cal::local_date,
            r: OPTIONAL cal::local_date) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: cal::local_date, r: cal::local_date) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>=` (l: cal::local_date, r: cal::local_date) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`<` (l: cal::local_date, r: cal::local_date) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<=` (l: cal::local_date, r: cal::local_date) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`+` (l: cal::local_date, r: std::duration) -> cal::local_date
{
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '+';
    SET force_return_cast := true;
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: cal::local_date) -> cal::local_date
{
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '+';
    SET force_return_cast := true;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_date, r: std::duration) -> cal::local_date
{
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '-';
    SET force_return_cast := true;
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_date, r: cal::local_date) -> std::duration {
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT make_interval(days => "l" - "r")
    $$;
};


## Operators on cal::local_time
## ----------------------------

CREATE INFIX OPERATOR
std::`=` (l: cal::local_time, r: cal::local_time) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL cal::local_time,
           r: OPTIONAL cal::local_time) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: cal::local_time, r: cal::local_time) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL cal::local_time,
            r: OPTIONAL cal::local_time) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: cal::local_time, r: cal::local_time) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>=` (l: cal::local_time, r: cal::local_time) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`<` (l: cal::local_time, r: cal::local_time) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<=` (l: cal::local_time, r: cal::local_time) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`+` (l: cal::local_time, r: std::duration) -> cal::local_time {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`+` (l: std::duration, r: cal::local_time) -> cal::local_time {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_time, r: std::duration) -> cal::local_time {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`-` (l: cal::local_time, r: cal::local_time) -> std::duration {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


## Date/time casts
## ---------------

CREATE CAST FROM cal::local_datetime TO cal::local_date {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM cal::local_datetime TO cal::local_time {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM cal::local_date TO cal::local_datetime {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::str TO cal::local_datetime {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'edgedb.local_datetime_in';
};


CREATE CAST FROM std::str TO cal::local_date {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'edgedb.local_date_in';
};


CREATE CAST FROM std::str TO cal::local_time {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'edgedb.local_time_in';
};


CREATE CAST FROM cal::local_datetime TO std::str {
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT trim(to_json(val)::text, '"');
    $$;
};

CREATE CAST FROM cal::local_date TO std::str {
    SET volatility := 'STABLE';
    USING SQL CAST;
};


CREATE CAST FROM cal::local_time TO std::str {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM cal::local_datetime TO std::json {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM cal::local_date TO std::json {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM cal::local_time TO std::json {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::json TO cal::local_datetime {
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT edgedb.local_datetime_in(
        edgedb.jsonb_extract_scalar(val, 'string'));
    $$;
};


CREATE CAST FROM std::json TO cal::local_date {
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT edgedb.local_date_in(edgedb.jsonb_extract_scalar(val, 'string'));
    $$;
};


CREATE CAST FROM std::json TO cal::local_time {
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT edgedb.local_time_in(edgedb.jsonb_extract_scalar(val, 'string'));
    $$;
};


## Modified functions
## ------------------

CREATE FUNCTION
std::datetime_get(dt: cal::local_datetime, el: std::str) -> std::float64
{
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT date_part("el", "dt")
    $$;
};


CREATE FUNCTION
std::to_str(dt: cal::local_datetime, fmt: OPTIONAL str={}) -> std::str
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
std::to_str(d: cal::local_date, fmt: OPTIONAL str={}) -> std::str
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


# Currently local time is formatted by composing it with the local
# current local date. This at least guarantees that the time
# formatting is accessible and consistent with full datetime
# formatting, but it exposes current date as well if it is included in
# the format.
# FIXME: date formatting should not have any special effect.
CREATE FUNCTION
std::to_str(nt: cal::local_time, fmt: OPTIONAL str={}) -> std::str
{
    # Helper functions raising exceptions are STABLE.
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT (
        CASE WHEN "fmt" IS NULL THEN
            "nt"::text
        WHEN "fmt" = '' THEN
            edgedb._raise_specific_exception(
                'invalid_parameter_value',
                'to_str(): "fmt" argument must be a non-empty string',
                '',
                NULL::text)
        ELSE
            edgedb._raise_exception_on_null(
                to_char(date_trunc('day', localtimestamp) + "nt", "fmt"),
                'invalid_parameter_value',
                'to_str(): format ''' || "fmt" || ''' is invalid',
                ''
            )
        END
    )
    $$;
};


CREATE FUNCTION
std::to_datetime(local: cal::local_datetime, zone: std::str)
    -> std::datetime
{
    # The version of timezone with these arguments is IMMUTABLE.
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT timezone("zone", "local");
    $$;
};
