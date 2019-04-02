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
    FROM SQL FUNCTION 'clock_timestamp';
};


CREATE FUNCTION
std::datetime_of_transaction() -> std::datetime
{
    FROM SQL FUNCTION 'transaction_timestamp';
};


CREATE FUNCTION
std::datetime_of_statement() -> std::datetime
{
    FROM SQL FUNCTION 'statement_timestamp';
};


CREATE FUNCTION
std::datetime_get(dt: std::datetime, el: std::str) -> std::float64
{
    FROM SQL $$
    SELECT date_part("el", "dt")
    $$;
};


CREATE FUNCTION
std::datetime_get(dt: std::local_datetime, el: std::str) -> std::float64
{
    FROM SQL $$
    SELECT date_part("el", "dt")
    $$;
};


CREATE FUNCTION
std::time_get(dt: std::local_time, el: std::str) -> std::float64
{
    FROM SQL $$
    SELECT date_part("el", "dt")
    $$;
};


CREATE FUNCTION
std::date_get(dt: std::local_date, el: std::str) -> std::float64
{
    FROM SQL $$
    SELECT date_part("el", "dt")
    $$;
};


CREATE FUNCTION
std::timedelta_get(dt: std::timedelta, el: std::str) -> std::float64
{
    FROM SQL $$
    SELECT date_part("el", "dt")
    $$;
};


CREATE FUNCTION
std::datetime_trunc(dt: std::datetime, unit: std::str) -> std::datetime
{
    FROM SQL $$
    SELECT date_trunc("unit", "dt")
    $$;
};


CREATE FUNCTION
std::timedelta_trunc(dt: std::timedelta, unit: std::str) -> std::timedelta
{
    FROM SQL $$
    SELECT date_trunc("unit", "dt")
    $$;
};


## Date/time operators
## -------------------

# std::datetime

CREATE INFIX OPERATOR
std::`=` (l: std::datetime, r: std::datetime) -> std::bool
    FROM SQL OPERATOR r'=';


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::datetime, r: OPTIONAL std::datetime) -> std::bool
    FROM SQL EXPRESSION;


CREATE INFIX OPERATOR
std::`!=` (l: std::datetime, r: std::datetime) -> std::bool
    FROM SQL OPERATOR r'<>';


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::datetime, r: OPTIONAL std::datetime) -> std::bool
    FROM SQL EXPRESSION;


CREATE INFIX OPERATOR
std::`>` (l: std::datetime, r: std::datetime) -> std::bool
    FROM SQL OPERATOR r'>';


CREATE INFIX OPERATOR
std::`>=` (l: std::datetime, r: std::datetime) -> std::bool
    FROM SQL OPERATOR r'>=';


CREATE INFIX OPERATOR
std::`<` (l: std::datetime, r: std::datetime) -> std::bool
    FROM SQL OPERATOR r'<';


CREATE INFIX OPERATOR
std::`<=` (l: std::datetime, r: std::datetime) -> std::bool
    FROM SQL OPERATOR r'<=';


CREATE INFIX OPERATOR
std::`+` (l: std::datetime, r: std::timedelta) -> std::datetime
    FROM SQL OPERATOR r'+';


CREATE INFIX OPERATOR
std::`+` (l: std::timedelta, r: std::datetime) -> std::datetime
    FROM SQL OPERATOR r'+';


CREATE INFIX OPERATOR
std::`-` (l: std::datetime, r: std::timedelta) -> std::datetime
    FROM SQL OPERATOR r'-';


CREATE INFIX OPERATOR
std::`-` (l: std::datetime, r: std::datetime) -> std::timedelta
    FROM SQL OPERATOR r'-';


# std::local_datetime

CREATE INFIX OPERATOR
std::`=` (l: std::local_datetime, r: std::local_datetime) -> std::bool
    FROM SQL OPERATOR r'=';


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::local_datetime,
           r: OPTIONAL std::local_datetime) -> std::bool
    FROM SQL EXPRESSION;


CREATE INFIX OPERATOR
std::`!=` (l: std::local_datetime, r: std::local_datetime) -> std::bool
    FROM SQL OPERATOR r'<>';


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::local_datetime,
            r: OPTIONAL std::local_datetime) -> std::bool
    FROM SQL EXPRESSION;


CREATE INFIX OPERATOR
std::`>` (l: std::local_datetime, r: std::local_datetime) -> std::bool
    FROM SQL OPERATOR r'>';


CREATE INFIX OPERATOR
std::`>=` (l: std::local_datetime, r: std::local_datetime) -> std::bool
    FROM SQL OPERATOR r'>=';


CREATE INFIX OPERATOR
std::`<` (l: std::local_datetime, r: std::local_datetime) -> std::bool
    FROM SQL OPERATOR r'<';


CREATE INFIX OPERATOR
std::`<=` (l: std::local_datetime, r: std::local_datetime) -> std::bool
    FROM SQL OPERATOR r'<=';


CREATE INFIX OPERATOR
std::`+` (l: std::local_datetime, r: std::timedelta) -> std::local_datetime
    FROM SQL OPERATOR r'+';


CREATE INFIX OPERATOR
std::`+` (l: std::timedelta, r: std::local_datetime) -> std::local_datetime
    FROM SQL OPERATOR r'+';


CREATE INFIX OPERATOR
std::`-` (l: std::local_datetime, r: std::timedelta) -> std::local_datetime
    FROM SQL OPERATOR r'-';


CREATE INFIX OPERATOR
std::`-` (l: std::local_datetime, r: std::local_datetime) -> std::timedelta
    FROM SQL OPERATOR r'-';


# std::local_date

CREATE INFIX OPERATOR
std::`=` (l: std::local_date, r: std::local_date) -> std::bool
    FROM SQL OPERATOR r'=';


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::local_date,
           r: OPTIONAL std::local_date) -> std::bool
    FROM SQL EXPRESSION;


CREATE INFIX OPERATOR
std::`!=` (l: std::local_date, r: std::local_date) -> std::bool
    FROM SQL OPERATOR r'<>';


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::local_date,
            r: OPTIONAL std::local_date) -> std::bool
    FROM SQL EXPRESSION;


CREATE INFIX OPERATOR
std::`>` (l: std::local_date, r: std::local_date) -> std::bool
    FROM SQL OPERATOR r'>';


CREATE INFIX OPERATOR
std::`>=` (l: std::local_date, r: std::local_date) -> std::bool
    FROM SQL OPERATOR r'>=';


CREATE INFIX OPERATOR
std::`<` (l: std::local_date, r: std::local_date) -> std::bool
    FROM SQL OPERATOR r'<';


CREATE INFIX OPERATOR
std::`<=` (l: std::local_date, r: std::local_date) -> std::bool
    FROM SQL OPERATOR r'<=';


CREATE INFIX OPERATOR
std::`+` (l: std::local_date, r: std::timedelta) -> std::local_date
{
    FROM SQL OPERATOR '+';
    SET force_return_cast := true;
};


CREATE INFIX OPERATOR
std::`+` (l: std::timedelta, r: std::local_date) -> std::local_date
{
    FROM SQL OPERATOR '+';
    SET force_return_cast := true;
};


CREATE INFIX OPERATOR
std::`-` (l: std::local_date, r: std::timedelta) -> std::local_date
{
    FROM SQL OPERATOR '-';
    SET force_return_cast := true;
};


CREATE INFIX OPERATOR
std::`-` (l: std::local_date, r: std::local_date) -> std::timedelta
    FROM SQL $$
    SELECT make_interval(days => "l" - "r")
    $$;


# std::local_time

CREATE INFIX OPERATOR
std::`=` (l: std::local_time, r: std::local_time) -> std::bool
    FROM SQL OPERATOR r'=';


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::local_time,
           r: OPTIONAL std::local_time) -> std::bool
    FROM SQL EXPRESSION;


CREATE INFIX OPERATOR
std::`!=` (l: std::local_time, r: std::local_time) -> std::bool
    FROM SQL OPERATOR r'<>';


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::local_time,
            r: OPTIONAL std::local_time) -> std::bool
    FROM SQL EXPRESSION;


CREATE INFIX OPERATOR
std::`>` (l: std::local_time, r: std::local_time) -> std::bool
    FROM SQL OPERATOR r'>';


CREATE INFIX OPERATOR
std::`>=` (l: std::local_time, r: std::local_time) -> std::bool
    FROM SQL OPERATOR r'>=';


CREATE INFIX OPERATOR
std::`<` (l: std::local_time, r: std::local_time) -> std::bool
    FROM SQL OPERATOR r'<';


CREATE INFIX OPERATOR
std::`<=` (l: std::local_time, r: std::local_time) -> std::bool
    FROM SQL OPERATOR r'<=';


CREATE INFIX OPERATOR
std::`+` (l: std::local_time, r: std::timedelta) -> std::local_time
    FROM SQL OPERATOR r'+';


CREATE INFIX OPERATOR
std::`+` (l: std::timedelta, r: std::local_time) -> std::local_time
    FROM SQL OPERATOR r'+';


CREATE INFIX OPERATOR
std::`-` (l: std::local_time, r: std::timedelta) -> std::local_time
    FROM SQL OPERATOR r'-';


CREATE INFIX OPERATOR
std::`-` (l: std::local_time, r: std::local_time) -> std::timedelta
    FROM SQL OPERATOR r'-';


# std::timedelta

CREATE INFIX OPERATOR
std::`=` (l: std::timedelta, r: std::timedelta) -> std::bool
    FROM SQL OPERATOR r'=';


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::timedelta, r: OPTIONAL std::timedelta) -> std::bool
    FROM SQL EXPRESSION;


CREATE INFIX OPERATOR
std::`!=` (l: std::timedelta, r: std::timedelta) -> std::bool
    FROM SQL OPERATOR r'<>';


CREATE INFIX OPERATOR
std::`?!=` (
        l: OPTIONAL std::timedelta,
        r: OPTIONAL std::timedelta
) -> std::bool
    FROM SQL EXPRESSION;


CREATE INFIX OPERATOR
std::`>` (l: std::timedelta, r: std::timedelta) -> std::bool
    FROM SQL OPERATOR r'>';


CREATE INFIX OPERATOR
std::`>=` (l: std::timedelta, r: std::timedelta) -> std::bool
    FROM SQL OPERATOR r'>=';


CREATE INFIX OPERATOR
std::`<` (l: std::timedelta, r: std::timedelta) -> std::bool
    FROM SQL OPERATOR r'<';


CREATE INFIX OPERATOR
std::`<=` (l: std::timedelta, r: std::timedelta) -> std::bool
    FROM SQL OPERATOR r'<=';


CREATE INFIX OPERATOR
std::`+` (l: std::timedelta, r: std::timedelta) -> std::timedelta
    FROM SQL OPERATOR r'+';


CREATE INFIX OPERATOR
std::`-` (l: std::timedelta, r: std::timedelta) -> std::timedelta
    FROM SQL OPERATOR r'-';


CREATE PREFIX OPERATOR
std::`-` (v: std::timedelta) -> std::timedelta
    FROM SQL OPERATOR r'-';


## Date/time casts
## ---------------

CREATE CAST FROM std::local_datetime TO std::local_date
    FROM SQL CAST;


CREATE CAST FROM std::local_datetime TO std::local_time
    FROM SQL CAST;


CREATE CAST FROM std::local_date TO std::local_datetime
    FROM SQL CAST;


## String casts

CREATE CAST FROM std::str TO std::datetime
    FROM SQL FUNCTION 'edgedb.timestamptz_in';


CREATE CAST FROM std::str TO std::local_datetime
    FROM SQL FUNCTION 'edgedb.timestamp_in';


CREATE CAST FROM std::str TO std::local_date
    FROM SQL FUNCTION 'edgedb.date_in';


CREATE CAST FROM std::str TO std::local_time
    FROM SQL FUNCTION 'edgedb.time_in';


CREATE CAST FROM std::str TO std::timedelta
    FROM SQL CAST;


# Normalize [local] datetime to text conversion to have
# the same format as one would get by serializing to JSON.
# Otherwise Postgres doesn't follow the ISO8601 standard
# and uses ' ' instead of 'T' as a separator between date
# and time.
CREATE CAST FROM std::datetime TO std::str
    FROM SQL $$
    SELECT trim(to_json(val)::text, '"');
    $$;


CREATE CAST FROM std::local_datetime TO std::str
    FROM SQL $$
    SELECT trim(to_json(val)::text, '"');
    $$;


CREATE CAST FROM std::local_date TO std::str
    FROM SQL CAST;


CREATE CAST FROM std::local_time TO std::str
    FROM SQL CAST;


CREATE CAST FROM std::timedelta TO std::str
    FROM SQL CAST;
