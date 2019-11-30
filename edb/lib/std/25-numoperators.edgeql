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


## Standard numeric operators
## --------------------------

# NOTE: we follow PostgreSQL in creating an explicit operator
# for each permutation of common integer and floating-point
# operand types to avoid casting overhead, as these operations
# are very common.
#
# Our implicit casts do not coincide with PostgreSQL. In particular we
# do not implicitly cast between decimals and floats. The philosophy
# behind that is that using decimal arithmetic should be opt-in. On
# the other hand, if decimals are used they should not be accidentally
# switched to floating point arithmetic. One of the consequences of
# this is that we need to explicitly define arithmetic operators for
# every legal combination of floats and decimals as unlike PostgreSQL
# we cannot rely on implicit casts between decimals and other numeric
# types.
#
# Floating point numbers are inherently imprecise. This means that
# casting a given float into another representation and back may yield
# a different value. This is especially important with float and
# decimal casts as both directions can lose precision. Discussion
# about precision loss of float to numeric casts can be found here:
# https://www.postgresql.org/message-id/5A937D7E.60305%40anastigmatix.net
# The practical consequence is that whenever the cast is needed, such
# as for comparison operators, it is preferable to cast decimal into
# float and not vice-versa. So we can go with Postgres behavior in
# regards to the comparison operators (but not arithmetic) with
# decimal as one of the operands.

# EQUALITY

CREATE INFIX OPERATOR
std::`=` (l: std::int16, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int16, r: OPTIONAL std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::int16, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int16, r: OPTIONAL std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::int16, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int16, r: OPTIONAL std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::int32, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int32, r: OPTIONAL std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::int32, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int32, r: OPTIONAL std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::int32, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int32, r: OPTIONAL std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::int64, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int64, r: OPTIONAL std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::int64, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int64, r: OPTIONAL std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::int64, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int64, r: OPTIONAL std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::float32, r: std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::float32, r: OPTIONAL std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::float32, r: std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::float32, r: OPTIONAL std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::float64, r: std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::float64, r: OPTIONAL std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::float64, r: std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::float64, r: OPTIONAL std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::bigint, r: std::bigint) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`=` (l: std::decimal, r: std::decimal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`=` (l: std::decimal, r: std::anyreal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::bigint, r: OPTIONAL std::bigint) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::decimal, r: OPTIONAL std::decimal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::decimal, r: OPTIONAL std::anyreal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::anyreal, r: std::decimal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::anyreal, r: OPTIONAL std::decimal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


# INEQUALITY

CREATE INFIX OPERATOR
std::`!=` (l: std::int16, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int16, r: OPTIONAL std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::int16, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int16, r: OPTIONAL std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::int16, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int16, r: OPTIONAL std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::int32, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int32, r: OPTIONAL std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::int32, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int32, r: OPTIONAL std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::int32, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int32, r: OPTIONAL std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::int64, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int64, r: OPTIONAL std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::int64, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int64, r: OPTIONAL std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::int64, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int64, r: OPTIONAL std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::float32, r: std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::float32, r: OPTIONAL std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::float32, r: std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::float32, r: OPTIONAL std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::float64, r: std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::float64, r: OPTIONAL std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::float64, r: std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::float64, r: OPTIONAL std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::bigint, r: std::bigint) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`!=` (l: std::decimal, r: std::decimal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`!=` (l: std::decimal, r: std::anyreal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::bigint, r: OPTIONAL std::bigint) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::decimal, r: OPTIONAL std::decimal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::decimal, r: OPTIONAL std::anyreal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::anyreal, r: std::decimal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::anyreal, r: OPTIONAL std::decimal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};



# GREATER THAN

CREATE INFIX OPERATOR
std::`>` (l: std::int16, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int16, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int16, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int32, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int32, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int32, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int32, r: std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '>(float8,float8)';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int64, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int64, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int64, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int64, r: std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>(float8,float8)';
};


CREATE INFIX OPERATOR
std::`>` (l: std::float32, r: std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::float32, r: std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::float32, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '>(float8,float8)';
};


CREATE INFIX OPERATOR
std::`>` (l: std::float64, r: std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::float64, r: std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::float64, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>(float8,float8)';
};


CREATE INFIX OPERATOR
std::`>` (l: std::bigint, r: std::bigint) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`>` (l: std::decimal, r: std::decimal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::anyreal, r: std::decimal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::decimal, r: std::anyreal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>';
};


# GREATER OR EQUAL

CREATE INFIX OPERATOR
std::`>=` (l: std::int16, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int16, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int16, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int32, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int32, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int32, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int32, r: std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '>=(float8,float8)';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int64, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int64, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int64, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int64, r: std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=(float8,float8)';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::float32, r: std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::float32, r: std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::float32, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '>=(float8,float8)';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::float64, r: std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::float64, r: std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::float64, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=(float8,float8)';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::bigint, r: std::bigint) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::decimal, r: std::decimal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::anyreal, r: std::decimal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::decimal, r: std::anyreal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'>=';
};


# LESS THAN

CREATE INFIX OPERATOR
std::`<` (l: std::int16, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int16, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int16, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int32, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int32, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int32, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int32, r: std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '<(float8,float8)';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int64, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int64, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int64, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int64, r: std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<(float8,float8)';
};


CREATE INFIX OPERATOR
std::`<` (l: std::float32, r: std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::float32, r: std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::float32, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '<(float8,float8)';
};


CREATE INFIX OPERATOR
std::`<` (l: std::float64, r: std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::float64, r: std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::float64, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<(float8,float8)';
};


CREATE INFIX OPERATOR
std::`<` (l: std::bigint, r: std::bigint) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`<` (l: std::decimal, r: std::decimal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::anyreal, r: std::decimal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::decimal, r: std::anyreal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<';
};


# LESS THAN OR EQUAL

CREATE INFIX OPERATOR
std::`<=` (l: std::int16, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int16, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int16, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int32, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int32, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int32, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int32, r: std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '<=(float8,float8)';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int64, r: std::int16) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int64, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int64, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int64, r: std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=(float8,float8)';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::float32, r: std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::float32, r: std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::float32, r: std::int32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '<=(float8,float8)';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::float64, r: std::float32) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::float64, r: std::float64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::float64, r: std::int64) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=(float8,float8)';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::bigint, r: std::bigint) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::decimal, r: std::decimal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::anyreal, r: std::decimal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::decimal, r: std::anyreal) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<=';
};


# INFIX PLUS

CREATE INFIX OPERATOR
std::`+` (l: std::int16, r: std::int16) -> std::int16 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`+` (l: std::int32, r: std::int32) -> std::int32 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`+` (l: std::int64, r: std::int64) -> std::int64 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`+` (l: std::float32, r: std::float32) -> std::float32 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`+` (l: std::float64, r: std::float64) -> std::float64 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`+` (l: std::bigint, r: std::bigint) -> std::bigint {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`+` (l: std::decimal, r: std::decimal) -> std::decimal {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+';
};


# PREFIX PLUS

CREATE PREFIX OPERATOR
std::`+` (l: std::int16) -> std::int16 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+';
};


CREATE PREFIX OPERATOR
std::`+` (l: std::int32) -> std::int32 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+';
};


CREATE PREFIX OPERATOR
std::`+` (l: std::int64) -> std::int64 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+';
};


CREATE PREFIX OPERATOR
std::`+` (l: std::float32) -> std::float32 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+';
};


CREATE PREFIX OPERATOR
std::`+` (l: std::float64) -> std::float64 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+';
};


CREATE PREFIX OPERATOR
std::`+` (l: std::bigint) -> std::bigint {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+(,numeric)';
};


CREATE PREFIX OPERATOR
std::`+` (l: std::decimal) -> std::decimal {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'+';
};



# INFIX MINUS

CREATE INFIX OPERATOR
std::`-` (l: std::int16, r: std::int16) -> std::int16 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`-` (l: std::int32, r: std::int32) -> std::int32 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`-` (l: std::int64, r: std::int64) -> std::int64 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`-` (l: std::float32, r: std::float32) -> std::float32 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`-` (l: std::float64, r: std::float64) -> std::float64 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`-` (l: std::bigint, r: std::bigint) -> std::bigint {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`-` (l: std::decimal, r: std::decimal) -> std::decimal {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


# PREFIX MINUS

CREATE PREFIX OPERATOR
std::`-` (l: std::int16) -> std::int16 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


CREATE PREFIX OPERATOR
std::`-` (l: std::int32) -> std::int32 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


CREATE PREFIX OPERATOR
std::`-` (l: std::int64) -> std::int64 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


CREATE PREFIX OPERATOR
std::`-` (l: std::float32) -> std::float32 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


CREATE PREFIX OPERATOR
std::`-` (l: std::float64) -> std::float64 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


CREATE PREFIX OPERATOR
std::`-` (l: std::bigint) -> std::bigint {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-(,numeric)';
};


CREATE PREFIX OPERATOR
std::`-` (l: std::decimal) -> std::decimal {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'-';
};


# MUL

CREATE INFIX OPERATOR
std::`*` (l: std::int16, r: std::int16) -> std::int16 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'*';
};


CREATE INFIX OPERATOR
std::`*` (l: std::int32, r: std::int32) -> std::int32 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'*';
};


CREATE INFIX OPERATOR
std::`*` (l: std::int64, r: std::int64) -> std::int64 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'*';
};


CREATE INFIX OPERATOR
std::`*` (l: std::float32, r: std::float32) -> std::float32 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'*';
};


CREATE INFIX OPERATOR
std::`*` (l: std::float64, r: std::float64) -> std::float64 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'*';
};


CREATE INFIX OPERATOR
std::`*` (l: std::bigint, r: std::bigint) -> std::bigint {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'*(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`*` (l: std::decimal, r: std::decimal) -> std::decimal {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'*';
};


# DIV

CREATE INFIX OPERATOR
std::`/` (l: std::int64, r: std::int64) -> std::float64
{
    SET volatility := 'IMMUTABLE';
    # We need both USING SQL OPERATOR and USING SQL to copy
    # the common attributes of the SQL division operator while
    # overriding the main operator function.
    USING SQL OPERATOR r'/';
    USING SQL 'SELECT "l" / ("r"::float8)';
};


CREATE INFIX OPERATOR
std::`/` (l: std::float32, r: std::float32) -> std::float32 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'/';
};


CREATE INFIX OPERATOR
std::`/` (l: std::float64, r: std::float64) -> std::float64 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'/';
};


CREATE INFIX OPERATOR
std::`/` (l: std::decimal, r: std::decimal) -> std::decimal {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'/';
};


# FLOORDIV

# PostgreSQL uses truncation division, so the -12 % 5 is -2, because
# -12 // 5 is -2, but EdgeQL uses floor division, so -12 // 5 is -3,
# and so -12 % 5 must be 3.  The correct divmod behavior is implemented
# in the C extension functions, with the exception of std::decimal,
# which is implemented using simple formulae instead.

CREATE INFIX OPERATOR
std::`//` (n: std::int16, d: std::int16) -> std::int16
{
    SET volatility := 'IMMUTABLE';
    # We need both USING SQL OPERATOR and USING SQL FUNCTION to copy
    # the common attributes of the SQL division operator while
    # overriding the main operator function.
    USING SQL OPERATOR r'/';
    USING SQL 'SELECT floor("n"::numeric / "d"::numeric)::int2';
};


CREATE INFIX OPERATOR
std::`//` (n: std::int32, d: std::int32) -> std::int32
{
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'/';
    USING SQL 'SELECT floor("n"::numeric / "d"::numeric)::int4';
};


CREATE INFIX OPERATOR
std::`//` (n: std::int64, d: std::int64) -> std::int64
{
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'/';
    USING SQL 'SELECT floor("n"::numeric / "d"::numeric)::int8';
};


CREATE INFIX OPERATOR
std::`//` (n: std::float32, d: std::float32) -> std::float32
{
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'/';
    USING SQL 'SELECT floor("n" / "d")::float4';
};


CREATE INFIX OPERATOR
std::`//` (n: std::float64, d: std::float64) -> std::float64
{
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'/';
    USING SQL 'SELECT floor("n" / "d")';
};


CREATE INFIX OPERATOR
std::`//` (n: std::bigint, d: std::bigint) -> std::bigint
{
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'/(numeric,numeric)';
    USING SQL 'SELECT floor("n" / "d")::edgedb.bigint_t;'
};


CREATE INFIX OPERATOR
std::`//` (n: std::decimal, d: std::decimal) -> std::decimal
{
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'/';
    USING SQL 'SELECT floor("n" / "d");'
};


# MODULO

CREATE INFIX OPERATOR
std::`%` (n: std::int16, d: std::int16) -> std::int16
{
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'%';
    USING SQL $$
        SELECT ((n % d) + d) % d;
    $$;
};


CREATE INFIX OPERATOR
std::`%` (n: std::int32, d: std::int32) -> std::int32
{
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'%';
    USING SQL $$
        SELECT ((n % d) + d) % d;
    $$;
};


CREATE INFIX OPERATOR
std::`%` (n: std::int64, d: std::int64) -> std::int64
{
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'%';
    USING SQL $$
        SELECT ((n % d) + d) % d;
    $$;
};


CREATE INFIX OPERATOR
std::`%` (n: std::float32, d: std::float32) -> std::float32
{
    SET volatility := 'IMMUTABLE';
    # We cheat here a bit by copying most of SQL operator metadata
    # from the `/` operator, since there is no float % in Postgres.
    USING SQL OPERATOR r'/';
    USING SQL $$
        SELECT n - floor(n / d)::float4 * d;
    $$;
};


CREATE INFIX OPERATOR
std::`%` (n: std::float64, d: std::float64) -> std::float64
{
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'/';
    USING SQL $$
        SELECT n - floor(n / d) * d;
    $$;
};


CREATE INFIX OPERATOR
std::`%` (n: std::bigint, d: std::bigint) -> std::bigint
{
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'%(numeric,numeric)';
    USING SQL $$
        SELECT (((n % d) + d) % d)::edgedb.bigint_t;
    $$;
};


CREATE INFIX OPERATOR
std::`%` (n: std::decimal, d: std::decimal) -> std::decimal
{
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'%';
    USING SQL $$
        SELECT ((n % d) + d) % d;
    $$;
};


# need an explicit operator for int64 in order to guarantee the result
# is float64 and not decimal
CREATE INFIX OPERATOR
std::`^` (n: std::int64, p: std::int64) -> std::float64
{
    SET volatility := 'IMMUTABLE';
    # We cheat here a bit by copying most of SQL operator metadata
    # from the `/` operator, since there is no int ^ in Postgres. The
    # power operator can behave like a division (negative power),
    # therefore it should have the same basic properties w.r.t. types,
    # etc. We don't use an explicit cast of the result because
    # Postgres will treat this as float8 already.
    USING SQL OPERATOR r'/';
    USING SQL 'SELECT ("n" ^ "p")';
};


CREATE INFIX OPERATOR
std::`^` (n: std::float32, p: std::float32) -> std::float32
{
    SET volatility := 'IMMUTABLE';
    # We cheat here a bit by copying most of SQL operator metadata
    # from the `/` operator, since there is no float4 ^ in Postgres.
    # The power operator can behave like a division (negative power),
    # therefore it should have the same basic properties w.r.t. types,
    # etc.
    USING SQL OPERATOR '/';
    USING SQL 'SELECT ("n" ^ "p")::float4';
};


CREATE INFIX OPERATOR
std::`^` (n: std::float64, p: std::float64) -> std::float64 {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '^';
};


CREATE INFIX OPERATOR
std::`^` (n: std::bigint, p: std::bigint) -> std::decimal {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '^(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`^` (n: std::decimal, p: std::decimal) -> std::decimal {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '^';
};


## Standard numerice casts
## -----------------------


## Implicit casts between numerics.

CREATE CAST FROM std::int16 TO std::int32 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
    ALLOW IMPLICIT;
};


CREATE CAST FROM std::int32 TO std::int64 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
    ALLOW IMPLICIT;
};


CREATE CAST FROM std::int16 TO std::float32 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
    ALLOW IMPLICIT;
};


CREATE CAST FROM std::int64 TO std::float64 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
    ALLOW IMPLICIT;
};


CREATE CAST FROM std::int64 TO std::bigint {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
    ALLOW IMPLICIT;
};


CREATE CAST FROM std::int64 TO std::decimal {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
    ALLOW IMPLICIT;
};


CREATE CAST FROM std::bigint TO std::decimal {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
    ALLOW IMPLICIT;
};


CREATE CAST FROM std::float32 TO std::float64 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
    ALLOW IMPLICIT;
};


## Explicit and assignment casts.

CREATE CAST FROM std::int32 TO std::int16 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::int64 TO std::int32 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
    ALLOW ASSIGNMENT;
};


CREATE CAST FROM std::int64 TO std::int16 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
    ALLOW ASSIGNMENT;
};


CREATE CAST FROM std::int64 TO std::float32 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
    ALLOW ASSIGNMENT;
};


CREATE CAST FROM std::float64 TO std::float32 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
    ALLOW ASSIGNMENT;
};


CREATE CAST FROM std::decimal TO std::int16 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::decimal TO std::int32 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::decimal TO std::int64 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::decimal TO std::float64 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::decimal TO std::float32 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::decimal TO std::bigint {
    SET volatility := 'IMMUTABLE';
    USING SQL 'SELECT round($1)::edgedb.bigint_t';
};


CREATE CAST FROM std::float32 TO std::int16 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::float32 TO std::int32 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::float32 TO std::int64 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::float32 TO std::bigint {
    SET volatility := 'IMMUTABLE';
    USING SQL 'SELECT round($1)::edgedb.bigint_t';
};


CREATE CAST FROM std::float32 TO std::decimal {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::float64 TO std::int16 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::float64 TO std::int32 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::float64 TO std::int64 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::float64 TO std::bigint {
    SET volatility := 'IMMUTABLE';
    USING SQL 'SELECT round($1)::edgedb.bigint_t';
};


CREATE CAST FROM std::float64 TO std::decimal {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


## String casts.

CREATE CAST FROM std::str TO std::int16 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::str TO std::int32 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::str TO std::int64 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::str TO std::float32 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::str TO std::float64 {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::str TO std::bigint {
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'edgedb.str_to_bigint';
};


CREATE CAST FROM std::str TO std::decimal {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::int16 TO std::str {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::int32 TO std::str {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::int64 TO std::str {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::float32 TO std::str {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::float64 TO std::str {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};


CREATE CAST FROM std::decimal TO std::str {
    SET volatility := 'IMMUTABLE';
    USING SQL CAST;
};
