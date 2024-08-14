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

# EQUALITY

CREATE INFIX OPERATOR
std::`=` (l: std::int16, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int16, r: OPTIONAL std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::int16, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int16, r: OPTIONAL std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::int16, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int16, r: OPTIONAL std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::int32, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int32, r: OPTIONAL std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::int32, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int32, r: OPTIONAL std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::int32, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int32, r: OPTIONAL std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::int64, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int64, r: OPTIONAL std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::int64, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int64, r: OPTIONAL std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::int64, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::int64, r: OPTIONAL std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::float32, r: std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::float32, r: OPTIONAL std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::float32, r: std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::float32, r: OPTIONAL std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::float64, r: std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::float64, r: OPTIONAL std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::float64, r: std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::float64, r: OPTIONAL std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::bigint, r: std::bigint) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`=` (l: std::decimal, r: std::decimal) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`=` (l: std::decimal, r: std::anyint) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::bigint, r: OPTIONAL std::bigint) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::decimal, r: OPTIONAL std::decimal) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::decimal, r: OPTIONAL std::anyint) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::anyint, r: std::decimal) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::anyint, r: OPTIONAL std::decimal) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


# INEQUALITY

CREATE INFIX OPERATOR
std::`!=` (l: std::int16, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int16, r: OPTIONAL std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::int16, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int16, r: OPTIONAL std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::int16, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int16, r: OPTIONAL std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::int32, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int32, r: OPTIONAL std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::int32, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int32, r: OPTIONAL std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::int32, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int32, r: OPTIONAL std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::int64, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int64, r: OPTIONAL std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::int64, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int64, r: OPTIONAL std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::int64, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::int64, r: OPTIONAL std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::float32, r: std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::float32, r: OPTIONAL std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::float32, r: std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::float32, r: OPTIONAL std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::float64, r: std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::float64, r: OPTIONAL std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::float64, r: std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::float64, r: OPTIONAL std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::bigint, r: std::bigint) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`!=` (l: std::decimal, r: std::decimal) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`!=` (l: std::decimal, r: std::anyint) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::bigint, r: OPTIONAL std::bigint) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::decimal, r: OPTIONAL std::decimal) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::decimal, r: OPTIONAL std::anyint) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::anyint, r: std::decimal) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::anyint, r: OPTIONAL std::decimal) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};



# GREATER THAN

CREATE INFIX OPERATOR
std::`>` (l: std::int16, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int16, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int16, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int32, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int32, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int32, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int32, r: std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR '>(float8,float8)';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int64, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int64, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int64, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::int64, r: std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>(float8,float8)';
};


CREATE INFIX OPERATOR
std::`>` (l: std::float32, r: std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::float32, r: std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::float32, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR '>(float8,float8)';
};


CREATE INFIX OPERATOR
std::`>` (l: std::float64, r: std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::float64, r: std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::float64, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>(float8,float8)';
};


CREATE INFIX OPERATOR
std::`>` (l: std::bigint, r: std::bigint) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`>` (l: std::decimal, r: std::decimal) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::anyint, r: std::decimal) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>` (l: std::decimal, r: std::anyint) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


# GREATER OR EQUAL

CREATE INFIX OPERATOR
std::`>=` (l: std::int16, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int16, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int16, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int32, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int32, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int32, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int32, r: std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR '>=(float8,float8)';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int64, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int64, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int64, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::int64, r: std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=(float8,float8)';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::float32, r: std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::float32, r: std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::float32, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR '>=(float8,float8)';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::float64, r: std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::float64, r: std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::float64, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=(float8,float8)';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::bigint, r: std::bigint) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::decimal, r: std::decimal) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::anyint, r: std::decimal) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::decimal, r: std::anyint) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


# LESS THAN

CREATE INFIX OPERATOR
std::`<` (l: std::int16, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int16, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int16, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int32, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int32, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int32, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int32, r: std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR '<(float8,float8)';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int64, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int64, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int64, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::int64, r: std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<(float8,float8)';
};


CREATE INFIX OPERATOR
std::`<` (l: std::float32, r: std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::float32, r: std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::float32, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR '<(float8,float8)';
};


CREATE INFIX OPERATOR
std::`<` (l: std::float64, r: std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::float64, r: std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::float64, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<(float8,float8)';
};


CREATE INFIX OPERATOR
std::`<` (l: std::bigint, r: std::bigint) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`<` (l: std::decimal, r: std::decimal) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::anyint, r: std::decimal) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<` (l: std::decimal, r: std::anyint) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


# LESS THAN OR EQUAL

CREATE INFIX OPERATOR
std::`<=` (l: std::int16, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int16, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int16, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int32, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int32, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int32, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int32, r: std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR '<=(float8,float8)';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int64, r: std::int16) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int64, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int64, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::int64, r: std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=(float8,float8)';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::float32, r: std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::float32, r: std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::float32, r: std::int32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR '<=(float8,float8)';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::float64, r: std::float32) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::float64, r: std::float64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::float64, r: std::int64) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=(float8,float8)';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::bigint, r: std::bigint) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::decimal, r: std::decimal) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::anyint, r: std::decimal) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::decimal, r: std::anyint) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


# INFIX PLUS

CREATE INFIX OPERATOR
std::`+` (l: std::int16, r: std::int16) -> std::int16 {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Arithmetic addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`+` (l: std::int32, r: std::int32) -> std::int32 {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Arithmetic addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`+` (l: std::int64, r: std::int64) -> std::int64 {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Arithmetic addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`+` (l: std::float32, r: std::float32) -> std::float32 {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Arithmetic addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`+` (l: std::float64, r: std::float64) -> std::float64 {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Arithmetic addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+';
};


CREATE INFIX OPERATOR
std::`+` (l: std::bigint, r: std::bigint) -> std::bigint {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Arithmetic addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    SET force_return_cast := true;
    USING SQL OPERATOR r'+(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`+` (l: std::decimal, r: std::decimal) -> std::decimal {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Arithmetic addition.';
    SET volatility := 'Immutable';
    SET commutator := 'std::+';
    USING SQL OPERATOR r'+';
};


# PREFIX PLUS

CREATE PREFIX OPERATOR
std::`+` (l: std::int16) -> std::int16 {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Arithmetic addition.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'+';
};


CREATE PREFIX OPERATOR
std::`+` (l: std::int32) -> std::int32 {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Arithmetic addition.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'+';
};


CREATE PREFIX OPERATOR
std::`+` (l: std::int64) -> std::int64 {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Arithmetic addition.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'+';
};


CREATE PREFIX OPERATOR
std::`+` (l: std::float32) -> std::float32 {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Arithmetic addition.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'+';
};


CREATE PREFIX OPERATOR
std::`+` (l: std::float64) -> std::float64 {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Arithmetic addition.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'+';
};


CREATE PREFIX OPERATOR
std::`+` (l: std::bigint) -> std::bigint {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Arithmetic addition.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL OPERATOR r'+(,numeric)';
};


CREATE PREFIX OPERATOR
std::`+` (l: std::decimal) -> std::decimal {
    CREATE ANNOTATION std::identifier := 'plus';
    CREATE ANNOTATION std::description := 'Arithmetic addition.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'+';
};



# INFIX MINUS

CREATE INFIX OPERATOR
std::`-` (l: std::int16, r: std::int16) -> std::int16 {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Arithmetic subtraction.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`-` (l: std::int32, r: std::int32) -> std::int32 {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Arithmetic subtraction.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`-` (l: std::int64, r: std::int64) -> std::int64 {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Arithmetic subtraction.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`-` (l: std::float32, r: std::float32) -> std::float32 {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Arithmetic subtraction.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`-` (l: std::float64, r: std::float64) -> std::float64 {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Arithmetic subtraction.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-';
};


CREATE INFIX OPERATOR
std::`-` (l: std::bigint, r: std::bigint) -> std::bigint {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Arithmetic subtraction.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL OPERATOR r'-(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`-` (l: std::decimal, r: std::decimal) -> std::decimal {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Arithmetic subtraction.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-';
};


# PREFIX MINUS

CREATE PREFIX OPERATOR
std::`-` (l: std::int16) -> std::int16 {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Arithmetic subtraction.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-';
};


CREATE PREFIX OPERATOR
std::`-` (l: std::int32) -> std::int32 {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Arithmetic subtraction.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-';
};


CREATE PREFIX OPERATOR
std::`-` (l: std::int64) -> std::int64 {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Arithmetic subtraction.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-';
};


CREATE PREFIX OPERATOR
std::`-` (l: std::float32) -> std::float32 {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Arithmetic subtraction.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-';
};


CREATE PREFIX OPERATOR
std::`-` (l: std::float64) -> std::float64 {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Arithmetic subtraction.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-';
};


CREATE PREFIX OPERATOR
std::`-` (l: std::bigint) -> std::bigint {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Arithmetic subtraction.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL OPERATOR r'-(,numeric)';
};


CREATE PREFIX OPERATOR
std::`-` (l: std::decimal) -> std::decimal {
    CREATE ANNOTATION std::identifier := 'minus';
    CREATE ANNOTATION std::description := 'Arithmetic subtraction.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'-';
};


# MUL

CREATE INFIX OPERATOR
std::`*` (l: std::int16, r: std::int16) -> std::int16 {
    CREATE ANNOTATION std::identifier := 'mult';
    CREATE ANNOTATION std::description := 'Arithmetic multiplication.';
    SET volatility := 'Immutable';
    SET commutator := 'std::*';
    USING SQL OPERATOR r'*';
};


CREATE INFIX OPERATOR
std::`*` (l: std::int32, r: std::int32) -> std::int32 {
    CREATE ANNOTATION std::identifier := 'mult';
    CREATE ANNOTATION std::description := 'Arithmetic multiplication.';
    SET volatility := 'Immutable';
    SET commutator := 'std::*';
    USING SQL OPERATOR r'*';
};


CREATE INFIX OPERATOR
std::`*` (l: std::int64, r: std::int64) -> std::int64 {
    CREATE ANNOTATION std::identifier := 'mult';
    CREATE ANNOTATION std::description := 'Arithmetic multiplication.';
    SET volatility := 'Immutable';
    SET commutator := 'std::*';
    USING SQL OPERATOR r'*';
};


CREATE INFIX OPERATOR
std::`*` (l: std::float32, r: std::float32) -> std::float32 {
    CREATE ANNOTATION std::identifier := 'mult';
    CREATE ANNOTATION std::description := 'Arithmetic multiplication.';
    SET volatility := 'Immutable';
    SET commutator := 'std::*';
    USING SQL OPERATOR r'*';
};


CREATE INFIX OPERATOR
std::`*` (l: std::float64, r: std::float64) -> std::float64 {
    CREATE ANNOTATION std::identifier := 'mult';
    CREATE ANNOTATION std::description := 'Arithmetic multiplication.';
    SET volatility := 'Immutable';
    SET commutator := 'std::*';
    USING SQL OPERATOR r'*';
};


CREATE INFIX OPERATOR
std::`*` (l: std::bigint, r: std::bigint) -> std::bigint {
    CREATE ANNOTATION std::identifier := 'mult';
    CREATE ANNOTATION std::description := 'Arithmetic multiplication.';
    SET volatility := 'Immutable';
    SET commutator := 'std::*';
    SET force_return_cast := true;
    USING SQL OPERATOR r'*(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`*` (l: std::decimal, r: std::decimal) -> std::decimal {
    CREATE ANNOTATION std::identifier := 'mult';
    CREATE ANNOTATION std::description := 'Arithmetic multiplication.';
    SET volatility := 'Immutable';
    SET commutator := 'std::*';
    USING SQL OPERATOR r'*';
};


# DIV

CREATE INFIX OPERATOR
std::`/` (l: std::int64, r: std::int64) -> std::float64 {
    CREATE ANNOTATION std::identifier := 'div';
    CREATE ANNOTATION std::description := 'Arithmetic division.';
    SET volatility := 'Immutable';
    # We need both USING SQL OPERATOR and USING SQL to copy
    # the common attributes of the SQL division operator while
    # overriding the main operator function.
    USING SQL OPERATOR r'/';
    USING SQL 'SELECT "l" / ("r"::float8)';
};


CREATE INFIX OPERATOR
std::`/` (l: std::float32, r: std::float32) -> std::float32 {
    CREATE ANNOTATION std::identifier := 'div';
    CREATE ANNOTATION std::description := 'Arithmetic division.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'/';
};


CREATE INFIX OPERATOR
std::`/` (l: std::float64, r: std::float64) -> std::float64 {
    CREATE ANNOTATION std::identifier := 'div';
    CREATE ANNOTATION std::description := 'Arithmetic division.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'/';
};


CREATE INFIX OPERATOR
std::`/` (l: std::decimal, r: std::decimal) -> std::decimal {
    CREATE ANNOTATION std::identifier := 'div';
    CREATE ANNOTATION std::description := 'Arithmetic division.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'/';
};


# FLOORDIV

# PostgreSQL uses truncation division, so the -12 % 5 is -2, because -12 // 5
# is -2, but EdgeQL uses floor division, so -12 // 5 is -3, and so -12 % 5
# must be 3. The correct divmod behavior is implemented via the floor
# function working specifically with numeric type. The numeric value needs to
# be forced into using arbitrary precision by getting multiplied by
# 1.0::numeric.

CREATE INFIX OPERATOR
std::`//` (n: std::int16, d: std::int16) -> std::int16
{
    CREATE ANNOTATION std::identifier := 'floordiv';
    CREATE ANNOTATION std::description :=
        'Floor division. Result is rounded down to the nearest integer';
    SET volatility := 'Immutable';
    # We need both USING SQL OPERATOR and USING SQL FUNCTION to copy
    # the common attributes of the SQL division operator while
    # overriding the main operator function.
    USING SQL OPERATOR r'/';
    USING SQL
        'SELECT floor(1.0::numeric * "n"::numeric / "d"::numeric)::int2';
};


CREATE INFIX OPERATOR
std::`//` (n: std::int32, d: std::int32) -> std::int32
{
    CREATE ANNOTATION std::identifier := 'floordiv';
    CREATE ANNOTATION std::description :=
        'Floor division. Result is rounded down to the nearest integer';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'/';
    USING SQL
        'SELECT floor(1.0::numeric * "n"::numeric / "d"::numeric)::int4';
};


CREATE INFIX OPERATOR
std::`//` (n: std::int64, d: std::int64) -> std::int64
{
    CREATE ANNOTATION std::identifier := 'floordiv';
    CREATE ANNOTATION std::description :=
        'Floor division. Result is rounded down to the nearest integer';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'/';
    USING SQL
        'SELECT floor(1.0::numeric * "n"::numeric / "d"::numeric)::int8';
};


CREATE INFIX OPERATOR
std::`//` (n: std::float32, d: std::float32) -> std::float32
{
    CREATE ANNOTATION std::identifier := 'floordiv';
    CREATE ANNOTATION std::description :=
        'Floor division. Result is rounded down to the nearest integer';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'/';
    USING SQL 'SELECT floor("n" / "d")::float4';
};


CREATE INFIX OPERATOR
std::`//` (n: std::float64, d: std::float64) -> std::float64
{
    CREATE ANNOTATION std::identifier := 'floordiv';
    CREATE ANNOTATION std::description :=
        'Floor division. Result is rounded down to the nearest integer';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'/';
    USING SQL 'SELECT floor("n" / "d")';
};


CREATE INFIX OPERATOR
std::`//` (n: std::bigint, d: std::bigint) -> std::bigint
{
    CREATE ANNOTATION std::identifier := 'floordiv';
    CREATE ANNOTATION std::description :=
        'Floor division. Result is rounded down to the nearest integer';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'/(numeric,numeric)';
    USING SQL $$
        SELECT floor(
            1.0::numeric * "n"::numeric / "d"::numeric
        )::edgedbt.bigint_t;
    $$;
};


CREATE INFIX OPERATOR
std::`//` (n: std::decimal, d: std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::identifier := 'floordiv';
    CREATE ANNOTATION std::description :=
        'Floor division. Result is rounded down to the nearest integer';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'/';
    USING SQL 'SELECT floor("n" / "d");'
};


# MODULO

# We have 2 issues to deal with:
# 1) Postgres will produce a negative remainder for a posisitve divisor,
#    whereas generally it's a bit more intuitive to have the remainder in the
#    range [0, divisor).
# 2) When implementing the modulo operator we need to make sure that addition
#    or subtraction doesn't cause an overflow.
#
# The easiest way to avoid overflow errors is to upcast values to a larger
# integer type. However, upcasting int64 to bigint and back is very slow
# (5x-6x slower), so we need a different approach here.
#
# The breakdown is like this:
# - We only want to add `d` if `n` and `d` have opposite signs.
# - XOR helps to isolate the sign bit if it is different.
# - Right arithmetic shift by 63 bits produces an "all 1" bitmask for
#   negative integers and 0 otherwise.
# - Performing AND using the above bitmask makes `d` go away if
#   `sign(n) = sign(d)` and keeps it as is otherwise.
# - Finally we want to perform another MOD `d` operation to address the corner
#   case of 10 % -5 = -5 instead of 0 (which is equivalent, but does not
#   conform to making 0 inclusive and `d` itself exclusive).
#
# According to our microbenchmarks this kind of bit magic is no worse and
# maybe slightly better than upcasting for int16 and int32 cases.

CREATE INFIX OPERATOR
std::`%` (n: std::int16, d: std::int16) -> std::int16
{
    CREATE ANNOTATION std::identifier := 'mod';
    CREATE ANNOTATION std::description := 'Remainder from division (modulo).';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'%';
    USING SQL $$
        SELECT (
            (n % d)
            +
            (d & ((n # d)>>15::int4))
        ) % d
    $$;
};


CREATE INFIX OPERATOR
std::`%` (n: std::int32, d: std::int32) -> std::int32
{
    CREATE ANNOTATION std::identifier := 'mod';
    CREATE ANNOTATION std::description := 'Remainder from division (modulo).';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'%';
    USING SQL $$
        SELECT (
            (n % d)
            +
            (d & ((n # d)>>31::int4))
        ) % d
    $$;
};


CREATE INFIX OPERATOR
std::`%` (n: std::int64, d: std::int64) -> std::int64
{
    CREATE ANNOTATION std::identifier := 'mod';
    CREATE ANNOTATION std::description := 'Remainder from division (modulo).';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'%';
    USING SQL $$
        SELECT (
            (n % d)
            +
            (d & ((n # d)>>63::int4))
        ) % d
    $$;
};


CREATE INFIX OPERATOR
std::`%` (n: std::float32, d: std::float32) -> std::float32
{
    CREATE ANNOTATION std::identifier := 'mod';
    CREATE ANNOTATION std::description := 'Remainder from division (modulo).';
    SET volatility := 'Immutable';
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
    CREATE ANNOTATION std::identifier := 'mod';
    CREATE ANNOTATION std::description := 'Remainder from division (modulo).';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'/';
    USING SQL $$
        SELECT n - floor(n / d) * d;
    $$;
};


CREATE INFIX OPERATOR
std::`%` (n: std::bigint, d: std::bigint) -> std::bigint
{
    CREATE ANNOTATION std::identifier := 'mod';
    CREATE ANNOTATION std::description := 'Remainder from division (modulo).';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'%(numeric,numeric)';
    USING SQL $$
        SELECT (((n % d) + d) % d)::edgedbt.bigint_t;
    $$;
};


CREATE INFIX OPERATOR
std::`%` (n: std::decimal, d: std::decimal) -> std::decimal
{
    CREATE ANNOTATION std::identifier := 'mod';
    CREATE ANNOTATION std::description := 'Remainder from division (modulo).';
    SET volatility := 'Immutable';
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
    CREATE ANNOTATION std::identifier := 'pow';
    CREATE ANNOTATION std::description := 'Power operation.';
    SET volatility := 'Immutable';
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
    CREATE ANNOTATION std::identifier := 'pow';
    CREATE ANNOTATION std::description := 'Power operation.';
    SET volatility := 'Immutable';
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
    CREATE ANNOTATION std::identifier := 'pow';
    CREATE ANNOTATION std::description := 'Power operation.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR '^';
};


CREATE INFIX OPERATOR
std::`^` (n: std::bigint, p: std::bigint) -> std::decimal {
    CREATE ANNOTATION std::identifier := 'pow';
    CREATE ANNOTATION std::description := 'Power operation.';
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL OPERATOR '^(numeric,numeric)';
};


CREATE INFIX OPERATOR
std::`^` (n: std::decimal, p: std::decimal) -> std::decimal {
    CREATE ANNOTATION std::identifier := 'pow';
    CREATE ANNOTATION std::description := 'Power operation.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR '^';
};


## Standard numeric casts
## ----------------------


## Implicit casts between numerics.

CREATE CAST FROM std::int16 TO std::int32 {
    SET volatility := 'Immutable';
    USING SQL CAST;
    ALLOW IMPLICIT;
};


CREATE CAST FROM std::int32 TO std::int64 {
    SET volatility := 'Immutable';
    USING SQL CAST;
    ALLOW IMPLICIT;
};


CREATE CAST FROM std::int16 TO std::float32 {
    SET volatility := 'Immutable';
    USING SQL CAST;
    ALLOW IMPLICIT;
};


CREATE CAST FROM std::int64 TO std::float64 {
    SET volatility := 'Immutable';
    USING SQL CAST;
    ALLOW IMPLICIT;
};


CREATE CAST FROM std::int64 TO std::bigint {
    SET volatility := 'Immutable';
    USING SQL CAST;
    ALLOW IMPLICIT;
};


CREATE CAST FROM std::int64 TO std::decimal {
    SET volatility := 'Immutable';
    USING SQL CAST;
    ALLOW IMPLICIT;
};


CREATE CAST FROM std::bigint TO std::decimal {
    SET volatility := 'Immutable';
    USING SQL CAST;
    ALLOW IMPLICIT;
};


CREATE CAST FROM std::float32 TO std::float64 {
    SET volatility := 'Immutable';
    USING SQL CAST;
    ALLOW IMPLICIT;
};


## Explicit and assignment casts.

CREATE CAST FROM std::int32 TO std::int16 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::int64 TO std::int32 {
    SET volatility := 'Immutable';
    USING SQL CAST;
    ALLOW ASSIGNMENT;
};


CREATE CAST FROM std::int64 TO std::int16 {
    SET volatility := 'Immutable';
    USING SQL CAST;
    ALLOW ASSIGNMENT;
};


CREATE CAST FROM std::int64 TO std::float32 {
    SET volatility := 'Immutable';
    USING SQL CAST;
    ALLOW ASSIGNMENT;
};


CREATE CAST FROM std::float64 TO std::float32 {
    SET volatility := 'Immutable';
    USING SQL CAST;
    ALLOW ASSIGNMENT;
};


CREATE CAST FROM std::decimal TO std::int16 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::decimal TO std::int32 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::decimal TO std::int64 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::decimal TO std::float64 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::decimal TO std::float32 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::decimal TO std::bigint {
    SET volatility := 'Immutable';
    USING SQL 'SELECT round($1)::edgedbt.bigint_t';
};


CREATE CAST FROM std::float32 TO std::int16 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::float32 TO std::int32 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::float32 TO std::int64 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::float32 TO std::bigint {
    SET volatility := 'Immutable';
    USING SQL 'SELECT round($1)::edgedbt.bigint_t';
};


CREATE CAST FROM std::float32 TO std::decimal {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT
            (CASE WHEN val != 'NaN'
                       AND val != 'Infinity'
                       AND val != '-Infinity'
            THEN
                val::numeric
            WHEN val IS NULL
            THEN
                NULL::numeric
            ELSE
                edgedb_VER.raise(
                    NULL::numeric,
                    'invalid_text_representation',
                    msg => 'invalid value for numeric: ' || quote_literal(val)
                )
            END)
        ;
    $$;
};


CREATE CAST FROM std::float64 TO std::int16 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::float64 TO std::int32 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::float64 TO std::int64 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::float64 TO std::bigint {
    SET volatility := 'Immutable';
    USING SQL 'SELECT round($1)::edgedbt.bigint_t';
};


CREATE CAST FROM std::float64 TO std::decimal {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT
            (CASE WHEN val != 'NaN'
                       AND val != 'Infinity'
                       AND val != '-Infinity'
            THEN
                val::numeric
            WHEN val IS NULL
            THEN
                NULL::numeric
            ELSE
                edgedb_VER.raise(
                    NULL::numeric,
                    'invalid_text_representation',
                    msg => 'invalid value for numeric: ' || quote_literal(val)
                )
            END)
        ;
    $$;
};


## String casts.

CREATE CAST FROM std::str TO std::int16 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::str TO std::int32 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::str TO std::int64 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::str TO std::float32 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::str TO std::float64 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::str TO std::bigint {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'edgedb.str_to_bigint';
};


CREATE CAST FROM std::str TO std::decimal {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'edgedb.str_to_decimal';
};


CREATE CAST FROM std::int16 TO std::str {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::int32 TO std::str {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::int64 TO std::str {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::float32 TO std::str {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::float64 TO std::str {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::decimal TO std::str {
    SET volatility := 'Immutable';
    USING SQL CAST;
};
