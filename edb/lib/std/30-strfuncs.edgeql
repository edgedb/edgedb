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


## String operators

CREATE INFIX OPERATOR
std::`=` (l: std::str, r: std::str) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::str, r: OPTIONAL std::str) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::str, r: std::str) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::str, r: OPTIONAL std::str) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


# Concatenation.
CREATE INFIX OPERATOR
std::`++` (l: std::str, r: std::str) -> std::str {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '||';
};


CREATE INFIX OPERATOR
std::`LIKE` (string: std::str, pattern: std::str) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`ILIKE` (string: std::str, pattern: std::str) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`NOT LIKE` (string: std::str, pattern: std::str) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`NOT ILIKE` (string: std::str, pattern: std::str) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`<` (l: std::str, r: std::str) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::str, r: std::str) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=';
};


CREATE INFIX OPERATOR
std::`>` (l: std::str, r: std::str) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::str, r: std::str) -> std::bool {
    SET volatility := 'IMMUTABLE';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=';
};


## String functions


CREATE FUNCTION
std::str_repeat(s: std::str, n: std::int64) -> std::str
{
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT repeat("s", "n"::int4)
    $$;
};


CREATE FUNCTION
std::str_lower(s: std::str) -> std::str
{
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'lower';
};


CREATE FUNCTION
std::str_upper(s: std::str) -> std::str
{
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'upper';
};


CREATE FUNCTION
std::str_title(s: std::str) -> std::str
{
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'initcap';
};


CREATE FUNCTION
std::str_lpad(s: std::str, n: std::int64, fill: std::str=' ') -> std::str
{
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT lpad("s", "n"::int4, "fill")
    $$;
};


CREATE FUNCTION
std::str_rpad(s: std::str, n: std::int64, fill: std::str=' ') -> std::str
{
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT rpad("s", "n"::int4, "fill")
    $$;
};


CREATE FUNCTION
std::str_ltrim(s: std::str, tr: std::str=' ') -> std::str
{
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'ltrim';
};


CREATE FUNCTION
std::str_rtrim(s: std::str, tr: std::str=' ') -> std::str
{
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'rtrim';
};


CREATE FUNCTION
std::str_trim(s: std::str, tr: std::str=' ') -> std::str
{
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'btrim';
};
