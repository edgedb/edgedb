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


## Standard boolean operators
## --------------------------


# Unlike SQL, EdgeQL does not have the three-valued boolean logic,
# and boolean operators must obey the same rules as all other
# operators: they must yield an empty set if any of the operands
# is an empty set, while in SQL `(True OR NULL) IS True`.
# To achieve this, we convert the boolean op into an equivalent
# bitwise OR expression (the shortest and fastest equivalent):
#
#    a OR b --> (a::int | b::int)::bool
#
# This transformation may break bitmap index scan optimization
# when inside a WHERE clause, so we must use the original
# boolean expression in conjunction.
CREATE INFIX OPERATOR
std::`OR` (a: std::bool, b: std::bool) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL $$
    SELECT ("a" OR "b") AND ("a"::int | "b"::int)::bool
    $$;
};


# `FROM SQL EXPRESSION` means that the operator is translated
# by the compiler into some SQL expression.
CREATE INFIX OPERATOR
std::`AND` (a: std::bool, b: std::bool) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL EXPRESSION;
};


CREATE PREFIX OPERATOR
std::`NOT` (v: std::bool) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`=` (l: std::bool, r: std::bool) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::bool, r: OPTIONAL std::bool) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::bool, r: std::bool) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::bool, r: OPTIONAL std::bool) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>=` (l: std::bool, r: std::bool) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR '>=';
};


CREATE INFIX OPERATOR
std::`>` (l: std::bool, r: std::bool) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR '>';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::bool, r: std::bool) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR '<=';
};


CREATE INFIX OPERATOR
std::`<` (l: std::bool, r: std::bool) -> std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL OPERATOR '<';
};


## Boolean casts
## -------------

CREATE CAST FROM std::str TO std::bool {
    SET volatility := 'IMMUTABLE';
    FROM SQL FUNCTION 'edgedb.str_to_bool';
};


CREATE CAST FROM std::bool TO std::str {
    SET volatility := 'IMMUTABLE';
    FROM SQL CAST;
};
