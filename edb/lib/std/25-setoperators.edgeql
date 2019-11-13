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


## Standard set operators
## --------------------------


# The set membership operators (IN, NOT IN) are defined
# in terms of the corresponding equality operator.

CREATE INFIX OPERATOR
std::`IN` (e: anytype, s: SET OF anytype) -> std::bool
{
    USING SQL EXPRESSION;
    SET volatility := 'IMMUTABLE';
    SET derivative_of := 'std::=';
};


CREATE INFIX OPERATOR
std::`NOT IN` (e: anytype, s: SET OF anytype) -> std::bool
{
    USING SQL EXPRESSION;
    SET volatility := 'IMMUTABLE';
    SET derivative_of := 'std::!=';
};


CREATE PREFIX OPERATOR
std::`EXISTS` (s: SET OF anytype) -> bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE PREFIX OPERATOR
std::`DISTINCT` (s: SET OF anytype) -> SET OF anytype {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`UNION` (s1: SET OF anytype, s2: SET OF anytype) -> SET OF anytype {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`??` (l: OPTIONAL anytype, r: SET OF anytype) -> SET OF anytype {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE TERNARY OPERATOR
std::`IF` (if_true: SET OF anytype, condition: bool,
           if_false: SET OF anytype) -> SET OF anytype {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};
