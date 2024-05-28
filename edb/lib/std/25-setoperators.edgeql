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
    CREATE ANNOTATION std::identifier := 'in';
    CREATE ANNOTATION std::description :=
        'Test the membership of an element in a set.';
    USING SQL EXPRESSION;
    SET volatility := 'Immutable';
    SET derivative_of := 'std::=';
    SET is_singleton_set_of := true;
};


CREATE INFIX OPERATOR
std::`NOT IN` (e: anytype, s: SET OF anytype) -> std::bool
{
    CREATE ANNOTATION std::identifier := 'not_in';
    CREATE ANNOTATION std::description :=
        'Test the membership of an element in a set.';
    USING SQL EXPRESSION;
    SET volatility := 'Immutable';
    SET derivative_of := 'std::!=';
    SET is_singleton_set_of := true;
};


CREATE PREFIX OPERATOR
std::`EXISTS` (s: SET OF anytype) -> bool {
    CREATE ANNOTATION std::identifier := 'exists';
    CREATE ANNOTATION std::description := 'Test whether a set is not empty.';
    SET volatility := 'Immutable';
    SET is_singleton_set_of := true;
    USING SQL EXPRESSION;
};


CREATE PREFIX OPERATOR
std::`DISTINCT` (s: SET OF anytype) -> SET OF anytype {
    CREATE ANNOTATION std::identifier := 'distinct';
    CREATE ANNOTATION std::description :=
        'Return a set without repeating any elements.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`UNION` (s1: SET OF anytype, s2: SET OF anytype) -> SET OF anytype {
    CREATE ANNOTATION std::identifier := 'union';
    CREATE ANNOTATION std::description := 'Merge two sets.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`EXCEPT` (s1: SET OF anytype, s2: SET OF anytype) -> SET OF anytype {
    CREATE ANNOTATION std::identifier := 'except';
    CREATE ANNOTATION std::description := 'Multiset difference.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`INTERSECT` (s1: SET OF anytype, s2: SET OF anytype) -> SET OF anytype {
    CREATE ANNOTATION std::identifier := 'intersect';
    CREATE ANNOTATION std::description := 'Multiset intersection.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`??` (l: OPTIONAL anytype, r: SET OF anytype) -> SET OF anytype {
    CREATE ANNOTATION std::identifier := 'coalesce';
    CREATE ANNOTATION std::description := 'Coalesce.';
    SET volatility := 'Immutable';
    SET is_singleton_set_of := true;
    USING SQL EXPRESSION;
};


CREATE TERNARY OPERATOR
std::`IF` (if_true: SET OF anytype, condition: bool,
           if_false: SET OF anytype) -> SET OF anytype {
    CREATE ANNOTATION std::identifier := 'if_else';
    CREATE ANNOTATION std::description :=
        'Conditionally provide one or the other result.';
    SET volatility := 'Immutable';
    SET is_singleton_set_of := true;
    USING SQL EXPRESSION;
};
