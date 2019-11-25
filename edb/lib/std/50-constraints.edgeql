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


## Standard constraints.


CREATE FUNCTION
std::_is_exclusive(s: SET OF anytype) -> std::bool
{
    SET volatility := 'IMMUTABLE';
    SET initial_value := True;
    USING SQL EXPRESSION;
};


CREATE ABSTRACT CONSTRAINT
std::constraint
{
    SET errmessage := 'invalid {__subject__}';
};


CREATE ABSTRACT CONSTRAINT
std::expression EXTENDING std::constraint
{
    USING (__subject__);
};


CREATE ABSTRACT CONSTRAINT
std::exclusive EXTENDING std::constraint
{
    SET errmessage := '{__subject__} violates exclusivity constraint';
    USING (std::_is_exclusive(__subject__));
};


CREATE ABSTRACT CONSTRAINT
std::one_of(VARIADIC vals: anytype) EXTENDING std::constraint
{
    SET errmessage := '{__subject__} must be one of: {vals}.';
    USING (contains(vals, __subject__));
};


CREATE ABSTRACT CONSTRAINT
std::len_value ON (len(<std::str>__subject__)) EXTENDING std::constraint
{
    SET errmessage := 'invalid {__subject__}';
};


CREATE ABSTRACT CONSTRAINT
std::max_value(max: anytype) EXTENDING std::constraint
{
    SET errmessage := 'Maximum allowed value for {__subject__} is {max}.';
    USING (__subject__ <= max);
};


CREATE ABSTRACT CONSTRAINT
std::min_value(min: anytype) EXTENDING std::constraint
{
    SET errmessage := 'Minimum allowed value for {__subject__} is {min}.';
    USING (__subject__ >= min);
};


CREATE ABSTRACT CONSTRAINT
std::max_ex_value(max: anytype) EXTENDING std::max_value
{
    SET errmessage := '{__subject__} must be less than {max}.';
};


CREATE ABSTRACT CONSTRAINT
std::min_ex_value(min: anytype) EXTENDING std::min_value
{
    SET errmessage := '{__subject__} must be greater than {min}.';
    USING (__subject__ > min);
};


CREATE ABSTRACT CONSTRAINT
std::regexp(pattern: std::str) EXTENDING std::constraint
{
    SET errmessage := 'invalid {__subject__}';
    USING (re_test(pattern, __subject__));
};


CREATE ABSTRACT CONSTRAINT
std::max_len_value(max: std::int64) EXTENDING std::max_value, std::len_value
{
    SET errmessage := '{__subject__} must be no longer than {max} characters.';
};


CREATE ABSTRACT CONSTRAINT
std::min_len_value(min: std::int64) EXTENDING std::min_value, std::len_value
{
    SET errmessage :=
        '{__subject__} must be no shorter than {min} characters.';
};
