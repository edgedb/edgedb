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


## Regular expression functions.


CREATE FUNCTION
std::re_match(pattern: std::str, str: std::str) -> array<std::str>
{
    CREATE ANNOTATION std::description :=
        'Find the first regular expression match in a string.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT array_replace(regexp_matches("str", "pattern"), NULL, '');
    $$;
};


CREATE FUNCTION
std::re_match_all(pattern: std::str, str: std::str) -> SET OF array<std::str>
{
    CREATE ANNOTATION std::description :=
        'Find all regular expression matches in a string.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT array_replace(regexp_matches("str", "pattern", 'g'), NULL, '');
    $$;
};


CREATE FUNCTION
std::re_test(pattern: std::str, str: std::str) -> std::bool
{
    CREATE ANNOTATION std::description :=
        'Test if a regular expression has a match in a string.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT "str" ~ "pattern";
    $$;
};


CREATE FUNCTION
std::re_replace(
    pattern: std::str,
    sub: std::str,
    str: std::str,
    NAMED ONLY flags: std::str = '') -> std::str
{
    CREATE ANNOTATION std::description :=
        'Replace matching substrings in a given string.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT regexp_replace("str", "pattern", "sub", "flags");
    $$;
};
