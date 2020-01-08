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

# Bits used for testing of the std-only functionality.
# These definitions are picked up if the EdgeDB instance is bootstrapped
# with --testmode.

CREATE TYPE cfg::SessionConfig {
    CREATE REQUIRED PROPERTY name -> std::str {
        CREATE CONSTRAINT std::exclusive;
    }
};


CREATE ABSTRACT TYPE cfg::Base {
    CREATE REQUIRED PROPERTY name -> std::str
};


CREATE TYPE cfg::Subclass1 EXTENDING cfg::Base {
    CREATE REQUIRED PROPERTY sub1 -> std::str;
};


CREATE TYPE cfg::Subclass2 EXTENDING cfg::Base {
    CREATE REQUIRED PROPERTY sub2 -> std::str;
};


CREATE TYPE cfg::SystemConfig {
    CREATE REQUIRED PROPERTY name -> std::str {
        CREATE CONSTRAINT std::exclusive;
    };

    CREATE LINK obj -> cfg::Base;
};


ALTER TYPE cfg::Config {
    CREATE MULTI LINK sessobj -> cfg::SessionConfig;
    CREATE MULTI LINK sysobj -> cfg::SystemConfig;

    CREATE PROPERTY __internal_testvalue -> std::int64 {
        CREATE ANNOTATION cfg::internal := 'true';
        CREATE ANNOTATION cfg::system := 'true';
        SET default := 0;
    };

    CREATE PROPERTY __internal_no_const_folding -> std::bool {
        CREATE ANNOTATION cfg::internal := 'true';
        SET default := false;
    };

    CREATE PROPERTY __internal_testmode -> std::bool {
        CREATE ANNOTATION cfg::internal := 'true';
        SET default := false;
    };

    CREATE PROPERTY __internal_restart -> std::bool {
        CREATE ANNOTATION cfg::internal := 'true';
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION cfg::requires_restart := 'true';
        SET default := false;
    };

    CREATE MULTI PROPERTY multiprop -> std::str {
        CREATE ANNOTATION cfg::internal := 'true';
    };
};


# std::_gen_series

CREATE FUNCTION
std::_gen_series(
    `start`: std::int64,
    stop: std::int64
) -> SET OF std::int64
{
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'generate_series';
};

CREATE FUNCTION
std::_gen_series(
    `start`: std::int64,
    stop: std::int64,
    step: std::int64
) -> SET OF std::int64
{
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'generate_series';
};

CREATE FUNCTION
std::_gen_series(
    `start`: std::bigint,
    stop: std::bigint
) -> SET OF std::bigint
{
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'generate_series';
};

CREATE FUNCTION
std::_gen_series(
    `start`: std::bigint,
    stop: std::bigint,
    step: std::bigint
) -> SET OF std::bigint
{
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'generate_series';
};
