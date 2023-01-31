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

CREATE TYPE cfg::TestSessionConfig {
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


CREATE TYPE cfg::TestInstanceConfig {
    CREATE REQUIRED PROPERTY name -> std::str {
        CREATE CONSTRAINT std::exclusive;
    };

    CREATE LINK obj -> cfg::Base;
};


CREATE SCALAR TYPE cfg::TestEnum extending enum<One, Two, Three>;


ALTER TYPE cfg::AbstractConfig {
    CREATE MULTI LINK sessobj -> cfg::TestSessionConfig {
        CREATE ANNOTATION cfg::internal := 'true';
    };
    CREATE MULTI LINK sysobj -> cfg::TestInstanceConfig {
        CREATE ANNOTATION cfg::internal := 'true';
    };

    CREATE PROPERTY __internal_testvalue -> std::int64 {
        CREATE ANNOTATION cfg::internal := 'true';
        CREATE ANNOTATION cfg::system := 'true';
        SET default := 0;
    };

    CREATE PROPERTY __internal_sess_testvalue -> std::int64 {
        CREATE ANNOTATION cfg::internal := 'true';
        SET default := 0;
    };


    CREATE PROPERTY __internal_no_const_folding -> std::bool {
        CREATE ANNOTATION cfg::internal := 'true';
        CREATE ANNOTATION cfg::affects_compilation := 'true';
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

    CREATE PROPERTY singleprop -> std::str {
        CREATE ANNOTATION cfg::internal := 'true';
        SET default := '';
    };

    CREATE PROPERTY memprop -> cfg::memory {
        CREATE ANNOTATION cfg::internal := 'true';
        SET default := <cfg::memory>'0';
    };

    CREATE PROPERTY durprop -> std::duration {
        CREATE ANNOTATION cfg::internal := 'true';
        SET default := <std::duration>'0 seconds';
    };

    CREATE PROPERTY enumprop -> cfg::TestEnum {
        CREATE ANNOTATION cfg::internal := 'true';
        SET default := cfg::TestEnum.One;
    };

    CREATE PROPERTY __pg_max_connections -> std::int64 {
        CREATE ANNOTATION cfg::internal := 'true';
        CREATE ANNOTATION cfg::backend_setting := '"max_connections"';
    };
};


# std::_gen_series

CREATE FUNCTION
std::_gen_series(
    `start`: std::int64,
    stop: std::int64
) -> SET OF std::int64
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'generate_series';
};

CREATE FUNCTION
std::_gen_series(
    `start`: std::int64,
    stop: std::int64,
    step: std::int64
) -> SET OF std::int64
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'generate_series';
};

CREATE FUNCTION
std::_gen_series(
    `start`: std::bigint,
    stop: std::bigint
) -> SET OF std::bigint
{
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL FUNCTION 'generate_series';
};

CREATE FUNCTION
std::_gen_series(
    `start`: std::bigint,
    stop: std::bigint,
    step: std::bigint
) -> SET OF std::bigint
{
    SET volatility := 'Immutable';
    SET force_return_cast := true;
    USING SQL FUNCTION 'generate_series';
};


CREATE FUNCTION
sys::_sleep(duration: std::float64) -> std::bool
{
    CREATE ANNOTATION std::description :=
        'Make the current session sleep for *duration* seconds.';
    # This function has side-effect.
    SET volatility := 'Volatile';
    USING SQL $$
    SELECT pg_sleep("duration") IS NOT NULL;
    $$;
};

CREATE FUNCTION
sys::_sleep(duration: std::duration) -> std::bool
{
    CREATE ANNOTATION std::description :=
        'Make the current session sleep for *duration* time.';
    # This function has side-effect.
    SET volatility := 'Volatile';
    USING SQL $$
    SELECT pg_sleep_for("duration") IS NOT NULL;
    $$;
};


CREATE FUNCTION
sys::_advisory_lock(key: std::int64) -> std::bool
{
    CREATE ANNOTATION std::description :=
        'Obtain an exclusive session-level advisory lock.';
    # This function has side-effect.
    SET volatility := 'Volatile';
    USING SQL $$
    SELECT CASE WHEN "key" < 0 THEN
        edgedb.raise(NULL::bool, msg => 'lock key cannot be negative')
    ELSE
        pg_advisory_lock("key") IS NOT NULL
    END;
    $$;
};


CREATE FUNCTION
sys::_advisory_unlock(key: std::int64) -> std::bool
{
    CREATE ANNOTATION std::description :=
        'Release an exclusive session-level advisory lock.';
    # This function has side-effect.
    SET volatility := 'Volatile';
    USING SQL $$
    SELECT CASE WHEN "key" < 0 THEN
        edgedb.raise(NULL::bool, msg => 'lock key cannot be negative')
    ELSE
        pg_advisory_unlock("key")
    END;
    $$;
};


CREATE FUNCTION
sys::_advisory_unlock_all() -> std::bool
{
    CREATE ANNOTATION std::description :=
        'Release all session-level advisory locks held by the current session.';
    # This function has side-effect.
    SET volatility := 'Volatile';
    USING SQL $$
    SELECT pg_advisory_unlock_all() IS NOT NULL;
    $$;
};


CREATE FUNCTION
std::_datetime_range_buckets(
    low: std::datetime,
    high: std::datetime,
    granularity: str,
) -> SET OF tuple<std::datetime, std::datetime>
{
    CREATE ANNOTATION std::description :=
        'Generate a set of datetime buckets for a given time period '
        ++ 'and a given granularity';
    # date_trunc of timestamptz is STABLE in PostgreSQL
    SET volatility := 'Stable';
    USING SQL $$
    SELECT
        lo::edgedb.timestamptz_t,
        hi::edgedb.timestamptz_t
    FROM
        (SELECT
            series AS lo,
            lead(series) OVER () AS hi
        FROM
            generate_series(
                "low",
                "high",
                "granularity"::interval
            ) AS series) AS q
    WHERE
        hi IS NOT NULL
    $$;
};

CREATE MODULE std::_test;


CREATE FUNCTION
std::_test::abs(x: std::anyreal) -> std::anyreal
{
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'abs';
};
