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


CREATE MODULE sys;


CREATE SCALAR TYPE sys::transaction_isolation_t
    EXTENDING enum<'REPEATABLE READ', 'SERIALIZABLE'>;


CREATE TYPE sys::Database {
    CREATE REQUIRED PROPERTY name -> std::str {
        SET readonly := True;
    };
};


CREATE TYPE sys::Role {
    CREATE REQUIRED PROPERTY name -> std::str {
        CREATE CONSTRAINT std::exclusive;
    };

    CREATE REQUIRED PROPERTY allow_login -> std::bool;
    CREATE REQUIRED PROPERTY is_superuser -> std::bool;
    CREATE PROPERTY password -> std::str;
};


ALTER TYPE sys::Role {
    CREATE MULTI LINK member_of -> sys::Role;
};


CREATE FUNCTION
sys::sleep(duration: std::float64) -> std::bool
{
    # This function has side-effect.
    SET volatility := 'VOLATILE';
    SET session_only := True;
    USING SQL $$
    SELECT pg_sleep("duration") IS NOT NULL;
    $$;
};


CREATE FUNCTION
sys::sleep(duration: std::duration) -> std::bool
{
    # This function has side-effect.
    SET volatility := 'VOLATILE';
    SET session_only := True;
    USING SQL $$
    SELECT pg_sleep_for("duration") IS NOT NULL;
    $$;
};


CREATE FUNCTION
sys::advisory_lock(key: std::int64) -> std::bool
{
    # This function has side-effect.
    SET volatility := 'VOLATILE';
    SET session_only := True;
    USING SQL $$
    SELECT CASE WHEN "key" < 0 THEN
        edgedb._raise_exception('lock key cannot be negative', NULL::bool)
    ELSE
        pg_advisory_lock("key") IS NOT NULL
    END;
    $$;
};


CREATE FUNCTION
sys::advisory_unlock(key: std::int64) -> std::bool
{
    # This function has side-effect.
    SET volatility := 'VOLATILE';
    SET session_only := True;
    USING SQL $$
    SELECT CASE WHEN "key" < 0 THEN
        edgedb._raise_exception('lock key cannot be negative', NULL::bool)
    ELSE
        pg_advisory_unlock("key")
    END;
    $$;
};


CREATE FUNCTION
sys::advisory_unlock_all() -> std::bool
{
    # This function has side-effect.
    SET volatility := 'VOLATILE';
    SET session_only := True;
    USING SQL $$
    SELECT pg_advisory_unlock_all() IS NOT NULL;
    $$;
};


CREATE SCALAR TYPE sys::version_stage
    EXTENDING enum<'dev', 'alpha', 'beta', 'rc', 'final'>;


# An intermediate function is needed because we can't
# cast JSON to tuples yet.  DO NOT use directly, it'll go away.
CREATE FUNCTION
sys::__version_internal() -> tuple<major: std::int64,
                                   minor: std::int64,
                                   stage: std::str,
                                   stage_no: std::int64,
                                   local: array<std::str>>
{
    # This function reads external data.
    SET volatility := 'VOLATILE';
    USING SQL $$
    SELECT
        (v ->> 'major')::int8,
        (v ->> 'minor')::int8,
        (v ->> 'stage')::text,
        (v ->> 'stage_no')::int8,
        (SELECT coalesce(array_agg(el), ARRAY[]::text[])
         FROM jsonb_array_elements_text(v -> 'local') AS el)
    FROM
        (SELECT edgedb.__syscache_instancedata() -> 'version' AS v) AS q;
    $$;
};


CREATE FUNCTION
sys::get_version() -> tuple<major: std::int64,
                            minor: std::int64,
                            stage: sys::version_stage,
                            stage_no: std::int64,
                            local: array<std::str>>
{
    # This function reads external data.
    SET volatility := 'VOLATILE';
    USING (
        SELECT <tuple<major: std::int64,
                    minor: std::int64,
                    stage: sys::version_stage,
                    stage_no: std::int64,
                    local: array<std::str>>>sys::__version_internal()
    );
};


CREATE FUNCTION
sys::get_version_as_str() -> std::str
{
    # This function reads external data.
    SET volatility := 'VOLATILE';
    USING (
        WITH v := sys::get_version()
        SELECT
            <str>v.major
            ++ '.' ++ <str>v.minor
            ++ ('-' ++ <str>v.stage
                ++ '.' ++ <str>v.stage_no
                ++ ('+' ++ std::to_str(v.local, '.')
                    IF len(v.local) > 0 ELSE '')
            ) IF v.stage != <sys::version_stage>'final' ELSE ''
    );
};


CREATE FUNCTION
sys::get_transaction_isolation() -> sys::transaction_isolation_t
{
    # This function only reads from a table.
    SET volatility := 'STABLE';
    SET force_return_cast := true;
    USING SQL FUNCTION 'edgedb._get_transaction_isolation';
};
