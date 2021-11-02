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


CREATE SCALAR TYPE sys::TransactionIsolation
    EXTENDING enum<RepeatableRead, Serializable>;


CREATE SCALAR TYPE sys::VersionStage
    EXTENDING enum<dev, alpha, beta, rc, final>;


CREATE ABSTRACT TYPE sys::SystemObject EXTENDING schema::AnnotationSubject;


CREATE TYPE sys::Database EXTENDING sys::SystemObject {
    ALTER PROPERTY name {
        CREATE CONSTRAINT std::exclusive;
    };
};


CREATE TYPE sys::ExtensionPackage EXTENDING sys::SystemObject {
    CREATE REQUIRED PROPERTY script -> str;
    CREATE REQUIRED PROPERTY version -> tuple<
                                            major: std::int64,
                                            minor: std::int64,
                                            stage: sys::VersionStage,
                                            stage_no: std::int64,
                                            local: array<std::str>,
                                        >;
};


ALTER TYPE schema::Extension {
    CREATE REQUIRED LINK package -> sys::ExtensionPackage {
        CREATE CONSTRAINT std::exclusive;
    }
};


CREATE TYPE sys::Role EXTENDING sys::SystemObject {
    ALTER PROPERTY name {
        CREATE CONSTRAINT std::exclusive;
    };

    CREATE REQUIRED PROPERTY superuser -> std::bool;
    # Backwards compatibility.
    CREATE PROPERTY is_superuser := .superuser;
    CREATE PROPERTY password -> std::str;
};


ALTER TYPE sys::Role {
    CREATE MULTI LINK member_of -> sys::Role;
};


# An intermediate function is needed because we can't
# cast JSON to tuples yet.  DO NOT use directly, it'll go away.
CREATE FUNCTION
sys::__version_internal() -> tuple<major: std::int64,
                                   minor: std::int64,
                                   stage: std::str,
                                   stage_no: std::int64,
                                   local: array<std::str>>
{
    # This function reads from a table.
    SET volatility := 'Stable';
    SET internal := true;
    USING SQL $$
    SELECT
        (v ->> 'major')::int8,
        (v ->> 'minor')::int8,
        (v ->> 'stage')::text,
        (v ->> 'stage_no')::int8,
        (SELECT coalesce(array_agg(el), ARRAY[]::text[])
         FROM jsonb_array_elements_text(v -> 'local') AS el)
    FROM
        edgedb._sys_version() as v
    $$;
};


CREATE FUNCTION
sys::get_version() -> tuple<major: std::int64,
                            minor: std::int64,
                            stage: sys::VersionStage,
                            stage_no: std::int64,
                            local: array<std::str>>
{
    CREATE ANNOTATION std::description :=
        'Return the server version as a tuple.';
    SET volatility := 'Stable';
    USING (
        SELECT <tuple<major: std::int64,
                    minor: std::int64,
                    stage: sys::VersionStage,
                    stage_no: std::int64,
                    local: array<std::str>>>sys::__version_internal()
    );
};


CREATE FUNCTION
sys::get_version_as_str() -> std::str
{
    CREATE ANNOTATION std::description :=
        'Return the server version as a string.';
    SET volatility := 'Stable';
    USING (
        WITH v := sys::get_version()
        SELECT
            <str>v.major
            ++ '.' ++ <str>v.minor
            ++ (('-' ++ <str>v.stage ++ '.' ++ <str>v.stage_no)
                IF v.stage != <sys::VersionStage>'final' ELSE '')
            ++ (('+' ++ std::array_join(v.local, '.')) IF len(v.local) > 0
                ELSE '')
    );
};


CREATE FUNCTION
sys::get_transaction_isolation() -> sys::TransactionIsolation
{
    CREATE ANNOTATION std::description :=
        'Return the isolation level of the current transaction.';
    # This function only reads from a table.
    SET volatility := 'Stable';
    SET force_return_cast := true;
    USING SQL FUNCTION 'edgedb._get_transaction_isolation';
};


CREATE FUNCTION
sys::get_current_database() -> str
{
    CREATE ANNOTATION std::description :=
        'Return the name of the current database as a string.';
    # The results won't change within a single statement.
    SET volatility := 'Stable';
    USING SQL FUNCTION 'edgedb.get_current_database';
};


CREATE FUNCTION
sys::_describe_roles_as_ddl() -> str
{
    # The results won't change within a single statement.
    SET volatility := 'Stable';
    SET internal := true;
    USING SQL FUNCTION 'edgedb._describe_roles_as_ddl';
};


CREATE FUNCTION
sys::__pg_and(a: OPTIONAL std::bool, b: OPTIONAL std::bool) -> std::bool
{
    SET volatility := 'Immutable';
    SET internal := true;
    USING SQL $$
        SELECT a AND b;
    $$;
};


CREATE FUNCTION
sys::__pg_or(a: OPTIONAL std::bool, b: OPTIONAL std::bool) -> std::bool
{
    SET volatility := 'Immutable';
    SET internal := true;
    USING SQL $$
        SELECT a OR b;
    $$;
};
