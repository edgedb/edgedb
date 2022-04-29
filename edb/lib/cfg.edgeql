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


CREATE MODULE cfg;

CREATE ABSTRACT INHERITABLE ANNOTATION cfg::backend_setting;
CREATE ABSTRACT INHERITABLE ANNOTATION cfg::report;
CREATE ABSTRACT INHERITABLE ANNOTATION cfg::internal;
CREATE ABSTRACT INHERITABLE ANNOTATION cfg::requires_restart;
CREATE ABSTRACT INHERITABLE ANNOTATION cfg::system;
CREATE ABSTRACT INHERITABLE ANNOTATION cfg::affects_compilation;

CREATE ABSTRACT TYPE cfg::ConfigObject EXTENDING std::BaseObject;

CREATE ABSTRACT TYPE cfg::AuthMethod EXTENDING cfg::ConfigObject;
CREATE TYPE cfg::Trust EXTENDING cfg::AuthMethod;
CREATE TYPE cfg::SCRAM EXTENDING cfg::AuthMethod;

CREATE SCALAR TYPE cfg::memory EXTENDING std::anyscalar;
CREATE SCALAR TYPE cfg::AllowBareDDL EXTENDING enum<AlwaysAllow, NeverAllow>;

CREATE TYPE cfg::Auth EXTENDING cfg::ConfigObject {
    CREATE REQUIRED PROPERTY priority -> std::int64 {
        CREATE CONSTRAINT std::exclusive;
        SET readonly := true;
    };

    CREATE MULTI PROPERTY user -> std::str {
        SET readonly := true;
        SET default := {'*'};
    };

    CREATE SINGLE LINK method -> cfg::AuthMethod {
        CREATE CONSTRAINT std::exclusive;
    };

    CREATE PROPERTY comment -> std::str {
        SET readonly := true;
    };
};


CREATE ABSTRACT TYPE cfg::AbstractConfig extending cfg::ConfigObject {
    CREATE REQUIRED PROPERTY session_idle_timeout -> std::duration {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION cfg::report := 'true';
        SET default := <std::duration>'60 seconds';
    };

    CREATE REQUIRED PROPERTY session_idle_transaction_timeout -> std::duration {
        CREATE ANNOTATION cfg::backend_setting :=
            '"idle_in_transaction_session_timeout"';
        SET default := <std::duration>'10 seconds';
    };

    CREATE REQUIRED PROPERTY query_execution_timeout -> std::duration {
        CREATE ANNOTATION cfg::backend_setting := '"statement_timeout"';
    };

    CREATE REQUIRED PROPERTY listen_port -> std::int16 {
        CREATE ANNOTATION cfg::system := 'true';
        SET default := 5656;
    };

    CREATE MULTI PROPERTY listen_addresses -> std::str {
        CREATE ANNOTATION cfg::system := 'true';
    };

    CREATE MULTI LINK auth -> cfg::Auth {
        CREATE ANNOTATION cfg::system := 'true';
    };

    CREATE PROPERTY allow_dml_in_functions -> std::bool {
        SET default := false;
        CREATE ANNOTATION cfg::affects_compilation := 'true';
        CREATE ANNOTATION cfg::internal := 'true';
    };

    CREATE PROPERTY allow_bare_ddl -> cfg::AllowBareDDL {
        SET default := cfg::AllowBareDDL.AlwaysAllow;
        CREATE ANNOTATION cfg::affects_compilation := 'true';
    };

    CREATE PROPERTY apply_access_policies -> std::bool {
        SET default := true;
        CREATE ANNOTATION cfg::affects_compilation := 'true';
    };

    # Exposed backend settings follow.
    # When exposing a new setting, remember to modify
    # the _read_sys_config function to select the value
    # from pg_settings in the config_backend CTE.
    CREATE PROPERTY shared_buffers -> cfg::memory {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION cfg::backend_setting := '"shared_buffers"';
        CREATE ANNOTATION cfg::requires_restart := 'true';
    };

    CREATE PROPERTY query_work_mem -> cfg::memory {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION cfg::backend_setting := '"work_mem"';
    };

    CREATE PROPERTY effective_cache_size -> cfg::memory {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION cfg::backend_setting := '"effective_cache_size"';
    };

    CREATE PROPERTY effective_io_concurrency -> std::int64 {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION cfg::backend_setting := '"effective_io_concurrency"';
    };

    CREATE PROPERTY default_statistics_target -> std::int64 {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION cfg::backend_setting := '"default_statistics_target"';
    };
};


CREATE TYPE cfg::Config EXTENDING cfg::AbstractConfig;
CREATE TYPE cfg::InstanceConfig EXTENDING cfg::AbstractConfig;
CREATE TYPE cfg::DatabaseConfig EXTENDING cfg::AbstractConfig;


CREATE FUNCTION
cfg::get_config_json(
    NAMED ONLY sources: OPTIONAL array<std::str> = {},
    NAMED ONLY max_source: OPTIONAL std::str = {}
) -> std::json
{
    USING SQL $$
    SELECT
        coalesce(jsonb_object_agg(cfg.name, cfg), '{}'::jsonb)
    FROM
        edgedb._read_sys_config(
            sources::edgedb._sys_config_source_t[],
            max_source::edgedb._sys_config_source_t
        ) AS cfg
    $$;
};

CREATE FUNCTION
cfg::_quote(text: std::str) -> std::str
{
    SET volatility := 'Stable';
    SET internal := true;
    USING SQL $$
        SELECT replace(quote_literal(text), '''''', '\\''')
    $$
};

CREATE FUNCTION
cfg::_describe_system_config_as_ddl() -> str
{
    # The results won't change within a single statement.
    SET volatility := 'Stable';
    SET internal := true;
    USING SQL FUNCTION 'edgedb._describe_system_config_as_ddl';
};


CREATE FUNCTION
cfg::_describe_database_config_as_ddl() -> str
{
    # The results won't change within a single statement.
    SET volatility := 'Stable';
    SET internal := true;
    USING SQL FUNCTION 'edgedb._describe_database_config_as_ddl';
};


CREATE CAST FROM std::int64 TO cfg::memory {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM cfg::memory TO std::int64 {
    SET volatility := 'Immutable';
    USING SQL CAST;
};


CREATE CAST FROM std::str TO cfg::memory {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'edgedb.str_to_cfg_memory';
};


CREATE CAST FROM cfg::memory TO std::str {
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'edgedb.cfg_memory_to_str';
};


CREATE CAST FROM std::json TO cfg::memory {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT edgedb.str_to_cfg_memory(
            edgedb.jsonb_extract_scalar(val, 'string')
        )
    $$;
};


CREATE CAST FROM cfg::memory TO std::json {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT to_jsonb(edgedb.cfg_memory_to_str(val))
    $$;
};


CREATE INFIX OPERATOR
std::`=` (l: cfg::memory, r: cfg::memory) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=(int8,int8)';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL cfg::memory, r: OPTIONAL cfg::memory) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: cfg::memory, r: cfg::memory) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>(int8,int8)';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL cfg::memory, r: OPTIONAL cfg::memory) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>` (l: cfg::memory, r: cfg::memory) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR r'>(int8,int8)';
};


CREATE INFIX OPERATOR
std::`>=` (l: cfg::memory, r: cfg::memory) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR r'>=(int8,int8)';
};


CREATE INFIX OPERATOR
std::`<` (l: cfg::memory, r: cfg::memory) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR r'<(int8,int8)';
};


CREATE INFIX OPERATOR
std::`<=` (l: cfg::memory, r: cfg::memory) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR r'<=(int8,int8)';
};
