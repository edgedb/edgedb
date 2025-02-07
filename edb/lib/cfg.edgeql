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

# If report is set to 'true', that *system* config will be included
# in the `system_config` ParameterStatus on each connection.
# Non-system config cannot be reported.
CREATE ABSTRACT INHERITABLE ANNOTATION cfg::report;

CREATE ABSTRACT INHERITABLE ANNOTATION cfg::internal;
CREATE ABSTRACT INHERITABLE ANNOTATION cfg::requires_restart;

# System config means that config value can only be modified using
# CONFIGURE INSTANCE command. System config is therefore *not* included
# in the binary protocol state.
CREATE ABSTRACT INHERITABLE ANNOTATION cfg::system;

CREATE ABSTRACT INHERITABLE ANNOTATION cfg::affects_compilation;

CREATE SCALAR TYPE cfg::memory EXTENDING std::anyscalar;
CREATE SCALAR TYPE cfg::AllowBareDDL EXTENDING enum<AlwaysAllow, NeverAllow>;
CREATE SCALAR TYPE cfg::StoreMigrationSDL EXTENDING enum<
    AlwaysStore, NeverStore,
>;
CREATE SCALAR TYPE cfg::ConnectionTransport EXTENDING enum<
    TCP, TCP_PG, HTTP, SIMPLE_HTTP, HTTP_METRICS, HTTP_HEALTH>;
CREATE SCALAR TYPE cfg::QueryCacheMode EXTENDING enum<
    InMemory, RegInline, PgFunc, Default>;
CREATE SCALAR TYPE cfg::QueryStatsOption EXTENDING enum<None, All>;

CREATE ABSTRACT TYPE cfg::ConfigObject EXTENDING std::BaseObject;

CREATE ABSTRACT TYPE cfg::AuthMethod EXTENDING cfg::ConfigObject {
    # Connection transports applicable to this auth entry.
    # An empty set means "apply to all transports".
    CREATE MULTI PROPERTY transports -> cfg::ConnectionTransport {
        SET readonly := true;
    };
};

CREATE TYPE cfg::Trust EXTENDING cfg::AuthMethod;
CREATE TYPE cfg::SCRAM EXTENDING cfg::AuthMethod {
    ALTER PROPERTY transports {
        SET default := { cfg::ConnectionTransport.TCP };
    };
};
CREATE TYPE cfg::JWT EXTENDING cfg::AuthMethod {
    ALTER PROPERTY transports {
        SET default := { cfg::ConnectionTransport.HTTP };
    };
};
CREATE TYPE cfg::Password EXTENDING cfg::AuthMethod {
    ALTER PROPERTY transports {
        SET default := { cfg::ConnectionTransport.SIMPLE_HTTP };
    };
};
CREATE TYPE cfg::mTLS EXTENDING cfg::AuthMethod {
    ALTER PROPERTY transports {
        SET default := {
            cfg::ConnectionTransport.HTTP_METRICS,
            cfg::ConnectionTransport.HTTP_HEALTH,
        };
    };
};

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
        SET readonly := true;
    };

    CREATE PROPERTY comment -> std::str {
        SET readonly := true;
    };
};

CREATE SCALAR TYPE cfg::SMTPSecurity EXTENDING enum<
    PlainText,
    TLS,
    STARTTLS,
    STARTTLSOrPlainText,
>;

CREATE ABSTRACT TYPE cfg::EmailProviderConfig EXTENDING cfg::ConfigObject {
    CREATE REQUIRED PROPERTY name -> std::str {
        CREATE CONSTRAINT std::exclusive;
        CREATE ANNOTATION std::description :=
            "The name of the email provider.";
    };
};

CREATE TYPE cfg::SMTPProviderConfig EXTENDING cfg::EmailProviderConfig {
    CREATE PROPERTY sender -> std::str {
        CREATE ANNOTATION std::description :=
            "\"From\" address of system emails sent for e.g. \
            password reset, etc.";
    };
    CREATE PROPERTY host -> std::str {
        CREATE ANNOTATION std::description :=
            "Host of SMTP server to use for sending emails. \
            If not set, \"localhost\" will be used.";
    };
    CREATE PROPERTY port -> std::int32 {
        CREATE ANNOTATION std::description :=
            "Port of SMTP server to use for sending emails. \
            If not set, common defaults will be used depending on security: \
            465 for TLS, 587 for STARTTLS, 25 otherwise.";
    };
    CREATE PROPERTY username -> std::str {
        CREATE ANNOTATION std::description :=
            "Username to login as after connected to SMTP server.";
    };
    CREATE PROPERTY password -> std::str {
        SET secret := true;
        CREATE ANNOTATION std::description :=
            "Password for login after connected to SMTP server.";
    };
    CREATE REQUIRED PROPERTY security -> cfg::SMTPSecurity {
        SET default := cfg::SMTPSecurity.STARTTLSOrPlainText;
        CREATE ANNOTATION std::description :=
            "Security mode of the connection to SMTP server. \
            By default, initiate a STARTTLS upgrade if supported by the \
            server, or fallback to PlainText.";
    };
    CREATE REQUIRED PROPERTY validate_certs -> std::bool {
        SET default := true;
        CREATE ANNOTATION std::description :=
            "Determines if SMTP server certificates are validated.";
    };
    CREATE REQUIRED PROPERTY timeout_per_email -> std::duration {
        SET default := <std::duration>'60 seconds';
        CREATE ANNOTATION std::description :=
            "Maximum time to send an email, including retry attempts.";
    };
    CREATE REQUIRED PROPERTY timeout_per_attempt -> std::duration {
        SET default := <std::duration>'15 seconds';
        CREATE ANNOTATION std::description :=
            "Maximum time for each SMTP request.";
    };
};

CREATE ABSTRACT TYPE cfg::AbstractConfig extending cfg::ConfigObject;

CREATE ABSTRACT TYPE cfg::ExtensionConfig EXTENDING cfg::ConfigObject {
    CREATE REQUIRED SINGLE LINK cfg -> cfg::AbstractConfig {
        CREATE DELEGATED CONSTRAINT std::exclusive;
    };
};

ALTER TYPE cfg::AbstractConfig {
    CREATE MULTI LINK extensions := .<cfg[IS cfg::ExtensionConfig];

    CREATE REQUIRED PROPERTY session_idle_timeout -> std::duration {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION cfg::report := 'true';
        CREATE ANNOTATION std::description :=
            'How long client connections can stay inactive before being \
            closed by the server.';
        SET default := <std::duration>'60 seconds';
    };

    CREATE REQUIRED PROPERTY session_idle_transaction_timeout -> std::duration {
        CREATE ANNOTATION cfg::backend_setting :=
            '"idle_in_transaction_session_timeout"';
        CREATE ANNOTATION std::description :=
            'How long client connections can stay inactive while in a \
            transaction.';
        SET default := <std::duration>'10 seconds';
    };

    CREATE REQUIRED PROPERTY query_execution_timeout -> std::duration {
        CREATE ANNOTATION cfg::backend_setting := '"statement_timeout"';
        CREATE ANNOTATION std::description :=
            'How long an individual query can run before being aborted.';
    };

    CREATE REQUIRED PROPERTY listen_port -> std::int32 {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION std::description :=
            'The TCP port the server listens on.';
        SET default := 5656;
        # Really we want a uint16, but oh well
        CREATE CONSTRAINT std::min_value(0);
        CREATE CONSTRAINT std::max_value(65535);
    };

    CREATE MULTI PROPERTY listen_addresses -> std::str {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION std::description :=
            'The TCP/IP address(es) on which the server is to listen for \
            connections from client applications.';
    };

    CREATE MULTI LINK auth -> cfg::Auth {
        CREATE ANNOTATION cfg::system := 'true';
    };

    CREATE MULTI LINK email_providers -> cfg::EmailProviderConfig {
        CREATE ANNOTATION std::description :=
            'The list of email providers that can be used to send emails.';
    };

    CREATE PROPERTY current_email_provider_name -> std::str {
        CREATE ANNOTATION std::description :=
            'The name of the current email provider.';
    };

    CREATE PROPERTY allow_dml_in_functions -> std::bool {
        SET default := false;
        CREATE ANNOTATION cfg::affects_compilation := 'true';
        CREATE ANNOTATION cfg::internal := 'true';
    };

    CREATE PROPERTY allow_bare_ddl -> cfg::AllowBareDDL {
        SET default := cfg::AllowBareDDL.AlwaysAllow;
        CREATE ANNOTATION cfg::affects_compilation := 'true';
        CREATE ANNOTATION std::description :=
            'Whether DDL is allowed to be executed outside a migration.';
    };

    CREATE PROPERTY store_migration_sdl -> cfg::StoreMigrationSDL {
        SET default := cfg::StoreMigrationSDL.NeverStore;
        CREATE ANNOTATION cfg::affects_compilation := 'true';
        CREATE ANNOTATION std::description :=
            'When to store resulting SDL of a Migration. This may be slow.';
    };

    CREATE PROPERTY apply_access_policies -> std::bool {
        SET default := true;
        CREATE ANNOTATION cfg::affects_compilation := 'true';
        CREATE ANNOTATION std::description :=
            'Whether access policies will be applied when running queries.';
    };

    CREATE PROPERTY apply_access_policies_pg -> std::bool {
        SET default := false;
        CREATE ANNOTATION cfg::affects_compilation := 'false';
        CREATE ANNOTATION std::description :=
            'Whether access policies will be applied when running queries over \
            SQL adapter.';
    };

    CREATE PROPERTY allow_user_specified_id -> std::bool {
        SET default := false;
        CREATE ANNOTATION cfg::affects_compilation := 'true';
        CREATE ANNOTATION std::description :=
            'Whether inserts are allowed to set the \'id\' property.';
    };

    CREATE PROPERTY simple_scoping -> std::bool {
        CREATE ANNOTATION cfg::affects_compilation := 'true';
        CREATE ANNOTATION std::description :=
            'Whether to use the new simple scoping behavior \
            (disable path factoring)';
    };

    CREATE PROPERTY warn_old_scoping -> std::bool {
        CREATE ANNOTATION cfg::affects_compilation := 'true';
        CREATE ANNOTATION std::description :=
            'Whether to warn when depending on old scoping behavior.';
    };

    CREATE MULTI PROPERTY cors_allow_origins -> std::str {
        CREATE ANNOTATION std::description :=
            'List of origins that can be returned in the \
            Access-Control-Allow-Origin HTTP header';
    };

    CREATE PROPERTY auto_rebuild_query_cache -> std::bool {
        SET default := true;
        CREATE ANNOTATION std::description :=
            'Recompile all cached queries on DDL if enabled.';
    };

    CREATE PROPERTY auto_rebuild_query_cache_timeout -> std::duration {
        CREATE ANNOTATION std::description :=
            'Maximum time to spend recompiling cached queries on DDL.';
        SET default := <std::duration>'60 seconds';
    };

    CREATE PROPERTY query_cache_mode -> cfg::QueryCacheMode {
        SET default := cfg::QueryCacheMode.Default;
        CREATE ANNOTATION cfg::affects_compilation := 'true';
        CREATE ANNOTATION std::description :=
            'Where the query cache is finally stored';
    };

    # HTTP Worker Configuration
    CREATE PROPERTY http_max_connections -> std::int64 {
        SET default := 10;
        CREATE ANNOTATION std::description :=
            'The maximum number of concurrent HTTP connections.';
        CREATE ANNOTATION cfg::system := 'true';
    };

    # Exposed backend settings follow.
    # When exposing a new setting, remember to modify
    # the _read_sys_config function to select the value
    # from pg_settings in the config_backend CTE.
    CREATE PROPERTY shared_buffers -> cfg::memory {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION cfg::backend_setting := '"shared_buffers"';
        CREATE ANNOTATION cfg::requires_restart := 'true';
        CREATE ANNOTATION std::description :=
            'The amount of memory used for shared memory buffers.';
    };

    CREATE PROPERTY query_work_mem -> cfg::memory {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION cfg::backend_setting := '"work_mem"';
        CREATE ANNOTATION std::description :=
            'The amount of memory used by internal query operations such as \
            sorting.';
    };

    CREATE PROPERTY maintenance_work_mem -> cfg::memory {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION cfg::backend_setting := '"maintenance_work_mem"';
        CREATE ANNOTATION std::description :=
            'The amount of memory used by operations such as \
            CREATE INDEX.';
    };

    CREATE PROPERTY effective_cache_size -> cfg::memory {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION cfg::backend_setting := '"effective_cache_size"';
        CREATE ANNOTATION std::description :=
            'An estimate of the effective size of the disk cache available \
            to a single query.';
    };

    CREATE PROPERTY effective_io_concurrency -> std::int64 {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION cfg::backend_setting := '"effective_io_concurrency"';
        CREATE ANNOTATION std::description :=
            'The number of concurrent disk I/O operations that can be \
            executed simultaneously.';
    };

    CREATE PROPERTY default_statistics_target -> std::int64 {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION cfg::backend_setting := '"default_statistics_target"';
        CREATE ANNOTATION std::description :=
            'The default data statistics target for the planner.';
    };

    CREATE PROPERTY force_database_error -> std::str {
        SET default := 'false';
        CREATE ANNOTATION cfg::affects_compilation := 'true';
        CREATE ANNOTATION std::description :=
            'A hook to force all queries to produce an error.';
    };

    CREATE REQUIRED PROPERTY _pg_prepared_statement_cache_size -> std::int16 {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION std::description :=
            'The maximum number of prepared statements each backend \
            connection could hold at the same time.';
        CREATE CONSTRAINT std::min_value(1);
        SET default := 100;
    };

    CREATE PROPERTY track_query_stats -> cfg::QueryStatsOption {
        CREATE ANNOTATION cfg::backend_setting := '"edb_stat_statements.track"';
        CREATE ANNOTATION std::description :=
            'Select what queries are tracked in sys::QueryStats';
    };
};


CREATE TYPE cfg::Config EXTENDING cfg::AbstractConfig;
CREATE TYPE cfg::InstanceConfig EXTENDING cfg::AbstractConfig;
CREATE TYPE cfg::DatabaseConfig EXTENDING cfg::AbstractConfig;
CREATE ALIAS cfg::BranchConfig := cfg::DatabaseConfig;


CREATE FUNCTION
cfg::get_config_json(
    NAMED ONLY sources: OPTIONAL array<std::str> = {},
    NAMED ONLY max_source: OPTIONAL std::str = {}
) -> std::json
{
    USING SQL $$
    SELECT
        coalesce(
            jsonb_object_agg(
                cfg.name,
                -- Redact config values from extension configs, since
                -- they might contain secrets, and it isn't worth the
                -- trouble right now to care about which ones actually do.
                (CASE WHEN
                     cfg.name LIKE '%::%'
                     AND cfg.value != 'null'::jsonb
                 THEN
                     jsonb_set(to_jsonb(cfg), '{value}',
                               '{"redacted": true}'::jsonb)
                 ELSE
                     to_jsonb(cfg)
                 END)
            ),
            '{}'::jsonb
        )
    FROM
        edgedb_VER._read_sys_config(
            sources::edgedb._sys_config_source_t[],
            max_source::edgedb._sys_config_source_t
        ) AS cfg
    $$;
};

CREATE FUNCTION
cfg::_quote(text: std::str) -> std::str
{
    SET volatility := 'Immutable';
    SET internal := true;
    USING SQL $$
        SELECT replace(quote_literal(text), '''''', '\\''')
    $$
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
        SELECT edgedb_VER.str_to_cfg_memory(
            edgedb_VER.jsonb_extract_scalar(val, 'string', detail => detail)
        )
    $$;
};


CREATE CAST FROM cfg::memory TO std::json {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT to_jsonb(edgedb_VER.cfg_memory_to_str(val))
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
