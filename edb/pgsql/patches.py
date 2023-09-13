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


"""Patches to apply to databases"""

from __future__ import annotations
from typing import *


def get_version_key(num_patches: int):
    """Produce a version key to add to instdata keys after major patches.

    Patches that modify the schema class layout and introspection queries
    are not safe to downgrade from. So for such patches, we add a version
    suffix to the names of the core instdata entries that we would need to
    update, so that we don't clobber the old version.

    After a downgrade, we'll have more patches applied than we
    actually know exist in the running version, but since we compute
    the key based on the number of schema layout patches that we can
    *see*, we still compute the right key.
    """
    num_major = sum(
        p.startswith('edgeql+schema') for p, _ in PATCHES[:num_patches])
    if num_major == 0:
        return ''
    else:
        return f'_v{num_major}'


def _setup_patches(patches: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Do postprocessing on the patches list

    For technical reasons, we can't run a user schema repair if there
    is a pending standard schema change, so when applying repairs we
    always defer them to the *last* repair patch, and we ensure that
    edgeql+schema is followed by a repair if necessary.
    """
    seen_repair = False
    npatches = []
    for kind, patch in patches:
        npatches.append((kind, patch))
        if kind.startswith('edgeql+schema') and seen_repair:
            npatches.append(('repair', ''))
        seen_repair |= kind == 'repair'
    return npatches


"""
The actual list of patches. The patches are (kind, script) pairs.

The current kinds are:
 * sql - simply runs a SQL script
 * metaschema-sql - create a function from metaschema
 * edgeql - runs an edgeql DDL command
 * edgeql+schema - runs an edgeql DDL command and updates the std schemas
 * ext-pkg - installs an extension package given a name
 * repair - fix up inconsistencies in *user* schemas
"""
PATCHES: list[tuple[str, str]] = _setup_patches([
    ('metaschema-sql', 'GetPgTypeForEdgeDBTypeFunction'),
    ('edgeql+schema+exts', '''
CREATE FUNCTION sys::_get_pg_type_for_edgedb_type(
    typeid: std::uuid,
    kind: std::str,
    elemid: OPTIONAL std::uuid,
    sql_type: OPTIONAL std::str,
) -> std::int64 {
    USING SQL FUNCTION 'edgedb.get_pg_type_for_edgedb_type';
    SET volatility := 'STABLE';
    SET impl_is_strict := false;
};
ALTER TYPE schema::ScalarType {
    CREATE PROPERTY arg_values -> array<std::str>;
};

CREATE module ext;

CREATE INFIX OPERATOR
std::`=` (l: anyscalar, r: anyscalar) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL anyscalar, r: OPTIONAL anyscalar) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: anyscalar, r: anyscalar) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL anyscalar, r: OPTIONAL anyscalar) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>=` (l: anyscalar, r: anyscalar) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR '>=';
};


CREATE INFIX OPERATOR
std::`>` (l: anyscalar, r: anyscalar) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR '>';
};


CREATE INFIX OPERATOR
std::`<=` (l: anyscalar, r: anyscalar) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR '<=';
};


CREATE INFIX OPERATOR
std::`<` (l: anyscalar, r: anyscalar) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR '<';
};
'''),
    ('ext-pkg', 'pgvector'),
    ('edgeql+schema+config', '''
ALTER TYPE cfg::AbstractConfig {
    CREATE PROPERTY maintenance_work_mem -> cfg::memory {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION cfg::backend_setting := '"maintenance_work_mem"';
        CREATE ANNOTATION std::description :=
            'The amount of memory used by operations such as \
            CREATE INDEX.';
    };
}
'''),
    ('edgeql+schema', ''),  # refresh function pg_table_is_visible
    ('metaschema-sql', 'RangeToJsonFunction'),
    ('edgeql', '''
DROP FUNCTION std::__range_validate_json(v: std::json);
CREATE FUNCTION
std::__range_validate_json(v: std::json) -> OPTIONAL std::json
{
    SET volatility := 'Immutable';
    SET internal := true;
    USING SQL $$
    SELECT
        CASE
        WHEN v = 'null'::jsonb THEN
            NULL
        WHEN
            empty
            AND (lower IS DISTINCT FROM upper
                 OR lower IS NOT NULL AND inc_upper AND inc_lower)
        THEN
            edgedb.raise(
                NULL::jsonb,
                'invalid_parameter_value',
                msg => 'conflicting arguments in range constructor:'
                        || ' "empty" is `true` while the specified'
                        || ' bounds suggest otherwise'
            )

        WHEN
            NOT empty
            AND inc_lower IS NULL
        THEN
            edgedb.raise(
                NULL::jsonb,
                'invalid_parameter_value',
                msg => 'JSON object representing a range must include an'
                        || ' "inc_lower" boolean property'
            )

        WHEN
            NOT empty
            AND inc_upper IS NULL
        THEN
            edgedb.raise(
                NULL::jsonb,
                'invalid_parameter_value',
                msg => 'JSON object representing a range must include an'
                        || ' "inc_upper" boolean property'
            )

        WHEN
            EXISTS (
                SELECT jsonb_object_keys(v)
                EXCEPT
                VALUES
                    ('lower'),
                    ('upper'),
                    ('inc_lower'),
                    ('inc_upper'),
                    ('empty')
            )
        THEN
            (SELECT edgedb.raise(
                NULL::jsonb,
                'invalid_parameter_value',
                msg => 'JSON object representing a range contains unexpected'
                        || ' keys: ' || string_agg(k.k, ', ' ORDER BY k.k)
            )
            FROM
                (SELECT jsonb_object_keys(v)
                EXCEPT
                VALUES
                    ('lower'),
                    ('upper'),
                    ('inc_lower'),
                    ('inc_upper'),
                    ('empty')
                ) AS k(k)
            )
        ELSE
            v
        END
    FROM
        (SELECT
            (v ->> 'lower') AS lower,
            (v ->> 'upper') AS upper,
            (v ->> 'inc_lower')::bool AS inc_lower,
            (v ->> 'inc_upper')::bool AS inc_upper,
            coalesce((v ->> 'empty')::bool, false) AS empty
        ) j
    $$;
};
'''),
    # Repair only if the database was originally created with rc1 or
    # rc2, prior to pgvector being added. This catches a schema
    # problem caused by adding prefer_subquery_args, where we
    # distinguish between None (from old dbs) and False (the default).
    ('repair', 'from {3.0-rc.1, 3.0-rc.2}'),
])
