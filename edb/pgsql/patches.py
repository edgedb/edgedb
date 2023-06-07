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
])
