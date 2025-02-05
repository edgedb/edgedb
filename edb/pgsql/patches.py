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


"""
The actual list of patches. The patches are (kind, script) pairs.

The current kinds are:
 * sql - simply runs a SQL script
 * metaschema-sql - create a function from metaschema
 * edgeql - runs an edgeql DDL command
 * edgeql+schema - runs an edgeql DDL command and updates the std schemas
 * edgeql+user_ext|<extname> - updates extensions installed in user databases
 *                           - should be paired with an ext-pkg patch
 * ...+config - updates config views
 * ext-pkg - installs an extension package given a name
 * repair - fix up inconsistencies in *user* schemas
 * sql-introspection - refresh all sql introspection views
 * ...+testmode - only run the patch in testmode. Works with any patch kind.
"""
PATCHES: list[tuple[str, str]] = [
    # 6.0b2
    # One of the sql-introspection's adds a param with a default to
    # uuid_to_oid, so we need to drop the original to avoid ambiguity.
    ('sql', '''
drop function if exists edgedbsql_v6_2f20b3fed0.uuid_to_oid(uuid) cascade
'''),
    ('sql-introspection', ''),
    ('metaschema-sql', 'SysConfigFullFunction'),
    # 6.0rc1
    ('edgeql+schema+config+testmode', '''
CREATE SCALAR TYPE cfg::TestEnabledDisabledEnum
    EXTENDING enum<Enabled, Disabled>;
ALTER TYPE cfg::AbstractConfig {
    CREATE PROPERTY __check_function_bodies -> cfg::TestEnabledDisabledEnum {
        CREATE ANNOTATION cfg::internal := 'true';
        CREATE ANNOTATION cfg::backend_setting := '"check_function_bodies"';
        SET default := cfg::TestEnabledDisabledEnum.Enabled;
    };
};
'''),
    ('metaschema-sql', 'PostgresConfigValueToJsonFunction'),
    ('metaschema-sql', 'SysConfigFullFunction'),
    ('edgeql', '''
ALTER FUNCTION
std::assert_single(
    input: SET OF anytype,
    NAMED ONLY message: OPTIONAL str = <str>{},
) {
    SET volatility := 'Immutable';
};
ALTER FUNCTION
std::assert_exists(
    input: SET OF anytype,
    NAMED ONLY message: OPTIONAL str = <str>{},
) {
    SET volatility := 'Immutable';
};
ALTER FUNCTION
std::assert_distinct(
    input: SET OF anytype,
    NAMED ONLY message: OPTIONAL str = <str>{},
) {
    SET volatility := 'Immutable';
};
'''),
     ('edgeql+schema+config', '''
CREATE SCALAR TYPE sys::TransactionAccessMode
    EXTENDING enum<ReadOnly, ReadWrite>;


CREATE SCALAR TYPE sys::TransactionDeferrability
    EXTENDING enum<Deferrable, NotDeferrable>;

ALTER TYPE cfg::AbstractConfig {
    CREATE REQUIRED PROPERTY default_transaction_isolation
        -> sys::TransactionIsolation
    {
        CREATE ANNOTATION cfg::affects_compilation := 'true';
        CREATE ANNOTATION cfg::backend_setting :=
            '"default_transaction_isolation"';
        CREATE ANNOTATION std::description :=
            'Controls the default isolation level of each new transaction, \
            including implicit transactions. Defaults to `Serializable`. \
            Note that changing this to a lower isolation level implies \
            that the transactions are also read-only by default regardless \
            of the value of the `default_transaction_access_mode` setting.';
        SET default := sys::TransactionIsolation.Serializable;
    };

    CREATE REQUIRED PROPERTY default_transaction_access_mode
        -> sys::TransactionAccessMode
    {
        CREATE ANNOTATION cfg::affects_compilation := 'true';
        CREATE ANNOTATION std::description :=
            'Controls the default read-only status of each new transaction, \
            including implicit transactions. Defaults to `ReadWrite`. \
            Note that if `default_transaction_isolation` is set to any value \
            other than Serializable this parameter is implied to be \
            `ReadOnly` regardless of the actual value.';
        SET default := sys::TransactionAccessMode.ReadWrite;
    };

    CREATE REQUIRED PROPERTY default_transaction_deferrable
        -> sys::TransactionDeferrability
    {
        CREATE ANNOTATION cfg::backend_setting :=
            '"default_transaction_deferrable"';
        CREATE ANNOTATION std::description :=
            'Controls the default deferrable status of each new transaction. \
            It currently has no effect on read-write transactions or those \
            operating at isolation levels lower than `Serializable`. \
            The default is `NotDeferrable`.';
        SET default := sys::TransactionDeferrability.NotDeferrable;
    };
};
'''),
]
