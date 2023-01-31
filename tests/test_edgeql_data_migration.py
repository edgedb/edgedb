#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

from __future__ import annotations
from typing import *

import json
import os.path
import re
import textwrap
import uuid

import edgedb

from edb.common import assert_data_shape

from edb.testbase import server as tb
from edb.testbase import serutils
from edb.tools import test


class EdgeQLDataMigrationTestCase(tb.DDLTestCase):
    """Test that migrations preserve data under certain circumstances.

    Renaming, changing constraints, increasing cardinality should not
    destroy data.

    Some of the test cases here use the same migrations as
    `test_schema_migrations_equivalence`, therefore the test numbers
    should match for easy reference, even if it means skipping some.
    """

    DEFAULT_MODULE = 'test'

    def normalize_statement(self, s: str) -> str:
        re_filter = re.compile(r'[\s]+|(#.*?(\n|$))|(,(?=\s*[})]))')
        stripped = textwrap.dedent(s.lstrip('\n')).rstrip('\n')
        folded = re_filter.sub('', stripped).lower()
        return folded

    def cleanup_migration_exp_json(self, exp_result_json):
        # Cleanup the expected values by dedenting/stripping them
        if 'confirmed' in exp_result_json:
            exp_result_json['confirmed'] = [
                self.normalize_statement(v)
                for v in exp_result_json['confirmed']
            ]
        if (
            'proposed' in exp_result_json
            and exp_result_json['proposed']
            and 'statements' in exp_result_json['proposed']
        ):
            for stmt in exp_result_json['proposed']['statements']:
                stmt['text'] = self.normalize_statement(stmt['text'])

    async def assert_describe_migration(self, exp_result_json, *, msg=None):
        self.cleanup_migration_exp_json(exp_result_json)

        try:
            res = await self.con.query_single(
                'DESCRIBE CURRENT MIGRATION AS JSON;')

            res = json.loads(res)
            self.cleanup_migration_exp_json(res)
            assert_data_shape.assert_data_shape(
                res, exp_result_json, self.fail, message=msg)
        except Exception:
            self.add_fail_notes(serialization='json')
            raise

    async def fast_forward_describe_migration(
        self,
        *,
        limit: Optional[int] = None,
        user_input: Optional[Iterable[str]] = None,
        commit: bool = True,
    ):
        '''Repeatedly get the next step from DESCRIBE and execute it.

        The point of this as opposed to just using "POPULATE
        MIGRATION; COMMIT MIGRATION;" is that we want to make sure
        that the generated DDL is valid and in case it's not, narrow
        down which step is causing issues.
        '''

        # Keep track of proposed DDL
        prevddl = ''

        if user_input is None:
            input_iter: Iterator[str] = iter(tuple())
        else:
            input_iter = iter(user_input)

        try:
            step = 0
            while True:
                mig = await self.con.query_single(
                    'DESCRIBE CURRENT MIGRATION AS JSON;')
                mig = json.loads(mig)
                if mig['proposed'] is None:
                    assert_data_shape.assert_data_shape(
                        mig, {'complete': True},
                        self.fail,
                        message='No more "proposed", but not "completed" '
                                'either.'
                    )
                    if commit:
                        await self.con.execute('COMMIT MIGRATION;')
                    break

                interpolations = {}

                user_input_reqs = mig['proposed']['required_user_input']
                if user_input_reqs:
                    for var in user_input_reqs:
                        var_name = var['placeholder']
                        var_desc = var['prompt']
                        try:
                            var_value = next(input_iter)
                        except StopIteration:
                            raise AssertionError(
                                f'missing input value for prompt: {var_desc}'
                            ) from None

                        interpolations[var_name] = var_value

                for stmt in mig['proposed']['statements']:
                    curddl = stmt['text']

                    if interpolations:
                        def _replace(match):
                            var_name = match.group(1)
                            var_value = interpolations.get(var_name)
                            if var_value is None:
                                raise AssertionError(
                                    f'missing value for '
                                    f'placeholder {var_name!r}'
                                )
                            return var_value

                        curddl = re.sub(r'\\\((\w+)\)', _replace, curddl)

                    if prevddl == curddl:
                        raise Exception(
                            f"Repeated previous proposed DDL {curddl!r}"
                        )
                    try:
                        await self.con.execute(curddl)
                    except Exception as exc:
                        raise Exception(
                            f"Error while processing {curddl!r}"
                        ) from exc
                    prevddl = curddl
                step += 1
                if limit is not None and step == limit:
                    break
        except Exception:
            self.add_fail_notes(serialization='json')
            raise

    async def start_migration(self, migration, *,
                              populate: bool = False,
                              module: str = 'test'):
        mig = f"""
            START MIGRATION TO {{
                module {module} {{
                    {migration}
                }}
            }};
        """
        await self.con.execute(mig)
        if populate:
            await self.con.execute('POPULATE MIGRATION;')

    async def migrate(
        self,
        migration,
        *,
        populate: bool = False,
        module: str = 'test',
        user_input: Optional[Iterable[str]] = None,
    ):
        async with self.con.transaction():
            await self.start_migration(
                migration, populate=populate, module=module)
            await self.fast_forward_describe_migration(user_input=user_input)

    async def interact(self, parts, check_complete=True):
        for part in parts:
            if isinstance(part, str):
                prompt = part
                ans = "y"
                user_input = None
            else:
                prompt, ans, *user_input = part

            await self.assert_describe_migration({
                'proposed': {'prompt': prompt}
            })

            if ans == "y":
                await self.fast_forward_describe_migration(
                    limit=1, user_input=user_input)
            else:
                await self.con.execute('''
                    ALTER CURRENT MIGRATION REJECT PROPOSED;
                ''')

        if check_complete:
            await self.assert_describe_migration({
                'complete': True
            })


class TestEdgeQLDataMigration(EdgeQLDataMigrationTestCase):
    async def test_edgeql_migration_simple_01(self):
        # Base case, ensuring a single SDL migration from a clean
        # state works.
        await self.migrate("""
            type NamedObject {
                required property name -> str;
                multi link related -> NamedObject {
                    property lang -> str;
                };
            };
        """)
        await self.con.execute("""
            SET MODULE test;

            INSERT NamedObject {
                name := 'Test'
            };

            INSERT NamedObject {
                name := 'Test 2',
                related := (SELECT DETACHED NamedObject
                            FILTER .name = 'Test')
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    NamedObject {
                        related: {
                            name,
                            @lang
                        }
                    }
                FILTER
                    .name = 'Test 2';
            """,
            [
                {
                    'related': [{'name': 'Test', '@lang': None}],
                }
            ]
        )

    async def test_edgeql_migration_link_inheritance(self):
        schema_f = os.path.join(os.path.dirname(__file__), 'schemas',
                                'links_1.esdl')

        with open(schema_f) as f:
            schema = f.read()

        await self.migrate(schema)

        await self.con.execute('''
            SET MODULE test;

            INSERT Target1 {
                name := 'Target1_linkinh_2'
            };

            INSERT ObjectType01 {
                target := (SELECT Target1
                           FILTER .name = 'Target1_linkinh_2'
                           LIMIT 1)
            };

            INSERT Target0 {
                name := 'Target0_linkinh_2'
            };

            INSERT ObjectType23 {
                target := (SELECT Target0
                           FILTER .name = 'Target0_linkinh_2'
                           LIMIT 1)
            };
        ''')

        await self.con.query('DECLARE SAVEPOINT t0;')

        with self.assertRaisesRegex(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'target' of object type "
                r"'test::ObjectType01': "
                r"'test::Target0' \(expecting 'test::Target1'\)"):
            # Target0 is not allowed to be targeted by ObjectType01, since
            # ObjectType01 inherits from ObjectType1 which requires more
            # specific Target1.
            await self.con.execute('''
                INSERT ObjectType01 {
                    target := (
                        SELECT
                            Target0
                        FILTER
                            .name = 'Target0_linkinh_2'
                        LIMIT 1
                    )
                };
            ''')

        schema_f = os.path.join(os.path.dirname(__file__), 'schemas',
                                'links_1_migrated.esdl')

        with open(schema_f) as f:
            schema = f.read()

        await self.con.query('ROLLBACK TO SAVEPOINT t0')
        await self.migrate(schema)

    async def test_edgeql_migration_describe_reject_01(self):
        await self.migrate('''
            type Foo;
        ''')

        await self.start_migration('''
            type Bar;
        ''')

        await self.assert_describe_migration({
            'proposed': {
                'statements': [{
                    'text': """
                        ALTER TYPE test::Foo RENAME TO test::Bar;
                    """
                }]
            }
        })

        await self.con.execute('''
            ALTER CURRENT MIGRATION REJECT PROPOSED;
        ''')

        await self.assert_describe_migration({
            'proposed': {
                'statements': [{
                    'text': """
                        CREATE TYPE test::Bar;
                    """
                }]
            }
        })

        await self.con.execute('''
            ALTER CURRENT MIGRATION REJECT PROPOSED;
        ''')

        await self.assert_describe_migration({
            'proposed': {
                'statements': [{
                    'text': """
                        DROP TYPE test::Foo;
                    """
                }]
            }
        })

        await self.con.execute('''
            ALTER CURRENT MIGRATION REJECT PROPOSED;
        ''')

        await self.assert_describe_migration({
            'proposed': None,
            'complete': False,
        })

    async def test_edgeql_migration_describe_reject_02(self):
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1a2l6lbzimqokzygdzbkyjrhbmjh3iljg7i2m6r2ias2z2de4x4cq',
            'confirmed': [],
            'complete': True,
            'proposed': None,
        })

        # Reject an empty proposal, which should be an idempotent
        # operation. So reject it several times.
        await self.con.execute('''
            ALTER CURRENT MIGRATION REJECT PROPOSED;
            ALTER CURRENT MIGRATION REJECT PROPOSED;
            ALTER CURRENT MIGRATION REJECT PROPOSED;
        ''')

    async def test_edgeql_migration_describe_reject_03(self):
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type0;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'CREATE TYPE test::Type0;'
                }],
                'prompt': "did you create object type 'test::Type0'?",
            },
        })

        # Reject a proposal until we run out of options.
        await self.con.execute('''
            ALTER CURRENT MIGRATION REJECT PROPOSED;
            ALTER CURRENT MIGRATION REJECT PROPOSED;
            ALTER CURRENT MIGRATION REJECT PROPOSED;
            ALTER CURRENT MIGRATION REJECT PROPOSED;
            ALTER CURRENT MIGRATION REJECT PROPOSED;
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': None,
        })

    async def test_edgeql_migration_describe_reject_04(self):
        # Migration involving 2 modules
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Test;
                };

                module other {
                    type Test;
                };
            };
        ''')
        await self.fast_forward_describe_migration()

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Test2;
                };

                module other {
                    type Test3;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'ALTER TYPE other::Test RENAME TO other::Test3;',
                }],
                'prompt': (
                    "did you rename object type 'other::Test' to "
                    "'other::Test3'?"
                ),
            },
        })

        await self.con.execute('''
            ALTER CURRENT MIGRATION REJECT PROPOSED;
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'ALTER TYPE other::Test RENAME TO test::Test2;',
                }],
                'prompt': (
                    "did you rename object type 'other::Test' to "
                    "'test::Test2'?"
                ),
            },
        })

        await self.con.execute('''
            ALTER TYPE other::Test RENAME TO test::Test2;
        ''')

        await self.assert_describe_migration({
            'confirmed': [
                'ALTER TYPE other::Test RENAME TO test::Test2;'
            ],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'ALTER TYPE test::Test RENAME TO other::Test3;',
                }],
                'prompt': (
                    "did you rename object type 'test::Test' to "
                    "'other::Test3'?"
                ),
            },
        })

        await self.con.execute('''
            ALTER CURRENT MIGRATION REJECT PROPOSED;
        ''')

        await self.assert_describe_migration({
            'confirmed': [
                'ALTER TYPE other::Test RENAME TO test::Test2;'
            ],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'CREATE TYPE other::Test3;',
                }],
                'prompt': (
                    "did you create object type 'other::Test3'?"
                ),
            },
        })

        # Change our mind and use a rejected operation to rename the
        # type after all. So, we should be done now.
        await self.con.execute('''
            ALTER TYPE test::Test RENAME TO other::Test3;
        ''')

        await self.assert_describe_migration({
            'confirmed': [
                'ALTER TYPE other::Test RENAME TO test::Test2;',
                'ALTER TYPE test::Test RENAME TO other::Test3;',
            ],
            'complete': True,
            'proposed': None,
        })

    async def test_edgeql_migration_describe_reject_05(self):
        await self.migrate('''
            type User {
                required property username -> str {
                    constraint exclusive;
                    constraint regexp(r'asdf');
                }
            }
        ''')

        await self.start_migration('''
            type User {
                required property username -> str {
                    constraint exclusive;
                    constraint regexp(r'foo');
                }
            }
        ''')

        await self.assert_describe_migration({
            'proposed': {
                'prompt': (
                    "did you drop constraint 'std::regexp' "
                    "of property 'username'?"
                )
            }
        })

        await self.con.execute('''
            ALTER CURRENT MIGRATION REJECT PROPOSED;
        ''')

        # ... nothing to do, we can't get it
        await self.assert_describe_migration({
            'complete': False,
            'proposed': None,
        })

    async def test_edgeql_migration_describe_module_01(self):
        # Migration that creates a new module.
        await self.con.execute('''
            START MIGRATION TO {
                module new_module {
                    type Type0;
                };
            };
        ''')

        # Validate that we create a 'new_module'
        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'CREATE MODULE new_module IF NOT EXISTS;'
                }],
            },
        })

        # Auto-complete migration
        await self.fast_forward_describe_migration()

        # Drop the 'new_module'
        await self.con.execute('''
            START MIGRATION TO {
                module default {};
            };
        ''')

        # Validate that we drop a 'new_module'
        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'DROP TYPE new_module::Type0;'
                }],
            },
        })
        await self.con.execute('''
            DROP TYPE new_module::Type0;
        ''')

        await self.assert_describe_migration({
            'confirmed': [
                'DROP TYPE new_module::Type0;'
            ],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'DROP MODULE new_module;'
                }],
            },
        })

        # Auto-complete migration
        await self.fast_forward_describe_migration()

        # Make sure that 'new_module' can be created again with no
        # problems (i.e. it was dropped cleanly).
        await self.con.execute('''
            START MIGRATION TO {
                module new_module {
                    type Type0;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'CREATE MODULE new_module IF NOT EXISTS;'
                }],
            },
        })
        await self.con.execute('''
            CREATE MODULE new_module;
        ''')

        await self.assert_describe_migration({
            'confirmed': [
                'CREATE MODULE new_module;',
            ],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'CREATE TYPE new_module::Type0;'
                }],
            },
        })
        await self.con.execute('''
            CREATE TYPE new_module::Type0;
            COMMIT MIGRATION;
        ''')

        await self.assert_query_result(
            r"""
                INSERT new_module::Type0;
            """,
            [{
                'id': uuid.UUID,
            }],
        )

    async def test_edgeql_migration_describe_type_01(self):
        # Migration that renames a type.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type1;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1a2l6lbzimqokzygdzbkyjrhbmjh3iljg7i2m6r2ias2z2de4x4cq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE TYPE test::Type1;'
                    )
                }],
                'prompt': "did you create object type 'test::Type1'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        res = await self.con.query(r'''INSERT test::Type1;''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type01;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1jywblj6c7z25ouifcicpxniu37jdpyunf62q4th7isdafcqu67gq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER TYPE test::Type1 RENAME TO test::Type01;'
                    )
                }],
                'prompt': (
                    "did you rename object type 'test::Type1' to "
                    "'test::Type01'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT test::Type01;
        ''', [{'id': res[0].id}])

    async def test_edgeql_migration_describe_type_02(self):
        # Migration that creates a type.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type02;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1a2l6lbzimqokzygdzbkyjrhbmjh3iljg7i2m6r2ias2z2de4x4cq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE TYPE test::Type02;'
                    )
                }],
                'prompt': "did you create object type 'test::Type02'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.con.query(r'''INSERT test::Type02;''')

        # Migration that drops a type.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1fcvk56n44i62qwjnw5nqgafnbpulfhhaeb6kxqhh4c6lc4elwysa',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'DROP TYPE test::Type02;'
                    )
                }],
                'prompt': (
                    "did you drop object type 'test::Type02'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            WITH MODULE schema
            SELECT ObjectType
            FILTER .name = 'test::Type02';
        ''', [])

        # Make sure that type dropped cleanly by re-creating and
        # using the type again.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type02;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1yee6qj63nps27cjnrcudwiupusdqkzrwistpvfbqf2fstcmwauwa',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE TYPE test::Type02;'
                    )
                }],
                'prompt': "did you create object type 'test::Type02'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            INSERT test::Type02;
        ''', [{'id': uuid.UUID}])

    async def test_edgeql_migration_describe_type_03(self):
        await self.migrate('''
            type Type0;
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type1;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': "ALTER TYPE test::Type0 RENAME TO test::Type1;"
                }],
                'prompt': (
                    "did you rename object type 'test::Type0' to "
                    "'test::Type1'?"
                ),
            },
        })

        # Instead of the suggestion do a couple of different, but
        # equivalent commands.
        await self.con.execute('''
            ALTER TYPE test::Type0 RENAME TO test::TypeXX;
            ALTER TYPE test::TypeXX RENAME TO test::Type1;
        ''')

        await self.assert_describe_migration({
            'confirmed': [
                'ALTER TYPE test::Type0 RENAME TO test::TypeXX;',
                'ALTER TYPE test::TypeXX RENAME TO test::Type1;',
            ],
            'complete': True,
            'proposed': None,
        })

    async def test_edgeql_migration_describe_type_04(self):
        self.maxDiff = None
        await self.migrate('''
            type Test;
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Test2;
                    type Test3;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1xh653zionj2aehqbh7x6km5lo3b2mjaftxdkvqoh3wluc3iv6k2a',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'ALTER TYPE test::Test RENAME TO test::Test2;',
                }],
                'prompt': (
                    "did you rename object type 'test::Test' to 'test::Test2'?"
                ),
            },
        })

        await self.con.execute('''
            ALTER CURRENT MIGRATION REJECT PROPOSED;
        ''')

        await self.assert_describe_migration({
            'parent': 'm1xh653zionj2aehqbh7x6km5lo3b2mjaftxdkvqoh3wluc3iv6k2a',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'ALTER TYPE test::Test RENAME TO test::Test3;',
                }],
                'prompt': (
                    "did you rename object type 'test::Test' to 'test::Test3'?"
                ),
            },
        })

        await self.con.execute('''
            ALTER TYPE test::Test RENAME TO test::Test3;
        ''')

        await self.assert_describe_migration({
            'parent': 'm1xh653zionj2aehqbh7x6km5lo3b2mjaftxdkvqoh3wluc3iv6k2a',
            'confirmed': ['ALTER TYPE test::Test RENAME TO test::Test3;'],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'CREATE TYPE test::Test2;',
                }],
                'prompt': (
                    "did you create object type 'test::Test2'?"
                ),
            },
        })

    async def test_edgeql_migration_describe_property_01(self):
        # Migration that renames a property.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type01 {
                        property field1 -> str;
                    };
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE TYPE test::Type01 {\n'
                        '    CREATE PROPERTY field1'
                        ' -> std::str;\n'
                        '};'
                    )
                }],
                'prompt': "did you create object type 'test::Type01'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.con.execute('''
            INSERT test::Type01 {
                field1 := 'prop_test'
            };
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type01 {
                        property field01 -> str;
                    };
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER TYPE test::Type01 {\n'
                        '    ALTER PROPERTY field1 {\n'
                        '        RENAME TO field01;\n'
                        '    };\n'
                        '};'
                    )
                }],
                'prompt': (
                    "did you rename property 'field1' of object type"
                    " 'test::Type01' to 'field01'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT test::Type01 {
                field01
            };
        ''', [{'field01': 'prop_test'}])

    async def test_edgeql_migration_describe_property_02(self):
        # Migration that creates a type with property.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type02 {
                        property field02 -> str;
                    };
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE TYPE test::Type02 {\n'
                        '    CREATE PROPERTY field02'
                        ' -> std::str;\n'
                        '};'
                    )
                }],
                'prompt': "did you create object type 'test::Type02'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        res = await self.con.query('''
            INSERT test::Type02 {
                field02 := 'prop_test'
            };
        ''')

        # Migration that drops a property.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type02;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER TYPE test::Type02 {\n'
                        '    DROP PROPERTY field02;\n'
                        '};'
                    )
                }],
                'prompt': (
                    "did you drop property 'field02'"
                    " of object type 'test::Type02'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT test::Type02 {
                id
            };
        ''', [{
            'id': res[0].id
        }])

        # Make sure that property dropped cleanly by re-creating and
        # using the property again.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type02 {
                        property field02 -> str;
                    };
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER TYPE test::Type02 {\n'
                        '    CREATE PROPERTY field02'
                        ' -> std::str;\n'
                        '};'
                    )
                }],
                'prompt': (
                    "did you create property 'field02'"
                    " of object type 'test::Type02'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT test::Type02 {
                id,
                field02,
            };
        ''', [{
            'id': res[0].id,
            'field02': None,
        }])

    async def test_edgeql_migration_describe_link_01(self):
        # Migration that renames a link.
        await self.con.execute(r'''
            START MIGRATION TO {
                module test {
                    type Foo;
                    type Type01 {
                        link foo1 -> Foo;
                    };
                };
            };

            # just initialize Foo, since we're interested in the other type
            CREATE TYPE test::Foo;
        ''')

        await self.assert_describe_migration({
            'confirmed': ['CREATE TYPE test::Foo;'],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE TYPE test::Type01 {\n'
                        '    CREATE LINK foo1'
                        ' -> test::Foo;\n'
                        '};'
                    )
                }],
                'prompt': "did you create object type 'test::Type01'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        res = await self.con.query('''
            WITH MODULE test
            SELECT (
                INSERT Type01 {
                    foo1 := (INSERT Foo)
                }
            ) {
                foo1
            };
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Foo;
                    type Type01 {
                        link foo01 -> Foo;
                    };
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER TYPE test::Type01 {\n'
                        '    ALTER LINK foo1 {\n'
                        '        RENAME TO foo01;\n'
                        '    };\n'
                        '};'
                    )
                }],
                'prompt': (
                    "did you rename link 'foo1' of object type"
                    " 'test::Type01' to 'foo01'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT test::Type01 {
                foo01: {
                    id
                }
            };
        ''', [{'foo01': {'id': res[0].foo1.id}}])

    async def test_edgeql_migration_describe_link_02(self):
        # Migration that creates a type with link.
        await self.con.execute(r'''
            START MIGRATION TO {
                module test {
                    type Foo;
                    type Type02 {
                        link foo02 -> Foo;
                    };
                };
            };

            # just initialize Foo, since we're interested in the other type
            CREATE TYPE test::Foo;
        ''')

        await self.assert_describe_migration({
            'confirmed': ['CREATE TYPE test::Foo;'],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE TYPE test::Type02 {\n'
                        '    CREATE LINK foo02'
                        ' -> test::Foo;\n'
                        '};'
                    )
                }],
                'prompt': "did you create object type 'test::Type02'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        res = await self.con.query('''
            WITH MODULE test
            SELECT (
                INSERT Type02 {
                    foo02 := (INSERT Foo)
                }
            ) {
                foo02
            }
        ''')

        # Migration that drops a link.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Foo;
                    type Type02;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER TYPE test::Type02 {\n'
                        '    DROP LINK foo02;\n'
                        '};'
                    )
                }],
                'prompt': (
                    "did you drop link 'foo02' of object type 'test::Type02'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT test::Type02 {
                id
            };
        ''', [{
            'id': res[0].id
        }])
        await self.assert_query_result('''
            SELECT test::Foo {
                id
            };
        ''', [{
            'id': res[0].foo02.id
        }])

        # Make sure that link dropped cleanly by re-creating and
        # using the link again.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Foo;
                    type Type02 {
                        link foo02 -> Foo;
                    };
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER TYPE test::Type02 {\n'
                        '    CREATE LINK foo02'
                        ' -> test::Foo;\n'
                        '};'
                    )
                }],
                'prompt': (
                    "did you create link 'foo02'"
                    " of object type 'test::Type02'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT test::Type02 {
                id,
                foo02: {
                    id
                },
            };
        ''', [{
            'id': res[0].id,
            'foo02': None,
        }])

    async def test_edgeql_migration_describe_link_03(self):
        # Migration that renames a link.
        await self.con.execute(r'''
            START MIGRATION TO {
                module test {
                    abstract link foo3;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE ABSTRACT LINK test::foo3;'
                    )
                }],
                'prompt': "did you create abstract link 'test::foo3'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract link foo03;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER ABSTRACT LINK test::foo3 '
                        'RENAME TO test::foo03;'
                    )
                }],
                'prompt': (
                    "did you rename abstract link 'test::foo3' to "
                    "'test::foo03'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'DROP ABSTRACT LINK test::foo03;'
                    )
                }],
                'prompt': (
                    "did you drop abstract link 'test::foo03'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

    async def test_edgeql_migration_describe_scalar_01(self):
        # Migration that renames a type.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type ScalarType1 extending int64;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE SCALAR TYPE test::ScalarType1'
                        ' EXTENDING std::int64;'
                    )
                }],
                'prompt': "did you create scalar type 'test::ScalarType1'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT <test::ScalarType1>'1' + 2;
        ''', [3])

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type ScalarType01 extending int64;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER SCALAR TYPE test::ScalarType1'
                        ' RENAME TO test::ScalarType01;'
                    )
                }],
                'prompt': (
                    "did you rename scalar type 'test::ScalarType1' to "
                    "'test::ScalarType01'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT <test::ScalarType01>'2' + 1;
        ''', [3])

    async def test_edgeql_migration_describe_scalar_02(self):
        # Migration that creates a type.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type ScalarType02 extending str;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE SCALAR TYPE test::ScalarType02'
                        ' EXTENDING std::str;'
                    )
                }],
                'prompt': "did you create scalar type 'test::ScalarType02'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT <test::ScalarType02>1 ++ '2';
        ''', ['12'])

        # Migration that drops a type.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'DROP SCALAR TYPE test::ScalarType02;'
                    )
                }],
                'prompt': (
                    "did you drop scalar type 'test::ScalarType02'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            WITH MODULE schema
            SELECT ScalarType
            FILTER .name = 'test::ScalarType02';
        ''', [])

        # Make sure that type dropped cleanly by re-creating and
        # using the type again.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type ScalarType02 extending str;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE SCALAR TYPE test::ScalarType02'
                        ' EXTENDING std::str;'
                    )
                }],
                'prompt': "did you create scalar type 'test::ScalarType02'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT <test::ScalarType02>2 ++ '1';
        ''', ['21'])

    async def test_edgeql_migration_describe_enum_01(self):
        # Migration that renames an enum.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type EnumType1 extending enum<foo, bar>;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        "CREATE SCALAR TYPE test::EnumType1"
                        " EXTENDING enum<foo, bar>;"
                    )
                }],
                'prompt': "did you create scalar type 'test::EnumType1'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT <test::EnumType1>'bar';
        ''', ['bar'])

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type EnumType01 extending enum<foo, bar>;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER SCALAR TYPE test::EnumType1'
                        ' RENAME TO test::EnumType01;'
                    )
                }],
                'prompt': (
                    "did you rename scalar type 'test::EnumType1' to "
                    "'test::EnumType01'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT <test::EnumType01>'foo';
        ''', ['foo'])

    async def test_edgeql_migration_describe_enum_02(self):
        # Migration that creates an enum.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type EnumType02 extending enum<foo, bar>;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        "CREATE SCALAR TYPE test::EnumType02"
                        " EXTENDING enum<foo, bar>;"
                    )
                }],
                'prompt': "did you create scalar type 'test::EnumType02'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT <test::EnumType02>'bar';
        ''', ['bar'])

        # Migration that drops an enum.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'DROP SCALAR TYPE test::EnumType02;'
                    )
                }],
                'prompt': (
                    "did you drop scalar type 'test::EnumType02'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            WITH MODULE schema
            SELECT ScalarType
            FILTER .name = 'test::EnumType02';
        ''', [])

        # Make sure that enum dropped cleanly by re-creating and
        # using the enum again.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type EnumType02 extending enum<foo, bar>;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        "CREATE SCALAR TYPE test::EnumType02"
                        " EXTENDING enum<foo, bar>;"
                    )
                }],
                'prompt': "did you create scalar type 'test::EnumType02'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT <test::EnumType02>'foo';
        ''', ['foo'])

    async def test_edgeql_migration_describe_annotation_01(self):
        # Migration that renames an annotation.
        await self.migrate('''
            abstract annotation my_anno1;
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract annotation renamed_anno1;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER ABSTRACT ANNOTATION test::my_anno1 '
                        'RENAME TO test::renamed_anno1;'
                    )
                }],
            },
        })

    async def test_edgeql_migration_describe_annotation_02(self):
        # Migration that creates an annotation.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract annotation my_anno2;

                    type AnnoType2 {
                        annotation my_anno2 := 'test_my_anno2';
                    }
                };
            };
        ''')

        await self.con.execute('''
            CREATE TYPE test::AnnoType2;
        ''')

        await self.assert_describe_migration({
            'confirmed': [
                'CREATE TYPE test::AnnoType2;'
            ],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE ABSTRACT ANNOTATION test::my_anno2;'
                    )
                }],
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        # Migration that drops an annotation.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type AnnoType2;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER TYPE test::AnnoType2 {\n'
                        '    DROP ANNOTATION test::my_anno2;\n'
                        '};'
                    )
                }],
            },
        })
        # Auto-complete migration
        await self.con.execute('''
            ALTER TYPE test::AnnoType2 {
                DROP ANNOTATION test::my_anno2;
            };
        ''')

        await self.assert_describe_migration({
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'DROP ABSTRACT ANNOTATION test::my_anno2;'
                    )
                }],
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        # Make sure that annotation dropped cleanly by re-creating and
        # using the annotation.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract annotation my_anno2;

                    type AnnoType2 {
                        annotation my_anno2 := 'retest_my_anno2';
                    }
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE ABSTRACT ANNOTATION test::my_anno2;'
                    )
                }],
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value,
                    },
                } FILTER .name = 'test::AnnoType2';
            """,
            [{
                'name': 'test::AnnoType2',
                'annotations': [{
                    'name': 'test::my_anno2',
                    '@value': 'retest_my_anno2',
                }]
            }],
        )

    async def test_edgeql_migration_describe_constraint_01(self):
        # Migration that renames a constraint.
        await self.migrate('''
            abstract constraint my_oneof(one_of: array<anytype>) {
                using (contains(one_of, __subject__));
            };

            type Foo {
                property note -> str {
                    constraint my_oneof(["foo", "bar"]);
                }
            }
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract constraint my_one_of(one_of: array<anytype>) {
                        using (contains(one_of, __subject__));
                    };

                    type Foo {
                        property note -> str {
                            constraint my_one_of(["foo", "bar"]);
                        }
                    }
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER ABSTRACT CONSTRAINT test::my_oneof '
                        'RENAME TO test::my_one_of;'
                    )
                }],
            },
        })

        await self.fast_forward_describe_migration()

    async def test_edgeql_migration_describe_constraint_02(self):
        # Migration that renames a link constraint.
        # Honestly I'm not sure if link constraints can really be
        # anything other than exclusive?
        await self.migrate('''
            abstract constraint my_exclusive() extending std::exclusive;

            type Foo;
            type Bar {
                link foo -> Foo {
                    constraint my_exclusive;
                }
            }
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract constraint myexclusive() extending std::exclusive;

                    type Foo;
                    type Bar {
                        link foo -> Foo {
                            constraint myexclusive;
                        }
                    }
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER ABSTRACT CONSTRAINT test::my_exclusive '
                        'RENAME TO test::myexclusive;'
                    )
                }],
            },
        })

        await self.fast_forward_describe_migration()

    async def test_edgeql_migration_describe_constraint_03(self):
        # Migration that renames a object constraint.
        await self.migrate('''
            abstract constraint my_oneof(one_of: array<anytype>) {
                using (contains(one_of, __subject__));
            };

            type Foo {
                property a -> str;
                property b -> str;
                constraint my_oneof(["foo", "bar"])
                    ON (__subject__.a++__subject__.b);
            }
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract constraint my_one_of(one_of: array<anytype>) {
                        using (contains(one_of, __subject__));
                    };

                    type Foo {
                        property a -> str;
                        property b -> str;
                        constraint my_one_of(["foo", "bar"])
                            ON (__subject__.a++__subject__.b);
                    }
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER ABSTRACT CONSTRAINT test::my_oneof '
                        'RENAME TO test::my_one_of;'
                    )
                }],
            },
        })

        await self.fast_forward_describe_migration()

    async def test_edgeql_migration_describe_constraint_04(self):
        # Migration that creates a constraint.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract constraint my_one_of(one_of: array<anytype>) {
                        using (contains(one_of, __subject__));
                    };

                    scalar type my_str extending str {
                        constraint my_one_of(['my', 'str']);
                    };
                };
            };
        ''')

        await self.con.execute('''
            CREATE SCALAR TYPE test::my_str EXTENDING std::str;
        ''')

        await self.assert_describe_migration({
            'confirmed': [
                'CREATE SCALAR TYPE test::my_str EXTENDING std::str;'
            ],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE ABSTRACT CONSTRAINT test::my_one_of('
                        'one_of: array<anytype>) {\n'
                        '    USING (std::contains(one_of, __subject__));\n'
                        '};'
                    ),
                }],
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()
        await self.con.query('DECLARE SAVEPOINT migration_01')

        await self.assert_query_result(
            r"""
                SELECT <test::my_str>'my';
            """,
            ['my'],
        )
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r"invalid my_str"):
            await self.con.execute(r"""
                SELECT <test::my_str>'nope';
            """)
        await self.con.query('ROLLBACK TO SAVEPOINT migration_01')

        # Migration that drops a constraint.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type my_str extending str;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        "ALTER SCALAR TYPE test::my_str {\n"
                        "    DROP CONSTRAINT test::my_one_of(['my', 'str']);\n"
                        "};"
                    ),
                }],
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result(
            r"""
                SELECT <test::my_str>'my';
            """,
            ['my'],
        )
        await self.assert_query_result(
            r"""
                SELECT <test::my_str>'nope';
            """,
            ['nope'],
        )

        # Test that dropping constraint was clean with a migration
        # that re-creates a constraint.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract constraint my_one_of(one_of: array<anytype>) {
                        using (contains(one_of, __subject__));
                    };

                    scalar type my_str extending str {
                        constraint my_one_of(['my2', 'str2']);
                    };
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE ABSTRACT CONSTRAINT '
                        'test::my_one_of(one_of: array<anytype>) {\n'
                        '    USING (std::contains(one_of, __subject__));\n'
                        '};'
                    ),
                }],
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result(
            r"""
                SELECT <test::my_str>'my2';
            """,
            ['my2'],
        )
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r"invalid my_str"):
            await self.con.execute(r"""
                SELECT <test::my_str>'my';
            """)

    async def test_edgeql_migration_describe_abs_ptr_01(self):
        await self.migrate('''
            abstract link abs_link;
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract link new_abs_link;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER ABSTRACT LINK test::abs_link '
                        'RENAME TO test::new_abs_link;'
                    )
                }],
            },
        })

    async def test_edgeql_migration_describe_function_01(self):
        await self.migrate('''
            function foo(x: str) -> str using (SELECT <str>random());
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    function bar(x: str) -> str using (SELECT <str>random());
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER FUNCTION test::foo(x: std::str) '
                        '{RENAME TO test::bar;};'
                    )
                }],
            },
        })

    async def test_edgeql_migration_function_01(self):
        await self.migrate('''
            type Note {
                required property name -> str;
            }

            function hello_note(x: Note) -> str {
                USING (SELECT x.name)
            }
        ''')

    async def test_edgeql_migration_function_02(self):
        await self.migrate('''
            type Foo;

            function foo(x: Foo) -> int64 {
                USING (SELECT 0)
            }
        ''')

        await self.migrate('''
            type Bar;

            function foo(x: Bar) -> int64 {
                USING (SELECT 0)
            }
        ''')

        await self.con.execute('''
            DROP FUNCTION test::foo(x: test::Bar);
        ''')

    async def test_edgeql_migration_function_03(self):
        await self.migrate('''
            type Foo;

            function foo(x: Foo) -> int64 {
                USING (SELECT 0)
            }
        ''')

        await self.migrate('''
            type Bar;

            function foo2(x: Bar) -> int64 {
                USING (SELECT 0)
            }
        ''')

        await self.con.execute('''
            DROP FUNCTION test::foo2(x: test::Bar);
        ''')

    async def test_edgeql_migration_function_04(self):
        await self.migrate('''
            function foo() -> str USING ('foo');
        ''')

        await self.start_migration('''
            function foo() -> str USING ('bar');
        ''')

        await self.interact([
            "did you alter function 'test::foo'?"
        ])
        await self.fast_forward_describe_migration()

        await self.assert_query_result(
            r"""
                SELECT test::foo()
            """,
            ["bar"],
        )

    async def test_edgeql_migration_function_05(self):
        await self.migrate("""
            type Person {
                required property name -> str {
                    constraint exclusive;
                }
                multi link places_visited -> Place;
            }

            type Place {
                required property name -> str {
                    constraint exclusive;
                }
            }

            function visited(person: str, city: str) -> bool
                using (
                    WITH person := (SELECT Person FILTER .name = person),
                    SELECT city IN person.places_visited.name
                );
        """)

    async def test_edgeql_migration_constraint_01(self):
        await self.migrate('''
            abstract constraint not_bad {
                using (__subject__ != "bad" and __subject__ != "terrible")
            }

            type Foo {
                property foo -> str {
                    constraint not_bad;
                }
            }
            type Bar extending Foo;
        ''')

        await self.start_migration('''
            abstract constraint not_bad {
                using (__subject__ != "bad" and __subject__ != "awful")
            }

            type Foo {
                property foo -> str {
                    constraint not_bad;
                }
            }
            type Bar extending Foo;
        ''')

        await self.interact([
            "did you alter abstract constraint 'test::not_bad'?"
        ])
        await self.fast_forward_describe_migration()

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "invalid foo",
        ):
            await self.con.execute(r"""
                INSERT test::Foo { foo := "awful" };
            """)

    async def test_edgeql_migration_describe_type_rename_01(self):
        await self.migrate('''
            type Foo;
            type Baz {
                link l -> Foo;
            };
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Bar;
                    type Baz {
                        link l -> Bar;
                    };
                }
            };
            POPULATE MIGRATION;
        ''')

        await self.assert_describe_migration({
            'complete': True,
            'confirmed': [
                'ALTER TYPE test::Foo RENAME TO test::Bar;'
            ],
        })

        await self.fast_forward_describe_migration()

    async def test_edgeql_migration_describe_populate_describe(self):
        await self.start_migration('''
            type Foo;
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE TYPE test::Foo;'
                    )
                }],
            },
        })

        await self.con.execute('POPULATE MIGRATION;')

        await self.assert_describe_migration({
            'confirmed': ['CREATE TYPE test::Foo;'],
            'complete': True,
            'proposed': None,
        })

    async def test_edgeql_migration_computed_01(self):
        await self.migrate(r'''
            type Foo {
                property val -> str;
                property comp := count((
                    # Use an alias in WITH block in a computable
                    WITH x := .val
                    # Use an alias in SELECT in a computable
                    SELECT y := Bar FILTER x = y.val
                ))
            }

            type Bar {
                property val -> str;
            }
        ''')

        await self.con.execute("""
            SET MODULE test;

            INSERT Foo {val := 'c'};
            INSERT Foo {val := 'd'};

            INSERT Bar {val := 'a'};
            INSERT Bar {val := 'b'};
            INSERT Bar {val := 'c'};
            INSERT Bar {val := 'c'};
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo {
                    val,
                    comp,
                } ORDER BY .val;
            """,
            [{
                'val': 'c',
                'comp': 2,
            }, {
                'val': 'd',
                'comp': 0,
            }],
        )

    async def test_edgeql_migration_computed_02(self):
        await self.migrate(r'''
            type Foo { property foo := '1' };
            type Bar extending Foo;
        ''')

        await self.migrate(r'''
            type Foo { property foo := 1 };
            type Bar extending Foo;
        ''')

    async def test_edgeql_migration_computed_03(self):
        await self.migrate(r'''
            type User {
                property name -> str;
                multi link tweets := Tweet;
            }
            type Tweet {
                property text -> str;
                link author -> User;
            }
        ''', module='default')

        await self.con.execute("""
            INSERT Tweet {
                text := 'Hello',
                author := (
                    INSERT User {name := 'Alice'}
                )
            };
            INSERT Tweet {
                text := 'Hi',
                author := (
                    INSERT User {name := 'Billie'}
                )
            };
        """)

        # Validate our structures
        await self.assert_query_result(
            r"""
                SELECT Tweet {
                    text,
                    author: {
                        name
                    },
                } ORDER BY .text;
            """,
            [{
                'text': 'Hello',
                'author': {
                    'name': 'Alice'
                },
            }, {
                'text': 'Hi',
                'author': {
                    'name': 'Billie'
                },
            }],
        )

        await self.assert_query_result(
            r"""
                SELECT User {
                    name,
                    tweets: {
                        text
                    } ORDER BY .text,
                } ORDER BY .name;
            """,
            [{
                'name': 'Alice',
                'tweets': [{
                    'text': 'Hello'
                }, {
                    'text': 'Hi'
                }],
            }, {
                'name': 'Billie',
                'tweets': [{
                    'text': 'Hello'
                }, {
                    'text': 'Hi'
                }],
            }],
        )

        await self.migrate(r'''
            type User {
                property name -> str;
                multi link tweets := User.<author[IS Tweet];
            }
            type Tweet {
                property text -> str;
                link author -> User;
            }
        ''', module='default')

        await self.assert_query_result(
            r"""
                SELECT User {
                    name,
                    tweets: {
                        text
                    } ORDER BY .text,
                } ORDER BY .name;
            """,
            [{
                'name': 'Alice',
                'tweets': [{
                    'text': 'Hello'
                }],
            }, {
                'name': 'Billie',
                'tweets': [{
                    'text': 'Hi'
                }],
            }],
        )

        await self.migrate(r'''
            type User {
                property name -> str;
                multi link tweets := .<author[IS Tweet];
            }
            type Tweet {
                property text -> str;
                link author -> User;
            }
        ''', module='default')

        await self.assert_query_result(
            r"""
                SELECT User {
                    name,
                    tweets: {
                        text
                    } ORDER BY .text,
                } ORDER BY .name;
            """,
            [{
                'name': 'Alice',
                'tweets': [{
                    'text': 'Hello'
                }],
            }, {
                'name': 'Billie',
                'tweets': [{
                    'text': 'Hi'
                }],
            }],
        )

        await self.migrate(r'''
            type User {
                property name -> str;
                multi link tweets := (
                    SELECT Tweet FILTER .author = User
                );
            }
            type Tweet {
                property text -> str;
                link author -> User;
            }
        ''', module='default')

        await self.assert_query_result(
            r"""
                SELECT User {
                    name,
                    tweets: {
                        text
                    } ORDER BY .text,
                } ORDER BY .name;
            """,
            [{
                'name': 'Alice',
                'tweets': [{
                    'text': 'Hello'
                }],
            }, {
                'name': 'Billie',
                'tweets': [{
                    'text': 'Hi'
                }],
            }],
        )

        await self.migrate(r'''
            type User {
                property name -> str;
                multi link tweets := (
                    WITH U := User
                    SELECT Tweet FILTER .author = U
                );
            }
            type Tweet {
                property text -> str;
                link author -> User;
            }
        ''', module='default')

        await self.assert_query_result(
            r"""
                SELECT User {
                    name,
                    tweets: {
                        text
                    } ORDER BY .text,
                } ORDER BY .name;
            """,
            [{
                'name': 'Alice',
                'tweets': [{
                    'text': 'Hello'
                }],
            }, {
                'name': 'Billie',
                'tweets': [{
                    'text': 'Hi'
                }],
            }],
        )

        await self.migrate(r'''
            type User {
                property name -> str;
                multi link tweets := (
                    WITH U := DETACHED User
                    SELECT Tweet FILTER .author = U
                );
            }
            type Tweet {
                property text -> str;
                link author -> User;
            }
        ''', module='default')

        await self.assert_query_result(
            r"""
                SELECT User {
                    name,
                    tweets: {
                        text
                    } ORDER BY .text,
                } ORDER BY .name;
            """,
            [{
                'name': 'Alice',
                'tweets': [{
                    'text': 'Hello'
                }, {
                    'text': 'Hi'
                }],
            }, {
                'name': 'Billie',
                'tweets': [{
                    'text': 'Hello'
                }, {
                    'text': 'Hi'
                }],
            }],
        )

        await self.migrate(r'''
            type User {
                property name -> str;
                multi link tweets := (
                    WITH U := User
                    SELECT U.<author[IS Tweet]
                );
            }
            type Tweet {
                property text -> str;
                link author -> User;
            }
        ''', module='default')

        await self.assert_query_result(
            r"""
                SELECT User {
                    name,
                    tweets: {
                        text
                    } ORDER BY .text,
                } ORDER BY .name;
            """,
            [{
                'name': 'Alice',
                'tweets': [{
                    'text': 'Hello'
                }],
            }, {
                'name': 'Billie',
                'tweets': [{
                    'text': 'Hi'
                }],
            }],
        )

        await self.migrate(r'''
            type User {
                property name -> str;
                multi link tweets := (
                    WITH User := DETACHED User
                    SELECT User.<author[IS Tweet]
                );
            }
            type Tweet {
                property text -> str;
                link author -> User;
            }
        ''', module='default')

        await self.assert_query_result(
            r"""
                SELECT User {
                    name,
                    tweets: {
                        text
                    } ORDER BY .text,
                } ORDER BY .name;
            """,
            [{
                'name': 'Alice',
                'tweets': [{
                    'text': 'Hello'
                }, {
                    'text': 'Hi'
                }],
            }, {
                'name': 'Billie',
                'tweets': [{
                    'text': 'Hello'
                }, {
                    'text': 'Hi'
                }],
            }],
        )

    async def test_edgeql_migration_computed_04(self):
        await self.migrate(r'''
            type User {
                property name -> str;
                multi property tweets := Tweet.text;
            }
            type Tweet {
                property text -> str;
                link author -> User;
            }
        ''', module='default')

        await self.con.execute("""
            INSERT Tweet {
                text := 'Hello',
                author := (
                    INSERT User {name := 'Alice'}
                )
            };
            INSERT Tweet {
                text := 'Hi',
                author := (
                    INSERT User {name := 'Billie'}
                )
            };
        """)

        # Validate our structures
        await self.assert_query_result(
            r"""
                SELECT Tweet {
                    text,
                    author: {
                        name
                    },
                } ORDER BY .text;
            """,
            [{
                'text': 'Hello',
                'author': {
                    'name': 'Alice'
                },
            }, {
                'text': 'Hi',
                'author': {
                    'name': 'Billie'
                },
            }],
        )

        await self.assert_query_result(
            r"""
                SELECT User {
                    name,
                    tweets,
                } ORDER BY .name;
            """,
            [{
                'name': 'Alice',
                'tweets': {'Hello', 'Hi'},
            }, {
                'name': 'Billie',
                'tweets': {'Hello', 'Hi'},
            }],
        )

        await self.migrate(r'''
            type User {
                property name -> str;
                multi property tweets := User.<author[IS Tweet].text;
            }
            type Tweet {
                property text -> str;
                link author -> User;
            }
        ''', module='default')

        await self.assert_query_result(
            r"""
                SELECT User {
                    name,
                    tweets,
                } ORDER BY .name;
            """,
            [{
                'name': 'Alice',
                'tweets': {'Hello'},
            }, {
                'name': 'Billie',
                'tweets': {'Hi'},
            }],
        )

        await self.migrate(r'''
            type User {
                property name -> str;
                multi property tweets := .<author[IS Tweet].text;
            }
            type Tweet {
                property text -> str;
                link author -> User;
            }
        ''', module='default')

        await self.assert_query_result(
            r"""
                SELECT User {
                    name,
                    tweets,
                } ORDER BY .name;
            """,
            [{
                'name': 'Alice',
                'tweets': {'Hello'},
            }, {
                'name': 'Billie',
                'tweets': {'Hi'},
            }],
        )

        await self.migrate(r'''
            type User {
                property name -> str;
                multi property tweets := (
                    SELECT Tweet FILTER .author = User
                ).text;
            }
            type Tweet {
                property text -> str;
                link author -> User;
            }
        ''', module='default')

        await self.assert_query_result(
            r"""
                SELECT User {
                    name,
                    tweets,
                } ORDER BY .name;
            """,
            [{
                'name': 'Alice',
                'tweets': {'Hello'},
            }, {
                'name': 'Billie',
                'tweets': {'Hi'},
            }],
        )

        await self.migrate(r'''
            type User {
                property name -> str;
                multi property tweets := (
                    WITH U := User
                    SELECT Tweet FILTER .author = U
                ).text;
            }
            type Tweet {
                property text -> str;
                link author -> User;
            }
        ''', module='default')

        await self.assert_query_result(
            r"""
                SELECT User {
                    name,
                    tweets,
                } ORDER BY .name;
            """,
            [{
                'name': 'Alice',
                'tweets': {'Hello'},
            }, {
                'name': 'Billie',
                'tweets': {'Hi'},
            }],
        )

        await self.migrate(r'''
            type User {
                property name -> str;
                multi property tweets := (
                    WITH U := DETACHED User
                    SELECT Tweet FILTER .author = U
                ).text;
            }
            type Tweet {
                property text -> str;
                link author -> User;
            }
        ''', module='default')

        await self.assert_query_result(
            r"""
                SELECT User {
                    name,
                    tweets,
                } ORDER BY .name;
            """,
            [{
                'name': 'Alice',
                'tweets': {'Hello', 'Hi'},
            }, {
                'name': 'Billie',
                'tweets': {'Hello', 'Hi'},
            }],
        )

        await self.migrate(r'''
            type User {
                property name -> str;
                multi property tweets := (
                    WITH U := User
                    SELECT U.<author[IS Tweet].text
                );
            }
            type Tweet {
                property text -> str;
                link author -> User;
            }
        ''', module='default')

        await self.assert_query_result(
            r"""
                SELECT User {
                    name,
                    tweets,
                } ORDER BY .name;
            """,
            [{
                'name': 'Alice',
                'tweets': {'Hello'},
            }, {
                'name': 'Billie',
                'tweets': {'Hi'},
            }],
        )

        await self.migrate(r'''
            type User {
                property name -> str;
                multi property tweets := (
                    WITH User := DETACHED User
                    SELECT User.<author[IS Tweet].text
                );
            }
            type Tweet {
                property text -> str;
                link author -> User;
            }
        ''', module='default')

        await self.assert_query_result(
            r"""
                SELECT User {
                    name,
                    tweets,
                } ORDER BY .name;
            """,
            [{
                'name': 'Alice',
                'tweets': {'Hello', 'Hi'},
            }, {
                'name': 'Billie',
                'tweets': {'Hello', 'Hi'},
            }],
        )

    async def test_edgeql_migration_computed_05(self):
        await self.migrate(r'''
            type Bar {
                multi link foo := Foo;
                property name -> str;
            };
            type Foo {
                link bar -> Bar;
                property val -> str;
            };
        ''', module='default')

        await self.con.execute("""
            INSERT Foo {
                val := 'foo0',
                bar := (
                    INSERT Bar {name := 'bar0'}
                ),
            };
            INSERT Foo {
                val := 'foo1',
                bar := (
                    INSERT Bar {name := 'bar1'}
                ),
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo {
                    val,
                    bar: {
                        name,
                        foo: {
                            val
                        } ORDER BY .val,
                    },
                } ORDER BY .val;
            """,
            [{
                'val': 'foo0',
                'bar': {
                    'name': 'bar0',
                    'foo': [{'val': 'foo0'}, {'val': 'foo1'}],
                },
            }, {
                'val': 'foo1',
                'bar': {
                    'name': 'bar1',
                    'foo': [{'val': 'foo0'}, {'val': 'foo1'}],
                },
            }],
        )

    async def test_edgeql_migration_computed_06(self):
        await self.migrate(r'''
            type Bar {
                multi property foo := Foo.val;
                property name -> str;
            };
            type Foo {
                link bar -> Bar;
                property val -> str;
            };
        ''', module='default')

        await self.con.execute("""
            INSERT Foo {
                val := 'foo0',
                bar := (
                    INSERT Bar {name := 'bar0'}
                ),
            };
            INSERT Foo {
                val := 'foo1',
                bar := (
                    INSERT Bar {name := 'bar1'}
                ),
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo {
                    val,
                    bar: {
                        name,
                        foo,
                    },
                } ORDER BY .val;
            """,
            [{
                'val': 'foo0',
                'bar': {
                    'name': 'bar0',
                    'foo': {'foo0', 'foo1'},
                },
            }, {
                'val': 'foo1',
                'bar': {
                    'name': 'bar1',
                    'foo': {'foo0', 'foo1'},
                },
            }],
        )

    async def test_edgeql_migration_reject_prop_01(self):
        await self.migrate('''
            type User {
                property foo -> str;
            };
        ''')

        await self.start_migration('''
            type User {
                property bar -> str;
            };
        ''')

        await self.interact([
            ("did you rename property 'foo' of object type "
             "'test::User' to 'bar'?", "n"),
            # XXX: or should this be split up?
            "did you alter object type 'test::User'?"
        ])
        await self.fast_forward_describe_migration()

    async def test_edgeql_migration_reject_prop_02(self):
        await self.migrate('''
            type User {
                required property foo -> str;
            };
        ''')

        await self.start_migration('''
            type User {
                property bar -> str;
            };
        ''')

        # Initial confidence should *not* be 1.0 here
        res = json.loads(await self.con.query_single(
            'DESCRIBE CURRENT MIGRATION AS JSON;'))
        self.assertLess(res['proposed']['confidence'], 1.0)

        await self.interact([
            ("did you rename property 'foo' of object type 'test::User' to "
             "'bar'?", "n"),
            # XXX: or should this be split up?
            "did you alter object type 'test::User'?"
        ])
        await self.fast_forward_describe_migration()

    async def test_edgeql_migration_reject_prop_03(self):
        await self.migrate('''
            type User {
                required property foo -> str;
            };
        ''')

        await self.start_migration('''
            type User {
                required property bar -> int64;
            };
        ''')

        await self.interact([
            # Or should this be split into rename and reset optionality?
            ("did you create property 'bar' of object type 'test::User'?",
             "n"),
            ("did you rename property 'foo' of object type 'test::User' to "
             "'bar'?"),
            ("did you alter the type of property 'bar' of object type "
             "'test::User'?",
             "y",
             "<int64>.bar"),
        ])
        await self.fast_forward_describe_migration()

    async def test_edgeql_migration_reject_prop_04(self):
        await self.migrate('''
            type Foo;
            type Bar;
        ''')

        await self.start_migration('''
            type Foo;
            type Bar extending Foo;
        ''')

        await self.interact([
            ("did you alter object type 'test::Bar'?", "n"),
            "did you drop object type 'test::Bar'?",
            "did you create object type 'test::Bar'?",
        ])
        await self.fast_forward_describe_migration()

    @test.xerror('Fails to rebase because the type is mismatched')
    async def test_edgeql_migration_reject_prop_05(self):
        await self.migrate('''
            scalar type Slug extending str;
            abstract type Named {
                required property name -> Slug;
            };
            type User {
                required property name -> str;
            };
        ''')

        await self.start_migration('''
            scalar type Slug extending str;
            abstract type Named {
                required property name -> Slug;
            };
            type User extending Named;
        ''')

        await self.interact([
            ("did you drop property 'name' of object type 'test::User'?", "n"),
        ], check_complete=False)
        await self.fast_forward_describe_migration()

    async def test_edgeql_migration_force_delete_01(self):
        await self.migrate('''
            type Base;
            type Foo;
            type Bar { link foo -> Foo; };
        ''')

        await self.start_migration('''
            type Base;
            type Foo extending Base;
            type Bar { link foo -> Foo; };
        ''')

        await self.interact([
            ("did you alter object type 'test::Foo'?", "n"),
            "did you drop link 'foo' of object type 'test::Bar'?"
        ], check_complete=False)
        await self.fast_forward_describe_migration()

    async def test_edgeql_migration_force_delete_02(self):
        await self.migrate('''
            type Base;
            type Foo;
            type Bar extending Foo;
        ''')

        await self.start_migration('''
            type Base;
            type Foo extending Base;
            type Bar extending Foo;
        ''')

        await self.interact([
            ("did you alter object type 'test::Foo'?", "n"),
            "did you drop object type 'test::Bar'?"
        ], check_complete=False)
        await self.fast_forward_describe_migration()

    async def test_edgeql_migration_eq_01(self):
        await self.migrate("""
            type Base;
        """)
        await self.con.execute("""
            SET MODULE test;

            INSERT Base;
        """)

        # Try altering the schema to a state inconsistent with current
        # data.
        with self.assertRaisesRegex(
            AssertionError,
            r"Please specify an expression to populate existing objects "
            r"in order to make property 'name' of object type 'test::Base' "
            r"required"
        ):
            await self.migrate("""
                type Base {
                    required property name -> str;
                }
            """)
        # Migration without making the property required.
        await self.migrate("""
            type Base {
                property name -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    name
                };
            """,
            [{
                'name': None,
            }],
        )

        await self.con.execute("""
            UPDATE
                Base
            SET {
                name := 'base_01'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    name
                };
            """,
            [{
                'name': 'base_01',
            }],
        )

        # Inherit from the Base, making name required.
        await self.migrate("""
            type Base {
                property name -> str;
            }

            type Derived extending Base {
                overloaded required property name -> str;
            }
        """)
        await self.con.execute("""
            INSERT Derived {
                name := 'derived_01'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base.name;
            """,
            {'base_01', 'derived_01'},
        )

    async def test_edgeql_migration_eq_02(self):
        await self.migrate(r"""
            type Base {
                property foo -> str;
            }

            type Derived extending Base {
                overloaded required property foo -> str;
            }
        """)
        await self.con.execute("""
            SET MODULE test;

            INSERT Base {
                foo := 'base_02',
            };
            INSERT Derived {
                foo := 'derived_02',
            };
        """)

        await self.migrate(r"""
            type Base {
                # rename 'foo'
                property foo2 -> str;
            }

            type Derived extending Base {
                overloaded required property foo2 -> str;
            }
        """)

        # the data still persists
        await self.assert_query_result(
            r"""
                SELECT Base {
                    __type__: {name},
                    foo2,
                } ORDER BY .foo2;
            """,
            [{
                '__type__': {'name': 'test::Base'},
                'foo2': 'base_02',
            }, {
                '__type__': {'name': 'test::Derived'},
                'foo2': 'derived_02',
            }],
        )

    async def test_edgeql_migration_eq_03(self):
        await self.migrate(r"""
            type Base {
                property foo -> str;
            }

            type Derived extending Base {
                overloaded required property foo -> str;
            }
        """)
        await self.con.execute("""
            SET MODULE test;

            INSERT Base {
                foo := 'base_03',
            };
            INSERT Derived {
                foo := 'derived_03',
            };
        """)

        await self.migrate(r"""
            type Base;
                # drop 'foo'

            type Derived extending Base {
                # completely different property
                property foo2 -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    __type__: {name},
                    [IS Derived].foo2,
                } ORDER BY .foo2;
            """,
            [{
                '__type__': {'name': 'test::Base'},
                'foo2': None,
            }, {
                '__type__': {'name': 'test::Derived'},
                'foo2': None,
            }],
        )

    async def test_edgeql_migration_eq_04(self):
        await self.migrate(r"""
            type Base {
                property foo -> str;
            }

            type Derived extending Base;

            type Further extending Derived {
                overloaded required property foo -> str;
            }
        """)
        await self.con.execute("""
            SET MODULE test;

            INSERT Base {
                foo := 'base_04',
            };
            INSERT Derived {
                foo := 'derived_04',
            };
            INSERT Further {
                foo := 'further_04',
            };
        """)

        await self.migrate(r"""
            type Base;
                # drop 'foo'

            type Derived extending Base;

            type Further extending Derived {
                # completely different property
                property foo2 -> str;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    __type__: {name},
                    [IS Further].foo2,
                } ORDER BY .__type__.name;
            """,
            [{
                '__type__': {'name': 'test::Base'},
                'foo2': None,
            }, {
                '__type__': {'name': 'test::Derived'},
                'foo2': None,
            }, {
                '__type__': {'name': 'test::Further'},
                'foo2': None,
            }],
        )

    async def test_edgeql_migration_eq_06(self):
        await self.migrate(r"""
            type Base {
                property foo -> int64;
            }

            type Derived extending Base {
                overloaded required property foo -> int64;
            }
        """)
        await self.con.execute("""
            SET MODULE test;

            INSERT Base {
                foo := 6,
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    __type__: {name},
                    foo,
                };
            """,
            [{
                '__type__': {'name': 'test::Base'},
                # the value was correctly inserted
                'foo': 6,
            }],
        )

        await self.migrate(r"""
            type Base {
                property foo -> float64;
            }

            type Derived extending Base {
                overloaded required property foo -> float64;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    __type__: {name},
                    foo,
                };
            """,
            [{
                '__type__': {'name': 'test::Base'},
                'foo': 6.0,
            }],
        )

    async def test_edgeql_migration_eq_07(self):
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.migrate(r"""
            type Child {
                required property name -> str {
                    constraint exclusive;
                }
            }

            type Base {
                required property name -> str;
                link bar -> Child;
            }
        """)

        await self.con.execute('''
            INSERT Child { name := 'c1' };
            INSERT Child { name := 'c2' };

            INSERT Base {
                name := 'b1',
                bar := (SELECT Child FILTER .name = 'c1'),
            };

            INSERT Base {
                name := 'b2',
            };
        ''')

        await self.assert_query_result(
            r"""
                SELECT Base {
                    bar: {
                        name
                    }
                } ORDER BY .name;
            """,
            [{
                'bar': {
                    'name': 'c1',
                },
            }, {
                'bar': None,
            }],
        )

        await self.migrate(r"""
            type Child {
                required property name -> str {
                    constraint exclusive;
                }
            }

            type Base {
                required property name -> str;
                required link bar -> Child {
                    # add a constraint
                    constraint exclusive;
                }
            }
        """, user_input=[
            "SELECT Child FILTER .name = 'c2'"
        ])

        await self.assert_query_result(
            r"""
                SELECT Base {
                    bar: {
                        name
                    }
                } ORDER BY .name;
            """,
            [{
                'bar': {
                    'name': 'c1',
                },
            }, {
                'bar': {
                    'name': 'c2',
                },
            }],
        )

    async def test_edgeql_migration_eq_08(self):
        await self.migrate(r"""
            type Base {
                property foo -> str;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := 'very_long_test_str_base_08',
            };
        """)

        # Try altering the schema to a state inconsistent with current
        # data.
        new_state = r"""
            type Base {
                property foo -> str {
                    # add a constraint
                    constraint max_len_value(10);
                }
            }
        """
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r"foo must be no longer than 10 characters"):
            await self.migrate(new_state)

        # Fix the data.
        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := 'base_08',
            };
        """)

        # Migrate to same state as before now that the data is fixed.
        await self.migrate(new_state)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                };
            """,
            [{
                'foo': 'base_08',
            }],
        )

    async def test_edgeql_migration_eq_09(self):
        await self.migrate(r"""
            scalar type constraint_length extending str {
                constraint max_len_value(10);
            }
            type Base {
                property foo -> constraint_length;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := 'b09',
            };
        """)

        # Try altering the schema to a state inconsistent with current
        # data.
        new_state = r"""
            scalar type constraint_length extending str {
                constraint max_len_value(10);
                # add a constraint
                constraint min_len_value(5);
            }
            type Base {
                property foo -> constraint_length;
            }
        """
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'Existing test::Base\.foo values violate the new constraint'):
            await self.migrate(new_state)

        # Fix the data.
        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := 'base_09',
            };
        """)

        # Migrate to same state as before now that the data is fixed.
        await self.migrate(new_state)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                };
            """,
            [{
                'foo': 'base_09',
            }],
        )

    async def test_edgeql_migration_eq_11(self):
        await self.migrate(r"""
            type Base {
                property foo -> str;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := 'base_11',
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                # change property to link with same name
                link foo -> Child {
                    # add a constraint
                    constraint exclusive;
                }
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                };
            """,
            [{
                'foo': None,
            }],
        )

    async def test_edgeql_migration_eq_12(self):
        await self.migrate(r"""
            type Child;

            type Base {
                property foo -> str {
                    constraint exclusive;
                }

                link bar -> Child {
                    constraint exclusive;
                }
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)
        data = await self.con.query(r"""
            SELECT (
                INSERT Base {
                    foo := 'base_12',
                    bar := (INSERT Child)
                })
            {
                foo,
                bar: {id}
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                # drop constraints
                property foo -> str;
                link bar -> Child;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                    bar: {id}
                };
            """,
            [{
                'foo': 'base_12',
                'bar': {'id': data[0].bar.id}
            }],
        )

    async def test_edgeql_migration_eq_13(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link bar -> Child;
            }

            type Derived extending Base {
                overloaded required link bar -> Child;
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)
        data = await self.con.query(r"""
            SELECT (
                INSERT Derived {
                    bar := (INSERT Child)
                })
            {
                bar: {id}
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base;
                # drop 'bar'

            type Derived extending Base {
                # no longer inherit link 'bar'
                link bar -> Child;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Derived {
                    bar: {id}
                };
            """,
            [{
                'bar': {'id': data[0].bar.id}
            }],
        )

    async def test_edgeql_migration_eq_14a(self):
        await self.migrate(r"""
            type Base;

            type Derived extending Base {
                property foo -> str;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Derived {
                foo := 'derived_14',
            };
        """)

        await self.migrate(r"""
            type Base {
                # move the property earlier in the inheritance
                property foo -> str;
            }

            type Derived extending Base {
                overloaded property foo -> str {
                    annotation title := 'overloaded'
                }
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Derived {
                    foo,
                };
            """,
            [{
                'foo': 'derived_14',
            }],
        )

    async def test_edgeql_migration_eq_14b(self):
        # Same as above, except POPULATE and inspect the query
        await self.migrate(r"""
            type Base;

            type Derived extending Base {
                property foo -> str;
            }
        """)

        await self.start_migration(r"""
            type Base {
                # move the property earlier in the inheritance
                property foo -> str;
            }

            type Derived extending Base {
                overloaded required property foo -> str;
            }
        """, populate=True)

        await self.assert_describe_migration({
            'confirmed': ["""
                ALTER TYPE test::Base {
                    CREATE PROPERTY foo -> std::str;
                };
            """, """
                ALTER TYPE test::Derived {
                    ALTER PROPERTY foo {
                        SET REQUIRED;
                    };
                };
            """],
            'complete': True,
        })

    async def test_edgeql_migration_eq_16(self):
        await self.migrate(r"""
            type Child;

            type Base;

            type Derived extending Base {
                link bar -> Child;
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)
        data = await self.con.query(r"""
            SELECT (
                INSERT Derived {
                    bar := (INSERT Child),
                }
            ) {
                bar: {id}
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                # move the link earlier in the inheritance
                link bar -> Child;
            }

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Derived {
                    bar,
                };
            """,
            [{
                'bar': {'id': data[0].bar.id},
            }],
        )

        await self.migrate(
            r"""
                type Child;

                type Base {
                    link bar -> Child;
                }

                type Derived extending Base {
                    # also make the link 'required'
                    overloaded required link bar -> Child;
                }
            """,
            user_input=[
                '.bar',
            ],
        )

        await self.assert_query_result(
            r"""
                SELECT Derived {
                    bar,
                };
            """,
            [{
                'bar': {'id': data[0].bar.id},
            }],
        )

    async def test_edgeql_migration_eq_18a(self):
        await self.migrate(r"""
            type Base {
                property name := 'computable'
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.migrate(r"""
            type Base {
                # change a property from a computable to regular with a default
                property name -> str {
                    default := 'something'
                }
            }
        """)

        # Insert a new object, this one should have a new default name.
        await self.con.execute(r"""
            INSERT Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    name,
                } ORDER BY .name EMPTY LAST;
            """,
            [{
                'name': 'something',
            }, {
                'name': 'something',
            }],
        )

    async def test_edgeql_migration_eq_18b(self):
        await self.migrate(r"""
            type Base {
                property name := 'computable'
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.migrate(r"""
            type Base {
                # change a property from a computable to regular with a default
                property name -> str {
                    default := <str>count(Object)
                }
            }
        """)

        # Insert a new object, this one should have a new default name.
        await self.con.execute(r"""
            INSERT Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    name,
                } ORDER BY .name EMPTY LAST;
            """,
            [{
                'name': str,
            }, {
                'name': str,
            }],
        )

    async def test_edgeql_migration_eq_18c(self):
        await self.migrate(r"""
            type Base {
                property name := 'computable'
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.migrate(r"""
            type Base {
                # change a property from a computable to regular with a default
                required property name -> str {
                    default := <str>count(Object)
                }
            }
        """)

        # Insert a new object, this one should have a new default name.
        await self.con.execute(r"""
            INSERT Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    name,
                } ORDER BY .name EMPTY LAST;
            """,
            [{
                'name': str,
            }, {
                'name': str,
            }],
        )

    async def test_edgeql_migration_eq_19(self):
        await self.migrate(r"""
            type Base {
                property name -> str
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                name := 'base_19'
            };
        """)

        await self.migrate(r"""
            type Base {
                # change a regular property to a computable
                property name := 'computable'
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    name,
                };
            """,
            [{
                'name': 'computable',
            }],
        )

    async def test_edgeql_migration_eq_21(self):
        await self.migrate(r"""
            type Base {
                property foo -> str;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := 'base_21'
            };
        """)

        await self.migrate(r"""
            type Base {
                property foo -> str;
                # add a property
                property bar -> int64;
            }
        """)

        await self.con.execute(r"""
            UPDATE Base
            SET {
                bar := 21
            };
        """)
        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                    bar
                };
            """,
            [{
                'foo': 'base_21',
                'bar': 21,
            }],
        )

        await self.migrate(r"""
            type Base {
                # make the old property into a computable
                property foo := <str>__source__.bar;
                property bar -> int64;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                    bar
                };
            """,
            [{
                'foo': '21',
                'bar': 21,
            }],
        )

    async def test_edgeql_migration_eq_22(self):
        await self.migrate(r"""
            type Base {
                property foo -> str;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := 'base_22'
            };
        """)

        await self.migrate(r"""
            # rename the type, although this test doesn't ensure that
            # renaming actually took place
            type NewBase {
                property foo -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT NewBase {
                    foo,
                };
            """,
            [{
                'foo': 'base_22',
            }],
        )

        await self.migrate(r"""
            type NewBase {
                property foo -> str;
                # add a property
                property bar -> int64;
            }
        """)

        await self.con.execute(r"""
            UPDATE NewBase
            SET {
                bar := 22
            };
        """)
        await self.assert_query_result(
            r"""
                SELECT NewBase {
                    foo,
                    bar
                };
            """,
            [{
                'foo': 'base_22',
                'bar': 22,
            }],
        )

        await self.migrate(r"""
            type NewBase {
                # drop 'foo'
                property bar -> int64;
            }

            # add a alias to emulate the original
            alias Base := (
                SELECT NewBase {
                    foo := <str>.bar
                }
            );
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                };
            """,
            [{
                'foo': '22',
            }],
        )

    @test.xerror('''
        edgedb.errors.SchemaError: ObjectType 'test::Base' is already
        present in the schema <Schema gen:3757 at 0x7fc3319fa820>

        Exception: Error while processing
        'CREATE ALIAS test::Base := (
            SELECT
                test::Child {
                    bar := test::Child
                }
        );'
    ''')
    async def test_edgeql_migration_eq_23(self):
        await self.migrate(r"""
            type Child {
                property foo -> str;
            }

            type Base {
                link bar -> Child;
            }

            alias Alias01 := (
                SELECT Base {
                    child_foo := .bar.foo
                }
            );
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                bar := (
                    INSERT Child {
                        foo := 'child_23'
                    }
                )
            };
        """)

        await self.migrate(r"""
            type Child {
                property foo -> str;
            }

            # exchange a type for a alias
            alias Base := (
                SELECT Child {
                    # bar is the same as the root object
                    bar := Child
                }
            );

            alias Alias01 := (
                # now this alias refers to another alias
                SELECT Base {
                    child_foo := .bar.foo
                }
            );
        """)

        await self.assert_query_result(
            r"""
                SELECT Alias01 {
                    child_foo,
                };
            """,
            [{
                'child_foo': 'child_23',
            }],
        )

    async def test_edgeql_migration_eq_24(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link bar -> Child;
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)
        data = await self.con.query(r"""
            SELECT (
                INSERT Base {
                    bar := (INSERT Child)
                }
            ) {
                bar: {id}
            }
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                # increase link cardinality
                multi link bar -> Child;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    bar: {id},
                };
            """,
            [{
                'bar': [{'id': data[0].bar.id}],
            }],
        )

    async def test_edgeql_migration_eq_25(self):
        await self.migrate(r"""
            type Child;

            type Base {
                multi link bar -> Child;
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)
        data = await self.con.query(r"""
            SELECT (
                INSERT Base {
                    bar := (INSERT Child)
                }
            ) {
                bar: {id}
            }
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                # reduce link cardinality
                link bar -> Child;
            }
        """, user_input=[
            '(SELECT .bar LIMIT 1)'
        ])

        await self.assert_query_result(
            r"""
                SELECT Base {
                    bar: {id},
                };
            """,
            [{
                'bar': {'id': data[0].bar[0].id},
            }],
        )

        await self.migrate(r"""
            type Child;

            type Base {
                link bar -> Child {
                    # further restrict the link
                    constraint exclusive
                }
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    bar: {id},
                };
            """,
            [{
                'bar': {'id': data[0].bar[0].id},
            }],
        )

    async def test_edgeql_migration_eq_26(self):
        await self.migrate(r"""
            type Child;

            type Parent {
                link bar -> Child;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Parent {
                bar := (INSERT Child)
            };
        """)

        await self.migrate(r"""
            type Child;

            type Parent {
                link bar -> Child;
            }

            # derive a type
            type DerivedParent extending Parent;
        """)

        await self.assert_query_result(
            r"""
                SELECT Parent {
                    type := .__type__.name,
                    bar_type := .bar.__type__.name
                };
            """,
            [{
                'type': 'test::Parent',
                'bar_type': 'test::Child',
            }],
        )

        await self.migrate(
            r"""
                type Child;

                type DerivedChild extending Child;

                type Parent {
                    link bar -> Child;
                }

                type DerivedParent extending Parent;
            """,
        )

        await self.migrate(
            r"""
                type Child;

                type DerivedChild extending Child;

                type Parent {
                    link bar -> Child;
                }

                # derive a type with a more restrictive link
                type DerivedParent extending Parent {
                    overloaded link bar -> DerivedChild;
                }
            """,
            user_input=[".bar[IS DerivedChild]"],
        )

        await self.con.execute(r"""
            INSERT DerivedParent {
                bar := (INSERT DerivedChild)
            }
        """)
        await self.assert_query_result(
            r"""
                SELECT Parent {
                    type := .__type__.name,
                    bar_type := .bar.__type__.name
                } ORDER BY .bar_type;
            """,
            [{
                'type': 'test::Parent',
                'bar_type': 'test::Child',
            }, {
                'type': 'test::DerivedParent',
                'bar_type': 'test::DerivedChild',
            }],
        )

    async def test_edgeql_migration_eq_27(self):
        await self.migrate(r"""
            abstract type Named {
                property name -> str;
            }

            type Foo extending Named;
            type Bar extending Named;
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Foo {
                name := 'foo_27',
            };
            INSERT Bar {
                name := 'bar_27',
            };
        """)

        await self.migrate(r"""
            abstract type Named {
                property name -> str;
            }

            # the types stop extending named, but retain the property
            # 'name'
            type Foo {
                property name -> str;
            };

            type Bar {
                property name -> str;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo.name;
            """,
            [
                'foo_27',
            ],
        )
        await self.assert_query_result(
            r"""
                SELECT Bar.name;
            """,
            [
                'bar_27',
            ],
        )

        await self.migrate(r"""
            abstract type Named {
                property name -> str;
            }

            type Foo {
                property name -> str;
            };

            type Bar {
                # rename 'name' to 'title'
                property title -> str;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo.name;
            """,
            [
                'foo_27',
            ],
        )
        await self.assert_query_result(
            r"""
                SELECT Bar.title;
            """,
            [
                'bar_27',
            ],
        )

    async def test_edgeql_migration_eq_29(self):
        await self.migrate(r"""
            type Child {
                property foo -> str;
            }

            alias Base := (
                SELECT Child {
                    bar := .foo
                }
            );
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Child {
                foo := 'child_29',
            };
        """)

        await self.migrate(r"""
            # drop everything
        """)

    async def test_edgeql_migration_eq_30(self):
        await self.migrate(r"""
            type Foo {
                property name -> str;
            };

            type Bar {
                property title -> str;
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Foo {
                name := 'foo_30',
            };
            INSERT Bar {
                title := 'bar_30',
            };
        """)

        await self.migrate(r"""
            type Foo {
                property name -> str;
            };

            type Bar {
                # rename 'title' to 'name'
                property name -> str;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo.name;
            """,
            [
                'foo_30',
            ],
        )
        await self.assert_query_result(
            r"""
                SELECT Bar.name;
            """,
            [
                'bar_30',
            ],
        )

        await self.migrate(r"""
            # both types have a name, so the name prop is factored out
            # into a more basic type.
            abstract type Named {
                property name -> str;
            }

            type Foo extending Named;
            type Bar extending Named;
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo.name;
            """,
            [
                'foo_30',
            ],
        )
        await self.assert_query_result(
            r"""
                SELECT Bar.name;
            """,
            [
                'bar_30',
            ],
        )

    async def test_edgeql_migration_eq_31(self):
        # Issue 727.
        #
        # Starting with the sample schema (from frontpage) migrate to
        # a schema with only type User.
        await self.migrate(r"""
            # This is an abstract object containing
            # text.
            abstract type Text {
              required property body -> str {
                # Maximum length of text is 10000
                # characters.
                constraint max_len_value(10000);
              }
            }

            type User {
              required property name -> str;
            }

            abstract type Owned {
              # By default links are optional.
              required link owner -> User;
            }

            # UniquelyNamed is a an abstract type that
            # enforces name uniqueness across all
            # instances of its subtype.
            abstract type UniquelyNamed {
              required property name -> str {
                delegated constraint exclusive;
              }
            }

            type Status extending UniquelyNamed;

            type Priority extending UniquelyNamed;

            # LogEntry is an Owned and a Text,
            # so it will have all of their links
            # and properties, in particular, the
            # "owner" link and the "body" property.
            type LogEntry extending Owned, Text {
              required property spent_time -> int64;
            }

            type Comment extending Text, Owned {
              required link issue -> Issue;
              link parent -> Comment;
            }
            # issue_num_t is defined as a concrete
            # sequence type, used to generate
            # sequential issue numbers.
            scalar type issue_num_t extending sequence;

            type Issue extending Owned, Text {
              required property title -> str;

              required property number -> issue_num_t {
                # The number values are automatically
                # generated, and are not supposed to be
                # directly writable.
                readonly := true;
              }

              property time_estimate -> int64;

              property start_date -> datetime {
                # The default value of start_date will be a
                # result of the EdgeQL expression above.
                default := (SELECT datetime_current());
              }

              property due_date -> datetime;

              required link status -> Status;

              link priority -> Priority;

              # The watchers link is mapped to User
              # type in many-to-many relation.
              multi link watchers -> User;

              multi link time_spent_log -> LogEntry {
                # Exclusive multi-link represents
                # a one-to-many relation.
                constraint exclusive;
              }

              multi link related_to -> Issue;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Status {
                name := 'Open'
            };
            INSERT Status {
                name := 'Closed'
            };

            INSERT User {
                name := 'cosmophile'
            };
        """)

        await self.migrate(r"""
            type User {
              required property name -> str;
            }
        """)

        # there's only the User left
        await self.assert_query_result(
            r"""
                SELECT User.name;
            """,
            [
                'cosmophile',
            ],
        )

    async def test_edgeql_migration_eq_32(self):
        # Issue 727.
        #
        # Starting with a small schema migrate to remove its elements.
        # There are non-zero default Objects existing in a fresh blank
        # database because of placeholder objects used for GraphQL.
        start_objects = await self.con.query_single(r"""
            SELECT count(Object);
        """)

        await self.migrate(r"""
            type LogEntry {
              required property spent_time -> int64;
            }
            type Issue {
              multi link time_spent_log -> LogEntry {
                constraint exclusive;
              }
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT LogEntry {
                spent_time := 100
            };

            INSERT Issue {
                time_spent_log := LogEntry
            };
        """)

        await self.migrate(r"""
            type LogEntry {
              required property spent_time -> int64;
            }
        """)

        # there's only the LogEntry left
        await self.assert_query_result(
            r"""
                SELECT LogEntry.spent_time;
            """,
            [
                100,
            ],
        )
        await self.assert_query_result(
            r"""
                SELECT count(Object);
            """,
            [
                start_objects + 1,
            ],
        )

        await self.migrate(r"""
            # empty schema
        """)

        # no more additional objects
        await self.assert_query_result(
            r"""
                SELECT count(Object);
            """,
            [
                start_objects,
            ],
        )

    async def test_edgeql_migration_eq_33(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Child;
            INSERT Base {
                foo := (SELECT Child LIMIT 1)
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        __type__: {name},
                    }
                };
            """,
            [{
                'foo': {
                    '__type__': {'name': 'test::Child'},
                }
            }],
        )

        await self.migrate(
            r"""
                type Child;
                type Child2;

                type Base {
                    link foo -> Child;
                }
            """,
        )

        await self.migrate(
            r"""
                type Child;
                type Child2;

                type Base {
                    # change link type
                    link foo -> Child2;
                }
            """,
            user_input=[
                '.foo[IS Child2]'
            ],
        )

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        __type__: {name},
                    }
                };
            """,
            [{
                # the link is empty because the target was changed
                'foo': None
            }],
        )

        await self.con.execute(r"""
            INSERT Child2;

            UPDATE Base
            SET {
                foo := (SELECT Child2 LIMIT 1)
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        __type__: {name},
                    }
                };
            """,
            [{
                'foo': {
                    '__type__': {'name': 'test::Child2'},
                }
            }],
        )

    async def test_edgeql_migration_eq_34(self):
        # this is the reverse of test_edgeql_migration_eq_11
        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    constraint exclusive;
                }
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Child;
            INSERT Base {
                foo := (SELECT Child LIMIT 1)
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        __type__: {name},
                    }
                };
            """,
            [{
                'foo': {
                    '__type__': {'name': 'test::Child'},
                }
            }],
        )

        await self.migrate(r"""
            type Base {
                # change link to property with same name
                property foo -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo
                };
            """,
            [{
                # the property is empty now
                'foo': None
            }],
        )

        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := 'base_foo_34'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo
                };
            """,
            [{
                'foo': 'base_foo_34'
            }],
        )

    async def test_edgeql_migration_eq_35(self):
        await self.migrate(r"""
            type Child {
                required property name -> str;
            }

            type Base {
                multi link foo := (
                    SELECT Child FILTER .name = 'computable_35'
                )
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Child {
                name := 'computable_35'
            };
            INSERT Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        name
                    },
                };
            """,
            [{
                'foo': [{
                    'name': 'computable_35',
                }]
            }]
        )

        await self.migrate(r"""
            type Child {
                required property name -> str;
            }

            type Base {
                # change a link from a computable to regular
                multi link foo -> Child;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        name
                    },
                };
            """,
            [{
                'foo': []
            }]
        )

        # Make sure that the new 'foo' can be updated.
        await self.con.execute(r"""
            INSERT Child {
                name := 'child_35'
            };
            UPDATE Base
            SET {
                foo := (
                    SELECT Child FILTER .name = 'child_35'
                )
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        name
                    },
                };
            """,
            [{
                'foo': [{
                    'name': 'child_35'
                }]
            }]
        )

    async def test_edgeql_migration_eq_36(self):
        await self.migrate(r"""
            type Child {
                required property name -> str;
            }

            type Base {
                multi link foo -> Child;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Child {
                name := 'computable_36'
            };
            INSERT Child {
                name := 'child_36'
            };
            INSERT Base {
                foo := (
                    SELECT Child FILTER .name = 'child_36'
                )
            };
        """)

        await self.migrate(r"""
            type Child {
                required property name -> str;
            }

            type Base {
                # change a regular link to a computable
                link foo := (
                    SELECT Child FILTER .name = 'computable_36'
                    LIMIT 1
                )
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        name
                    },
                };
            """,
            [{
                'foo': {
                    'name': 'computable_36'
                }
            }]
        )

    async def test_edgeql_migration_eq_37(self):
        # testing schema alias
        await self.migrate(r"""
            type Base;

            alias BaseAlias := (
                SELECT Base {
                    foo := 'base_alias_37'
                }
            )
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo
                };
            """,
            [{
                'foo': 'base_alias_37'
            }]
        )

        await self.migrate(r"""
            type Base;

            alias BaseAlias := (
                SELECT Base {
                    # "rename" a computable, since the value is given and
                    # not stored, this is no different from dropping
                    # original and creating a new property
                    foo2 := 'base_alias_37'
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo2
                };
            """,
            [{
                'foo2': 'base_alias_37'
            }]
        )

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"object type 'test::Base' has no link or property 'foo'"):
            await self.con.execute(r"""
                SELECT BaseAlias {
                    foo
                };
            """)

    async def test_edgeql_migration_eq_38(self):
        # testing schema alias
        await self.migrate(r"""
            type Base;

            alias BaseAlias := (
                SELECT Base {
                    foo := 'base_alias_38'
                }
            )
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo
                };
            """,
            [{
                'foo': 'base_alias_38'
            }]
        )

        await self.migrate(r"""
            type Base;

            alias BaseAlias := (
                SELECT Base {
                    # keep the name, but change the type
                    foo := 38
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo
                };
            """,
            [{
                'foo': 38
            }]
        )

    async def test_edgeql_migration_eq_39(self):
        # testing schema alias
        await self.migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (SELECT Foo FILTER .name = 'base_alias_39')
                }
            )
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
            INSERT Foo {name := 'base_alias_39'};
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo: {
                        name
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'base_alias_39'
                }]
            }]
        )

        await self.migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    # "rename" a computable, since the value is given and
                    # not stored, this is no different from dropping
                    # original and creating a new multi-link
                    foo2 := (SELECT Foo FILTER .name = 'base_alias_39')
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo2: {
                        name
                    }
                };
            """,
            [{
                'foo2': [{
                    'name': 'base_alias_39'
                }]
            }]
        )

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"object type 'test::Base' has no link or property 'foo'"):
            await self.con.execute(r"""
                SELECT BaseAlias {
                    foo: {
                        name
                    }
                };
            """)

    async def test_edgeql_migration_eq_40(self):
        # testing schema alias
        await self.migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            type Bar {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (SELECT Foo FILTER .name = 'foo_40')
                }
            )
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
            INSERT Foo {name := 'foo_40'};
            INSERT Bar {name := 'bar_40'};
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo: {
                        name
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'foo_40'
                }]
            }]
        )

        await self.migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            type Bar {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    # keep the name, but change the type
                    foo := (SELECT Bar FILTER .name = 'bar_40')
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo: {
                        name
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'bar_40'
                }]
            }]
        )

    async def test_edgeql_migration_eq_41(self):
        # testing schema alias
        await self.migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (
                        SELECT Foo {
                            @bar := 'foo_bar_alias_41'
                        }
                        FILTER .name = 'base_alias_41'
                    )
                }
            )
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
            INSERT Foo {name := 'base_alias_41'};
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo: {
                        name,
                        @bar
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'base_alias_41',
                    '@bar': 'foo_bar_alias_41',
                }]
            }]
        )

        await self.migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (
                        SELECT Foo {
                            # "rename" a computable link property, since
                            # the value is given and not stored, this is
                            # no different from dropping original and
                            # creating a new multi-link
                            @baz := 'foo_bar_alias_41'
                        }
                        FILTER .name = 'base_alias_41'
                    )
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo: {
                        name,
                        @baz
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'base_alias_41',
                    '@baz': 'foo_bar_alias_41'
                }]
            }]
        )

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"link 'foo' .* has no property 'bar'"):
            await self.con.execute(r"""
                SELECT BaseAlias {
                    foo: {
                        name,
                        @bar
                    }
                };
            """)

    async def test_edgeql_migration_eq_42(self):
        # testing schema alias
        await self.migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (
                        SELECT Foo {
                            @bar := 'foo_bar_alias_42'
                        }
                        FILTER .name = 'base_alias_42'
                    )
                }
            )
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
            INSERT Foo {name := 'base_alias_42'};
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo: {
                        name,
                        @bar
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'base_alias_42',
                    '@bar': 'foo_bar_alias_42',
                }]
            }]
        )

        await self.migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (
                        SELECT Foo {
                            # keep the name, but change the type
                            @bar := 42
                        }
                        FILTER .name = 'base_alias_42'
                    )
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo: {
                        name,
                        @bar
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'base_alias_42',
                    '@bar': 42,
                }]
            }]
        )

    async def test_edgeql_migration_eq_43(self):
        await self.migrate(r"""
            abstract link Ordered {
                property index -> int32;
            }
            type User;
            abstract type Permissions {
                multi link owners extending Ordered -> User;
            };
        """)
        await self.migrate(r"")

    async def test_edgeql_migration_index_01(self):
        await self.migrate('''
            type Message {
                required property text -> str;
                index on (.text);
            };
        ''')

        await self.migrate('''
            type Message {
                required property text -> str;
                property ts -> datetime;
                index on (.text);
                index on (.ts);
            };
        ''')

        await self.assert_query_result(
            r"""
                SELECT count((SELECT schema::ObjectType
                              FILTER .name = 'test::Message').indexes)
            """,
            [2],
        )

    async def test_edgeql_migration_rebase_01(self):
        await self.migrate(r"""
            abstract type C;
            abstract type P {
                property p -> str;
                property p2 -> str;
                index on (.p);
            };
            type Foo extending C {
                property foo -> str;
            }
        """)

        await self.migrate(r"""
            abstract type C;
            abstract type P {
                property p -> str;
                property p2 -> str;
                index on (.p);
            };
            type Foo extending C, P {
                property foo -> str;
            }
        """)

    async def test_edgeql_migration_rebase_02(self):
        await self.migrate('''
            type User;
            abstract type Event {
              required property createdAt -> datetime {
                default := datetime_current();
              }

              required link user -> User;
            }

            type Post extending Event {
              required property content -> str {
                constraint min_len_value(1);
                constraint max_len_value(280);
              }
            }
        ''')

        await self.start_migration('''
            type User;
            abstract type Event {
              required property createdAt -> datetime {
                default := datetime_current();
              }

              required link user -> User;
            }

            abstract type HasContent {
              required property content -> str {
                constraint min_len_value(1);
                constraint max_len_value(280);
              }
            }

            type Post extending Event, HasContent {
            }

            type Reply extending Event, HasContent {
              required link post -> Post;
            }
        ''')

        # N.B.: these prompts are OK but not canonical; if they are
        # broken in favor of something better, just fix them.
        await self.interact([
            "did you create object type 'test::HasContent'?",
            "did you alter object type 'test::Post'?",
            "did you alter constraint 'std::max_len_value' of property "
            "'content'?",
            "did you create object type 'test::Reply'?",
            "did you alter property 'content' of object type 'test::Post'?",
        ])

    async def test_edgeql_migration_rebase_03(self):
        await self.migrate('''
            abstract type Named {
                required property name -> str;
            };

            type Org;

            abstract type OrgBound {
                required link org -> Org;
            };

            abstract type OrgUniquelyNamed
                extending Named, OrgBound
            {
                constraint exclusive on ((.name, .org))
            }
        ''')

        await self.start_migration('''
            abstract type Named {
                required property name -> str;
            };

            type Org;
            abstract type Resource;

            abstract type OrgBound {
                required link org -> Org;
            };

            abstract type OrgUniquelyNamedResource
                extending Named, Resource, OrgBound
            {
                delegated constraint exclusive on ((.name, .org))
            }
        ''')

        # N.B.: these prompts are OK but not canonical; if they are
        # broken in favor of something better, just fix them.
        await self.interact([
            ("did you drop object type 'test::OrgUniquelyNamed'?", "n"),
            "did you create object type 'test::Resource'?",
            "did you rename object type 'test::OrgUniquelyNamed' to "
            "'test::OrgUniquelyNamedResource'?",

            "did you alter object type 'test::OrgUniquelyNamedResource'?",
        ])

    async def test_edgeql_migration_rename_01(self):
        await self.migrate('''
            type Foo;
        ''')

        await self.start_migration('''
            type Bar {
                property asdf -> str;
            };
        ''')

        await self.interact([
            "did you rename object type 'test::Foo' to 'test::Bar'?",
            "did you create property 'asdf' of object type 'test::Bar'?",
        ])

    async def test_edgeql_migration_rename_02(self):
        await self.migrate('''
            type Foo {
                property asdf -> str;
            };
            type Bar extending Foo {
                overloaded property asdf -> str;
            };
        ''')

        await self.start_migration('''
            type Foo {
                property womp -> str;
            };
            type Bar extending Foo {
                overloaded property womp -> str {
                    annotation title := "foo";
                };
            };
        ''')

        await self.interact([
            "did you rename property 'asdf' of object type 'test::Foo' to "
            "'womp'?" ,

            "did you create annotation 'std::title' of property 'womp'?",
        ])

    async def test_edgeql_migration_rename_03(self):
        await self.migrate('''
            abstract constraint Asdf { using (__subject__ < 10) };
            type Foo {
                property x -> int64 {
                    constraint Asdf;
                }
            }
            type Bar extending Foo;
        ''')

        await self.start_migration('''
            abstract constraint Womp { using (__subject__ < 10) };
            type Foo {
                property x -> int64 {
                    constraint Womp;
                }
            }
            type Bar extending Foo;
        ''')

        await self.interact([
            "did you rename abstract constraint 'test::Asdf' to "
            "'test::Womp'?",
        ])

    async def test_edgeql_migration_eq_function_01(self):
        await self.migrate(r"""
            function hello01(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello01(1);""",
            ['hello1'],
        )

        # add an extra parameter with a default (so it can be omitted
        # in principle)
        await self.migrate(r"""
            function hello01(a: int64, b: int64=42) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>(a + b)
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello01(1);""",
            ['hello43'],
        )

        await self.assert_query_result(
            r"""SELECT hello01(1, 2);""",
            ['hello3'],
        )

    async def test_edgeql_migration_eq_function_02(self):
        await self.migrate(r"""
            function hello02(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello02(1);""",
            ['hello1'],
        )

        # add an extra parameter with a default (so it can be omitted
        # in principle)
        await self.migrate(r"""
            function hello02(a: int64, b: OPTIONAL int64=42) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>(a + (b ?? -1))
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello02(1);""",
            ['hello43'],
        )

        await self.assert_query_result(
            r"""SELECT hello02(1, 2);""",
            ['hello3'],
        )

        await self.assert_query_result(
            r"""SELECT hello02(1, <int64>{});""",
            ['hello0'],
        )

    async def test_edgeql_migration_eq_function_03(self):
        await self.migrate(r"""
            function hello03(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello03(1);""",
            ['hello1'],
        )

        # add an extra parameter with a default (so it can be omitted
        # in principle)
        await self.migrate(r"""
            function hello03(a: int64, NAMED ONLY b: int64=42) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>(a + b)
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello03(1);""",
            ['hello43'],
        )

        await self.assert_query_result(
            r"""SELECT hello03(1, b := 2);""",
            ['hello3'],
        )

    async def test_edgeql_migration_eq_function_04(self):
        await self.migrate(r"""
            function hello04(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello04(1);""",
            ['hello1'],
        )

        # same parameters, different return type
        await self.migrate(r"""
            function hello04(a: int64) -> int64
                using edgeql $$
                    SELECT -a
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello04(1);""",
            [-1],
        )

    async def test_edgeql_migration_eq_function_05(self):
        await self.migrate(r"""
            function hello05(a: int64) -> str
                using edgeql $$
                    SELECT <str>a
                $$
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello05(1);""",
            ['1'],
        )

        # same parameters, different return type (array)
        await self.migrate(r"""
            function hello05(a: int64) -> array<int64>
                using edgeql $$
                    SELECT [a]
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello05(1);""",
            [[1]],
        )

    @test.xfail('''
        It should be possible to change the underlying function (to a
        compatible one) of a default value without explicitly dropping
        the default first.

        Currently this kind of works... by proposing we delete the property
        and recreate it.
    ''')
    async def test_edgeql_migration_eq_function_06(self):
        await self.migrate(r"""
            function hello06(a: int64) -> str
                using edgeql $$
                    SELECT <str>a
                $$;

            type Base {
                property foo -> int64 {
                    # use the function in default value computation
                    default := len(hello06(2) ++ hello06(123))
                }
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            {4},
        )

        # same parameters, different return type (array)
        await self.migrate(r"""
            function hello06(a: int64) -> array<int64>
                using edgeql $$
                    SELECT [a]
                $$;

            type Base {
                property foo -> int64 {
                    # use the function in default value computation
                    default := len(hello06(2) ++ hello06(123))
                }
            }
        """)
        await self.con.execute(r"""
            INSERT Base;
        """)

        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            {4, 2},
        )

    async def test_edgeql_migration_eq_function_07(self):
        await self.migrate(r"""
            function hello07(a: int64) -> str
                using edgeql $$
                    SELECT <str>a
                $$;

            type Base {
                # use the function in computable value
                property foo := len(hello07(2) ++ hello07(123))
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            {4},
        )

        # same parameters, different return type (array)
        await self.migrate(r"""
            function hello07(a: int64) -> array<int64>
                using edgeql $$
                    SELECT [a]
                $$;

            type Base {
                # use the function in computable value
                property foo := len(hello07(2) ++ hello07(123))
            }
        """)

        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            {2},
        )

    async def test_edgeql_migration_eq_function_08(self):
        await self.migrate(r"""
            function hello08(a: int64) -> str
                using edgeql $$
                    SELECT <str>a
                $$;

            # use the function in a alias directly
            alias foo := len(hello08(2) ++ hello08(123));
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT foo;""",
            {4},
        )

        # same parameters, different return type (array)
        await self.migrate(r"""
            function hello08(a: int64) -> array<int64>
                using edgeql $$
                    SELECT [a]
                $$;

            # use the function in a alias directly
            alias foo := len(hello08(2) ++ hello08(123));
        """)

        await self.assert_query_result(
            r"""SELECT foo;""",
            {2},
        )

    async def test_edgeql_migration_eq_function_09(self):
        await self.migrate(r"""
            function hello09(a: int64) -> str
                using edgeql $$
                    SELECT <str>a
                $$;

            type Base;

            # use the function in a alias directly
            alias BaseAlias := (
                SELECT Base {
                    foo := len(hello09(2) ++ hello09(123))
                }
            );
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias.foo;""",
            {4},
        )

        # same parameters, different return type (array)
        await self.migrate(r"""
            function hello09(a: int64) -> array<int64>
                using edgeql $$
                    SELECT [a]
                $$;

            type Base;

            # use the function in a alias directly
            alias BaseAlias := (
                SELECT Base {
                    foo := len(hello09(2) ++ hello09(123))
                }
            );
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias.foo;""",
            {2},
        )

    async def test_edgeql_migration_eq_function_10(self):
        await self.migrate(r"""
            function hello10(a: int64) -> str
                using edgeql $$
                    SELECT <str>a
                $$;

            type Base {
                required property foo -> int64 {
                    # use the function in a constraint expression
                    constraint expression on (len(hello10(__subject__)) < 2)
                }
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;
        """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'invalid foo'):
            async with self.con.transaction():
                await self.con.execute(r"""
                    INSERT Base {foo := 42};
                """)

        # same parameters, different return type (array)
        await self.migrate(r"""
            function hello10(a: int64) -> array<int64>
                using edgeql $$
                    SELECT [a]
                $$;

            type Base {
                required property foo -> int64 {
                    # use the function in a constraint expression
                    constraint expression on (len(hello10(__subject__)) < 2)
                }
            }
        """)

        # no problem with the constraint now
        await self.con.execute(r"""
            INSERT Base {foo := 42};
        """)

    async def test_edgeql_migration_eq_function_11(self):
        await self.migrate(r"""
            function hello11(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello11(1);""",
            ['hello1'],
        )

        await self.migrate(r"""
            # replace the function with a new one by the same name
            function hello11(a: str) -> str
                using edgeql $$
                    SELECT 'hello' ++ a
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello11(' world');""",
            ['hello world'],
        )

        # make sure that the old one is gone
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'function "hello11\(arg0: std::int64\)" does not exist'):
            await self.con.execute(
                r"""SELECT hello11(1);"""
            )

    async def test_edgeql_migration_eq_function_12(self):
        await self.migrate(r"""
            function hello12(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$;
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello12(1);""",
            ['hello1'],
        )

        await self.migrate(r"""
            function hello12(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$;

            # make the function polymorphic
            function hello12(a: str) -> str
                using edgeql $$
                    SELECT 'hello' ++ a
                $$;
        """)

        await self.assert_query_result(
            r"""SELECT hello12(' world');""",
            ['hello world'],
        )

        # make sure that the old one still works
        await self.assert_query_result(
            r"""SELECT hello12(1);""",
            ['hello1'],
        )

    async def test_edgeql_migration_eq_function_13(self):
        # this is the inverse of test_edgeql_migration_eq_function_12
        await self.migrate(r"""
            # start with a polymorphic function
            function hello13(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$;

            function hello13(a: str) -> str
                using edgeql $$
                    SELECT 'hello' ++ a
                $$;
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello13(' world');""",
            ['hello world'],
        )
        await self.assert_query_result(
            r"""SELECT hello13(1);""",
            ['hello1'],
        )

        await self.migrate(r"""
            # remove one of the 2 versions
            function hello13(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$;
        """)

        await self.assert_query_result(
            r"""SELECT hello13(1);""",
            ['hello1'],
        )

        # make sure that the other one is gone
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'function "hello13\(arg0: std::str\)" does not exist'):
            await self.con.execute(
                r"""SELECT hello13(' world');"""
            )

    async def test_edgeql_migration_eq_function_14(self):
        await self.migrate(r"""
            function hello14(a: str, b: str) -> str
                using edgeql $$
                    SELECT a ++ b
                $$
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello14('hello', '14');""",
            ['hello14'],
        )

        await self.migrate(r"""
            # Replace the function with a new one by the same name,
            # but working with arrays.
            function hello14(a: array<str>, b: array<str>) -> array<str>
                using edgeql $$
                    SELECT a ++ b
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello14(['hello'], ['14']);""",
            [['hello', '14']],
        )

        # make sure that the old one is gone
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'function "hello14\(arg0: std::str, arg1: std::str\)" '
                r'does not exist'):
            await self.assert_query_result(
                r"""SELECT hello14('hello', '14');""",
                ['hello14'],
            )

    async def test_edgeql_migration_eq_function_15(self):
        await self.migrate(r"""
            function hello15(a: str, b: str) -> str
                using edgeql $$
                    SELECT a ++ b
                $$
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello15('hello', '15');""",
            ['hello15'],
        )

        await self.migrate(r"""
            # Replace the function with a new one by the same name,
            # but working with arrays.
            function hello15(a: tuple<str, str>) -> str
                using edgeql $$
                    SELECT a.0 ++ a.1
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello15(('hello', '15'));""",
            ['hello15'],
        )

        # make sure that the old one is gone
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'function "hello15\(arg0: std::str, arg1: std::str\)" '
                r'does not exist'):
            await self.assert_query_result(
                r"""SELECT hello15('hello', '15');""",
                ['hello15'],
            )

    async def test_edgeql_migration_eq_function_16(self):
        # Test prop default and function order of definition. The
        # function happens to be shadowing a "std" function. We expect
        # that the function `test::to_upper` will actually be used.
        #
        # See also `test_schema_get_migration_21`
        await self.migrate(r"""
            type Foo16 {
                property name -> str {
                    default := str_upper('some_name');
                };
            }

            function str_upper(val: str) -> str {
                using (SELECT '^^' ++ std::str_upper(val) ++ '^^');
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT str_upper('hello');""",
            ['^^HELLO^^'],
        )

        await self.con.execute("""
            INSERT Foo16;
        """)
        await self.assert_query_result(
            r"""SELECT Foo16.name;""",
            ['^^SOME_NAME^^'],
        )

    async def test_edgeql_migration_enum_and_var_function_01(self):
        # Create an enum and a variadic function that references it
        # in the same migration. Issue #4213.
        await self.migrate(r"""
            scalar type E extending enum<a, b, c>;
            function f(variadic e: E) -> bool using (true);
        """)

    async def test_edgeql_migration_eq_linkprops_01(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child;
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := (INSERT Child)
            };
        """)

        # Migration adding a link property.
        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    property bar -> str
                }
            };
        """)

        # actually record a link property
        await self.con.execute(r"""
            UPDATE
                Base
            SET {
                foo := .foo {
                    @bar := 'lp01'
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: { @bar }
                };
            """,
            [{'foo': {'@bar': 'lp01'}}],
        )

    async def test_edgeql_migration_eq_linkprops_02(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    property bar -> str
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {foo := (INSERT Child)};
            UPDATE Base
            SET {
                foo := .foo { @bar := 'lp02' },
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    # change the link property name
                    property bar2 -> str
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: { @bar2 }
                };
            """,
            [{'foo': {'@bar2': 'lp02'}}],
        )

    async def test_edgeql_migration_eq_linkprops_03(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    property bar -> int64
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {foo := (INSERT Child)};
            UPDATE Base
            SET {
                foo := .foo { @bar := 3 },
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    # change the link property type
                    property bar -> int32
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: { @bar }
                };
            """,
            [{'foo': {'@bar': 3}}],
        )

    async def test_edgeql_migration_eq_linkprops_04(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    property bar -> str
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {foo := (INSERT Child)};
            UPDATE Base
            SET {
                foo := .foo { @bar := 'lp04' },
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                # change the link cardinality
                multi link foo -> Child {
                    property bar -> str
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: { @bar }
                };
            """,
            [{'foo': [{'@bar': 'lp04'}]}],
        )

    async def test_edgeql_migration_eq_linkprops_05(self):
        await self.migrate(r"""
            type Child;

            type Base {
                multi link foo -> Child {
                    property bar -> str
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {foo := (INSERT Child)};
            UPDATE Base
            SET {
                foo := .foo { @bar := 'lp05' },
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                # change the link cardinality
                link foo -> Child {
                    property bar -> str
                }
            };
        """, user_input=[
            'SELECT .foo LIMIT 1'
        ])

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: { @bar }
                };
            """,
            [{'foo': {'@bar': 'lp05'}}],
        )

    async def test_edgeql_migration_eq_linkprops_06(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link child -> Child {
                    property foo -> str;
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {child := (INSERT Child)};
            UPDATE Base
            SET {
                child := .child {
                    @foo := 'lp06',
                },
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                link child -> Child {
                    property foo -> str;
                    # add another link prop
                    property bar -> int64;
                }
            };
        """)
        # update the existing data with a new link prop 'bar'
        await self.con.execute(r"""
            UPDATE Base
            SET {
                child := .child {
                    @bar := 111,
                    @foo := 'lp06',
                },
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    child: {
                        @foo,
                        @bar
                    }
                };
            """,
            [{
                'child': {
                    '@foo': 'lp06',
                    '@bar': 111
                }
            }],
        )

    async def test_edgeql_migration_eq_linkprops_07(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link child -> Child
            };

            type Derived extending Base {
                overloaded link child -> Child {
                    property foo -> str
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Derived {child := (INSERT Child)};
            UPDATE Derived
            SET {
                child := .child {
                    @foo := 'lp07',
                },
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                # move the link property earlier in the inheritance tree
                link child -> Child {
                    property foo -> str
                }
            };

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    child: {
                        @foo,
                    }
                };
            """,
            [{
                'child': {
                    '@foo': 'lp07',
                }
            }],
        )

    async def test_edgeql_migration_eq_linkprops_08(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link child -> Child {
                    property foo -> str
                }
            };

            type Derived extending Base;
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Derived {child := (INSERT Child)};
        """)
        await self.con.execute(r"""
            UPDATE Derived
            SET {
                child := .child {
                    @foo := 'lp08',
                },
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                link child -> Child
            };

            type Derived extending Base {
                overloaded link child -> Child {
                    # move the link property later in the inheritance tree
                    property foo -> str
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Derived {
                    children := count(.child)
                };
            """,
            [{
                'children': 1,
            }],
        )

        await self.assert_query_result(
            r"""
                SELECT Derived {
                    child: {
                        @foo,
                    }
                };
            """,
            [{
                'child': {
                    '@foo': 'lp08',
                }
            }],
        )

    async def test_edgeql_migration_eq_linkprops_09(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link child -> Child
            };

            type Derived extending Base {
                overloaded link child -> Child {
                    property foo -> str
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Derived {child := (INSERT Child)};
            UPDATE Derived
            SET {
                child := .child {
                    @foo := 'lp09',
                },
            };
        """)

        await self.migrate(r"""
            type Child;

            # factor out link property all the way to an abstract link
            abstract link base_child {
                property foo -> str;
            }

            type Base {
                link child extending base_child -> Child;
            };

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    child: {
                        @foo,
                    }
                };
            """,
            [{
                'child': {
                    '@foo': 'lp09',
                }
            }],
        )

    async def test_edgeql_migration_eq_linkprops_10(self):
        # reverse of the test_edgeql_migration_eq_linkprops_09 refactoring
        await self.migrate(r"""
            type Child;

            abstract link base_child {
                property foo -> str;
            }

            type Base {
                link child extending base_child -> Child;
            };

            type Derived extending Base;
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Derived {child := (INSERT Child)};
            UPDATE Derived
            SET {
                child := .child {
                    @foo := 'lp10',
                },
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                link child -> Child
            };

            type Derived extending Base {
                overloaded link child -> Child {
                    # move the link property later in the inheritance tree
                    property foo -> str
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Derived {
                    child: {
                        @foo,
                    }
                };
            """,
            [{
                'child': {
                    '@foo': 'lp10',
                }
            }],
        )

        await self.migrate("")

    async def test_edgeql_migration_eq_linkprops_11(self):
        # merging a link with the same properties
        await self.migrate(r"""
            type Thing;

            type Owner {
                link item -> Thing {
                    property foo -> str;
                }
            };

            type Renter {
                link item -> Thing {
                    property foo -> str;
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Owner {item := (INSERT Thing)};
            UPDATE Owner
            SET {
                item := .item {
                    @foo := 'owner_lp11',
                },
            };

            INSERT Renter {item := (INSERT Thing)};
            UPDATE Renter
            SET {
                item := .item {
                    @foo := 'renter_lp11',
                },
            };
        """)

        await self.migrate(r"""
            type Thing;

            type Base {
                link item -> Thing {
                    property foo -> str;
                }
            };

            type Owner extending Base;

            type Renter extending Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Owner {
                    item: {
                        @foo,
                    }
                };
            """,
            [{
                'item': {
                    '@foo': 'owner_lp11',
                }
            }],
        )

        await self.assert_query_result(
            r"""
                SELECT Renter {
                    item: {
                        @foo,
                    }
                };
            """,
            [{
                'item': {
                    '@foo': 'renter_lp11',
                }
            }],
        )

    async def test_edgeql_migration_eq_linkprops_12(self):
        # merging a link with different properties
        await self.migrate(r"""
            type Thing;

            type Owner {
                link item -> Thing {
                    property foo -> str;
                }
            };

            type Renter {
                link item -> Thing {
                    property bar -> str;
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Owner {item := (INSERT Thing)};
            UPDATE Owner
            SET {
                item := .item {
                    @foo := 'owner_lp11',
                },
            };

            INSERT Renter {item := (INSERT Thing)};
            UPDATE Renter
            SET {
                item := .item {
                    @bar := 'renter_lp11',
                },
            };
        """)

        await self.migrate(r"""
            type Thing;

            type Base {
                link item -> Thing {
                    property foo -> str;
                    property bar -> str;
                }
            };

            type Owner extending Base;

            type Renter extending Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Owner {
                    item: {
                        @foo,
                        @bar,
                    }
                };
            """,
            [{
                'item': {
                    '@foo': 'owner_lp11',
                    '@bar': None,
                }
            }],
        )

        await self.assert_query_result(
            r"""
                SELECT Renter {
                    item: {
                        @foo,
                        @bar,
                    }
                };
            """,
            [{
                'item': {
                    '@foo': None,
                    '@bar': 'renter_lp11',
                }
            }],
        )

    async def test_edgeql_migration_eq_annotation_01(self):
        await self.migrate(r"""
            type Base;
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [],
            }],
        )

        await self.migrate(r"""
            type Base {
                # add a title annotation
                annotation title := 'Base description 01'
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 01'
                }],
            }],
        )

        await self.migrate(r"""
            # add inheritable and non-inheritable annotations
            abstract annotation foo_anno;
            abstract inheritable annotation bar_anno;

            type Base {
                annotation title := 'Base description 01';
                annotation foo_anno := 'Base foo_anno 01';
                annotation bar_anno := 'Base bar_anno 01';
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 01'
                }, {
                    'name': 'test::bar_anno',
                    '@value': 'Base bar_anno 01'
                }, {
                    'name': 'test::foo_anno',
                    '@value': 'Base foo_anno 01'
                }],
            }],
        )

        await self.migrate(r"""
            abstract annotation foo_anno;
            abstract inheritable annotation bar_anno;

            type Base {
                annotation title := 'Base description 01';
                annotation foo_anno := 'Base foo_anno 01';
                annotation bar_anno := 'Base bar_anno 01';
            }

            # extend Base
            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 01'
                }, {
                    'name': 'test::bar_anno',
                    '@value': 'Base bar_anno 01'
                }, {
                    'name': 'test::foo_anno',
                    '@value': 'Base foo_anno 01'
                }],
            }, {
                'name': 'test::Derived',
                'annotations': [{
                    'name': 'test::bar_anno',
                    '@value': 'Base bar_anno 01'
                }],
            }],
        )

    async def test_edgeql_migration_eq_annotation_02(self):
        await self.migrate(r"""
            type Base;
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [],
            }],
        )

        await self.migrate(r"""
            abstract annotation foo_anno;

            type Base {
                annotation title := 'Base description 02';
                annotation foo_anno := 'Base foo_anno 02';
            }

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 02'
                }, {
                    'name': 'test::foo_anno',
                    '@value': 'Base foo_anno 02'
                }],
            }, {
                'name': 'test::Derived',
                # annotation not inherited
                'annotations': [],
            }],
        )

        await self.migrate(r"""
            # remove foo_anno
            type Base {
                annotation title := 'Base description 02';
            }

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 02'
                }],
            }, {
                'name': 'test::Derived',
                'annotations': [],
            }],
        )

    async def test_edgeql_migration_eq_annotation_03(self):
        await self.migrate(r"""
            type Base;
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [],
            }],
        )

        await self.migrate(r"""
            abstract inheritable annotation bar_anno;

            type Base {
                annotation title := 'Base description 03';
                annotation bar_anno := 'Base bar_anno 03';
            }

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 03'
                }, {
                    'name': 'test::bar_anno',
                    '@value': 'Base bar_anno 03'
                }],
            }, {
                'name': 'test::Derived',
                # annotation inherited
                'annotations': [{
                    'name': 'test::bar_anno',
                    '@value': 'Base bar_anno 03'
                }],
            }],
        )

        await self.migrate(r"""
            # remove bar_anno
            type Base {
                annotation title := 'Base description 03';
            }

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 03'
                }],
            }, {
                'name': 'test::Derived',
                'annotations': [],
            }],
        )

    async def test_edgeql_migration_eq_annotation_04(self):
        # Test migration of annotation value and nothing else.
        await self.migrate(r"""
            abstract annotation description;

            type Base {
                annotation description := "1";
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'test::description',
                    '@value': '1',
                }],
            }],
        )

        await self.migrate(r"""
            abstract annotation description;

            type Base {
                annotation description := "2";
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'test::description',
                    '@value': '2',
                }],
            }],
        )

    async def test_edgeql_migration_describe_annot_01(self):
        await self.migrate('''
            abstract annotation foo;

            type Base {
                annotation foo := "1";
            };
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract annotation bar;

                    type Base {
                        annotation bar := "1";
                    };
                }
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER ABSTRACT ANNOTATION test::foo '
                        'RENAME TO test::bar;'
                    )
                }],
            },
        })

    async def test_edgeql_migration_eq_index_01(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self.migrate(r"""
            type Base {
                property name -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [],
            }],
        )

        await self.migrate(r"""
            type Base {
                property name -> str;
                # an index
                index on (.name);
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.name'
                }]
            }],
        )

        await self.migrate(r"""
            type Base {
                # rename the indexed property
                property title -> str;
                index on (.title);
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.title'
                }]
            }],
        )

    async def test_edgeql_migration_eq_index_02(self):
        await self.migrate(r"""
            type Base {
                property name -> str;
                index on (.name);
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.name'
                }]
            }],
        )

        await self.migrate(r"""
            type Base {
                property name -> str;
                # remove the index
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [],
            }],
        )

    async def test_edgeql_migration_eq_index_03(self):
        await self.migrate(r"""
            type Base {
                property name -> int64;
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [],
            }],
        )

        await self.migrate(r"""
            type Base {
                property name -> int64;
                # an index
                index on (.name);
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.name'
                }]
            }],
        )

        await self.migrate(r"""
            type Base {
                # change the indexed property type
                property name -> int32;
                index on (.name);
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.name'
                }]
            }],
        )

    async def test_edgeql_migration_eq_index_04(self):
        await self.migrate(r"""
            type Base {
                property first_name -> str;
                property last_name -> str;
                property name := .first_name ++ ' ' ++ .last_name;
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [],
            }],
        )

        await self.migrate(r"""
            type Base {
                property first_name -> str;
                property last_name -> str;
                property name := .first_name ++ ' ' ++ .last_name;
                # an index on a computable
                index on (.name);
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.name'
                }]
            }],
        )

    async def test_edgeql_migration_eq_collections_01(self):
        await self.migrate(r"""
            type Base;
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.migrate(r"""
            type Base {
                property foo -> array<float32>;
            }
        """)

        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := [1.2, 4.5]
            };
        """)
        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [[1.2, 4.5]],
        )

    async def test_edgeql_migration_eq_collections_02(self):
        await self.migrate(r"""
            type Base;
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.migrate(r"""
            type Base {
                property foo -> tuple<str, int32>;
            }
        """)

        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := ('hello', 42)
            };
        """)
        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [['hello', 42]],
        )

    async def test_edgeql_migration_eq_collections_03(self):
        await self.migrate(r"""
            type Base;
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.migrate(r"""
            type Base {
                # nested collection
                property foo -> tuple<str, int32, array<float32>>;
            }
        """)

        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := ('test', 42, [1.2, 4.5])
            };
        """)
        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [['test', 42, [1.2, 4.5]]],
        )

    async def test_edgeql_migration_eq_collections_04(self):
        await self.migrate(r"""
            type Base;
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.migrate(r"""
            type Base {
                property foo -> tuple<a: str, b: int32>;
            }
        """)

        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := (a := 'hello', b := 42)
            };
        """)
        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [{'a': 'hello', 'b': 42}],
        )

    async def test_edgeql_migration_eq_collections_06(self):
        await self.migrate(r"""
            type Base {
                property foo -> array<int32>;
            }
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := [1, 2]
            }
        """)
        # sanity check
        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [[1, 2]],
        )

        await self.migrate(r"""
            type Base {
                property foo -> array<float64>;
            }
        """)

        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [[1.0, 2.0]],
        )

    async def test_edgeql_migration_eq_collections_07(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self.migrate(r"""
            type Base {
                # convert property type to tuple
                property bar -> array<str>;
                property foo -> tuple<str, int32>;
            }
        """)

        await self.con.execute(r"""
            INSERT Base {
                bar := ['123'],
                foo := ('test', <int32>7),
            }
        """)

        await self.migrate(
            r"""
                type Base {
                    property bar -> array<int64>;
                    property foo -> tuple<str, int32, int32>;
                }
            """,
            user_input=[
                "<array<int64>>.bar",
                "(.foo.0, .foo.1, 0)",
            ]
        )

        await self.assert_query_result(
            r"""SELECT Base.bar;""",
            [[123]],
        )

        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [['test', 7, 0]],
        )

    async def test_edgeql_migration_eq_collections_08(self):
        await self.migrate(r"""
            type Base {
                property foo -> tuple<int32, int32>;
            }
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := (0, 8)
            }
        """)

        await self.migrate(r"""
            type Base {
                # convert property type to a tuple with different (but
                # cast-compatible) element types
                property foo -> tuple<int64, int32>;
            }
        """)

        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [[0, 8]],
        )

    async def test_edgeql_migration_eq_collections_09(self):
        await self.migrate(r"""
            type Base {
                property foo -> tuple<str, int32>;
            }
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := ('test', 9)
            }
        """)

        await self.migrate(r"""
            type Base {
                # convert property type from unnamed to named tuple
                property foo -> tuple<a: str, b: int32>;
            }
        """)

        # In theory, since under normal circumstances we can cast an
        # unnamed tuple into named, it's reasonable to expect this
        # migration to preserve data here.
        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [{'a': 'test', 'b': 9}],
        )

    async def test_edgeql_migration_eq_collections_13(self):
        await self.migrate(r"""
            type Base {
                property foo -> float32;
            };

            # alias that don't have arrays
            alias BaseAlias := Base { bar := Base.foo };
            alias CollAlias := Base.foo;
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := 13.5,
            }
        """)

        # make sure that the alias initialized correctly
        await self.assert_query_result(
            r"""SELECT BaseAlias{bar};""",
            [{'bar': 13.5}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [13.5],
        )

        await self.migrate(r"""
            type Base {
                property foo -> float32;
            };

            # "same" alias that now have arrays
            alias BaseAlias := Base { bar := [Base.foo] };
            alias CollAlias := [Base.foo];
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias{bar};""",
            [{'bar': [13.5]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [[13.5]],
        )

    async def test_edgeql_migration_eq_collections_14(self):
        await self.migrate(r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # alias that don't have tuples
            alias BaseAlias := Base { bar := Base.foo };
            alias CollAlias := Base.foo;
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                name := 'coll_14',
                foo := 14.5,
            }
        """)

        # make sure that the alias initialized correctly
        await self.assert_query_result(
            r"""SELECT BaseAlias{bar};""",
            [{'bar': 14.5}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [14.5],
        )

        await self.migrate(r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # "same" alias that now have tuples
            alias BaseAlias := Base { bar := (Base.name, Base.foo) };
            alias CollAlias := (Base.name, Base.foo);
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias{bar};""",
            [{'bar': ['coll_14', 14.5]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [['coll_14', 14.5]],
        )

    async def test_edgeql_migration_eq_collections_15(self):
        await self.migrate(r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # alias that don't have nested collections
            alias BaseAlias := Base { bar := Base.foo };
            alias CollAlias := Base.foo;
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                name := 'coll_15',
                number := 15,
                foo := 15.5,
            }
        """)

        # make sure that the alias initialized correctly
        await self.assert_query_result(
            r"""SELECT BaseAlias{bar};""",
            [{'bar': 15.5}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [15.5],
        )

        await self.migrate(r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # "same" alias that now have nested collections
            alias BaseAlias := Base {
                bar := (Base.name, Base.number, [Base.foo])
            };
            alias CollAlias := (Base.name, Base.number, [Base.foo]);
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias{bar};""",
            [{'bar': ['coll_15', 15, [15.5]]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [['coll_15', 15, [15.5]]],
        )

    async def test_edgeql_migration_eq_collections_16(self):
        await self.migrate(r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # alias that don't have named tuples
            alias BaseAlias := Base { bar := Base.foo };
            alias CollAlias := Base.foo;
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                name := 'coll_16',
                foo := 16.5,
            }
        """)

        # make sure that the alias initialized correctly
        await self.assert_query_result(
            r"""SELECT BaseAlias{bar};""",
            [{'bar': 16.5}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [16.5],
        )

        await self.migrate(r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # "same" alias that now have named tuples
            alias BaseAlias := Base {
                bar := (a := Base.name, b := Base.foo)
            };
            alias CollAlias := (a := Base.name, b := Base.foo);
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias{bar};""",
            [{'bar': {'a': 'coll_16', 'b': 16.5}}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [{'a': 'coll_16', 'b': 16.5}],
        )

    async def test_edgeql_migration_eq_collections_17(self):
        await self.migrate(r"""
            type Base {
                property foo -> float32;
                property bar -> int32;
            };

            # alias with array<int32>
            alias BaseAlias := Base { data := [Base.bar] };
            alias CollAlias := [Base.bar];
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := 17.5,
                bar := 17,
            }
        """)

        # make sure that the alias initialized correctly
        await self.assert_query_result(
            r"""SELECT BaseAlias{data};""",
            [{'data': [17]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [[17]],
        )

        await self.migrate(r"""
            type Base {
                property foo -> float32;
                property bar -> int32;
            };

            # alias with array<float32>
            alias BaseAlias := Base { data := [Base.foo] };
            alias CollAlias := [Base.foo];
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias{data};""",
            [{'data': [17.5]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [[17.5]],
        )

    async def test_edgeql_migration_eq_collections_18(self):
        await self.migrate(r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # alias with tuple<str, int32>
            alias BaseAlias := Base {
                data := (Base.name, Base.number)
            };
            alias CollAlias := (Base.name, Base.number);
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                name := 'coll_18',
                number := 18,
                foo := 18.5,
            }
        """)

        # make sure that the alias initialized correctly
        await self.assert_query_result(
            r"""SELECT BaseAlias{data};""",
            [{'data': ['coll_18', 18]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [['coll_18', 18]],
        )

        await self.migrate(r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # alias with tuple<str, int32, float32>
            alias BaseAlias := Base {
                data := (Base.name, Base.number, Base.foo)
            };
            alias CollAlias := (Base.name, Base.number, Base.foo);
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias{data};""",
            [{'data': ['coll_18', 18, 18.5]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [['coll_18', 18, 18.5]],
        )

    async def test_edgeql_migration_eq_collections_20(self):
        await self.migrate(r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # alias with tuple<str, int32>
            alias BaseAlias := Base {
                data := (Base.name, Base.number)
            };
            alias CollAlias := (Base.name, Base.number);
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                name := 'test20',
                number := 20,
                foo := 123.5,
            }
        """)

        # make sure that the alias initialized correctly
        await self.assert_query_result(
            r"""SELECT BaseAlias{data};""",
            [{'data': ['test20', 20]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [['test20', 20]],
        )

        await self.migrate(r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # alias with tuple<str, float32>
            alias BaseAlias := Base {
                data := (Base.name, Base.foo)
            };
            alias CollAlias := (Base.name, Base.foo);
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias {data};""",
            [{'data': ['test20', 123.5]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [['test20', 123.5]],
        )

    async def test_edgeql_migration_eq_collections_21(self):
        await self.migrate(r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # alias with tuple<str, float32>
            alias BaseAlias := Base {
                data := (Base.name, Base.foo)
            };
            alias CollAlias := (Base.name, Base.foo);
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                name := 'coll_21',
                foo := 21.5,
            }
        """)

        # make sure that the alias initialized correctly
        await self.assert_query_result(
            r"""SELECT BaseAlias{data};""",
            [{'data': ['coll_21', 21.5]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [['coll_21', 21.5]],
        )

        await self.migrate(r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # alias with named tuple<a: str, b: float32>
            alias BaseAlias := Base {
                data := (a := Base.name, b := Base.foo)
            };
            alias CollAlias := (a := Base.name, b := Base.foo);
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias{data};""",
            [{'data': {'a': 'coll_21', 'b': 21.5}}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [{'a': 'coll_21', 'b': 21.5}],
        )

    async def test_edgeql_migration_eq_drop_module(self):
        await self.migrate(r"""
            type Base;
        """, module='test')

        await self.migrate(r"""
            scalar type foo extending std::str;
        """, module='newtest')

        await self.assert_query_result(
            'SELECT (SELECT schema::Module FILTER .name LIKE "%test").name;',
            {'newtest', 'std::_test'},
        )

    async def test_edgeql_migration_inherited_optionality_01(self):
        await self.migrate(r"""
            type User;

            type Message {
                required link author -> User;
                required property body -> str;
            };
        """)

        await self.start_migration(r"""
            type User;

            type BaseMessage {
                required link author -> User;
                required property body -> str;
            }

            type Message extending BaseMessage;
        """)

        await self.fast_forward_describe_migration()

    async def test_edgeql_migration_rename_type_02(self):
        await self.migrate(r"""
            type Note {
                property note -> str;
            }
            type Subtype extending Note;
            type Link {
                link a -> Note;
            }
            type Uses {
                required property x -> str {
                    default := (SELECT Note.note LIMIT 1)
                }
            };
            type ComputeLink {
                property foo -> str;
                multi link x := (
                    SELECT Note FILTER Note.note = ComputeLink.foo);
            };
            alias Alias := Note;
        """)

        await self.migrate(r"""
            type Remark {
                property note -> str;
            }
            type Subtype extending Remark;
            type Link {
                link a -> Remark;
            }
            type Uses {
                required property x -> str {
                    default := (SELECT Remark.note LIMIT 1)
                }
            };
            type ComputeLink {
                property foo -> str;
                multi link x := (
                    SELECT Remark FILTER Remark.note = ComputeLink.foo);
            };
            alias Alias := Remark;
        """)

        await self.migrate("")

    async def test_edgeql_migration_rename_type_03(self):
        await self.migrate(r"""
            type Note {
                property note -> str;
            }
        """)

        await self.migrate(r"""
            type Remark {
                property note -> str;
            }
            type Subtype extending Remark;
            type Link {
                link a -> Remark;
            }
            type Uses {
                required property x -> str {
                    default := (SELECT Remark.note LIMIT 1)
                }
            };
            type ComputeLink {
                property foo -> str;
                multi link x := (
                    SELECT Remark FILTER Remark.note = ComputeLink.foo);
            };
            alias Alias := Remark;
        """)

        await self.migrate("")

    async def test_edgeql_migration_annotation_05(self):
        await self.migrate(r"""
            abstract inheritable annotation my_anno;

            type Base {
                property my_prop -> str {
                    annotation my_anno := 'Base my_anno 05';
                }
            }

            type Derived extending Base {
                overloaded property my_prop -> str {
                    annotation my_anno := 'Derived my_anno 05';
                }
            }
        """)

        await self.migrate(r"""
            # rename annotated & inherited property
            abstract inheritable annotation my_anno;

            type Base {
                property renamed_prop -> str {
                    annotation my_anno := 'Base my_anno 05';
                }
            }

            type Derived extending Base {
                overloaded property renamed_prop -> str {
                    annotation my_anno := 'Derived my_anno 05';
                }
            }
        """)

        await self.migrate("")

    async def test_edgeql_migration_reset_optional_01(self):
        await self.migrate(r'''
            abstract type Person {
                required property name -> str;
            }

            type PC extending Person;
        ''')

        await self.migrate(r'''
            abstract type Person {
                property name -> str;
            }

            type PC extending Person;
        ''')

        await self.migrate(r'''
            abstract type Person {
                required property name -> str;
            }

            type PC extending Person;
        ''', user_input=['""'])

    async def test_edgeql_migration_reset_optional_02(self):
        await self.migrate(r'''
            abstract type Person {
                required property name -> str;
            }

            type PC extending Person {
                overloaded required property name -> str;
            }
        ''')

        await self.migrate(r'''
            abstract type Person {
                property name -> str;
            }

            type PC extending Person {
                overloaded property name -> str;
            }
        ''')

    async def test_edgeql_migration_reset_optional_03(self):
        await self.migrate(r'''
            abstract type Person {
                required property name -> str;
            }

            type PC extending Person {
                overloaded required property name -> str;
            }
        ''')

        await self.migrate(r'''
            abstract type Person {
                optional property name -> str;
            }

            type PC extending Person {
                overloaded optional property name -> str;
            }
        ''')

    @test.xerror('''
        This fails because we try to set the parent as required while the
        child is still optional.

        For this to work, we need to process the *child* first,
        but in order for the reverse case above to work we need to
        process the *parent* first.

        I don't know if there is any way to fix this sort of thing without
        exposing this kind of semantic understanding to ordering.

        Maybe suppressing some kinds of intermediate-state errors during
        migrations would be OK?
    ''')
    async def test_edgeql_migration_reset_optional_04(self):
        await self.migrate(r'''
            abstract type Person {
                optional property name -> str;
            }

            type PC extending Person {
                overloaded optional property name -> str;
            }
        ''')

        await self.migrate(r'''
            abstract type Person {
                required property name -> str;
            }

            type PC extending Person {
                overloaded required property name -> str;
            }
        ''', user_input=["''", "''"])

    async def test_edgeql_migration_invalid_scalar_01(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r"may not have more than one concrete base type"):
            await self.con.execute(r"""
                START MIGRATION TO {
                    abstract scalar type test::lol extending str;
                    scalar type test::myint extending int64, test::lol;
                };
                POPULATE MIGRATION;
            """)

    async def test_edgeql_migration_inherited_default_01(self):
        await self.migrate(r"""
            abstract type Foo {
                multi link link -> Obj {
                    default := ( select Obj filter .name = 'X' )
                };
            }

            type Bar extending Foo {}

            type Obj {
                required property name -> str {
                    constraint exclusive;
                }
            }
        """)

    async def test_edgeql_migration_inherited_default_02(self):
        await self.migrate(r"""
            abstract type Foo {
                multi link link -> Obj {
                };
            }

            type Bar extending Foo {}

            type Obj {
                required property name -> str {
                    constraint exclusive;
                }
            }
        """)

        await self.migrate(r"""
            abstract type Foo {
                multi link link -> Obj {
                    default := ( select Obj filter .name = 'X' )
                };
            }

            type Bar extending Foo {}

            type Obj {
                required property name -> str {
                    constraint exclusive;
                }
            }
        """)

    async def test_edgeql_migration_scalar_array_01(self):
        await self.migrate(r"""
            type User {
                required property scopes -> array<scope>;
            }
            scalar type scope extending int64 {
                constraint one_of (1, 2);
            }
        """)

        await self.migrate(r"""
            type User {
                required property scopes -> array<scope>;
            }
            scalar type scope extending int64 {
                constraint one_of (1, 2, 3);
            }
        """)

    async def test_edgeql_migration_scalar_array_02(self):
        await self.migrate(r"""
            scalar type scope extending int64;
        """)

        await self.migrate(r"""
            type User {
                required property scopes -> array<scope>;
            }
            scalar type scope extending int64 {
                constraint one_of (1, 2);
            }
        """)

    async def test_edgeql_migration_force_alter(self):
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Obj1 {
                        property foo -> str;
                        property bar -> str;
                    }

                    type Obj2 {
                        property o -> int64;
                        link o1 -> Obj1;
                    }
                };
            };
        ''')
        await self.fast_forward_describe_migration()

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Obj1 {
                        property foo -> str;
                        property bar -> str;
                    }

                    type NewObj2 {
                        property name -> str;
                        annotation title := 'Obj2';
                    }
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text':
                        'CREATE TYPE test::NewObj2 {\n'
                        "    CREATE ANNOTATION std::title := 'Obj2';\n"
                        '    CREATE PROPERTY name'
                        ' -> std::str;\n'
                        '};'
                }],
            },
        })

        await self.con.execute('''
            ALTER CURRENT MIGRATION REJECT PROPOSED;
        ''')

        # We get the parts suggested to us granularly. We only bother
        # to check the first one.
        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text':
                        'ALTER TYPE test::Obj2 {\n'
                        '    DROP LINK o1;\n'
                        '\n'
                        '};'
                }],
            },
        })
        await self.fast_forward_describe_migration()

    async def test_edgeql_migration_non_ddl_statements(self):
        await self.con.execute('SET MODULE test')

        await self.start_migration('''
            type Obj1 {
                property foo -> str;
            }
        ''')

        await self.con.execute('SELECT 1')

        await self.fast_forward_describe_migration(commit=False)

        await self.con.execute('INSERT Obj1 { foo := "test" }')

        await self.assert_describe_migration({
            'confirmed': [
                'SELECT 1;',
                'CREATE TYPE test::Obj1 { CREATE PROPERTY foo -> std::str; };',
                "INSERT Obj1 { foo := 'test' };"
            ],
            'complete': True,
        })

        await self.con.execute('COMMIT MIGRATION')

        await self.assert_query_result(
            'SELECT Obj1 { foo }',
            [{'foo': 'test'}],
        )

    async def test_edgeql_migration_future_01(self):
        await self.con.execute('''
            START MIGRATION TO {
                using future nonrecursive_access_policies;
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text':
                        "CREATE FUTURE nonrecursive_access_policies;"
                }],
                'confidence': 1.0,
            },
        })

        await self.fast_forward_describe_migration()

        await self.assert_query_result(
            r"""
                SELECT schema::FutureBehavior {
                    name,
                }
                FILTER .name = 'nonrecursive_access_policies'
            """,
            [{
                'name': 'nonrecursive_access_policies',
            }]
        )

        await self.con.execute('''
            START MIGRATION TO {
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text':
                        'DROP FUTURE nonrecursive_access_policies;'
                }],
                'confidence': 1.0,
            },
        })

        await self.fast_forward_describe_migration()

        await self.assert_query_result(
            r"""
                SELECT schema::FutureBehavior {
                    name,
                }
                FILTER .name = 'nonrecursive_access_policies'
            """,
            []
        )

    async def test_edgeql_migration_extensions_01(self):
        await self.con.execute('''
            START MIGRATION TO {
                using extension graphql;
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text':
                        "CREATE EXTENSION graphql VERSION '1.0';"
                }],
                'confidence': 1.0,
            },
        })

        await self.fast_forward_describe_migration()

        await self.assert_query_result(
            r"""
                SELECT schema::Extension {
                    name,
                }
                FILTER .name = 'graphql'
            """,
            [{
                'name': 'graphql',
            }]
        )

        await self.con.execute('''
            START MIGRATION TO {
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text':
                        'DROP EXTENSION graphql;'
                }],
                'confidence': 1.0,
            },
        })

        await self.fast_forward_describe_migration()

        await self.assert_query_result(
            r"""
                SELECT schema::Extension {
                    name,
                }
                FILTER .name = 'graphql'
            """,
            [],
        )

        await self.con.execute('''
            START MIGRATION TO {
                using extension graphql version '1.0';
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text':
                        "CREATE EXTENSION graphql VERSION '1.0';"
                }],
                'confidence': 1.0,
            },
        })

        await self.fast_forward_describe_migration()

    async def test_edgeql_migration_confidence_01(self):
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Obj1 {
                        property foo -> str;
                        property bar -> str;
                    }
                };
            };
        ''')
        await self.fast_forward_describe_migration()

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type NewObj1 {
                        property foo -> str;
                        property bar -> str;
                    }
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text':
                        'ALTER TYPE test::Obj1 RENAME TO test::NewObj1;'
                }],
                'confidence': 0.637027,
            },
        })

        await self.con.execute('''
            ALTER CURRENT MIGRATION REJECT PROPOSED;
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text':
                        'CREATE TYPE test::NewObj1 {\n'
                        '    CREATE PROPERTY bar -> std::str;'
                        '\n    CREATE PROPERTY foo -> std::str;'
                        '\n};'
                }],
                'confidence': 1.0,
            },
        })

    async def test_edgeql_migration_confidence_02(self):
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Obj1;
                };
            };
        ''')
        await self.fast_forward_describe_migration()

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Obj1;
                    type Obj2;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text':
                        'CREATE TYPE test::Obj2;'
                }],
                'confidence': 1.0,
            },
        })

    async def test_edgeql_migration_confidence_03(self):
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Obj1;
                };
            };
        ''')
        await self.fast_forward_describe_migration()

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Obj1 {
                        property x -> str;
                    }
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text':
                        'ALTER TYPE test::Obj1 {\n    '
                        'CREATE PROPERTY x -> std::str;\n};'
                }],
                'confidence': 1.0,
            },
        })

    async def test_edgeql_migration_confidence_04(self):
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Obj1 {
                        link link -> Object;
                    }
                };
            };
        ''')
        await self.fast_forward_describe_migration()

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Obj1 {
                        link link -> Object {
                            property x -> str;
                         }
                    }
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text':
                        'ALTER TYPE test::Obj1 {\n    '
                        'ALTER LINK link {\n        '
                        'CREATE PROPERTY x -> std::str;\n    };\n};'
                }],
                'confidence': 1.0,
            },
        })

    async def test_edgeql_migration_data_safety_01(self):
        await self.start_migration('''
            type Obj1;
        ''')
        await self.fast_forward_describe_migration()

        await self.start_migration('''
            type Obj1;
            type Obj2;
        ''')

        # Creations are safe
        await self.assert_describe_migration({
            'confirmed': [],
            'proposed': {
                'statements': [{
                    'text':
                        'CREATE TYPE test::Obj2;'
                }],
                'data_safe': True,
            },
        })

        await self.fast_forward_describe_migration()

        await self.start_migration('''
            type Obj1;
        ''')

        # Deletions are NOT safe
        await self.assert_describe_migration({
            'confirmed': [],
            'proposed': {
                'statements': [{
                    'text': 'DROP TYPE test::Obj2;'
                }],
                'data_safe': False,
            },
        })

        await self.fast_forward_describe_migration()

        # Renames are safe
        await self.start_migration('''
            type Obj11;
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'proposed': {
                'statements': [{
                    'text': 'ALTER TYPE test::Obj1 RENAME TO test::Obj11;'
                }],
                'data_safe': True,
            },
        })

        await self.fast_forward_describe_migration()

        # Again, creations are safe.
        await self.start_migration('''
            type Obj11 {
                property name -> str {
                    constraint exclusive;
                }
            }
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'proposed': {
                'statements': [{
                    'text': """
                        ALTER TYPE test::Obj11 {
                            CREATE PROPERTY name -> std::str {
                                CREATE CONSTRAINT std::exclusive;
                            };
                        };
                    """,
                }],
                'data_safe': True,
            },
        })

        await self.fast_forward_describe_migration()

        await self.start_migration('''
            type Obj11 {
                property name -> str;
            }
        ''')

        # Dropping constraints is safe.
        await self.assert_describe_migration({
            'confirmed': [],
            'proposed': {
                'statements': [{
                    'text': """
                        ALTER TYPE test::Obj11 {
                            ALTER PROPERTY name {
                                DROP CONSTRAINT std::exclusive;
                            };
                        };
                    """,
                }],
                'data_safe': True,
            },
        })

        await self.fast_forward_describe_migration()

        await self.start_migration('''
            type Obj11 {
                property name -> str {
                    annotation title := 'name';
                }
            }
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'proposed': {
                'statements': [{
                    'text': """
                        ALTER TYPE test::Obj11 {
                            ALTER PROPERTY name {
                                CREATE ANNOTATION std::title := 'name';
                            };
                        };
                    """,
                }],
                'data_safe': True,
            },
        })

        await self.fast_forward_describe_migration()

        # Dropping annotations is fine also.
        await self.start_migration('''
            type Obj11 {
                property name -> str;
            }
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'proposed': {
                'statements': [{
                    'text': """
                        ALTER TYPE test::Obj11 {
                            ALTER PROPERTY name {
                                DROP ANNOTATION std::title;
                            };
                        };
                    """,
                }],
                'data_safe': True,
            },
        })

        await self.fast_forward_describe_migration()

        await self.start_migration('''
            scalar type foo extending str;
            type Obj11 {
                property name -> str;
            }
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'proposed': {
                'statements': [{
                    'text': "CREATE SCALAR TYPE test::foo EXTENDING std::str;",
                }],
                'data_safe': True,
            },
        })

        await self.fast_forward_describe_migration()

        # Dropping scalar types is fine also.
        await self.start_migration('''
            type Obj11 {
                property name -> str;
            }
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'proposed': {
                'statements': [{
                    'text': "DROP SCALAR TYPE test::foo;",
                }],
                'data_safe': True,
            },
        })

        await self.fast_forward_describe_migration()

        await self.start_migration('''
            type Obj11 {
                property name -> str;
                index on (.name);
            }
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'proposed': {
                'statements': [{
                    'text': """
                        ALTER TYPE test::Obj11 {
                            CREATE INDEX ON (.name);
                        };
                    """,
                }],
                'data_safe': True,
            },
        })

        await self.fast_forward_describe_migration()

        # Dropping indexes is fine also.
        await self.start_migration('''
            type Obj11 {
                property name -> str;
            }
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'proposed': {
                'statements': [{
                    'text': """
                        ALTER TYPE test::Obj11 {
                            DROP INDEX ON (.name);
                        };
                    """,
                }],
                'data_safe': True,
            },
        })

        await self.fast_forward_describe_migration()

        # Changing single to multi is fine.
        await self.start_migration('''
            type Obj11 {
                multi property name -> str;
            }
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'proposed': {
                'statements': [{
                    'text': """
                        ALTER TYPE test::Obj11 {
                            ALTER PROPERTY name {
                                SET MULTI;
                            };
                        };
                    """,
                }],
                'data_safe': True,
            },
        })

        await self.fast_forward_describe_migration()

        # But changing multi to single is NOT
        await self.start_migration('''
            type Obj11 {
                single property name -> str;
            }
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'proposed': {
                'statements': [{
                    'text': r"""
                        ALTER TYPE test::Obj11 {
                            ALTER PROPERTY name {
                                SET SINGLE USING (\(conv_expr));
                            };
                        };
                    """,
                }],
                'data_safe': False,
            },
        })

        await self.fast_forward_describe_migration(
            user_input=[
                '(SELECT .name LIMIT 1)'
            ]
        )

    async def test_edgeql_migration_prompt_id_01(self):
        await self.start_migration('''
            type Bar { link spam -> Spam };
            type Spam { link bar -> Bar };
        ''')

        await self.assert_describe_migration({
            'proposed': {
                'prompt_id': 'CreateObjectType TYPE test::Bar',
                'statements': [{
                    'text': 'CREATE TYPE test::Bar;'
                }],
                'confidence': 1.0,
            },
        })

        await self.fast_forward_describe_migration(limit=1)

        await self.assert_describe_migration({
            'proposed': {
                'prompt_id': 'CreateObjectType TYPE test::Spam',
                'statements': [{
                    'text': """
                        CREATE TYPE test::Spam {
                            CREATE LINK bar -> test::Bar;
                        };
                    """,
                }],
                'confidence': 1.0,
            },
        })

        await self.fast_forward_describe_migration(limit=1)

        # N.B: It is important that the prompt_id here match the
        # prompt_id in the first migration, so that the migration tool
        # will automatically apply this proposal as part of the
        # earlier action.
        await self.assert_describe_migration({
            'proposed': {
                'prompt_id': 'CreateObjectType TYPE test::Bar',
                'statements': [{
                    'text': """
                        ALTER TYPE test::Bar {
                            CREATE LINK spam -> test::Spam;
                        };
                    """,
                }],
                'confidence': 1.0,
            },
        })

    async def test_edgeql_migration_user_input_01(self):
        await self.migrate('''
            type Bar { property foo -> str };
        ''')

        await self.start_migration('''
            type Bar { property foo -> int64 };
        ''')

        await self.assert_describe_migration({
            'proposed': {
                'statements': [{
                    'text': '''
                        ALTER TYPE test::Bar {
                            ALTER PROPERTY foo {
                                SET TYPE std::int64 USING (\\(cast_expr));
                            };
                        };
                    '''
                }],
                'required_user_input': [{
                    'placeholder': 'cast_expr',
                    'prompt': (
                        "Please specify a conversion expression"
                        " to alter the type of property 'foo'"
                    ),
                }],
            },
        })

    async def test_edgeql_migration_user_input_02(self):
        await self.migrate('''
            type Bar { multi property foo -> str };
        ''')

        await self.start_migration('''
            type Bar { single property foo -> str };
        ''')

        await self.assert_describe_migration({
            'proposed': {
                'statements': [{
                    'text': '''
                        ALTER TYPE test::Bar {
                            ALTER PROPERTY foo {
                                SET SINGLE USING (\\(conv_expr));
                            };
                        };
                    '''
                }],
                'required_user_input': [{
                    'placeholder': 'conv_expr',
                    'prompt': (
                        "Please specify an expression in order to convert"
                        " property 'foo' of object type 'test::Bar' to"
                        " 'single' cardinality"
                    ),
                }],
            },
        })

    async def test_edgeql_migration_user_input_03(self):
        await self.migrate('''
            type Bar {
                required property foo -> int64;
            };
        ''')
        await self.con.execute('''
            SET MODULE test;
            INSERT Bar { foo := 42 };
            INSERT Bar { foo := 1337 };
        ''')

        await self.start_migration('''
            type Bar {
                required property foo -> int64;
                required property bar -> str;
            };
        ''')

        await self.assert_describe_migration({
            'proposed': {
                'statements': [{
                    'text': '''
                        ALTER TYPE test::Bar {
                            CREATE REQUIRED PROPERTY bar -> std::str {
                                SET REQUIRED USING (\\(fill_expr));
                            };
                        };
                    '''
                }],
                'required_user_input': [{
                    'placeholder': 'fill_expr',
                    'prompt': (
                        "Please specify an expression to populate existing "
                        "objects in order to make property 'bar' of object "
                        "type 'test::Bar' required"
                    ),
                }],
            },
        })

        await self.fast_forward_describe_migration(
            user_input=[
                '<str>.foo ++ "!"'
            ]
        )

        await self.assert_query_result(
            '''
                SELECT Bar {foo, bar} ORDER BY .foo
            ''',
            [
                {'foo': 42, 'bar': "42!"},
                {'foo': 1337, 'bar': "1337!"},
            ],
        )

    async def test_edgeql_migration_user_input_04(self):
        await self.migrate('''
            type BlogPost {
                property title -> str;
            }
        ''')
        await self.con.execute('''
            SET MODULE test;
            INSERT BlogPost { title := "Programming Considered Harmful" }
        ''')

        await self.start_migration('''
            abstract type HasContent {
                required property content -> str;
            }
            type BlogPost extending HasContent {
                property title -> str;
            }
        ''')

        await self.interact([
            "did you create object type 'test::HasContent'?",
            ("did you alter object type 'test::BlogPost'?", "y",
             '"This page intentionally left blank"'),
            # XXX: There is a final follow-up prompt, since the DDL
            # generated above somewhat wrongly leaves 'content' owned
            # by the child. This is kind of wrong, but also *works*, so
            # maybe it's fine for now.
            "did you alter property 'content' of object type "
            "'test::BlogPost'?",
        ])
        await self.fast_forward_describe_migration()

        await self.assert_query_result(
            '''
                SELECT BlogPost {title, content}
            ''',
            [
                {
                    'title': "Programming Considered Harmful",
                    'content': "This page intentionally left blank",
                },
            ],
        )

    async def test_edgeql_migration_user_input_05(self):
        await self.migrate(
            '''
            type Organization {
                required property name -> str;
            }
            type Department {
                required property name -> str;
            }
            '''
        )
        await self.start_migration(
            '''
            type Organization {
                required property name -> str;
            }
            type Department {
                required link org -> Organization;
            };
            '''
        )

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError, "cannot include mutating statements"
        ):
            try:
                await self.fast_forward_describe_migration(
                    user_input=[
                        'insert test::Organization { name := "default" }'
                    ]
                )
            except Exception as e:
                raise e.__cause__

    async def test_edgeql_migration_union_01(self):
        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            "it is illegal to create a type union that causes a "
            "computed property 'deleted' to mix with other versions of the "
            "same property 'deleted'"
        ):
            await self.migrate('''
                type Category {
                    required property title -> str;
                    required property deleted :=
                        EXISTS(.<element[IS DeletionRecord]);
                };
                type Article {
                    required property title -> str;
                    required property deleted :=
                        EXISTS(.<element[IS DeletionRecord]);
                };
                type DeletionRecord {
                    link element -> Article | Category;
                }
            ''')

    async def test_edgeql_migration_backlink_01(self):
        await self.migrate('''
            type User {
                link posts := .<user;
            }

            abstract type Action {
                required link user -> User;
            }

            type Post extending Action;
        ''')

        # Make sure that the objects can actually be created and
        # queried.
        await self.con.execute('''
            SET MODULE test;
            INSERT User;
        ''')
        post = await self.con.query_single('''
            INSERT Post {
                user := (SELECT User LIMIT 1),
            };
        ''')

        await self.assert_query_result(
            r'''
                SELECT User{
                    id,
                    posts: {
                        id
                    } LIMIT 1  # this LIMIT is needed as a workaround
                               # for another bug
                }
            ''',
            [
                {
                    'posts': [{'id': str(post.id)}],
                },
            ],
        )

    async def test_edgeql_migration_misplaced_commands(self):
        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"cannot execute ALTER CURRENT MIGRATION"
            r" outside of a migration block",
        ):
            await self.con.execute('''
                ALTER CURRENT MIGRATION REJECT PROPOSED;
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"cannot execute DESCRIBE CURRENT MIGRATION"
            r" outside of a migration block",
        ):
            await self.con.execute('''
                DESCRIBE CURRENT MIGRATION;
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"cannot execute COMMIT MIGRATION"
            r" outside of a migration block",
        ):
            await self.con.execute('''
                COMMIT MIGRATION;
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"cannot execute ABORT MIGRATION"
            r" outside of a migration block",
        ):
            await self.con.execute('''
                ABORT MIGRATION;
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"cannot execute POPULATE MIGRATION"
            r" outside of a migration block",
        ):
            await self.con.execute('''
                POPULATE MIGRATION;
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"cannot execute CREATE DATABASE"
            r" in a migration block",
        ):
            await self.start_migration('type Foo;')
            await self.con.execute('''
                CREATE DATABASE should_not_happen;
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"cannot execute CREATE ROLE"
            r" in a migration block",
        ):
            await self.start_migration('type Foo;')
            await self.con.execute('''
                CREATE ROLE should_not_happen;
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"cannot execute CREATE MIGRATION"
            r" in a migration block",
        ):
            await self.start_migration('type Foo;')
            await self.con.execute('''
                CREATE MIGRATION blah;
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"cannot execute START MIGRATION"
            r" in a migration block",
        ):
            await self.start_migration('type Foo;')
            await self.con.execute('''
                START MIGRATION TO { module test { type Foo; }};
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"cannot execute START TRANSACTION"
            r" in a migration block",
        ):
            await self.start_migration('type Foo;')
            await self.con.query('''
                START TRANSACTION;
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"cannot execute START TRANSACTION"
            r" in a migration block",
        ):
            await self.start_migration('type Foo;')
            await self.con.query('''
                START TRANSACTION;
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"cannot execute CONFIGURE INSTANCE"
            r" in a migration block",
        ):
            await self.start_migration('type Foo;')
            await self.con.execute('''
                CONFIGURE INSTANCE SET _foo := 123;
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"cannot execute CONFIGURE DATABASE"
            r" in a migration block",
        ):
            await self.start_migration('type Foo;')
            await self.con.execute('''
                CONFIGURE CURRENT DATABASE SET _foo := 123;
            ''')

    @test.xerror('''
        Referring to alias unsupported from computable
    ''')
    async def test_edgeql_migration_alias_01(self):
        await self.migrate(r'''
            type Foo {
                property name -> str;
                property comp := Alias;
            };

            alias Alias := {0, 1, 2, 3};
        ''')

        # Make sure that the objects can actually be created and
        # queried.
        await self.con.execute('''
            SET MODULE test;
            INSERT Foo {name := 'foo'};
        ''')

        await self.assert_query_result(
            r'''
                SELECT Foo {
                    name,
                    comp,
                }
            ''',
            [
                {
                    'name': 'foo',
                    'comp': {0, 1, 2, 3},
                },
            ],
        )

    @test.xerror('''
       Referring to alias unsupported from computable
       This is the only test that broke when we disallowed that!
    ''')
    async def test_edgeql_migration_alias_02(self):
        await self.migrate(r'''
            type Foo {
                property name -> str;
                property comp := Alias + 0;
            };

            alias Alias := {0, 1, 2, 3};
        ''')

        # Make sure that the objects can actually be created and
        # queried.
        await self.con.execute('''
            SET MODULE test;
            INSERT Foo {name := 'foo'};
        ''')

        await self.assert_query_result(
            r'''
                SELECT Foo {
                    name,
                    comp,
                }
            ''',
            [
                {
                    'name': 'foo',
                    'comp': {0, 1, 2, 3},
                },
            ],
        )

        await self.migrate(r'''
            type Foo {
                property name -> str;
                property comp := Alias + 0;
            };

            alias Alias := {4, 5};
        ''')

        await self.assert_query_result(
            r'''
                SELECT Foo {
                    name,
                    comp,
                }
            ''',
            [
                {
                    'name': 'foo',
                    'comp': {4, 5},
                },
            ],
        )

    @test.xerror('''
        Referring to alias unsupported from computable
    ''')
    async def test_edgeql_migration_alias_03(self):
        await self.migrate(r'''
            type Foo {
                property name -> str;
                property comp := {Alias, Alias};
            };

            alias Alias := 42;
        ''')

        # Make sure that the objects can actually be created and
        # queried.
        await self.con.execute('''
            SET MODULE test;
            INSERT Foo {name := 'foo'};
        ''')

        await self.assert_query_result(
            r'''
                SELECT Foo {
                    name,
                    comp,
                }
            ''',
            [
                {
                    'name': 'foo',
                    'comp': [42, 42],
                },
            ],
        )

        await self.migrate(r'''
            type Foo {
                property name -> str;
                property comp := {Alias, Alias};
            };

            alias Alias := 'alias';
        ''')

        await self.assert_query_result(
            r'''
                SELECT Foo {
                    name,
                    comp,
                }
            ''',
            [
                {
                    'name': 'foo',
                    'comp': ['alias', 'alias'],
                },
            ],
        )

    @test.xerror('''
        Referring to alias unsupported from computable
    ''')
    async def test_edgeql_migration_alias_04(self):
        # Same as the previous test, but using a single DDL command to
        # migrate.
        await self.migrate(r'''
            type Foo {
                property name -> str;
                property comp := {Alias, Alias};
            };

            alias Alias := 42;
        ''')

        # Make sure that the objects can actually be created and
        # queried.
        await self.con.execute('''
            SET MODULE test;
            INSERT Foo {name := 'foo'};
        ''')

        await self.assert_query_result(
            r'''
                SELECT Foo {
                    name,
                    comp,
                }
            ''',
            [
                {
                    'name': 'foo',
                    'comp': [42, 42],
                },
            ],
        )

        # Instead of using an SDL migration, use a single DDL command.
        await self.con.execute('''
            ALTER ALIAS Alias USING ('alias');
        ''')

        await self.assert_query_result(
            r'''
                SELECT Foo {
                    name,
                    comp,
                }
            ''',
            [
                {
                    'name': 'foo',
                    'comp': ['alias', 'alias'],
                },
            ],
        )

    @test.xerror('''
        Referring to alias unsupported from computable
    ''')
    async def test_edgeql_migration_alias_05(self):
        await self.migrate(r'''
            type Foo {
                property name -> str;
                link comp := Alias;
            };

            type Bar;

            alias Alias := Bar {val := 42};
        ''')

        # Make sure that the objects can actually be created and
        # queried.
        await self.con.execute('''
            SET MODULE test;
            INSERT Bar;
            INSERT Foo {name := 'foo'};
        ''')

        await self.assert_query_result(
            r'''
                SELECT Foo {
                    name,
                    comp: {
                        val
                    },
                }
            ''',
            [
                {
                    'name': 'foo',
                    'comp': {
                        'val': 42,
                    },
                },
            ],
        )

    @test.xerror('''
        Referring to alias unsupported from computable
    ''')
    async def test_edgeql_migration_alias_06(self):
        await self.migrate(r'''
            type Foo {
                property name -> str;
                property comp := Alias.val;
            };

            type Bar;

            alias Alias := Bar {val := 42};
        ''')

        # Make sure that the objects can actually be created and
        # queried.
        await self.con.execute('''
            SET MODULE test;
            INSERT Bar;
            INSERT Foo {name := 'foo'};
        ''')

        await self.assert_query_result(
            r'''
                SELECT Foo {
                    name,
                    comp,
                }
            ''',
            [
                {
                    'name': 'foo',
                    'comp': {42},
                },
            ],
        )

        await self.migrate(r'''
            type Foo {
                property name -> str;
                property comp := Alias.val;
            };

            type Bar;

            alias Alias := Bar {val := 'val'};
        ''')

        await self.assert_query_result(
            r'''
                SELECT Foo {
                    name,
                    comp,
                }
            ''',
            [
                {
                    'name': 'foo',
                    'comp': {'val'},
                },
            ],
        )

    @test.xerror('''
        Referring to alias unsupported from computable
    ''')
    async def test_edgeql_migration_alias_07(self):
        await self.migrate(r'''
            type Foo {
                property name -> str;
                link comp := Alias.alias_link;
            };

            type Bar {
                property val -> str;
            };
            type Fuz {
                property val -> str;
            };

            alias Alias := Bar {
                alias_link := Fuz {
                    alias_comp := 42,
                }
            };
        ''')

        # Make sure that the objects can actually be created and
        # queried.
        await self.con.execute('''
            SET MODULE test;
            INSERT Bar {val := 'bar'};
            INSERT Fuz {val := 'fuz'};
            INSERT Foo {name := 'foo'};
        ''')

        await self.assert_query_result(
            r'''
                SELECT Foo {
                    name,
                    comp: {
                        val,
                        alias_comp,
                    },
                }
            ''',
            [
                {
                    'name': 'foo',
                    'comp': [{
                        'val': 'fuz',
                        'alais_comp': 42,
                    }],
                },
            ],
        )

        await self.migrate(r'''
            type Foo {
                property name -> str;
                link comp := Alias.alias_link;
            };

            type Bar {
                property val -> str;
            };
            type Fuz {
                property val -> str;
            };

            alias Alias := Bar {
                alias_link := Fuz {
                    alias_comp := 42,
                }
            };
        ''')

        await self.assert_query_result(
            r'''
                SELECT Foo {
                    name,
                    comp: {
                        val
                    },
                }
            ''',
            [
                {
                    'name': 'foo',
                    'comp': [{
                        'val': 'bar',
                        'alais_comp': 42,
                    }],
                },
            ],
        )

    async def test_edgeql_migration_alias_08(self):
        await self.migrate(r'''
            type Foo;
            type Bar;
            alias X := Foo;
        ''')

        await self.migrate(r'''
            type Foo;
            type Bar;
            alias X := Bar;
        ''')

        await self.migrate(r'''
            type Foo;
            type Bar;
            alias X := 30;
        ''')

        await self.migrate(r'''
            type Foo;
            type Bar;
            alias X := "30";
        ''')

        await self.migrate(r'''
            type Foo;
            type Bar;
            alias X := (Bar { z := 10 }, 30);
        ''')

        # delete it
        await self.migrate(r'''
            type Foo;
            type Bar;
        ''')

    async def test_edgeql_migration_alias_09(self):
        await self.migrate(r'''
            type Foo;
            type Bar;
            alias X := Foo { bar := Bar { z := 1 } };
        ''')

        await self.migrate(r'''
            type Foo;
            type Bar;
            alias X := Bar;
        ''')

    async def test_edgeql_migration_tuple_01(self):
        await self.migrate(r'''
            type Bag {
                property switching_tuple -> tuple<name: str, weight: int64>;
            };
        ''')

        with self.assertRaisesRegex(
            AssertionError,
            r"Please specify a conversion expression to alter the type of "
            r"property 'switching_tuple'"
        ):
            await self.migrate(r'''
                type Bag {
                    property switching_tuple -> tuple<name: str, age: int64>;
                };
            ''')

    async def test_edgeql_migration_inheritance_to_empty_01(self):
        await self.migrate(r'''
            type A {
                property name -> str;
            }
            type B {
                property name -> str;
            }
            type C extending A, B {
            }
        ''')

        await self.migrate('')

    async def test_edgeql_migration_inheritance_to_empty_02(self):
        await self.migrate(r'''
            abstract type Named {
                required property name -> str {
                    delegated constraint exclusive;
                }
            }

            type User {
                link avatar -> Card {
                    property text -> str;
                    property tag := .name ++ (("-" ++ @text) ?? "");
                }
            }

            type Card extending Named;

            type SpecialCard extending Card;
        ''')

        await self.migrate('')

    async def test_edgeql_migration_drop_constraint_01(self):
        await self.migrate(r'''
            abstract type Named {
                required property name -> str {
                    delegated constraint exclusive;
                }
            }

            type User {
                link avatar -> Card {
                    property text -> str;
                    property tag := .name ++ (("-" ++ @text) ?? "");
                }
            }

            type Card extending Named;

            type SpecialCard extending Card;
        ''')

        await self.migrate(r'''
            abstract type Named {
                required property name -> str;
            }

            type User {
                link avatar -> Card {
                    property text -> str;
                    property tag := .name ++ (("-" ++ @text) ?? "");
                }
            }

            type Card extending Named;

            type SpecialCard extending Card;
        ''')

    async def test_edgeql_migration_drop_constraint_02(self):
        await self.migrate(r'''
            abstract type Named {
                required property name -> str {
                    delegated constraint exclusive;
                }
            }

            type User {
                link avatar -> Card {
                    property text -> str;
                    property tag := .name ++ (("-" ++ @text) ?? "");
                }
            }

            type Card extending Named;

            type SpecialCard extending Card;
            type SpecialCard2 extending Card;
            type VerySpecialCard extending SpecialCard, SpecialCard2;
        ''')

        await self.migrate(r'''
            abstract type Named {
                required property name -> str;
            }

            type User {
                link avatar -> Card {
                    property text -> str;
                    property tag := .name ++ (("-" ++ @text) ?? "");
                }
            }

            type Card extending Named;

            type SpecialCard extending Card;
            type SpecialCard2 extending Card;
            type VerySpecialCard extending SpecialCard, SpecialCard2;
        ''')

    async def test_edgeql_migration_drop_constraint_03(self):
        await self.migrate(r'''
            type C {
                required property val -> str {
                    constraint exclusive;
                }
            }

            type Foo {
                required link foo -> C {
                    default := (SELECT C FILTER .val = 'D00');
                }
            }
        ''')

        await self.migrate('')

    async def test_edgeql_migration_drop_constraint_04(self):
        await self.migrate(r'''
            type C {
                required property val -> str {
                    constraint exclusive;
                }
            }

            type Foo {
                required link foo -> C {
                    default := (SELECT C FILTER .val = 'D00');
                }
            }
        ''')

        await self.migrate(r'''
            type C {
                required property val -> str;
            }

            type Foo {
                required multi link foo -> C {
                    default := (SELECT C FILTER .val = 'D00');
                }
            }
        ''')

    async def test_edgeql_migration_drop_constraint_05(self):
        await self.migrate(r'''
            type C {
                required property val -> str {
                    constraint exclusive;
                }
                required property val2 -> str {
                    constraint exclusive;
                }
            }

            type Foo {
                required link foo -> C {
                    default := (SELECT C FILTER .val = 'D00');
                }
            }
        ''')

        await self.migrate(r'''
            type C {
                required property val2 -> str {
                    constraint exclusive;
                }
            }

            type Foo {
                required link foo -> C {
                    default := (SELECT C FILTER .val2 = 'D00');
                }
            }
        ''')

    async def test_edgeql_migration_fiddly_delete_01(self):
        await self.migrate(r'''
            type Document {
              multi link entries -> Entry {
                constraint exclusive;
              }
              multi link fields := .entries.field;
              required link form -> Form;
            }

            type Entry {
              required link field -> Field;
              required property value -> str;
              link form := .field.form;
            }

            type Field {
              required property name -> str;

              link form := .<fields[IS Form];
            }

            type Form {
              required property name -> str {
                constraint exclusive;
              }

              multi link fields -> Field;
            }
        ''')
        await self.migrate(r'''
            type Entry {
              required link field -> Field;
              required property value -> str;
              link form := .field.form;
            }

            type Field {
              required property name -> str;

              link form := .<fields[IS Form];
            }

            type Form {
              required property name -> str {
                constraint exclusive;
              }

              multi link fields -> Field;
            }
        ''')

    async def test_edgeql_migration_uuid_array_01(self):
        await self.migrate(r'''
            type Foo {
                property x -> array<uuid>;
            }
        ''')

    async def test_edgeql_migration_on_target_delete_01(self):
        await self.migrate(
            r"""
                type User {
                    multi link workspaces -> Workspace {
                        property title -> str;
                        on target delete allow;
                    }
                }

                type Workspace {
                    multi link users := .<workspaces[is User];
                }
            """
        )

        await self.migrate(
            r"""
                type User {
                    multi link workspaces := .<users[is Workspace];
                }

                type Workspace {
                    multi link users -> User {
                        property title -> str;
                        on target delete allow;
                    }
                }
            """
        )

    async def test_edgeql_migration_on_target_delete_02(self):
        await self.migrate(
            r"""
                type Tgt;
                type Foo {
                    link tgt -> Tgt {
                        on target delete allow;
                    }
                }
                type Bar extending Foo {
                    overloaded link tgt -> Tgt {
                        on target delete restrict;
                    }
                }
            """
        )

        await self.con.execute("""
            with module test
            insert Bar { tgt := (insert Tgt) };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'prohibited by link target policy',
        ):
            await self.con.execute("""
                with module test
                delete Tgt;
            """)

        await self.migrate(
            r"""
                type Tgt;
                type Foo {
                    link tgt -> Tgt {
                        on target delete allow;
                    }
                }
                type Bar extending Foo;
            """
        )

        await self.con.execute("""
            with module test
            delete Tgt;
        """)

    async def test_edgeql_migration_rename_with_stuff_01(self):
        await self.migrate(
            r"""
                type Base {
                        property x -> str;
                        property xbang := .x ++ "!";
                }

                type NamedObject extending Base {
                        required property foo -> str;
                }
            """
        )

        await self.migrate(
            r"""
                type Base {
                        property x -> str;
                        property xbang := .x ++ "!";
                }

                type ReNamedObject extending Base {
                        required property foo -> str;
                }
            """
        )

    async def test_edgeql_migration_access_policy_01(self):
        await self.migrate(r"""
            type Test2 {
                access policy asdf allow all using (true);
            }
        """)

        await self.migrate(r"""
            type Test2 {
                access policy asdf allow all;
            }
        """)

    async def test_edgeql_migration_access_policy_02(self):
        # Make sure policies don't interfere with constraints or indexes
        await self.migrate(r"""
            required global foo -> bool { default := true };
            abstract type Base {
                access policy locked allow all using (global foo);
            }

            type Tgt extending Base;

            type Src {
                required link tgt -> Tgt {
                    constraint exclusive;
                }
                index on (.tgt)
            }
        """)

    async def test_edgeql_migration_globals_01(self):
        schema = r"""
            global current_user_id -> uuid;
            global current_user := (
              select Member filter .id = global current_user_id
            );

            type Foo {
              link owner := .<avatar[is Member];
            };
            type Member {
              link avatar -> Foo {
                constraint exclusive;
              }
            }
        """
        # Make sure it doesn't get into a wedged state
        await self.migrate(schema)
        await self.migrate(schema)

    async def test_edgeql_migration_globals_02(self):
        await self.migrate(r"""
            global current_user_id -> uuid;
            global current_user := (
              select Member filter .id = global current_user_id
            );

            type Foo;
            type Base {
              link avatar -> Foo {
                constraint exclusive;
              }
            }
            type Member;
        """)

        schema = r"""
            global current_user_id -> uuid;
            global current_user := (
              select Member filter .id = global current_user_id
            );

            type Foo;
            type Base {
              link avatar -> Foo {
                constraint exclusive;
              }
            }
            type Member extending Base;
        """

        # Make sure it doesn't get into a wedged state
        await self.migrate(schema)
        await self.migrate(schema)

    async def test_edgeql_migration_policies_and_collections(self):
        # An infinite recursion bug with this this was found by accident
        # when a number of tests accidentally were in the non isolated test.
        # (Simplified a bit.)
        await self.migrate(r"""
            abstract type Base {
                access policy locked allow all using (false);
            }

            type Src {
                required link tgt -> Base {
                    constraint exclusive;
                }
            }
        """)

        await self.migrate(r"""
            alias Foo := 20;
        """)

    async def test_edgeql_migration_drop_required_01(self):
        await self.migrate(r"""
            abstract type AbstractLinkTarget {
                multi link linkSources := .<abstractTarget[is LinkSource];
            }

            type ImplementationType extending AbstractLinkTarget {}

            type LinkSource {
                required link abstractTarget -> AbstractLinkTarget;
            }
        """)

        await self.migrate(r"""
            abstract type AbstractLinkTarget {
                multi link linkSources := .<abstractTarget[is LinkSource];
            }

            type ImplementationType extending AbstractLinkTarget {}

            type LinkSource {
                link abstractTarget -> AbstractLinkTarget;
            }
        """)

    async def test_edgeql_migration_link_to_sub_with_ref_01(self):
        # Test moving a link to a subtype while a ref exists to it
        await self.migrate(r"""
            type Athlete {
                multi link schedules := Athlete.<owner[IS AthleteSchedule];
            }

            abstract type Schedule  {
                required property name -> str;
                required link owner -> Athlete;
            }
            type AthleteSchedule extending Schedule;
        """)

        await self.migrate(r"""
            type Athlete {
                multi link schedules := Athlete.<owner[IS AthleteSchedule];
            }

            abstract type Schedule  {
                required property name -> str;
            }
            type AthleteSchedule extending Schedule {
                required link owner -> Athlete;
            }
        """)

    async def test_edgeql_migration_alias_linkprop_01(self):
        await self.migrate(r"""
            alias UserAlias := User;

            type User {
              multi link ml -> Target {
                property lp -> str;
              };
            }

            type Target;
        """)

    async def test_edgeql_migration_lift_01(self):
        await self.migrate(r"""
            abstract type A;
            abstract type B;

            abstract type Foo extending A;
            type Bar extending Foo, B;
        """)

        await self.migrate(r"""
            abstract type A;
            abstract type B;

            abstract type Foo extending A, B;
            type Bar extending Foo;
        """)


class TestEdgeQLDataMigrationNonisolated(EdgeQLDataMigrationTestCase):
    TRANSACTION_ISOLATION = False

    async def test_edgeql_migration_eq_collections_25(self):
        await self.con.execute(r"""
            START MIGRATION TO {
                module test {
                    alias Foo := [20];
                }
            };
            POPULATE MIGRATION;
            COMMIT MIGRATION;
        """)

        await self.con.execute(r"""
            START MIGRATION TO {
                module test {
                }
            };
            POPULATE MIGRATION;
            COMMIT MIGRATION;
        """)

    async def test_edgeql_ddl_collection_cleanup_06(self):
        for _ in range(2):
            await self.con.execute(r"""
                CREATE FUNCTION cleanup_06(
                    a: int64
                ) -> tuple<int64, tuple<int64>>
                    USING EdgeQL $$
                        SELECT (a, ((a + 1),))
                    $$;
            """)

            await self.con.execute(r"""
                DROP FUNCTION cleanup_06(a: int64)
            """)

    async def test_edgeql_migration_enum_01(self):
        # Test some enum stuff. This needs to be nonisolated because postgres
        # won't let you *use* an enum until it has been committed!
        await self.migrate('''
            scalar type Status extending enum<pending, in_progress, finished>;
            scalar type ImportStatus extending Status;
            scalar type ImportAnalyticsStatus extending Status;

            type Foo { property x -> ImportStatus };
        ''')
        await self.con.execute('''
            with module test
            insert Foo { x := 'pending' };
        ''')

        await self.migrate('''
            scalar type Status extending enum<
                pending, in_progress, finished, wontfix>;
            scalar type ImportStatus extending Status;
 scalar type ImportAnalyticsStatus extending Status;

            type Foo { property x -> ImportStatus };
            function f(x: Status) -> str USING (<str>x);
        ''')

        await self.migrate('''
            scalar type Status extending enum<
                pending, in_progress, finished, wontfix, again>;
            scalar type ImportStatus extending Status;
            scalar type ImportAnalyticsStatus extending Status;

            type Foo { property x -> ImportStatus };
            function f(x: Status) -> str USING (<str>x);
        ''')

        await self.assert_query_result(
            r"""
                with module test
                select <ImportStatus>'wontfix'
            """,
            ['wontfix'],
        )

        await self.assert_query_result(
            r"""
                with module test
                select f(<ImportStatus>'wontfix')
            """,
            ['wontfix'],
        )

        await self.migrate('''
            scalar type Status extending enum<
                pending, in_progress, wontfix, again>;
            scalar type ImportStatus extending Status;
            scalar type ImportAnalyticsStatus extending Status;

            type Foo { property x -> ImportStatus };
            function f(x: Status) -> str USING (<str>x);
        ''')

        await self.migrate('')

    async def test_edgeql_migration_recovery(self):
        await self.con.execute(r"""
            START MIGRATION TO {
                module test {
                    type Foo;
                }
            };
        """)
        await self.con.execute('POPULATE MIGRATION')

        with self.assertRaises(edgedb.EdgeQLSyntaxError):
            await self.con.execute(r"""
                ALTER TYPE Foo;
            """)

        with self.assertRaises(edgedb.TransactionError):
            await self.con.execute("COMMIT MIGRATION")

        await self.con.execute("ABORT MIGRATION")

        self.assertEqual(await self.con.query_single("SELECT 1"), 1)

    async def test_edgeql_script_partial_migration(self):
        with self.assertRaisesRegex(edgedb.QueryError, "incomplete migration"):
            await self.con.execute(r"""
                START MIGRATION TO {
                    module test {
                        type Foo;
                    }
                };
                POPULATE MIGRATION;
            """)

    async def test_edgeql_migration_recovery_in_tx(self):
        await self.con.execute("START TRANSACTION")
        try:
            await self.con.execute("CREATE TYPE Bar")
            await self.con.execute(r"""
                START MIGRATION TO {
                    module test {
                        type Foo;
                    }
                };
            """)

            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.execute(r"""
                    ALTER TYPE Foo;
                """)

            await self.con.execute("ABORT MIGRATION")

            self.assertEqual(await self.con.query("SELECT Bar"), [])
        finally:
            await self.con.execute("ROLLBACK")

    async def test_edgeql_migration_recovery_in_script(self):
        await self.migrate("""
            type Base;
        """)
        await self.con.execute("""
            SET MODULE test;

            INSERT Base;
        """)
        res = await self.con.query(r"""
            CREATE TYPE Bar;
            START MIGRATION TO {
                module test {
                    type Base {
                        required property name -> str;
                    }
                }
            };
            POPULATE MIGRATION;
            ABORT MIGRATION;
            SELECT Bar;
        """)
        self.assertEqual(res, [])

        await self.migrate('')

    async def test_edgeql_migration_recovery_commit_fail(self):
        con2 = await self.connect(database=self.con.dbname)
        try:
            await con2.execute('START MIGRATION TO {}')
            await con2.execute('POPULATE MIGRATION')

            await self.migrate("type Base;")

            with self.assertRaises(edgedb.TransactionError):
                await con2.execute("COMMIT MIGRATION")

            await con2.execute("ROLLBACK")

            self.assertEqual(await con2.query_single("SELECT 1"), 1)
        finally:
            await con2.aclose()

    async def test_edgeql_migration_reset_schema(self):
        await self.migrate(r'''
            type Bar;

            alias Alias := Bar {val := 42};
        ''')
        await self.migrate(r'''
            type Foo {
                property name -> str;
                link comp := Bar;
            };

            type Bar;
        ''')

        res = await self.con.query('''
            select schema::ObjectType { name } filter .name ilike 'test::%'
        ''')
        self.assertEqual(len(res), 2)

        await self.con.query('reset schema to initial')

        res = await self.con.query('''
            select schema::ObjectType { name } filter .name ilike 'test::%'
        ''')
        self.assertEqual(res, [])

        res = await self.con.query('''
            select schema::Migration { script, name };
        ''')
        self.assertEqual(res, [])

        await self.migrate(r'''
            type SomethingElse;
        ''')

        res = await self.con.query('''
            select schema::Migration { script, name };
        ''')
        self.assertEqual(len(res), 1)


class EdgeQLMigrationRewriteTestCase(EdgeQLDataMigrationTestCase):
    DEFAULT_MODULE = 'default'

    async def migrate(self, *args, module: str = 'default', **kwargs):
        await super().migrate(*args, module=module, **kwargs)

    async def get_migrations(self):
        res = await self.con.query(
            '''
            select schema::Migration {
                id, name, script, parents: {name, id}, generated_by
            }
            '''
        )
        if not res:
            return []
        children = {m.parents[0].id: m for m in res if m.parents}
        root = [m for m in res if not m.parents][0]

        sorted_migs = []
        while root:
            sorted_migs.append(root)
            root = children.get(root.id)

        return sorted_migs

    async def assert_migration_history(self, exp_result):
        res = await self.get_migrations()
        res = serutils.serialize(res)
        assert_data_shape.assert_data_shape(
            res, exp_result, self.fail)


class TestEdgeQLMigrationRewrite(EdgeQLMigrationRewriteTestCase):
    # N.B: These test cases get duplicated as nonisolated test cases,
    # to verify that it all works *outside* a transaction also.
    # If that is a problem, a test case can be made to skip it

    async def test_edgeql_migration_rewrite_01(self):
        # Split one migration up into several
        await self.migrate(r"""
            type A;
            type B;
            type C;
            type D;
        """)
        # Try a bunch of different ways to do it!
        await self.con.execute(r"""
            start migration rewrite;
            start migration to {
                module default {
                    type A;
                }
            };
            populate migration;
            commit migration;

            start migration to {
                module default {
                    type A;
                    type B;
                }
            };
            CREATE type B;
            commit migration;

            create migration {
                create type C;
            };

            CREATE TYPE D;

            commit migration rewrite;
        """)

        await self.assert_migration_history([
            {'script': 'CREATE TYPE default::A;', 'generated_by': None},
            {'script': 'CREATE TYPE B;', 'generated_by': None},
            {'script': 'create type C;', 'generated_by': None},
            {'script': 'CREATE TYPE D;', 'generated_by': 'DDLStatement'},
        ])

    async def test_edgeql_migration_rewrite_02(self):
        # Simulate a potential migration squashing flow from the CLI,
        # where we generate a script using start migration and then apply it
        # later.
        await self.con.execute(r"""
            create type Foo;
            create type Tgt;
            alter type Foo { create link tgt -> Tgt; };
        """)

        await self.con.execute(r"""
            start migration rewrite;
        """)
        await self.con.execute(r"""
            start migration to committed schema;
        """)
        await self.con.execute(r"""
            populate migration;
        """)
        res = json.loads(await self.con.query_json(r"""
            describe current migration as json;
        """))

        await self.con.execute(r"""
            commit migration;
        """)
        await self.con.execute(r"""
            abort migration rewrite;
        """)

        self.assertTrue(res[0]['complete'])
        commands = '\n'.join(res[0]['confirmed'])
        script = textwrap.dedent('''\
            start migration rewrite;
            create migration {
            %s
            };
            commit migration rewrite;
        ''') % textwrap.indent(commands, ' ' * 4)

        await self.con.execute(script)

        await self.assert_migration_history([
            {'script': commands}
        ])

    async def test_edgeql_migration_rewrite_03(self):
        # Test rolling back to a savepoint after a commit failure
        await self.con.execute(r"""
            create type A;
            create type B;
        """)

        await self.con.execute(r"""
            start migration rewrite;
        """)
        await self.con.execute(r"""
            declare savepoint s0;
        """)
        await self.con.execute(r"""
            create type B;
        """)
        await self.con.execute(r"""
            declare savepoint s1;
        """)
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"does not match"):
            await self.con.execute(r"""
                commit migration rewrite;
            """)

        # Rollback and try again
        await self.con.execute(r"""
            rollback to savepoint s1;
        """)
        await self.con.execute(r"""
            create type A;
        """)
        await self.con.execute(r"""
            commit migration rewrite
        """)

        await self.assert_migration_history([
            {'script': 'CREATE TYPE B;'},
            {'script': 'CREATE TYPE A;'},
        ])

    async def test_edgeql_migration_rewrite_05(self):
        # Test ABORT MIGRATION REWRITE
        await self.con.execute(r"""
            create type A;
            create type B;
        """)

        await self.con.execute(r"""
            start migration rewrite;
        """)
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"does not match"):
            await self.con.execute(r"""
                commit migration rewrite;
            """)
        await self.con.execute(r"""
            abort migration rewrite;
        """)

    async def test_edgeql_migration_rewrite_06(self):
        # Test doing the interactive migration flow
        await self.con.execute(r"""
            create type A;
            create type B;
        """)

        await self.con.execute(r"""
            start migration rewrite;
        """)

        await self.start_migration(r"""
            type A;
            type B;
        """, module='default')
        await self.fast_forward_describe_migration()

        await self.con.execute(r"""
            commit migration rewrite;
        """)

        await self.assert_migration_history([
            {'script': 'CREATE TYPE default::A;\nCREATE TYPE default::B;'},
        ])


class TestEdgeQLMigrationRewriteNonisolated(TestEdgeQLMigrationRewrite):
    TRANSACTION_ISOLATION = False

    TEARDOWN_COMMANDS = [
        'rollback;',  # just in case, avoid extra errors
        '''
            start migration to { module default {}; };
            populate migration;
            commit migration;

            start migration rewrite;
            commit migration rewrite;
        ''',
    ]

    async def test_edgeql_migration_rewrite_raw_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"Cannot leave an incomplete migration rewrite in scripts"):
            await self.con.execute(r"""
                START MIGRATION REWRITE;
                START MIGRATION TO {
                    module default {
                        type A;
                    }
                };
                POPULATE MIGRATION;
                COMMIT MIGRATION;
            """)
