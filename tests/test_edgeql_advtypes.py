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


import os.path

from edb.testbase import server as tb


class TestEdgeQLAdvancedTypes(tb.QueryTestCase):
    '''Test type expressions'''

    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'advtypes.esdl')

    async def test_edgeql_advtypes_overlapping_union(self):
        await self.con.execute('''
            INSERT V {name:= 'v0', s := 's0', t := 't0', u := 'u0'};

            INSERT Z {
                name := 'z0',
                stw0 := (
                    SELECT V FILTER .name = 'v0'
                    LIMIT 1
                ),
            };
        ''')

        await self.assert_query_result(
            r'''
                SELECT Z {stw0: {name}} FILTER .name = 'z0';
            ''',
            [{
                'stw0': {'name': 'v0'}
            }]
        )

    async def test_edgeql_advtypes_overlapping_link_union(self):
        await self.con.execute("""
            INSERT A { name := 'a1' };
            INSERT V {
                name:= 'v1',
                s := 's1',
                t := 't1',
                u := 'u1',
                l_a := (SELECT A FILTER .name = 'a1'),
            };
        """)

        await self.assert_query_result(
            r"""
            SELECT (DISTINCT (SELECT S UNION T)) {
                cla := count(.l_a)
            }
            """,
            [{
                'cla': 1,
            }]
        )
