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


from edb.server import _testbase as tb


class TestIndexes(tb.DDLTestCase):

    async def test_index_01(self):
        result = await self.query("""
            # setup delta
            #
            CREATE MIGRATION test::d1 TO eschema $$
                type Person:
                    property first_name -> str
                    property last_name -> str

                    index name_index on (__subject__.first_name,
                                         __subject__.last_name)
            $$;

            COMMIT MIGRATION test::d1;

            SELECT
                schema::ObjectType {
                    indexes: {
                        expr
                    }
                }
            FILTER schema::ObjectType.name = 'test::Person';

            INSERT test::Person {
                first_name := 'Elon',
                last_name := 'Musk'
            };

            WITH MODULE test
            SELECT
                Person {
                    first_name
                }
            FILTER
                Person.first_name = 'Elon' AND Person.last_name = 'Musk';
            """)

        self.assert_data_shape(result, [
            None,

            None,

            [{
                'indexes': [{
                    'expr': 'SELECT (test::Person.first_name, '
                            'test::Person.last_name)'
                }]
            }],

            [1],

            [{
                'first_name': 'Elon'
            }]
        ])
