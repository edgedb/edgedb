##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import uuid

from edgedb.server import _testbase as tb


class TestIndexes(tb.QueryTestCase):

    async def test_index_01(self):
        result = await self.con.execute("""
            # setup delta
            #
            CREATE MIGRATION test::d1 TO eschema $$
                concept Person:
                    link first_name to str
                    link last_name to str

                    index name_index := (self.first_name, self.last_name)
            $$;

            COMMIT MIGRATION test::d1;

            SELECT
                schema::Concept {
                    indexes: {
                        expr
                    }
                }
            WHERE schema::Concept.name = 'test::Person';

            INSERT test::Person {
                first_name := 'Elon',
                last_name := 'Musk'
            };

            WITH MODULE test
            SELECT
                Person {
                    first_name
                }
            WHERE
                Person.first_name = 'Elon' AND Person.last_name = 'Musk';
            """)

        self.assert_data_shape(result, [
            None,

            None,

            [{
                'indexes': [{
                    'expr': 'SELECT ((test::Person).(test::first_name)'
                            '[TO std::str], (test::Person).(test::last_name)'
                            '[TO std::str])'
                }]
            }],

            [{
                'id': uuid.UUID
            }],

            [{
                'id': uuid.UUID,
                'first_name': 'Elon'
            }]
        ])
