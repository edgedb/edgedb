##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.server import _testbase as tb


class TestIndexes(tb.DDLTestCase):

    async def test_index_01(self):
        result = await self.con.execute("""
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
