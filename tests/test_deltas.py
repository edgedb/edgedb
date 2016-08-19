##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import uuid

from edgedb.server import _testbase as tb


class TestDeltas(tb.QueryTestCase):
    async def test_delta_simple01(self):
        result = await self.con.execute("""
            # setup delta
            #
            CREATE DELTA {test::d1} TO $$
                link name:
                    linkproperty lang to str

                concept NamedObject:
                    required link name to str
            $$;

            COMMIT DELTA {test::d1};

            # test updated schema
            #
            INSERT {test::NamedObject} {
                name := 'Test'
            };

            SELECT
                {test::NamedObject} {
                    name {
                        @lang
                    }
                }
            WHERE
                {test::NamedObject}.name = 'Test';

            """)

        self.assert_data_shape(result, [
            None,

            None,

            [{
                'id': uuid.UUID,
            }],

            [{
                'id': uuid.UUID,
                'name': {'@target': 'Test', '@lang': None},
            }]
        ])
