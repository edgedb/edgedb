##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.server import _testbase as tb


class TestDeltas(tb.QueryTestCase):
    async def _test_delta_simple(self):
        """
        CREATE DELTA [test.d1] TO $$
            concept NamedObject:
                required link name -> str
        $$

        COMMIT DELTA [test.d1]

        INSERT [test.NamedObject] [
            name := 'Test'
        ]

        SELECT [test.NamedObject]
        WHERE [test.NamedObject].name = 'Test'


        % OK %

        {"result": "aaa"}
        """
