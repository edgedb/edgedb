##
# Copyright (c) 2012-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path

from edgedb.server import _testbase as tb
from edgedb.client import exceptions


class TestCaosConstraintsUniqueLink(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'constraints.eschema')

    async def test_constraints_unique_link(self):
        with self.assertRaises(exceptions.UniqueConstraintViolationError):
            await self.con.execute("""
                INSERT {test::UniqueName} {
                    name := 'Test'
                };

                INSERT {test::UniqueName} {
                    name := 'Test'
                };
            """)
