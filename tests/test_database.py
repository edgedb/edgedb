##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.server import _testbase as tb


class TestDatabase(tb.ConnectedTestCase):
    async def test_database_create(self):
        await self.con.execute('CREATE DATABASE mytestdb;')

        try:
            conn = await self.cluster.connect(
                user='edgedb', database='mytestdb', loop=self.loop)

            conn.close()

        finally:
            await self.con.execute('DROP DATABASE mytestdb;')
