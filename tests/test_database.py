##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.server import _testbase as tb


class TestDatabase(tb.ConnectedTestCase):
    async def test_database_create01(self):
        await self.con.execute('CREATE DATABASE mytestdb;')

        try:
            conn = await self.cluster.connect(
                user='edgedb', database='mytestdb', loop=self.loop)

            conn.close()

        finally:
            await self.con.execute('DROP DATABASE mytestdb;')

    async def test_database_create02(self):
        await self.con.execute('CREATE DATABASE `mytestdb`;')

        try:
            conn = await self.cluster.connect(
                user='edgedb', database='mytestdb', loop=self.loop)

            conn.close()

        finally:
            await self.con.execute('DROP DATABASE `mytestdb`;')

    async def test_database_create03(self):
        await self.con.execute(r'CREATE DATABASE `mytest"db"`;')

        try:
            conn = await self.cluster.connect(
                user='edgedb', database='mytest"db"', loop=self.loop)

            conn.close()

        finally:
            await self.con.execute(r'DROP DATABASE `mytest"db"`;')

    async def test_database_create04(self):
        await self.con.execute(r"CREATE DATABASE `mytest'db'`;")

        try:
            conn = await self.cluster.connect(
                user='edgedb', database="mytest'db'", loop=self.loop)

            conn.close()

        finally:
            await self.con.execute(r"DROP DATABASE `mytest'db'`;")

    async def test_database_create05(self):
        await self.con.execute('CREATE DATABASE `SET`;')

        try:
            conn = await self.cluster.connect(
                user='edgedb', database='SET', loop=self.loop)

            conn.close()

        finally:
            await self.con.execute('DROP DATABASE `SET`;')

    async def test_database_create06(self):
        await self.con.execute('CREATE DATABASE `CREATE`;')

        try:
            conn = await self.cluster.connect(
                user='edgedb', database='CREATE', loop=self.loop)

            conn.close()

        finally:
            await self.con.execute('DROP DATABASE `CREATE`;')
