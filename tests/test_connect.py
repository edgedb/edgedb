##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.server import _testbase as tb


class TestConnect(tb.ClusterTestCase):
    async def test_connect_1(self):
        conn = await self.cluster.connect(
            user='edgedb', database='edgedb0', loop=self.loop)

        conn.close()
