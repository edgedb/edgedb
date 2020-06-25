from edb.testbase import server as tb


class TestDescribeRoles(tb.QueryTestCase):

    async def test_describe_system_config(self):
        result = list(await self.con.fetchall("DESCRIBE SYSTEM CONFIG"))
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], str)
        self.assertIn('CONFIGURE SYSTEM SET', result[0])
