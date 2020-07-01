from edb.testbase import server as tb


class TestDescribeRoles(tb.QueryTestCase):

    async def test_describe_system_config(self):
        result = list(await self.con.fetchall("DESCRIBE SYSTEM CONFIG"))
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], str)
        self.assertIn('CONFIGURE SYSTEM SET', result[0])

    async def test_describe_roles(self):
        await self.con.execute("""
            CREATE SUPERUSER ROLE base1;
            CREATE SUPERUSER ROLE `base 2`;
            CREATE SUPERUSER ROLE child1 EXTENDING base1;
            CREATE SUPERUSER ROLE child2 EXTENDING `base 2`;
            CREATE SUPERUSER ROLE child3 EXTENDING base1, child2 {
                SET password := 'test'
            };
        """)
        roles = next(iter(await self.con.fetchall("DESCRIBE ROLES")))
        base1 = roles.index('CREATE SUPERUSER ROLE `base1`;')
        base2 = roles.index('CREATE SUPERUSER ROLE `base 2`;')
        child1 = roles.index('CREATE SUPERUSER ROLE `child1`')
        child2 = roles.index('CREATE SUPERUSER ROLE `child2`')
        child3 = roles.index('CREATE SUPERUSER ROLE `child3`')
        self.assertGreater(child1, base1, roles)
        self.assertGreater(child2, base2, roles)
        self.assertGreater(child3, child2, roles)
        self.assertGreater(child3, base1, roles)
        self.assertIn("SET password_hash := 'SCRAM-SHA-256$4096:", roles)
