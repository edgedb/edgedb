##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.server import _testbase as tb


class TestInsert(tb.QueryTestCase):
    SETUP = """
        CREATE LINK [test.l1] {
            SET mapping = '11'
            SET readonly = False
        }
        CREATE LINK [test.l2] {
            SET mapping = '11'
            SET readonly = False
        }
        CREATE LINK [test.l3] {
            SET mapping = '11'
            SET readonly = False
        }

        CREATE CONCEPT [test.InsertTest] INHERITING [std.Object] {
            CREATE LINK [test.l1] TO [std.int] {
                SET mapping = '11'
                SET readonly = False
            }
            CREATE REQUIRED LINK [test.l2] TO [std.int] {
                SET mapping = '11'
                SET readonly = False
            }
            CREATE LINK [test.l3] TO [std.str] {
                SET mapping = '11'
                SET readonly = False
                SET default = 'test'
            }
        }
    """

    TEARDOWN = """
    """

    async def test_insert_fail_1(self):
        """
        INSERT [test.InsertTest]

        % ERROR %

        {
            "code": "23502"
        }
        """
