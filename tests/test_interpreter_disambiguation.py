
from edb.common import assert_data_shape
import os
from edb.testbase import server as tb
class TestInterpreterDisambiguationSmokeTests(tb.QueryTestCase):

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'interpreter_disambiguation.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'interpreter_disambiguation_setup.edgeql')

    async def test_example_test_01(self):
        await self.assert_query_result(
            "SELECT 1",
            [1],
        )

    async def test_link_dedup_test_02(self):
        await self.assert_query_result(
            """"select (
                 (select A filter True), (select A filter True)
                 ).b""",
            [{}],
        )

    async def test_link_dedup_test_03(self):
        await self.assert_query_result(
            """"select (
                 (select A filter True), (select A filter True)
                 ).b@b_lp""",
            [{}, {}],
        )
