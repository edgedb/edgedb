
from edb.common import assert_data_shape
import os
from edb.testbase import server as tb



class TestInterpreterDisambiguationSmokeTests(tb.QueryTestCase):
    """Unit tests for the toy evaluator model.

    These are intended as smoke tests. Since obviously we don't want
    two totally parallel sets of tests for EdgeQL queries, an eventual
    goal should be that we could run the real tests against the model.
    """

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'interpreter_disambiguation.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'interpreter_disambiguation_setup.edgeql')

    async def example_test_01(self):
        await self.assert_query_result(
            "SELECT 1",
            [1],
        )

    async def link_dedup_test_02(self):
        await self.assert_query_result(
            """"select (
                 (select A filter .name = "a1), (select A filter True)
                 ).b""",
            [{}],
        )

    async def link_dedup_test_03(self):
        await self.assert_query_result(
            """"select (
                 (select A filter .name = "a1), (select A filter True)
                 ).b@b_lp""",
            [{}, {}],
        )
