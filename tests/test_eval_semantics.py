from edb.common import assert_data_shape
import os
from edb.testbase import server as tb

class TestNewInterpreterModelSmokeTestsSemantics(tb.QueryTestCase):
    """Unit tests for the toy evaluator model.

    These are smoke tests of mainly corner cases. 
    These are intended for the regression of the interpreter
    """

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'smoke_test_interp.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'smoke_test_interp_setup.edgeql')

    

    async def test_model_basic_01(self):
        await self.assert_query_result(
            "SELECT 1",
            [1],
        )

    async def test_model_basic_02(self):
        await self.assert_query_result(
            r"""
            SELECT Person.name
            """,
            ['Phil Emarg', 'Madeline Hatch', 'Emmanuel Villip'],
        )