import os
from edb.testbase import experimental_interpreter as tb


class TestInterpreterDisambiguationSmokeTests(
    tb.ExperimentalInterpreterTestCase
):
    INTERPRETER_USE_SQLITE = False

    SCHEMA = os.path.join(
        os.path.dirname(__file__), 'schemas', 'interpreter_disambiguation.esdl'
    )

    SETUP = os.path.join(
        os.path.dirname(__file__),
        'schemas',
        'interpreter_disambiguation_setup.edgeql',
    )

    def test_example_test_01(self):
        self.assert_query_result(
            "SELECT 1",
            [1],
        )

    def test_link_dedup_test_02(self):
        self.assert_query_result(
            """select {
                 (select A filter True), (select A filter True)
                 }.b""",
            [{}],
        )

    def test_link_dedup_test_03(self):
        self.assert_query_result(
            """select {
                 (select A filter True), (select A filter True)
                 }.b@b_lp""",
            ['a1_b1_lp', 'a1_b1_lp'],
        )


class TestInterpreterDisambiguationSmokeTestsSQLite(
    TestInterpreterDisambiguationSmokeTests
):
    INTERPRETER_USE_SQLITE = True
