
from __future__ import annotations
from typing import (
    Any,
    Optional,
)
import unittest


from edb.common import assert_data_shape
from edb.tools.experimental_interpreter.new_interpreter import EdgeQLInterpreter

bag = assert_data_shape.bag


class ExperimentalInterpreterTestCase(unittest.TestCase):
    SCHEMA: Optional[str] = None
    SETUP: Optional[str] = None
    INTERPRETER_USE_SQLITE = False

    client: EdgeQLInterpreter
    initial_state: object

    @classmethod
    def setUpClass(cls):
        if cls.SCHEMA is not None:
            with open(cls.SCHEMA) as f:
                schema_content = f.read()
        else:
            schema_content = ""

        sqlite_filename = None
        if cls.INTERPRETER_USE_SQLITE:
            sqlite_filename = ":memory:"

            try:
                import sqlite3
            except ModuleNotFoundError:
                raise unittest.SkipTest("sqlite is not installed")

            if sqlite3.sqlite_version_info < (3, 37):
                raise unittest.SkipTest("sqlite version is too old (need 3.37)")

        cls.client = EdgeQLInterpreter(schema_content, sqlite_filename)

        if cls.SETUP is not None:
            with open(cls.SETUP) as f:
                setup_content = f.read()
            cls.client.query_str(setup_content)

        cls.initial_state = cls.client.db.dump_state()

    def setUp(self):
        self.client.db.restore_state(self.initial_state)

    def execute(self, query: str, *, variables=None) -> Any:
        return self.client.run_single_str_get_json_with_cache(
            query, variables=variables)

    def execute_single(self, query: str, *, variables=None) -> Any:
        return self.client.query_single_json(query, variables=variables)

    def assert_query_result(self, query,
                                  exp_result_json,
                                  exp_result_binary=...,
                                  *,
                                  msg: Optional[str] = None,
                                  sort: Optional[bool] = None,
                                  variables=None,
                                  ):
        if (hasattr(self, "use_experimental_interpreter") and
                self.use_experimental_interpreter):
            result = self.client.run_single_str_get_json_with_cache(
                query, variables=variables)
            res = result
            if sort is not None:
                assert_data_shape.sort_results(res, sort)
            if exp_result_binary is not ...:
                assert_data_shape.assert_data_shape(
                    res, exp_result_binary, self.fail, message=msg)
            else:
                assert_data_shape.assert_data_shape(
                    res, exp_result_json, self.fail, message=msg)
