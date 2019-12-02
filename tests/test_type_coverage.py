from __future__ import annotations

import ast
from dataclasses import dataclass
import os
from pathlib import Path
import tokenize
from typing import List, Union
import unittest

import edb

EDB_DIR = Path(edb.__file__).parent


@dataclass
class TypeCoverage(ast.NodeVisitor):
    typed_functions: int
    untyped_functions: int
    typed_lines: int
    untyped_lines: int

    def __init__(self, path: Path) -> None:
        self.path = path
        self.typed_functions = 0
        self.untyped_functions = 0
        self.typed_lines = 0
        self.untyped_lines = 0
        self._classdefs: List[ast.ClassDef] = []
        self._last_seen_lineno = 0
        super().__init__()

    @property
    def all_functions(self) -> int:
        return self.typed_functions + self.untyped_functions

    @property
    def all_lines(self) -> int:
        return self.typed_lines + self.untyped_lines

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._classdefs.append(node)
        self.generic_visit(node)
        self._classdefs.pop()

    def visit_FunctionDef(
        self, node: Union[ast.FunctionDef, ast.AsyncFunctionDef]
    ) -> None:
        args = node.args
        arg_count = 1  # returns
        typed_count = 0
        for index, arg in enumerate(args.args):
            arg_count += 1
            if arg.annotation:
                self.assert_no_strings(arg.annotation)
                typed_count += 1
            elif index == 0 and self._classdefs and arg.arg in {"self", "cls"}:
                typed_count += 1
        for arg in args.kwonlyargs:
            arg_count += 1
            if arg.annotation:
                self.assert_no_strings(arg.annotation)
                typed_count += 1
        if args.vararg:
            arg_count += 1
            if args.vararg.annotation:
                self.assert_no_strings(args.vararg.annotation)
                typed_count += 1
        if args.kwarg:
            arg_count += 1
            if args.kwarg.annotation:
                self.assert_no_strings(args.kwarg.annotation)
                typed_count += 1
        if node.returns:
            self.assert_no_strings(node.returns)
            typed_count += 1
        start_line = node.lineno
        self.generic_visit(node)
        end_line = self._last_seen_lineno
        if arg_count == typed_count:
            self.typed_functions += 1
            self.typed_lines += end_line - start_line + 1
        else:
            self.untyped_functions += 1
            self.untyped_lines += end_line - start_line + 1

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.visit_FunctionDef(node)

    def visit(self, node: ast.AST) -> None:
        if hasattr(node, "lineno"):
            self._last_seen_lineno = max(self._last_seen_lineno, node.lineno)
        super().visit(node)

    def assert_no_strings(self, annotation: ast.AST) -> None:
        if isinstance(annotation, ast.Str):
            raise AssertionError(
                f"String annotation at {self.path}:"
                f"{annotation.lineno}:{annotation.col_offset}"
            )

    def __iadd__(self, other: TypeCoverage) -> TypeCoverage:
        if not isinstance(other, TypeCoverage):
            return NotImplemented

        self.typed_functions += other.typed_functions
        self.untyped_functions += other.untyped_functions
        self.typed_lines += other.typed_lines
        self.untyped_lines += other.untyped_lines
        return self


def assert_from_future_annotations(code: ast.AST, file: Path) -> None:
    """Raises AssertionError unless the "annotations" future-import is present.

    Only raises for modules.
    """
    if not isinstance(code, ast.Module):
        return

    for top_level_statement in code.body:
        if isinstance(top_level_statement, ast.ImportFrom):
            import_from = top_level_statement
            if import_from.module == "__future__":
                for alias in import_from.names:
                    if not isinstance(alias, ast.alias):
                        raise ValueError(
                            f"Unsupported from-import name type in {file}"
                        )

                    if alias.name == "annotations":
                        return

    raise AssertionError(
        f"Missing `from __future__ import annotations` "
        f"in type-annotated file {file}"
    )


def cover_file(file: Path) -> TypeCoverage:
    with tokenize.open(file) as f:
        parsed = ast.parse(f.read())
    type_coverage = TypeCoverage(file)
    type_coverage.visit(parsed)
    if type_coverage.typed_lines > 0:
        assert_from_future_annotations(parsed, file)
    return type_coverage


def cover_directory(directory: Path) -> TypeCoverage:
    type_coverage = TypeCoverage(directory)
    for root, _dirs, files in os.walk(directory):
        for file in files:
            if not file.endswith(".py"):
                continue

            type_coverage += cover_file(Path(root) / file)
    return type_coverage


class TypeCoverageTests(unittest.TestCase):
    def assertFunctionCoverage(
        self, directory: Path, expected_percentage: float
    ) -> None:
        """Assert Python files under `directory` have an expected percentage of
        type-covered functions.

        Percentage is a float between 0.0% and 100.0%.

        This also fails when actual percentage is *better* than the known
        number.  That's deliberate, the goal is to tighten the expectations as
        we get better coverage in time.
        """
        coverage = cover_directory(directory)
        if coverage.all_functions > 0:
            actual_percentage = (
                100 * coverage.typed_functions / coverage.all_functions
            )
        else:
            actual_percentage = 0
        delta = abs(expected_percentage - actual_percentage)
        if delta > 0.01:  # we're allowing miniscule differences
            self.fail(
                f"Expected {expected_percentage:.2f}% of functions "
                f"under {directory} to have fully-typed signatures. Actual "
                f"value: {actual_percentage:.2f}%"
            )

    def test_cqa_type_coverage_self(self) -> None:
        coverage = cover_file(Path(__file__))
        self.assertTrue(coverage.typed_functions >= 49)
        self.assertEqual(coverage.untyped_functions, 0)
        self.assertTrue(coverage.typed_lines >= 187)
        self.assertEqual(coverage.untyped_lines, 0)

    def test_cqa_type_coverage_cli(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "cli", 0)

    def test_cqa_type_coverage_common(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "common", 24.40)

    def test_cqa_type_coverage_common_ast(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "common" / "ast", 7.14)

    def test_cqa_type_coverage_common_markup(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "common" / "markup", 0)

    def test_cqa_type_coverage_edgeql(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "edgeql", 41.21)

    def test_cqa_type_coverage_edgeql_compiler(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "edgeql" / "compiler", 100.00)

    def test_cqa_type_coverage_edgeql_parser(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "edgeql" / "parser", 0.16)

    def test_cqa_type_coverage_edgeql_pygments(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "edgeql" / "pygments", 0)

    def test_cqa_type_coverage_errors(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "errors", 0)

    def test_cqa_type_coverage_graphql(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "graphql", 0)

    def test_cqa_type_coverage_graphql_pygments(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "graphql" / "pygments", 0)

    def test_cqa_type_coverage_ir(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "ir", 42.62)

    def test_cqa_type_coverage_pgsql(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "pgsql", 41.22)

    def test_cqa_type_coverage_pgsql_compiler(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "pgsql" / "compiler", 100.00)

    def test_cqa_type_coverage_pgsql_datasources(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "pgsql" / "datasources", 48.39)

    def test_cqa_type_coverage_pgsql_dbops(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "pgsql" / "dbops", 34.48)

    def test_cqa_type_coverage_repl(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "repl", 100.0)

    def test_cqa_type_coverage_schema(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "schema", 38.85)

    def test_cqa_type_coverage_server(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "server", 7.56)

    def test_cqa_type_coverage_server_cache(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "server" / "cache", 0)

    def test_cqa_type_coverage_server_compiler(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "server" / "compiler", 14.74)

    def test_cqa_type_coverage_server_config(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "server" / "config", 18.75)

    def test_cqa_type_coverage_server_daemon(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "server" / "daemon", 0)

    def test_cqa_type_coverage_server_dbview(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "server" / "dbview", 0)

    def test_cqa_type_coverage_server_http(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "server" / "http", 0)

    def test_cqa_type_coverage_server_http_edgeql_port(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "server" / "http_edgeql_port", 0)

    def test_cqa_type_coverage_server_http_graphql_port(self) -> None:
        self.assertFunctionCoverage(
            EDB_DIR / "server" / "http_graphql_port", 0
        )

    def test_cqa_type_coverage_server_mng_port(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "server" / "mng_port", 0)

    def test_cqa_type_coverage_server_pgcon(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "server" / "pgcon", 0)

    def test_cqa_type_coverage_server_pgproto(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "server" / "pgproto", 0)

    def test_cqa_type_coverage_server_procpool(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "server" / "procpool", 5.36)

    def test_cqa_type_coverage_testbase(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "testbase", 1.04)

    def test_cqa_type_coverage_tools(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "tools", 19.52)

    def test_cqa_type_coverage_tools_docs(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "tools" / "docs", 0)

    def test_cqa_type_coverage_tools_mypy(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "tools" / "mypy", 36.36)

    def test_cqa_type_coverage_tools_test(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "tools" / "test", 4.17)

    def test_cqa_type_coverage_tests(self) -> None:
        self.assertFunctionCoverage(EDB_DIR / "tests", 0)
