import os
import pathlib
import subprocess

from edb.testbase import server
from edb.testbase import http


class WebAssemblyTestCase(http.BaseHttpExtensionTest, server.QueryTestCase):

    @classmethod
    def get_extension_name(cls):
        return 'webassembly'

    @classmethod
    def get_extension_path(cls):
        return 'wasm'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cargo_dir = pathlib.Path(os.getcwd()) / "build" / "rust" / "wasm-test"
        cargo_home = cargo_dir / "cargo_home"

        # in CI (github actions) we expect example to be already built
        # (and cached)
        if not os.environ.get('CI'):
            subprocess.check_call(
                [
                    "cargo", "build",
                    "--package=edgedb-wasm-test1",
                    "--target=wasm32-wasi",
                    "--release",
                ],
                env=os.environ | {
                    "CARGO_TARGET_DIR": cargo_dir,
                    "CARGO_HOME": cargo_home,
                })

    def test_wasm_query(self):
        with self.http_con() as con:
            data, headers, status = self.http_con_request(
                con,
                "",
                path="edgedb-wasm-test1/any-route",
            )
            self.assertEqual(status, 200)
            self.assertEqual(data, b"7 times 8 equals 56")
