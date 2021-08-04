#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import asyncio
import sys
import unittest

from edb.common import devmode
from . import server

exec(sys.argv[1], globals(), locals())


class ProcTest(server.TestCase):
    def notify_parent(self, mark):
        print(str(mark), flush=True)

    async def wait_for_parent(self, mark):
        self.assertEqual(
            (await self.stdin.readline()).strip(),
            str(mark).encode(),
        )

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.stdin = asyncio.StreamReader()
        cls.loop.run_until_complete(
            cls.loop.connect_read_pipe(
                lambda: asyncio.StreamReaderProtocol(cls.stdin),
                sys.stdin.buffer,
            )
        )

    exec(sys.argv[2], globals(), locals())


def main():
    cov_config = devmode.CoverageConfig.from_environ()
    if cov_config:
        cov = cov_config.new_coverage_object()
        cov.start()
        try:
            unittest.main(argv=sys.argv[:1], verbosity=2)
        finally:
            cov.stop()
            cov.save()
    else:
        unittest.main(argv=sys.argv[:1], verbosity=2)


if __name__ == "__main__":
    main()
