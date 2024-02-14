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
import socket
import sys
import unittest

from edb.common import devmode
from . import server

exec(sys.argv[1], globals(), locals())


class ProcTest(server.TestCase):
    def notify_parent(self, mark):
        self.parent_writer.write(str(mark).encode() + b"\n")

    async def wait_for_parent(self, mark):
        self.assertEqual(
            (await self.parent_reader.readline()).strip(),
            str(mark).encode(),
        )

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        async def _setup():
            sock = socket.fromfd(
                int(sys.argv[3]), socket.AF_UNIX, socket.SOCK_STREAM
            )
            cls.parent_reader, cls.parent_writer = (
                await asyncio.open_connection(sock=sock)
            )
        cls.loop.run_until_complete(_setup())

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
