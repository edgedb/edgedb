#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


import shutil
import tempfile

from edb.server import _testbase as tb
from edb.server import ctl
from edb.server import cluster as edgedb_cluster


class TestCtl(tb.TestCase):
    def test_ctl_init_1(self):
        data_dir = tempfile.mkdtemp(prefix='edgedbtest-')
        conn = cluster = None

        try:
            env = {'EDGEDB_LOG_LEVEL': 'silent'}

            ctl.main(['-D', data_dir, 'init'], env=env)

            cluster = edgedb_cluster.Cluster(
                data_dir, port='dynamic', env=env)
            cluster_status = cluster.get_status()

            self.assertEqual(cluster_status, 'stopped')

            cluster.start()

            cluster_status = cluster.get_status()

            self.assertEqual(cluster_status, 'running')

        finally:
            if conn is not None:
                conn.close()

            if cluster is not None:
                cluster.stop()
                cluster.destroy()

            shutil.rmtree(data_dir, ignore_errors=True)
