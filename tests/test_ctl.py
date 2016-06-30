##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import shutil
import tempfile

from edgedb.server import _testbase as tb
from edgedb.server import ctl
from edgedb.server import cluster as edgedb_cluster


class TestCtl(tb.TestCase):
    async def test_ctl_init_1(self):
        data_dir = tempfile.mkdtemp(prefix='edgedbtest-')
        conn = cluster = None

        try:
            ctl.main([
                '-D', data_dir,
                'init'
            ])

            cluster = edgedb_cluster.Cluster(data_dir=data_dir,
                                             port='dynamic')
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
