##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.server import cluster as edgedb_cluster


def main(args, env):
    """Initialize EdgeDB database cluster."""
    if args.data_dir:
        cluster = edgedb_cluster.Cluster(
            args.data_dir, port='dynamic', env=env)
        cluster.init()
