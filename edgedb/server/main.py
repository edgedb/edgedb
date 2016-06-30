##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import argparse
import asyncio
import signal
import sys

import importkit

from asyncpg import cluster as pg_cluster

from . import cluster as edgedb_cluster


def abort(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


def terminate_server(server, loop):
    loop.stop()


def init_import_system():
    importkit.install()
    importkit.register_package('edgedb')

    # We need YAML language to import datasources
    from importkit import yaml


def run_server(cluster, port):
    loop = asyncio.get_event_loop()
    srv = None

    from edgedb.server import protocol as edgedb_protocol

    def protocol_factory():
        return edgedb_protocol.Protocol(cluster, loop=loop)

    try:
        srv = loop.run_until_complete(loop.create_server(
            protocol_factory, host='localhost', port=port))

        loop.add_signal_handler(signal.SIGTERM, terminate_server, srv, loop)
        loop.run_forever()

    finally:
        if srv is not None:
            srv.close()


def main(argv=sys.argv[1:]):
    init_import_system()

    from edgedb.server import defines as edgedb_defines

    parser = argparse.ArgumentParser(description='EdgeDB Server')
    backend_info = parser.add_mutually_exclusive_group(required=True)
    backend_info.add_argument('-D', '--data-dir', type=str,
                              help='database cluster directory')
    backend_info.add_argument('-P', '--postgres', type=str,
                              help='address of Postgres backend server')
    parser.add_argument('-p', '--port', type=int,
                        default=edgedb_defines.EDGEDB_PORT,
                        help='port to listen on')

    args = parser.parse_args(argv)
    pg_cluster_started_by_us = False

    if args.data_dir:
        cluster = pg_cluster.Cluster(data_dir=args.data_dir)
        cluster_status = cluster.get_status()
        if cluster_status == 'not-initialized':
            abort('there is no valid EdgeDB cluster in {}'.format(
                args.data_dir))
        elif cluster_status == 'stopped':
            cluster.start(
                port=edgedb_cluster.find_available_port(),
                server_settings={
                    'log_connections': 'yes',
                    'log_statement': 'all',
                    'log_disconnections': 'yes',
                    'log_min_messages': 'INFO',
                },
            )
            pg_cluster_started_by_us = True
    else:
        cluster = pg_cluster.RunningCluster(host=args.postgres)

    run_server(cluster, args.port)

    if pg_cluster_started_by_us:
        cluster.stop()


if __name__ == '__main__':
    main()
