##
# Copyright (c) 2008-2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Common test harness for PostgreSQL-related functionality"""

import tempfile

import postgresql
import postgresql.iri
import postgresql.string
import postgresql.python

from postgresql import installation as pg_installation
from postgresql import cluster as pg_cluster
import postgresql.driver.dbapi20 as pg_driver

from metamagic.test.funcarg import FunctionArgument
from metamagic.utils import config

from . import Config


class PGMockCluster:
    def __init__(self, uri):
        self.uri = uri


class PGCluster(FunctionArgument):
    scope = 'global'

    def setup(self):
        if Config.test_server_uri:
            self.value = PGMockCluster(Config.test_server_uri)
            return

        config = pg_installation.pg_config_dictionary(Config.get_pg_config_path())
        install = pg_installation.Installation(config)
        tempdir = tempfile.mkdtemp(dir='/tmp', prefix='metamagic-pgsql-data')
        cluster = None

        try:
            self.value = cluster = pg_cluster.Cluster(install, tempdir)
            port = str(postgresql.python.socket.find_available_port())
            if cluster.initialized():
                cluster.drop()
            cluster.init(user='test', encoding='utf-8')
            cluster.settings.update(dict(
                listen_addresses='localhost',
                port=port,
                log_destination='stderr',
                log_min_messages='FATAL',
                unix_socket_directory=tempdir,
            ))
            cluster.start()
            cluster.wait_until_started(timeout=20)
            c = cluster.connection(user='test', database='template1')
            with c:
                c.execute('CREATE DATABASE test')
        except:
            if cluster:
                self.teardown()
            raise

    def teardown(self):
        cluster = self.value
        if not cluster or isinstance(cluster, PGMockCluster):
            return

        try:
            c = cluster.connection(user='test')
            with c:
                vi = c.version_info[:2]
                if vi >= (9, 2):
                    c.sys.terminate_backends_92()
                else:
                    c.sys.terminate_backends()
        except postgresql.exceptions.ConnectionError:
            pass

        cluster.stop()
        cluster.wait_until_stopped(timeout=20)
        cluster.drop()


class Database(FunctionArgument):
    scope = 'class'
    name = Config.test_database_name

    def get_cluster_uri(self, cluster):
        if isinstance(cluster, PGMockCluster):
            return cluster.uri
        else:
            host, port = cluster.address()
            server_uri = 'pq://{user}@{host}:{port}'.format(user='test', host=host, port=port)
            return server_uri

    def setup(self):
        cluster = self.space.get_funcarg('cluster', PGCluster).value
        server_uri = self.get_cluster_uri(cluster)
        data_url = '{}/{}'.format(server_uri, self.name)

        options = postgresql.iri.parse(data_url)

        admin_connection = pg_driver.connect(database='postgres',
                                             user=options.get('user'),
                                             host=options.get('host'),
                                             port=options.get('port'))

        admin_connection.autocommit = False
        admin_connection._xact.abort()
        cursor = admin_connection.cursor()
        quote = postgresql.string.quote_ident

        try:
            cursor.execute('CREATE DATABASE {}'.format(quote(self.name)))
        except postgresql.exceptions.DuplicateDatabaseError:
            pass

        admin_connection.close()

        self.data_url = data_url
        self.value = self

    def teardown(self):
        self.data_url = None


class DatabaseConnection(FunctionArgument):
    scope = 'class'
    database_funcarg_cls = Database

    def setup(self):
        data_url = self.space.get_funcarg('db', self.database_funcarg_cls).data_url
        connection = postgresql.open(data_url)

        self.cleanup_before_use(connection)
        self.value = connection
        self.data_url = data_url

        return connection

    def teardown(self):
        connection = self.value
        self.cleanup_after_use(connection)
        connection.close()
        self.value = None
        self.data_url = None

    def cleanup_before_use(self, connection):
        pass

    def cleanup_after_use(self, connection):
        pass


class GlobalDatabase(Database):
    scope = 'global'


class GlobalDatabaseConnection(DatabaseConnection):
    scope = 'global'
    database_funcarg_cls = GlobalDatabase
