##
# Copyright (c) 2010, 2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.server import pool


class ConnectionPool(pool.ConnectionPool):
    def __init__(self, connector, backend):
        super().__init__()
        self.connector = connector
        self.backend = backend

    def create(self):
        c = self.connector(pool=self)
        c.connect()
        return c

    def recycle(self, connection):
        try:
            connection.reset()
        except Exception:
            # Failure to reset the connection for any reason means we should try to close it
            # and forget about it.
            self.remove(connection)
            raise
        else:
            super().recycle(connection)

    def remove(self, connection):
        try:
            connection.close()
        finally:
            super().remove(connection)
