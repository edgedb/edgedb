##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import time

from metamagic.caos.error import CaosError
from metamagic.utils import config

from metamagic.spin.pools import connection as connection_pool


class PoolError(CaosError):
    pass


class ConnectionOvercommitError(PoolError):
    pass


class ConnectionPool(connection_pool.LimitedPool):
    max_hold_time = config.cvalue(default=0,
                                  doc=("Maximum time in seconds a connection can be held before the"
                                       " pool will be allowed to forcibly recover the connection."
                                       " Set to 0 to disable forced connection recovery."))

    max_connections = config.cvalue(default=5, doc="maximum number of connections")

    def __init__(self):
        super().__init__()
        self._connection_records = {}

    def get_limits(self):
        return 0, self.__class__.max_connections

    def recover_item(self):
        for connection, taken_at in self._connection_records.items():
            if (time.time() - taken_at) > self.__class__.max_hold_time:
                # The current holder has exceeded the maximum allowed time to
                # hold the connection, so the pool needs to forcibly recover the
                # connection.
                #
                connection.close()
                self.remove_item(connection)
                self._connection_records.pop(connection)
                connection = self.create()
                break

        return connection

    def __call__(self, requestor):
        connection = super().__call__(requestor)

        if not connection:
            msg = 'no free connections available'
            details = 'The maximum number of allowed connections is exceeded'
            raise ConnectionOvercommitError(msg, details=details)

        self._connection_records[connection] = time.time()
        return connection

    def remove_item(self, item):
        super().remove_item(item)
        self._connection_records.pop(item)


class Connection:
    def __init__(self, pool):
        self._pool = pool

    def release(self):
        self._pool.recycle(self)
