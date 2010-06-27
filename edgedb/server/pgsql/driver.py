##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import uuid
from functools import partial

import postgresql
from postgresql.driver import pq3
from postgresql import types as pg_types
from postgresql.string import quote_ident
from postgresql.python.functools import Composition as compose

from semantix.caos.objects.datetime import TimeDelta, DateTime, Time
from semantix.caos.objects import numeric

from semantix.caos.backends import pool as caos_pool

from . import session as pg_caos_session


def interval_pack(x):
    months, days, seconds, microseconds = x.to_months_days_seconds_microseconds()
    return (months, days, (seconds, microseconds))

def interval_unpack(mds):
    months, days, (seconds, microseconds) = mds
    return TimeDelta(months=months, days=days, seconds=seconds, microseconds=microseconds)


oid_to_io = {
    postgresql.types.INTERVALOID: (
        compose((interval_pack, postgresql.types.io.lib.interval64_pack)),
        compose((postgresql.types.io.lib.interval64_unpack, interval_unpack))
    )
}

oid_to_type = {
    postgresql.types.UUIDOID: uuid.UUID,
    postgresql.types.TIMESTAMPTZOID: DateTime,
    postgresql.types.TIMEOID: Time,
    postgresql.types.NUMERICOID: numeric.Decimal,
    postgresql.types.INTERVALOID: TimeDelta
}


def resolve(typid):
    return oid_to_io.get(typid) or postgresql.types.io.resolve(typid)


class TypeIO(pq3.TypeIO):
    def __init__(self, database):
        super().__init__(database)
        self._pool = database._pool

    def resolve(self, typid, from_resolution_of=(), builtins=resolve, quote_ident=quote_ident):
        return super().resolve(typid, from_resolution_of, builtins, quote_ident)

    def RowTypeFactory(self, attribute_map={}, _Row=pg_types.Row.from_sequence,
                       composite_relid = None):

        if self._pool:
            session = self._pool.get_holder(self.database)
        else:
            session = None

        if session:
            backend = session.realm.backend('data')
            source = backend.source_name_from_relid(composite_relid)
            if source:
                return partial(backend.entity_from_row, session, source, attribute_map)
        return partial(_Row, attribute_map)


class Connection(pq3.Connection, caos_pool.Connection):
    def __init__(self, connector, pool=None):
        caos_pool.Connection.__init__(self, pool)
        pq3.Connection.__init__(self, connector)

    def reset(self):
        if self.state in ('failed', 'negotiating', 'busy'):
            self.execute('ROLLBACK')
        self.execute("SET SESSION AUTHORIZATION DEFAULT;")
        self.execute("CLOSE ALL;")
        self.execute("RESET ALL;")
        self.execute("UNLISTEN *;")
        self.execute("SELECT pg_advisory_unlock_all();")
        self.execute("DISCARD PLANS;")
        self.execute("DISCARD TEMP;")


class SocketConnector:
    pass


class IP4(pq3.IP4, SocketConnector):
    pass


class IP6(pq3.IP6, SocketConnector):
    pass


class Host(pq3.Host, SocketConnector):
    pass


class Unix(pq3.Unix, SocketConnector):
    pass


class Driver(pq3.Driver):
    def __init__(self):
        super().__init__(typio=TypeIO, connection=Connection)

    def ip4(self, **kw):
        return IP4(driver = self, **kw)

    def ip6(self, **kw):
        return IP6(driver = self, **kw)

    def host(self, **kw):
        return Host(driver = self, **kw)

    def unix(self, **kw):
        return Unix(driver = self, **kw)


driver = Driver()


def connector(iri):
    params = postgresql.iri.parse(iri)
    settings = params.setdefault('settings', {})
    settings['standard_conforming_strings'] = 'on'
    return driver.fit(**params)
