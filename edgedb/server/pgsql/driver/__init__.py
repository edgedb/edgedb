##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import datetime
import uuid
from functools import partial

import postgresql
from postgresql.python import socket as pg_socket
from postgresql.driver import pq3
from postgresql import types as pg_types
from postgresql.string import quote_ident

from semantix.caos.objects import numeric, string
from semantix.caos.backends import pool as caos_pool
from semantix.caos import CaosError
from semantix.utils import datetime as sx_datetime

from semantix.caos.backends.pgsql.driver import io as pg_types_io

_GREEN_SOCKET = False
try:
    from semantix.spin.green import socket as green_socket
    _GREEN_SOCKET = True
except ImportError:
    pass


from .. import session as pg_caos_session


oid_to_type = {
    postgresql.types.UUIDOID: uuid.UUID,
    postgresql.types.NUMERICOID: numeric.Decimal,
    postgresql.types.TEXTOID: string.Str
}


def resolve(typid):
    return pg_types_io.resolve(typid)


class TypeIO(pq3.TypeIO):
    def __init__(self, database):
        super().__init__(database)
        self._pool = database._pool

    def identify(self, **identity_mappings):
        id = list(identity_mappings.items())
        ios = [pg_types_io.resolve(x[0]) for x in id]
        oids = list(self.database.sys.regtypes([x[1] for x in id]))

        update = [
            (oid, io if io.__class__ is tuple else io(oid, self))
            for oid, io in zip(oids, ios)
        ]
        self._cache.update(update)

    def resolve(self, typid, from_resolution_of=(), builtins=resolve, quote_ident=quote_ident):
        return super().resolve(typid, from_resolution_of, builtins, quote_ident)

    def get_session(self):
        if self._pool:
            return self._pool.get_holder(self.database)
        else:
            return None

    def RowTypeFactory(self, attribute_map={}, _Row=pg_types.Row.from_sequence,
                       composite_relid = None):

        session = self.get_session()
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
    @staticmethod
    def msghook(msg):
        if msg.message == '=> is deprecated as an operator name':
            return True

    def create_socket_factory(self, **params):
        params['socket_extra'] = {'async': self.async}
        return SocketFactory(**params)

    def __init__(self, async=False):
        self.async = async


class IPConnector(pq3.IPConnector, SocketConnector):
    def __init__(self, host, port, ipv, async=False, **kw):
        pq3.IPConnector.__init__(self, host, port, ipv, **kw)
        SocketConnector.__init__(self, async=async)


class IP4(IPConnector, pq3.IP4):
    pass


class IP6(IPConnector, pq3.IP6):
    pass


class Host(SocketConnector, pq3.Host):
    def __init__(self, host=None, port=None, ipv=None, address_family=None, async=False, **kw):
        pq3.Host.__init__(self, host=host, port=port, ipv=ipv, address_family=address_family, **kw)
        SocketConnector.__init__(self, async=async)


class Unix(SocketConnector, pq3.Unix):
    def __init__(self, unix=None, async=False, **kw):
        pq3.Unix.__init__(self, unix=unix, **kw)
        SocketConnector.__init__(self, async=async)


class SocketFactory(pg_socket.SocketFactory):
    def __init__(self, socket_create, socket_connect, socket_secure=None, socket_extra=None):
        super().__init__(socket_create, socket_connect, socket_secure)
        self.async = socket_extra.get('async', False) if socket_extra else False

    def __call__(self, timeout = None):
        if self.async:
            if not _GREEN_SOCKET:
                raise CaosError('missing green socket implementation, '
                                'unable to start async session')

            s = green_socket.Socket(*self.socket_create)

            timeout = float(timeout) if timeout is not None else None
            if timeout != 0:
                s.settimeout(timeout)

            s.connect(self.socket_connect)
            s.settimeout(None)
            return s

        else:
            return super().__call__(timeout)


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


def connector(iri, async=False):
    params = postgresql.iri.parse(iri)
    settings = params.setdefault('settings', {})
    settings['standard_conforming_strings'] = 'on'
    params['async'] = async
    return driver.fit(**params)
