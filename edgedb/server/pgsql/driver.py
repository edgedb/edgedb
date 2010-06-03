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
    postgresql.types.NUMERICOID: numeric.Decimal
}


def resolve(typid):
    return oid_to_io.get(typid) or postgresql.types.io.resolve(typid)


class TypeIO(pq3.TypeIO):
    def __init__(self, database):
        super().__init__(database)

    def resolve(self, typid, from_resolution_of=(), builtins=resolve, quote_ident=quote_ident):
        return super().resolve(typid, from_resolution_of, builtins, quote_ident)

    def RowTypeFactory(self, attribute_map={}, _Row=pg_types.Row.from_sequence,
                       composite_relid = None):

        session = pg_caos_session.Session.from_connection(self.database)
        if session:
            backend = session.realm.backend('data')
            source = backend.source_name_from_relid(composite_relid)
            if source:
                return partial(backend.entity_from_row, session, source, attribute_map)
        return partial(_Row, attribute_map)


class Driver(pq3.Driver):
    def __init__(self):
        super().__init__(typio=TypeIO)

driver = Driver()


def connect(iri):
    params = postgresql.iri.parse(iri)
    settings = params.setdefault('settings', {})
    settings['standard_conforming_strings'] = 'on'
    connection = driver.fit(**params)()
    connection.connect()

    return connection
