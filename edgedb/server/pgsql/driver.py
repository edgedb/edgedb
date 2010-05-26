##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import uuid

import postgresql
from postgresql.driver import pq3
from postgresql.string import quote_ident
from postgresql.python.functools import Composition as compose

from semantix.caos.objects.datetime import TimeDelta, DateTime


def interval_pack(x):
    return (0, x.days, (x.seconds, x.microseconds))

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
    postgresql.types.TIMESTAMPTZOID: DateTime
}


def resolve(typid):
    return oid_to_io.get(typid) or postgresql.types.io.resolve(typid)


class TypeIO(pq3.TypeIO):
    def __init__(self, database):
        super().__init__(database)

    def resolve(self, typid, from_resolution_of=(), builtins=resolve, quote_ident=quote_ident):
        return super().resolve(typid, from_resolution_of, builtins, quote_ident)


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
