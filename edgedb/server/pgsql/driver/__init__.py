##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import datetime
import uuid
from functools import partial

import postgresql
from postgresql.python.functools import process_tuple
from postgresql.driver import pq3
from postgresql import types as pg_types
from postgresql.types.io import lib as pg_types_io_lib
from postgresql.string import quote_ident
from postgresql.protocol import element3 as element

from metamagic.caos.objects import numeric, string
from metamagic.caos.backends import pool as caos_pool
from metamagic.caos import CaosError
from metamagic.utils import datetime as sx_datetime
from metamagic.utils.datastructures import Void, xvalue

from metamagic.caos.backends.pgsql import exceptions as backend_exc
from metamagic.caos.backends.pgsql.driver import io as pg_types_io

from metamagic.spin.green import postgres as green_postgres

from .. import session as pg_caos_session

from metamagic.utils import tracepoints
from .. import tracepoints as pgsql_trace



oid_to_type = {
    postgresql.types.UUIDOID: uuid.UUID,
    postgresql.types.NUMERICOID: numeric.Decimal,
    postgresql.types.TEXTOID: string.Str
}


def resolve(typid):
    return pg_types_io.resolve(typid)


class Array(pg_types.Array, collections.Container):
    def __mm_serialize__(self):
        return tuple(self)

    def __contains__(self, element):
        return element in set(self.elements())

    def __bool__(self):
        return bool(list(self))


JSON_OUTPUT_FORMAT = ('pgjson.caos', 1)
FREEFORM_RECORD_ID = '6e51108d-7440-47f7-8c65-dc4d43fd90d2'


class Json(bytes):
    def __new__(cls, value, *, format_info=JSON_OUTPUT_FORMAT):
        result = super().__new__(cls, value)
        result._fmt = format_info
        return result

    def __mm_json__(self):
        fmt = '["{}",{}]'.format(*self._fmt).encode('ascii')
        return b'{"$sxjson$":{"format":' + fmt  + b',"data":[' + self + b']}}'


class TypeIO(pq3.TypeIO):
    RECORD_OID = 2249

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
        if typid == pg_types.JSONOID:
            return self.json_io_factory()
        else:
            return super().resolve(typid, from_resolution_of, builtins, quote_ident)

    def get_session(self):
        if self._pool:
            return self._pool.get_holder(self.database)
        else:
            return None

    def array_from_parts(self, parts):
        return super().array_from_parts(parts, ArrayType=Array)

    def json_io_factory(self):
        def _unpack_json(data):
            return Json(data)
        return (None, _unpack_json, Json)

    def array_io_factory(self, pack_element, unpack_element, typoid, hasbin_input, hasbin_output,
                               array_unpack=pg_types_io_lib.array_unpack):
        pack_array, unpack_array, atype = super().array_io_factory(pack_element, unpack_element,
                                                                   typoid,
                                                                   hasbin_input, hasbin_output)

        if typoid == self.RECORD_OID:
            def unpack_array(data, array_from_parts = self.array_from_parts):
                # Override unpacking of arrays containing source node records to filter
                # out None values.  This is necessary due to the absense of support for FILTER
                # in aggregates in Postgres.  In absolute majority of calls to array_agg(<concept>),
                # None values in the resulting array are an unndesired side effect of an outer join.
                #
                flags, typoid, dims, lbs, elements = array_unpack(data)
                elements = tuple(unpack_element(x) for x in elements if x is not None)
                dims[0] = len(elements)
                return array_from_parts((elements, dims, lbs))

        return (pack_array, unpack_array, atype)

    def anon_record_io_factory(self):
        def raise_unpack_tuple_error(cause, procs, tup, itemnum):
            data = repr(tup[itemnum])
            if len(data) > 80:
                # Be sure not to fill screen with noise.
                data = data[:75] + ' ...'
            self.raise_client_error(element.ClientError((
                (b'C', '--cIO',),
                (b'S', 'ERROR',),
                (b'M', 'Could not unpack element {} from anonymous record'.format(itemnum)),
                (b'W', data,),
                (b'P', str(itemnum),)
            )), cause = cause)

        def _unpack_record(data, unpack = pg_types_io_lib.record_unpack,
                                 process_tuple = process_tuple,
                                 _Row=pg_types.Row.from_sequence):

            tinfo = 'query-unpack-record'
            with tracepoints.if_tracing(pgsql_trace.ResultUnpack, info=tinfo):

                record = list(unpack(data))
                coloids = tuple(x[0] for x in record)

                record_info = None

                try:
                    marker_oid = self.database._sx_known_record_marker_oid_
                except AttributeError:
                    pass
                else:
                    if record and record[0][0] == marker_oid:
                        session = self.get_session()
                        if session is not None:
                            recid = record[0][1].decode('ascii')
                            record_info = session.backend._get_record_info_by_id(recid)

                colio = map(self.resolve, coloids)
                column_unpack = tuple(c[1] or self.decode for c in colio)

                data = tuple(x[1] for x in record)
                data = process_tuple(column_unpack, data, raise_unpack_tuple_error)

                if record_info is not None:
                    data = dict(zip(record_info.attribute_map, data[1:]))

                    if record_info.is_xvalue:
                        assert data['value'] is not None
                        data = xvalue(data['value'], **data['attrs'])

                    elif record_info.proto_class == 'metamagic.caos.proto.Concept':
                        data = session.backend.entity_from_row(session, record_info, data)

                return data

        return (None, _unpack_record)

    def RowTypeFactory(self, attribute_map={}, _Row=pg_types.Row.from_sequence,
                       composite_relid = None):
        session = self.get_session()
        if session is not None:
            backend = session.backend
            source = backend.source_name_from_relid(composite_relid)
            if source is not None:
                return partial(backend.entity_from_row_compat, session, source, attribute_map)
        return partial(_Row, attribute_map)


class ProxyBase:
    _intercepted_attrs = ('__repr__', '__wrapped__')

    def __init__(self, obj):
        self.__wrapped__ = obj

    def __getattribute__(self, name):
        if name in object.__getattribute__(self, '_intercepted_attrs'):
            return object.__getattribute__(self, name)

        wrapped = object.__getattribute__(self, '__wrapped__')
        return getattr(wrapped, name)

    def __setattr__(self, name, value):
        if name == '__wrapped__':
            return object.__setattr__(self, name, value)

        wrapped = object.__getattribute__(self, '__wrapped__')
        return setattr(wrapped, name, value)

    def __call__(self, *args, **kwargs):
        return self.__wrapped__(*args, **kwargs)


class InstructionProxy(ProxyBase):
    _intercepted_attrs = ProxyBase._intercepted_attrs + ('__next__',)

    def __next__(self):
        tinfo = 'query-fetch-chunk: {}'.format(self.statement.statement_id)
        with tracepoints.if_tracing(pgsql_trace.Query,
                                    info=tinfo, id=self.statement.statement_id):
            return self.__wrapped__.__next__()

    def __iter__(self):
        return self


class CursorProxy(ProxyBase):
    _intercepted_attrs = ProxyBase._intercepted_attrs + ('_fetch', 'seek', 'clone')

    def clone(self, *args, **kwargs):
        result = self.__wrapped__.clone(*args, **kwargs)
        return CursorProxy(result)

    def _fetch(self, *args, **kwargs):
        tinfo = 'query-cursor-fetch: {}'.format(self.statement.statement_id)
        with tracepoints.if_tracing(pgsql_trace.Query,
                                    info=tinfo, id=self.statement.statement_id):
            return self.__wrapped__._fetch(*args, **kwargs)

    def seek(self, *args, **kwargs):
        tinfo = 'query-cursor-seek: {}'.format(self.statement.statement_id)
        with tracepoints.if_tracing(pgsql_trace.Query,
                                    info=tinfo, id=self.statement.statement_id):
            return self.__wrapped__.seek(*args, **kwargs)


class StatementProxy(ProxyBase):
    _intercepted_attrs = ProxyBase._intercepted_attrs + \
        ('declare', 'rows', '__iter__', 'chunks', 'column', 'first', '__call__', 'clone', '_fini')

    def clone(self, *args, **kwargs):
        return StatementProxy(self.__wrapped__.clone(*args, **kwargs))

    def declare(self, *args, **kwargs):
        cursor = self.__wrapped__.declare(*args, **kwargs)
        return CursorProxy(cursor)

    def rows(self, *args, **kwargs):
        return self.__wrapped__.__class__.rows(self, *args, **kwargs)

    def __iter__(self, *args, **kwargs):
        return self.__wrapped__.__class__.__iter__(self, *args, **kwargs)

    def chunks(self, *args, **kwargs):
        tinfo = 'query-fetch-chunk-init: {}'.format(self.statement_id)
        with tracepoints.if_tracing(pgsql_trace.Query, info=tinfo, id=self.statement_id):
            result = self.__wrapped__.__class__.chunks(self, *args, **kwargs)
            return InstructionProxy(result)

    def column(self, *args, **kwargs):
        return self.__wrapped__.__class__.column(self, *args, **kwargs)

    def first(self, *args, **kwargs):
        tinfo = 'query-fetch-first: {}'.format(self.statement_id)
        with tracepoints.if_tracing(pgsql_trace.Query, info=tinfo, id=self.statement_id):
            return self.__wrapped__.__class__.first(self, *args, **kwargs)

    def __call__(self, *args, **kwargs):
        tinfo = 'query-fetch-all: {}'.format(self.statement_id)
        with tracepoints.if_tracing(pgsql_trace.Query, info=tinfo, id=self.statement_id):
            return self.__wrapped__.__class__.__call__(self, *args, **kwargs)

    def _fini(self, *args, **kwargs):
        tinfo = 'query-fetch-chunk-fini: {}'.format(self.statement_id)
        with tracepoints.if_tracing(pgsql_trace.Query, info=tinfo, id=self.statement_id):
            return self.__wrapped__._fini(*args, **kwargs)


class Connection(pq3.Connection, caos_pool.Connection):
    def __init__(self, connector, pool=None):
        caos_pool.Connection.__init__(self, pool)
        pq3.Connection.__init__(self, connector)

        self._prepared_statements = {}

    def get_prepared_statement(self, query, raw=True):
        try:
            statement = self._prepared_statements[query, raw]
        except KeyError:
            try:
                with tracepoints.if_tracing(pgsql_trace.Query, info='query-prepare'):
                    if raw:
                        stmt_id = 'sx_{:x}'.format(hash(query)).replace('-', '_')
                        prefix = 'PREPARE {} AS '.format(stmt_id)
                        self.execute('{}{}'.format(prefix, query))
                        statement = self.statement_from_id(stmt_id)
                    else:
                        prefix = ''
                        statement = self.prepare(query)

                    statement = StatementProxy(statement)

            except postgresql.exceptions.Error as e:
                e.__suppress_context__ = True
                raise backend_exc.QueryError(driver_err=e, query_text=query,
                                             query_offset=len(prefix)) from e

            except Exception as e:
                raise e from None

            else:
                self._prepared_statements[query, raw] = statement

        return statement

    def connect(self):
        super().connect()
        if self._pool:
            self._pool.backend.init_connection(self)

    def reset(self, hard=False):
        if self.state in ('failed', 'negotiating', 'busy'):
            self.execute('ROLLBACK')

        if hard:
            self.execute('DISCARD ALL')
            self._prepared_statements.clear()
        else:
            self.execute("SET SESSION AUTHORIZATION DEFAULT;")
            self.execute("RESET ALL;")
            self.execute("CLOSE ALL;")
            self.execute("UNLISTEN *;")
            self.execute("SELECT pg_advisory_unlock_all();")

    def close(self):
        self._prepared_statements.clear()
        super().close()


class Driver(green_postgres.Driver):
    def __init__(self):
        super().__init__(typio=TypeIO, connection=Connection)


driver = Driver()


def connector(iri, async=False):
    return green_postgres.connector(iri, async=async, driver=driver)
