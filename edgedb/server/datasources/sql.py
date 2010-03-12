##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from postgresql.driver.dbapi20 import Cursor as CompatCursor
from semantix.caos.datasources.base import Datasource, DatasourceError


class SqlDatasourceError(DatasourceError): pass


class Sql(Datasource):
    def __init__(self, connection):
        super().__init__()

        self.connection = connection

        self.cache = {}
        self.caching = 'cache' in self.descriptor and self.descriptor['cache'] == 'memory'

    def fetch(self, **params):
        cursor = CompatCursor(self.connection)
        params = self._filter_params(params)

        key = None
        if self.caching:
            key = str(params)
            if key in self.cache:
                return self.cache[key]

        query = self.descriptor['source']

        query, pxf, nparams = cursor._convert_query(query)
        ps = self.connection.prepare(query)
        if params:
            rows = ps(*pxf(params))
        else:
            rows = ps()

        if 'filters' in self.descriptor and self.descriptor['filters'] is not None:
            for filter in self.descriptor['filters']:
                if 'format' in filter:
                    if filter['format'] == 'dict':
                        rows = [dict((key, row[key]) for key in row.keys()) for row in rows]

        if self.caching:
            self.cache[key] = rows

        return rows
