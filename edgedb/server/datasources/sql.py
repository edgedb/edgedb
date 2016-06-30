##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re

from edgedb.server.datasources.base import Datasource, DatasourceError


class SqlDatasourceError(DatasourceError):
    pass


class Sql(Datasource):
    def __init__(self, connection):
        super().__init__()
        self.connection = connection

    def _convert_query(self, query, params):
        pmap = {}
        plist = []

        def _repl(match):
            argname = match.group(1)
            try:
                argnum = pmap[argname]
            except KeyError:
                argnum = len(pmap)
                pmap[argname] = argnum
                plist.append(params[argname])

            return '${:d}'.format(argnum + 1)

        return re.sub(r'%\(([a-zA-Z]\w*)\)\w', _repl, query), plist

    async def fetch(self, **params):
        params, extra_filters = self._filter_params(params)
        query = self.descriptor['source']
        query, plist = self._convert_query(query, params)
        ps = await self.connection.prepare(query)
        return await ps.get_list(*plist)
