##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.server.datasources.base import Datasource, DatasourceError


class YamlDatasourceError(DatasourceError):
    pass


class Yaml(Datasource):
    def fetch(self, **params):
        return self.descriptor['source']
