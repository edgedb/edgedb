##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos.datasources.base import Datasource, DatasourceError

class YamlDatasourceError(DatasourceError): pass
class Yaml(Datasource):
    def fetch(self, **params):
        return self.descriptor['source']
