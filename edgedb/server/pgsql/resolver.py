##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import postgresql
import urllib.parse

from semantix.caos.backends.resolver.shell import BackendShell, BackendResolverHelper
from semantix.caos.backends.resolver.error import BackendResolverError
from semantix.caos.backends.pgsql.backend import Backend

class BackendResolver(BackendResolverHelper):
    def resolve(self, url):
        url = urllib.parse.urlunsplit(url)
        try:
            connection = postgresql.open(url)
        except postgresql.exceptions.ClientCannotConnectError as e:
            raise BackendResolverError(msg='could not connect to caos backend at %s' % url,
                                       hint='check that the database server is running, is accessible and '\
                                            'that the database exists and is accessible ',
                                       details=str(e)) from e

        return BackendShell(backend_class=Backend, connection=connection)
