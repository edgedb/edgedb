import postgresql.iri
import postgresql.driver.dbapi20 as pg_driver
import urllib.parse

from semantix.caos.backends.resolver.shell import BackendShell, BackendResolverHelper
from semantix.caos.backends.resolver.error import BackendResolverError
from semantix.caos.backends.pgsql import Backend

class BackendResolver(BackendResolverHelper):
    def resolve(self, url):
        url = urllib.parse.urlunsplit(url)
        params = postgresql.iri.parse(url)
        connection = pg_driver.connect(**params)
        return BackendShell(backend_class=Backend, connection=connection)
