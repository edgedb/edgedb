from semantix.lib.caos.backends.meta.base import BaseMetaBackend
from semantix.lib.caos.backends.meta.pgsql import semantics, domain
from .common import DatabaseConnection

class MetaBackend(BaseMetaBackend):
    def __init__(self, connection):
        super(MetaBackend, self).__init__()

        self.connection = DatabaseConnection(connection)

        self.domain_backend = domain.MetaBackendHelper(self.connection, self)
        self.semantics_backend = semantics.MetaBackendHelper(self.connection, self)

    def commit(self):
        self.connection.commit()
