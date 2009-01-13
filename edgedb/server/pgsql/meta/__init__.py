from semantix.caos.backends.meta import BaseMetaBackend
from semantix.caos.backends.pgsql.common import DatabaseConnection

from . import semantics, domain

class MetaBackend(BaseMetaBackend):
    def __init__(self, connection):
        super(MetaBackend, self).__init__()

        self.connection = DatabaseConnection(connection)

        self.domain_backend = domain.MetaBackendHelper(self.connection, self)
        self.semantics_backend = semantics.MetaBackendHelper(self.connection, self)

    def commit(self):
        self.connection.commit()
