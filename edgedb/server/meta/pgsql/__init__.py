from semantix.lib.caos.backends.meta.base import BaseMetaBackend
from semantix.lib.caos.backends.meta.pgsql import semantics, domain


class MetaBackend(BaseMetaBackend):
    def __init__(self, connection):
        super(MetaBackend, self).__init__()

        self.cursor = connection.cursor()

        self.semantics_backend = semantics.MetaBackendHelper(connection, self)
        self.domain_backend = domain.MetaBackendHelper(connection, self)

    def commit(self):
        self.cursor.connection.commit()
