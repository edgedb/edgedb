from semantix.caos.backends.resolver.shell import BackendShell, BackendResolverHelper
from semantix.caos.backends.dummy import Backend

class BackendResolver(BackendResolverHelper):
    def resolve(self, url):
        return BackendShell(backend_class=Backend)
