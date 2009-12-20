from semantix.caos.backends.resolver.shell import BackendShell, BackendResolverHelper
from semantix.caos.backends.resolver.error import BackendResolverError
from semantix.caos.backends.yaml import Backend

class BackendResolver(BackendResolverHelper):
    def resolve(self, url):
        if url.scheme != 'file' or url.netloc:
            raise BackendResolverError('loading schemas from sources other than local files is not supported: %' % url)

        return BackendShell(backend_class=Backend, source_path=url.path)