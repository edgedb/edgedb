from semantix.utils import urlparse
from semantix.caos.backends.resolver.error import BackendResolverError
from semantix.caos.backends.yaml.resolver import BackendResolver as YamlBackendResolver
from semantix.caos.backends.pgsql.resolver import BackendResolver as PgsqlBackendResolver
from semantix.caos.backends.dummy.resolver import BackendResolver as DummyBackendResolver

class BackendResolver(object):
    def __init__(self):
        self.cache = {}

    def resolve(self, url):
        obj = self.cache.get(url, None)
        if not obj:
            self.cache[url] = self.do_resolve(url)
            obj = self.cache[url]

        return obj.instantiate()

    def do_resolve(self, url):
        parsed_url = urlparse.urlparse(url)

        if parsed_url[0] == 'yaml':
            resolver = YamlBackendResolver()
        elif parsed_url[0] == 'pq':
            resolver = PgsqlBackendResolver()
        elif parsed_url[0] == 'dummy':
            resolver = DummyBackendResolver()
        else:
            raise BackendResolverError('unsupported source protocol: %' % type)

        return resolver.resolve(parsed_url[1])
