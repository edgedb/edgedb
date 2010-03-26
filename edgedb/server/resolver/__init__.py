##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import urlparse
from semantix.caos.backends.resolver.error import BackendResolverError
from semantix.caos.backends.yaml.resolver import BackendResolver as YamlBackendResolver
from semantix.caos.backends.pgsql.resolver import BackendResolver as PgsqlBackendResolver
from semantix.caos.backends.dummy.resolver import BackendResolver as DummyBackendResolver
from semantix.caos.backends.resolver.pymod import PyModResolver


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

        if parsed_url[0] == 'pq':
            resolver = PgsqlBackendResolver()
        elif parsed_url[0] == 'dummy':
            resolver = DummyBackendResolver()
        elif parsed_url[0] == 'pymod':
            resolver = PyModResolver()
        else:
            raise BackendResolverError('unsupported source protocol: %s' % parsed_url[0])

        return resolver.resolve(parsed_url[1])
