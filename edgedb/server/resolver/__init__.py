##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils import url as url_module
from metamagic.caos.backends.resolver.error import BackendResolverError
from metamagic.caos.backends.yaml.resolver import BackendResolver as YamlBackendResolver
from metamagic.caos.backends.pgsql.resolver import BackendResolver as PgsqlBackendResolver
from metamagic.caos.backends.dummy.resolver import BackendResolver as DummyBackendResolver
from metamagic.caos.backends.resolver.pymod import PyModResolver
from metamagic.caos.backends.resolver.data import DataResolver


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
        parsed_url = url_module.parse(url)

        if parsed_url[0] == 'pq':
            resolver = PgsqlBackendResolver()
        elif parsed_url[0] == 'dummy':
            resolver = DummyBackendResolver()
        elif parsed_url[0] == 'pymod':
            resolver = PyModResolver()
        elif parsed_url[0] == 'data':
            resolver = DataResolver()
        else:
            raise BackendResolverError('unsupported source protocol: %s' % parsed_url[0])

        return resolver.resolve(parsed_url[1])
