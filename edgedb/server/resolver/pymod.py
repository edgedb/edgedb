##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib

from semantix.caos import proto
from semantix.caos.backends.resolver.shell import BackendShell, BackendResolverHelper
from semantix.caos.backends.yaml import Backend as YamlBackend

from semantix.utils.lang import yaml


class PyModResolver(BackendResolverHelper):
    def resolve(self, url):
        import_context = proto.ImportContext(url.path, toplevel=True)
        mod = importlib.import_module(import_context)

        if mod._language_ is yaml.Language:
            return BackendShell(backend_class=YamlBackend, module=mod)
        else:
            raise BackendResolverHelper('unsupported Caos module language: %s' % mod._language_)
