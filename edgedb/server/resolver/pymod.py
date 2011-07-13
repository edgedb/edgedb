##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib

from semantix.caos import proto
from . import shell
from . import error


class PyModResolver(shell.BackendResolverHelper):
    def resolve(self, url):
        import_context = proto.ImportContext(url.path, toplevel=True)
        mod = importlib.import_module(import_context)

        handler = shell.BackendResolverHelperMeta.get('languages', mod.__language__)
        deltarepo = self.get_delta_repo(url)

        if handler:
            return handler().resolve_module(mod, deltarepo)
        else:
            err = 'unsupported Caos module language: %s' % mod.__language__
            raise error.BackendResolverError(err)
