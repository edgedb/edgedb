##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from importkit.import_ import utils as imp_utils

from . import shell
from . import error


class PyModResolver(shell.BackendResolverHelper):
    def resolve(self, url):
        mod = imp_utils.import_module(url.path)

        handler = shell.BackendResolverHelperMeta.get('languages', mod.__language__)
        deltarepo = self.get_delta_repo(url)

        if handler:
            return handler().resolve_module(mod, deltarepo)
        else:
            err = 'unsupported Caos module language: %s' % mod.__language__
            raise error.BackendResolverError(err)
