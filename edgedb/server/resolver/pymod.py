##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.lang.import_ import utils as imp_utils
from semantix.utils.lang.protoschema.loader import ProtoSchemaModuleLoader

from . import shell
from . import error


class PyModResolver(shell.BackendResolverHelper):
    def resolve(self, url):
        mod = imp_utils.import_module(url.path, loader=ProtoSchemaModuleLoader)

        handler = shell.BackendResolverHelperMeta.get('languages', mod.__language__)
        deltarepo = self.get_delta_repo(url)

        if handler:
            return handler().resolve_module(mod, deltarepo)
        else:
            err = 'unsupported Caos module language: %s' % mod.__language__
            raise error.BackendResolverError(err)
