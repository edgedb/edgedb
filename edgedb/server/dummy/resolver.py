##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.caos.backends.resolver.shell import BackendShell, BackendResolverHelper
from semantix.caos.backends.dummy import Backend

from .deltarepo import MetaDeltaRepository

class BackendResolver(BackendResolverHelper):
    def resolve(self, url):
        return BackendShell(backend_class=Backend, delta_repo_class=MetaDeltaRepository)
