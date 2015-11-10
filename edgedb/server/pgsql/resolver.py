##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import functools
import urllib.parse

from metamagic.caos.backends.resolver.shell import BackendShell, BackendResolverHelper
from metamagic.caos.backends.pgsql.backend import Backend

from .deltarepo import MetaDeltaRepository

from . import driver


class BackendResolver(BackendResolverHelper):
    def resolve(self, url):
        url = urllib.parse.urlunsplit(url)
        connector_factory = functools.partial(driver.connector, url)
        return BackendShell(backend_class=Backend, delta_repo_class=MetaDeltaRepository,
                            connector_factory=connector_factory)
