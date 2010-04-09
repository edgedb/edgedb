##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib
from semantix.utils import lang
from semantix.caos.backends.resolver import shell, BackendResolverError
from semantix.caos.backends.yaml import Backend

from .deltarepo import MetaDeltaRepository


class BackendResolver(shell.BackendResolverDataHelper, shell.BackendResolverModuleHelper):
    data_mime_types = ('application/x-yaml',)
    languages = (lang.yaml.Language,)

    def resolve_module(self, module, delta_repo_class_name):
        if delta_repo_class_name:
            try:
                mod, _, name = delta_repo_class_name.rpartition('.')
                delta_repo_class = getattr(importlib.import_module(mod), name)
            except (ImportError, AttributeError) as e:
                raise BackendResolverError('could not find delta repo class %s' % \
                                           delta_repo_class_name) from e
        else:
            delta_repo_class = MetaDeltaRepository

        return shell.BackendShell(backend_class=Backend, delta_repo_class=delta_repo_class,
                                  module=module)

    def resolve_data(self, data):
        return shell.BackendShell(backend_class=Backend, delta_repo_class=MetaDeltaRepository,
                                  data=data)
