##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import lang
from semantix.caos.backends.resolver import shell
from semantix.caos.backends.yaml import Backend


class BackendResolver(shell.BackendResolverDataHelper, shell.BackendResolverModuleHelper):
    data_mime_types = ('application/x-yaml',)
    languages = (lang.yaml.Language,)

    def resolve_module(self, module):
        return shell.BackendShell(backend_class=Backend, module=module)

    def resolve_data(self, data):
        return shell.BackendShell(backend_class=Backend, data=data)
