##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class BackendResolverHelper(object):
    def resolve(self, url):
        pass


class BackendShell(object):
    def __init__(self, backend_class, **kwargs):
        self.backend_class = backend_class
        self.args = kwargs

    def instantiate(self):
        return self.backend_class(**self.args)
