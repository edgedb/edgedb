##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .error import BackendResolverError


class BackendResolverHelper:
    def get_delta_repo(self, url):
        if url.query:
            deltarepo = url.query.get('deltarepo')
            if deltarepo:
                deltarepo = deltarepo[0]
        else:
            deltarepo = None

        return deltarepo

    def resolve(self, url):
        pass


class BackendResolverHelperMeta(type):
    index = {}

    def __init__(cls, name, bases, clsdict, *, indexkey=None):
        super().__init__(name, bases, clsdict)

        if indexkey:
            for indexkey, indextitle in indexkey.items():
                index = type(cls).index.get(indexkey)
                if not index:
                    type(cls).index[indexkey] = index = {}

                handles = clsdict.get(indexkey)
                if handles:
                    for handle in handles:
                        existing = index.get(handle)
                        if existing:
                            raise BackendResolverError('%s %s is already handled by %s' % \
                                                       (indextitle, handle, existing))
                        index[handle] = cls

    @classmethod
    def get(mcls, key, value):
        return mcls.index[key].get(value)


class BackendResolverModuleHelperMeta(BackendResolverHelperMeta):
    def __init__(cls, name, bases, clsdict, *, indexkey=None):
        if indexkey:
            indexkey['languages'] = 'Language'
        else:
            indexkey = {'languages': 'Language'}
        super().__init__(name, bases, clsdict, indexkey=indexkey)


class BackendResolverModuleHelper(metaclass=BackendResolverModuleHelperMeta):
    def resolve_module(self, module):
        raise NotImplementedError


class BackendResolverDataHelperMeta(BackendResolverModuleHelperMeta):
    def __init__(cls, name, bases, clsdict):
        super().__init__(name, bases, clsdict, indexkey={'data_mime_types': 'MIME type'})


class BackendResolverDataHelper(metaclass=BackendResolverDataHelperMeta):
    def resolve_data(self, data):
        raise NotImplementedError



class BackendShell(object):
    def __init__(self, backend_class, delta_repo_class, **kwargs):
        self.backend_class = backend_class
        self.delta_repo_class = delta_repo_class
        self.args = kwargs

    def instantiate(self):
        return self.backend_class(deltarepo=self.delta_repo_class, **self.args)
