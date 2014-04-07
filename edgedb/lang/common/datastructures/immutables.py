##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import abc
import collections


class ImmutableMeta(type):
    def __new__(mcls, name, bases, dct):
        if '_shadowed_methods_' in dct:
            shadowed = dct['_shadowed_methods_']
            del dct['_shadowed_methods_']

            for method in shadowed:
                def meth(self, *args, _allow_mutation_=False, **kwargs):
                    if not _allow_mutation_:
                        raise TypeError('%r is immutable' % self.__class__.__name__)
                    return super()[method](*args, **kwargs)
                meth.__name__ = method

                dct[method] = meth

        return super().__new__(mcls, name, bases, dct)


class frozendict(dict, metaclass=ImmutableMeta):
    """Immutable dict (like ``frozenset`` for ``set``.)"""

    _shadowed_methods_ = ('__setitem__', '__delitem__', 'update', 'clear',
                          'pop', 'popitem', 'setdefault')

    def __reduce_ex__(self, protocol):
        return type(self), (dict(self),)

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, super().__repr__())

    __eq__ = dict.__eq__

    def __hash__(self):
        return hash(frozenset(self.items()))
