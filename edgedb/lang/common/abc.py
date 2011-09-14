##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


__all__ = ['AbstractMeta', 'abstractmethod', 'abstractproperty',
           'abstractclassmethod', 'abstractstaticmethod',
           'abstractattribute']

from abc import abstractmethod, abstractproperty, abstractclassmethod, abstractstaticmethod


class AbstractMeta(type):
    """
    Lightweight re-implementation of abc.ABCMeta metaclass, that does not add
    __instancecheck__ and __subclasscheck__ methods, which can cut down
    performance significantly in some cases.

    Supports standard @abstractmethod and @abstractproperty decorators.
    """

    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)

        abstracts = {name for name, value in dct.items() \
                                                if getattr(value, '__isabstractmethod__', False)}

        try:
            abstracts |= getattr(cls, '__abstractmethods__')
        except AttributeError:
            pass

        for base in bases:
            for name in getattr(base, '__abstractmethods__', set()):
                value = getattr(cls, name, None)
                if value and getattr(value, '__isabstractmethod__', False):
                    abstracts.add(name)

        cls.__abstractmethods__ = frozenset(abstracts)


class abstractattribute:
    __isabstractmethod__ = True

    def __init__(self, *, doc=None):
        self.__doc__ = doc


_empty_set = frozenset()
def push_abstract(cls, *names):
    cls.__abstractmethods__ = frozenset(set(names) | getattr(cls, '__abstractmethods__', _empty_set))
