##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.datastructures import Struct, Field, typed


class MarkupMeta(type(Struct)):
    def __new__(mcls, name, bases, dct, ns=None, **kwargs):
        cls = super().__new__(mcls, name, bases, dct, **kwargs)
        cls._markup_ns = ns

        ns_name = [name]
        for base in cls.__mro__:
            try:
                base_ns = base._markup_ns
            except AttributeError:
                pass
            else:
                if base_ns is not None:
                    ns_name.append(base_ns)

        cls._markup_name = '.'.join(reversed(ns_name))
        cls._markup_name_safe = '_'.join(reversed(ns_name))

        return cls

    def __init__(cls, name, bases, dct, ns=None, **kwargs):
        super().__init__(name, bases, dct, **kwargs)


class Markup(Struct, metaclass=MarkupMeta, use_slots=True):
    """Base class for all markup elements"""


class MarkupList(typed.TypedList, type=Markup):
    """List of BaseMarkup elements"""


class MarkupMapping(typed.TypedDict, keytype=str, valuetype=Markup):
    """Mapping ``str -> BaseMarkup``"""
