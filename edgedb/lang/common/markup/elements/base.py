##
# Copyright (c) 2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.common.datastructures import Struct, Field, typed


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

    def __instancecheck__(cls, inst):
        # We make OverflowBarier and SerializationError be instanceof
        # and subclassof any Markup class.  This avoids errors when
        # they are being added to various TypedList & TypedMaps
        # collections.
        parent_check = type(Struct).__instancecheck__
        if parent_check(cls, inst):
            return True
        return type(inst) in (OverflowBarier, SerializationError)

    def __subclasscheck__(cls, subcls):
        parent_check = type(Struct).__subclasscheck__
        if parent_check(cls, subcls):
            return True
        return subcls in (OverflowBarier, SerializationError)


class Markup(Struct, metaclass=MarkupMeta, use_slots=True):
    """Base class for all markup elements."""


class MarkupList(typed.TypedList, type=Markup):
    """List of BaseMarkup elements."""


class MarkupMapping(typed.OrderedTypedDict, keytype=str, valuetype=Markup):
    """Mapping ``str -> BaseMarkup``."""


class OverflowBarier(Markup):
    """Represents that the nesting level of objects was too big."""


class SerializationError(Markup):
    """An error during object serialization occurred."""

    text = Field(str)
    cls = Field(str)
