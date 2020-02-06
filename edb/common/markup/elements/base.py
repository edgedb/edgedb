#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2011-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations

from edb.common.struct import Struct, StructMeta, Field
from edb.common import checked


class MarkupMeta(StructMeta):
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
        # they are being added to various CheckedList & CheckedDict
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


MarkupList = checked.CheckedList[Markup]
MarkupMapping = checked.CheckedDict[str, Markup]


class OverflowBarier(Markup):
    """Represents that the nesting level of objects was too big."""


class SerializationError(Markup):
    """An error during object serialization occurred."""

    text = Field(str)
    cls = Field(str)
