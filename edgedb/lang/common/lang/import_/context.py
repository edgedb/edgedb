##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class ImportContext(str):
    def __new__(cls, value, *, loader=None):
        result = super().__new__(cls, value)
        return result

    def __init__(self, value, *, loader=None):
        self.loader = loader

    def __getitem__(self, key):
        result = super().__getitem__(key)
        return self.__class__.copy(result, self)

    @classmethod
    def copy(cls, name, other):
        if isinstance(other, ImportContext):
            return cls(name, loader=other.loader)
        else:
            return cls(name)

    @classmethod
    def from_parent(cls, name, parent):
        if isinstance(parent, ImportContext):
            return cls(name, loader=parent.loader)
        else:
            return cls(name)
