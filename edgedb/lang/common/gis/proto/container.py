##
# Copyright (c) 2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import abc
from .geometry import Geometry



class GeometryContainer(Geometry, abc.GeometryContainer):
    def append(self, element):
        self._elements.append(element)

    def __len__(self):
        return len(self._elements)

    def __iter__(self):
        return iter(self._elements)

    def __getitem__(self, i):
        return self._elements[i]

    @classmethod
    def copy(cls, value):
        return cls(value._elements, dimensions=value.dimensions,
                                    srid=value.srid)
