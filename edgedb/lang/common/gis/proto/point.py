##
# Copyright (c) 2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from .geometry import Geometry, GeometryError


class Point(Geometry):
    geo_class_id = 1
    geo_classname = 'Point'

    def __new__(
            cls, value=None, *, x=None, y=None, z=None, m=None, srid=0,
            dimensions=None):
        result = None

        if value is not None:
            if (x is not None or y is not None or
                    z is not None or m is not None):
                raise TypeError(
                    'geometry can either be specified by a single value or '
                    'separate coordinates, not both')
        else:
            if dimensions:
                value = (locals()[c] for c in dimensions if c is not None)
            else:
                value = [x, y]
                dimensions = ['x', 'y']
                if z is not None:
                    value.append(z)
                    dimensions.append('z')
                else:
                    dimensions.append(None)

                if m is not None:
                    value.append(m)
                    dimensions.append('m')
                else:
                    dimensions.append(None)

                value = tuple(value)
                dimensions = tuple(dimensions)

        if not isinstance(value, tuple):
            result = super().__new__(cls, value, srid=srid)
            if not isinstance(result, Point):
                raise GeometryError('invalid geometry type for Point')
        else:
            result = object.__new__(cls)
            (x, y, z, m), dimensions = cls.normalize_coords(value, dimensions)

            result._is_empty = x is None
            result._dimensions = tuple(dimensions)
            result._nonempty_dimensions = \
                tuple(d for d in dimensions if d is not None)
            result.x, result.y, result.z, result.m, result.srid = \
                x, y, z, m, srid

        return result

    @property
    def has_z_dimension(self):
        return self.z is not None

    @property
    def has_m_dimension(self):
        return self.m is not None

    @property
    def coords(self):
        return tuple(getattr(self, attr) for attr in self._nonempty_dimensions)

    @property
    def dimensions(self):
        return self._nonempty_dimensions

    @property
    def dimension_map(self):
        return self._dimensions

    def is_empty(self):
        return self._is_empty

    def __iter__(self):
        return iter(self.coords)

    @classmethod
    def copy(cls, value):
        return cls(x=value.x, y=value.y, z=value.z, m=value.m, srid=value.srid)
