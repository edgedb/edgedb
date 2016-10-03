##
# Copyright (c) 2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import collections

from .errors import GeometryError
from .container import GeometryContainer
from .curve import LineString, LinearRing


class Surface(GeometryContainer):
    def __new__(cls, value=None, *, srid=0, dimensions=None):
        result = None

        if value is None:
            value = ()

        if isinstance(value, collections.Sequence) and not isinstance(
                value, str):
            result = object.__new__(cls)

            elements = value

            rings = []

            if not elements:
                details = ('{} must consist of at least '
                           'one LinearRing'.format(cls.__name__))
                raise GeometryError(
                    'geometry requires more elements', details=details)

            for element in elements:
                is_sequence = (
                    isinstance(element, collections.Sequence)
                    and not isinstance(element, str)
                )
                if is_sequence:
                    rings.append(LinearRing(element))

                elif isinstance(element, (str, LineString)):
                    if isinstance(element, str):
                        ring = LinearRing(element, srid=srid)
                    else:
                        ring = element

                    rings.append(ring)

                else:
                    details = ('{} must consist of LinearRing '
                               'elements'.format(cls.__name__))
                    raise GeometryError(
                        'invalid geometry element', details=details)

                dimensions = cls._validate_dimensionality(rings, dimensions)

                result._elements = rings
                result._dimensions = dimensions

            result.srid = srid
        else:
            result = super().__new__(cls, value, srid=srid)

        return result

    @classmethod
    def _validate_dimensionality(cls, elements, expected_dimensions):
        dimensions = expected_dimensions

        for element in elements:
            if dimensions is None:
                dimensions = element.dimensions
            else:
                if dimensions != element.dimensions:
                    details = (
                        'all rings in a {} must have the '
                        'same dimensionality'.format(cls.__name__))
                    raise GeometryError(
                        'cannot mix dimensionality in a geometry',
                        details=details)

        return dimensions

    def is_empty(self):
        return not self._elements

    @property
    def dimensions(self):
        return self._dimensions

    @property
    def has_z_dimension(self):
        return 'z' in self._dimensions

    @property
    def has_m_dimension(self):
        return 'm' in self._dimensions


class Polygon(Surface):
    geo_class_id = 3
    geo_classname = 'Polygon'
