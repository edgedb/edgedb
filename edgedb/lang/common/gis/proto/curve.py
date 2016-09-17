##
# Copyright (c) 2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import collections

from .errors import GeometryError
from .container import GeometryContainer
from .point import Point


class Curve(GeometryContainer):
    def is_closed(self):
        return self.elements[0] == self.elements[-1]

    def __new__(cls, value=None, *, srid=0, dimensions=None):
        result = None

        if value is None:
            value = ()

        if isinstance(value, collections.Sequence) and not isinstance(
                value, str):
            result = object.__new__(cls)

            elements = value

            if len(elements) == 1:
                details = ('{} must consist of at least two '
                           'points').format(cls.__name__)
                raise GeometryError(
                    'geometry requires more points', details=details)
            elif len(elements) > 1:
                raw_points = []
                points = []

                if dimensions:
                    num_dimensions = len(
                        tuple(d for d in dimensions if d is not None))
                else:
                    num_dimensions = None

                for element in elements:
                    is_sequence = (
                        isinstance(element, collections.Sequence) and
                        not isinstance(element, str)
                    )
                    if is_sequence:
                        if num_dimensions is None:
                            num_dimensions = len(element)
                        elif num_dimensions != len(element):
                            details = (
                                'all points in a {} must have the same '
                                'dimensionality'
                            ).format(cls.__name__)
                            raise GeometryError(
                                'cannot mix dimensionality in a geometry',
                                details=details)

                        points.append(element)
                        raw_points.append(len(points) - 1)

                    elif isinstance(element, (str, Point)):
                        if isinstance(element, str):
                            point = Point(element, srid=srid)
                        else:
                            point = element

                        point, num_dimensions, dimensions = \
                            cls._validate_point(
                                point, num_dimensions, dimensions)

                        points.append(element)

                    else:
                        details = '%s must consist of points' % cls.__name__
                        raise GeometryError(
                            'invalid geometry element', details=details)

                if raw_points:
                    for i in raw_points:
                        points[i] = Point(
                            points[i], srid=srid, dimensions=dimensions)

                if dimensions is None:
                    dimensions = points[0].dimension_map

                result._elements = points
                result._dimensions = dimensions

            else:
                result._elements = []
                result._dimensions = None

            result.srid = srid
        else:
            result = super().__new__(cls, value, srid=srid)

        return result

    @classmethod
    def _validate_point(cls, point, num_dimensions, dimensions):
        pt_dimensions = point.dimensions

        if num_dimensions is None:
            num_dimensions = len(pt_dimensions)
            dimensions = point.dimension_map
        else:
            if num_dimensions != len(pt_dimensions):
                details = (
                    'all points in a {} must have the same '
                    'dimensionality'
                ).format(cls.__name__)
                raise GeometryError(
                    'cannot mix dimensionality in a geometry',
                    details=details)

            if dimensions is None:
                dimensions = point.dimension_map
            elif dimensions != point.dimension_map:
                details = (
                    'all points in a {} must have the same '
                    'dimensionality'
                ).format(cls.__name__)
                raise GeometryError(
                    'cannot mix dimensionality in a geometry',
                    details=details)

        return point, num_dimensions, dimensions

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


class LineString(Curve):
    geo_class_id = 2
    geo_class_name = 'LineString'


class Line(LineString):
    """A line is a LineString with exactly two Points."""

    geo_class_name = 'Line'

    def __new__(cls, value, *, srid=0, dimensions=None):
        result = super().__new__(value, srid=srid)
        if not result.is_empty() and len(result) != 2:
            details = \
                '{} must consist of exactly two points'.format(cls.__name__)
            raise GeometryError(
                'invalid number of points in geometry', details=details)
        return result


class LinearRing(LineString):
    """A LinearRing is a LineString which is both closed and simple."""

    geo_class_name = 'LinearRing'
