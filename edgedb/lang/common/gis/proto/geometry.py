##
# Copyright (c) 2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.common.gis import serialization
from edgedb.lang.common.gis.serialization import wkb, wkt
from . import factory
from . import meta
from .errors import GeometryError


class Geometry(metaclass=meta.GeometryMeta):
    geo_class_id = 0

    def __new__(cls, value, *, srid=0):
        result = None

        if isinstance(value, str):
            # String input can be either (E)WKT or EWKB
            factory = cls.get_factory()

            try:
                serializer = wkb.HEXEWKBSerializer(factory)
                result = serializer.loads(value)
            except serialization.SerializerError:
                result = None

            if result is None:
                try:
                    serializer = wkt.WKTSerializer(factory)
                    result = serializer.loads(value)
                except serialization.SerializerError as e:
                    raise GeometryError('invalid geometry') from e

        elif isinstance(value, cls) or issubclass(cls, value.__class__):
            result = cls.copy(value)

        else:
            raise GeometryError('unexpected geometry data format')

        return result

    @classmethod
    def normalize_coords(cls, coords, dimensions=None):
        if dimensions is None:
            len_coords = len(coords)
            has_z, has_m = len_coords >= 3, len_coords >= 4
        else:
            has_z, has_m = bool(dimensions[2]), bool(dimensions[3])

        if has_z and has_m:
            if len(coords) != 4:
                cls._raise_coord_error(coords, 4)
        elif has_z or has_m:
            if len(coords) != 3:
                cls._raise_coord_error(coords, 3)
            if has_z:
                coords = (coords[0], coords[1], coords[2], None)
            else:
                coords = (coords[0], coords[1], None, coords[2])
        else:
            if len(coords) != 2:
                cls._raise_coord_error(coords, 2)

            coords = (coords[0], coords[1], None, None)

        nonempty = tuple(c for c in coords if c is not None)

        if dimensions is not None:
            nonempty_dims = tuple(d for d in dimensions if d is not None)
            if len(nonempty) > 0 and len(nonempty) != len(nonempty_dims):
                cls._raise_coord_error(coords, len(nonempty_dims))
        else:
            if has_z and has_m:
                dimensions = ('x', 'y', 'z', 'm')
            elif has_z:
                dimensions = ('x', 'y', 'z', None)
            elif has_m:
                dimensions = ('x', 'y', None, 'm')
            else:
                dimensions = ('x', 'y', None, None)

        return coords, dimensions

    @classmethod
    def _raise_coord_error(cls, coords, expected_count):
        errmsg = 'invalid geometry specification'
        errdetails = 'expected %d coordinate values, got %d' % (
            expected_count, len(coords))
        errvalue = coords[-1] if len(coords) < expected_count else coords[
            expected_count]
        raise GeometryError(errmsg, details=errdetails, errvalue=errvalue)

    @classmethod
    def get_factory(cls):
        return factory.GeometryFactory()

    def as_text(self):
        factory = self.__class__.get_factory()
        serializer = wkt.WKTSerializer(factory)
        return serializer.dumps(self)

    def as_binary(self):
        factory = self.__class__.get_factory()
        serializer = wkb.EWKBSerializer(factory)
        return serializer.dumps(self)

    __str__ = as_text

    def __repr__(self):
        return '%s.%s(%r)' % (
            self.__class__.__module__, self.__class__.__name__, self.as_text())
