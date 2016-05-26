##
# Copyright (c) 2008-2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos.error import AtomValueError
from metamagic.utils.gis import proto as gis_proto
from metamagic.utils.gis import serialization as gis_serialization

from . import base as s_types


class GeometryMeta(gis_proto.meta.GeometryMeta):
    def __new__(mcls, name, bases, dct):
        return gis_proto.meta.GeometryMeta.__new__(mcls, name, bases, dct)


class GeometryFactory(gis_proto.factory.GeometryFactory):
    def __init__(self, geometry_meta=GeometryMeta):
        super().__init__(geometry_meta)


class Geometry(gis_proto.geometry.Geometry, metaclass=GeometryMeta):
    def __new__(cls, value, *, srid=0):
        try:
            result = gis_proto.geometry.Geometry.__new__(cls, value, srid=srid)
        except (gis_proto.errors.GeometryError, gis_serialization.SerializerError) as e:
            raise AtomValueError('invalid Geometry value') from e

        cls.validate(result)
        return result

    @classmethod
    def get_factory(cls):
        return GeometryFactory()


_add_impl = s_types.BaseTypeMeta.add_implementation
_add_map = s_types.BaseTypeMeta.add_mapping


class Point(Geometry, gis_proto.point.Point):
    def __new__(cls, value=None, *, x=None, y=None, z=None, m=None, srid=0, dimensions=None):
        try:
            result = gis_proto.point.Point.__new__(cls, value=value, x=x, y=y, z=z, m=m, srid=srid,
                                                   dimensions=dimensions)
        except (gis_proto.errors.GeometryError, gis_serialization.SerializerError) as e:
            raise AtomValueError('invalid Point value') from e

        return result


_add_impl('metamagic.caos.geo.point', Point)
_add_map(Point, 'metamagic.caos.geo.point')
_add_map(gis_proto.point.Point, 'metamagic.caos.geo.point')


class LineString(Geometry, gis_proto.curve.LineString):
    def __new__(cls, value=None, *, srid=0, dimensions=None):
        try:
            result = gis_proto.curve.LineString.__new__(cls, value=value, srid=srid,
                                                        dimensions=dimensions)
        except (gis_proto.errors.GeometryError, gis_serialization.SerializerError) as e:
            raise AtomValueError('invalid LineString value') from e

        return result


_add_impl('metamagic.caos.geo.linestring', LineString)
_add_map(LineString, 'metamagic.caos.geo.linestring')
_add_map(gis_proto.curve.LineString, 'metamagic.caos.geo.linestring')


class Polygon(Geometry, gis_proto.surface.Polygon):
    def __new__(cls, value=None, *, srid=0, dimensions=None):
        try:
            result = gis_proto.surface.Polygon.__new__(cls, value=value, srid=srid,
                                                       dimensions=dimensions)
        except (gis_proto.errors.GeometryError, gis_serialization.SerializerError) as e:
            raise AtomValueError('invalid Polygon value') from e

        return result


_add_impl('metamagic.caos.geo.polygon', Polygon)
_add_map(Polygon, 'metamagic.caos.geo.polygon')
_add_map(gis_proto.surface.Polygon, 'metamagic.caos.geo.polygon')
