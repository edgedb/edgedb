##
# Copyright (c) 2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import binascii
import struct
import types

from .base import Serializer, SerializerError

GEOMETRYTYPE = 0
POINTTYPE = 1
LINETYPE = 2
POLYGONTYPE = 3
MULTIPOINTTYPE = 4
MULTILINETYPE = 5
MULTIPOLYGONTYPE = 6
COLLECTIONTYPE = 7
CIRCSTRINGTYPE = 8
COMPOUNDTYPE = 9
CURVEPOLYTYPE = 10
MULTICURVETYPE = 11
MULTISURFACETYPE = 12
CURVETYPE = 13
SURFACETYPE = 14
POLYHEDRALSURFACETYPE = 15
TIN = 16

WKBZOFFSET = 0x80000000
WKBMOFFSET = 0x40000000
WKBSRIDFLAG = 0x20000000
WKBBBOXFLAG = 0x10000000


def _mk_pack(x, byteorder='!'):
    s = struct.Struct(byteorder + x)
    if len(x) > 1:

        def pack(y, p=s.pack):
            return p(*y)

        return (pack, s.unpack_from)
    else:

        def unpack(y, p=s.unpack_from):
            return p(y)[0]

        return (s.pack, unpack)


def mk_pack(x, width, byteorder='!'):
    pack, unpack = _mk_pack(x, byteorder)

    def _unpack(self, unpack=unpack, width=width):
        i = self._pos
        self._pos = j = i + width
        data = self.data[i:j]
        if len(data) < width:
            raise EOFError
        return unpack(data)

    return (pack, _unpack)


byte_pack, byte_unpack = mk_pack('B', 1)
uint32_pack_le, uint32_unpack_le = mk_pack('I', 4, '<')
uint32_pack_be, uint32_unpack_be = mk_pack('I', 4, '>')
double_pack_le, double_unpack_le = mk_pack('d', 8, '<')
double_pack_be, double_unpack_be = mk_pack('d', 8, '>')


class IOBase:
    def set_byteorder(self, byteorder):
        if byteorder == self._byteorder:
            return

        if byteorder not in ('<', '>'):
            raise ValueError(
                'invalid byteorder: %s, expected either "<" or ">"' %
                byteorder)
        self._byteorder = byteorder

    def get_byteorder(self):
        return self._byteorder


class Unpacker(IOBase):
    def __init__(self, data, byteorder='>'):
        self._byteorder = None
        self.set_byteorder(byteorder)
        self.byte_unpack = types.MethodType(byte_unpack, self)
        self.data = data
        self._pos = 0

    def set_byteorder(self, byteorder):
        super().set_byteorder(byteorder)

        if byteorder == '>':
            self.uint32_unpack = types.MethodType(uint32_unpack_be, self)
            self.double_unpack = types.MethodType(double_unpack_be, self)
        else:
            self.uint32_unpack = types.MethodType(uint32_unpack_le, self)
            self.double_unpack = types.MethodType(double_unpack_le, self)


class EWKBParser:
    dimensionality_map = {(False, False): ('x', 'y', None, None),
                          (True, False): ('x', 'y', 'z', None),
                          (False, True): ('x', 'y', None, 'm'),
                          (True, True): ('x', 'y', 'z', 'm'), }

    def __init__(self, factory):
        self.factory = factory
        self._handlers = {
            POINTTYPE: self.parse_point,
            LINETYPE: self.parse_line,
            POLYGONTYPE: self.parse_polygon,
            MULTIPOINTTYPE: self.parse_multipoint,
            MULTILINETYPE: self.parse_multiline,
            MULTIPOLYGONTYPE: self.parse_multipolygon,
            COLLECTIONTYPE: self.parse_collection,
            #  CIRCSTRINGTYPE: self.parse_circstring,
            #  COMPOUNDTYPE: self.parse_compound,
            #  CURVEPOLYTYPE: self.parse_curvepoly,
            #  MULTICURVETYPE: self.parse_milticurve,
            #  MULTISURFACETYPE: self.parse_multisurface
        }

    def parse(self, data):
        unpacker = Unpacker(data)
        return self.parse_geometry(unpacker)

    def parse_geometry(self, unpacker):
        byteorder = unpacker.byte_unpack()
        unpacker.set_byteorder('<' if byteorder else '>')

        geotype = unpacker.uint32_unpack()

        z_dimension = bool(geotype & WKBZOFFSET)
        m_dimension = bool(geotype & WKBMOFFSET)
        srid = unpacker.uint32_unpack() if geotype & WKBSRIDFLAG else None

        geotype &= 0x0000FFFF

        handler = self._handlers.get(geotype)

        if handler is None:
            raise ValueError(
                'unexpected geotype ({}) in EWKB data at offset {}'.format(
                    geotype, unpacker.get_offset()))

        return handler(
            unpacker, z_dimension=z_dimension, m_dimension=m_dimension,
            srid=srid)

    def parse_point(
            self, unpacker, z_dimension=False, m_dimension=False, srid=None):
        coords = [unpacker.double_unpack(), unpacker.double_unpack()]

        dimensions = self.dimensionality_map[z_dimension, m_dimension]

        if z_dimension:
            coords.append(unpacker.double_unpack())

        if m_dimension:
            coords.append(unpacker.double_unpack())

        return self.factory.new_node(
            'POINT', srid=srid, values=tuple(coords), dimensions=dimensions)

    def parse_line(
            self, unpacker, z_dimension=False, m_dimension=False, srid=None):
        num_points = unpacker.uint32_unpack()

        points = []
        for _ in range(num_points):
            point = self.parse_point(unpacker, z_dimension, m_dimension, srid)
            points.append(point)

        dimensions = self.dimensionality_map[z_dimension, m_dimension]
        line_string = self.factory.new_node(
            type='LINESTRING', values=points, dimensions=dimensions, srid=srid)

        return line_string

    def parse_polygon(
            self, unpacker, z_dimension=False, m_dimension=False, srid=None):
        num_rings = unpacker.uint32_unpack()

        rings = []
        for _ in range(num_rings):
            ring = self.parse_line(unpacker, z_dimension, m_dimension, srid)
            rings.append(ring)

        dimensions = self.dimensionality_map[z_dimension, m_dimension]
        polygon = self.factory.new_node(
            'POLYGON', srid=srid, values=tuple(rings), dimensions=dimensions)

        return polygon

    def parse_multipoint(
            self, unpacker, z_dimension=False, m_dimension=False, srid=None):
        return self.parse_collection(
            unpacker, z_dimension, m_dimension, srid, type=MULTIPOINTTYPE)

    def parse_multiline(
            self, unpacker, z_dimension=False, m_dimension=False, srid=None):
        return self.parse_collection(
            unpacker, z_dimension, m_dimension, srid, type=MULTILINETYPE)

    def parse_multipolygon(
            self, unpacker, z_dimension=False, m_dimension=False, srid=None):
        return self.parse_collection(
            unpacker, z_dimension, m_dimension, srid, type=MULTIPOLYGONTYPE)

    def parse_collection(
            self, unpacker, z_dimension=False, m_dimension=False, srid=None,
            type=COLLECTIONTYPE):
        num_elems = unpacker.uint32_unpack()

        geometry = self.factory.new_node(type, srid=srid)
        for _ in range(num_elems):
            element = self.parse_geometry(unpacker)
            geometry.append(element)

        return geometry


class EWKBPacker:
    def __init__(self):
        self.buf = bytearray()

        self._handlers = {
            POINTTYPE: self.write_point,
            LINETYPE: self.write_line,
            POLYGONTYPE: self.write_polygon,
            MULTIPOINTTYPE: self.write_multipoint,
            MULTILINETYPE: self.write_multiline,
            MULTIPOLYGONTYPE: self.write_multipolygon,
            COLLECTIONTYPE: self.write_collection,
            #  CIRCSTRINGTYPE: self.parse_circstring,
            #  COMPOUNDTYPE: self.parse_compound,
            #  CURVEPOLYTYPE: self.parse_curvepoly,
            #  MULTICURVETYPE: self.parse_milticurve,
            #  MULTISURFACETYPE: self.parse_multisurface
        }

    def pack(self, geometry):
        self.write_geometry(geometry)
        return bytes(self.buf)

    def write_geometry(self, geometry):
        self.buf.extend(byte_pack(0))

        type = geometry.geo_class_id
        if geometry.has_z_dimension:
            type |= WKBZOFFSET

        if geometry.has_m_dimension:
            type |= WKBMOFFSET

        srid = geometry.srid
        type |= WKBSRIDFLAG

        self.buf.extend(uint32_pack_be(type))
        self.buf.extend(uint32_pack_be(srid))

        handler = self._handlers.get(geometry.geo_class_id)

        if handler is None:
            raise ValueError('unexpected WKB type: %d' % geometry.geo_clas_id)

        handler(geometry)

    def write_point(self, geometry):
        v = b''.join(self.double_pack(c) for c in geometry.coords)
        self.buf.extend(v)

    def write_line(self, geometry):
        self.buf.extend(uint32_pack_be(len(geometry)))
        for element in geometry:
            self.write_point(element)

    def write_polygon(self, geometry):
        self.buf.extend(uint32_pack_be(len(geometry)))
        for element in geometry:
            self.write_line(element)

    def write_collection(self, geometry):
        self.buf.extend(uint32_pack_be(len(geometry)))
        for element in geometry:
            self.write_geometry(element)

    write_multipoint = write_collection
    write_multiline = write_collection
    write_multipolygon = write_collection


class EWKBSerializer(Serializer):
    def __init__(self, factory):
        self.packer = None
        self.parser = None
        self.factory = factory

    def dumps(self, geometry):
        if self.packer is None:
            self.packer = EWKBPacker()

        return self.packer.pack(geometry)

    def loads(self, data):
        if self.parser is None:
            self.parser = EWKBParser(self.factory)

        return self.parser.parse(data)


class HEXEWKBSerializer(EWKBSerializer):
    def dumps(self, geometry):
        bin = super().dumps(geometry)
        return binascii.hexlify(bin)

    def loads(self, data):
        try:
            bin = binascii.unhexlify(data)
        except binascii.Error as e:
            raise SerializerError(e.args[0]) from e
        return super().loads(bin)
