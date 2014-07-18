/*
* Copyright (c) 2012, 2013 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


// %from metamagic.utils.lang.javascript import sx, byteutils, ieee754


sx.wkb = (function() {
    'use strict';

    var GEOMETRYTYPE = 0;
    var POINTTYPE = 1;
    var LINETYPE = 2;
    var POLYGONTYPE = 3;
    var MULTIPOINTTYPE = 4;
    var MULTILINETYPE = 5;
    var MULTIPOLYGONTYPE = 6;
    var COLLECTIONTYPE = 7;
    var CIRCSTRINGTYPE = 8;
    var COMPOUNDTYPE = 9;
    var CURVEPOLYTYPE = 10;
    var MULTICURVETYPE = 11;
    var MULTISURFACETYPE = 12;
    var CURVETYPE = 13;
    var SURFACETYPE = 14;
    var POLYHEDRALSURFACETYPE = 15;
    var TIN = 16;

    var WKBZOFFSET = 0x80000000;
    var WKBMOFFSET = 0x40000000;
    var WKBSRIDFLAG = 0x20000000;
    var WKBBBOXFLAG = 0x10000000;

    var dimensionality_map = {
        "falsefalse": ['x', 'y', null, null],
        "truefalse": ['x', 'y', 'z', null],
        "falsetrue": ['x', 'y', null, 'm'],
        "truetrue": ['x', 'y', 'z', 'm'],
    };

    var get_dimensions = function(z_dimension, m_dimension) {
        var key = z_dimension.toString() + m_dimension.toString();
        return dimensionality_map[key];
    };

    var parse_point = function(factory, unpacker, z_dimension, m_dimension, srid) {
        var coords = [unpacker.double_unpack(), unpacker.double_unpack()];
        var dimensions = get_dimensions(z_dimension, m_dimension);

        if (z_dimension) {
            coords.push(unpacker.double_unpack());
        }

        if (m_dimension) {
            coords.push(unpacker.double_unpack());
        }

        return factory('POINT', coords, dimensions, srid=srid);
    };

    var parse_line = function(factory, unpacker, z_dimension, m_dimension, srid) {
        var num_points = unpacker.uint32_unpack();

        var points = [];
        for (var i = 0; i < num_points; i++) {
            var point = parse_point(unpacker, z_dimension, m_dimension, srid);
            points.push(point);
        }

        var dimensions = get_dimensions(z_dimension, m_dimension);
        return factory('LINESTRING', points, dimensions, srid);
    };

    var parse_polygon = function(factory, unpacker, z_dimension, m_dimension, srid) {
        var num_rings = unpacker.uint32_unpack();

        for (var i = 0; i < num_rings; i++) {
            var ring = self.parse_line(unpacker, z_dimension, m_dimension, srid);
            rings.push(ring);
        }

        var dimensions = get_dimensions(z_dimension, m_dimension);
        return factory('POLYGON', rings, dimensions, srid);
    };

    var _handlers = {};

    _handlers['t' + POINTTYPE] = parse_point;
    _handlers['t' + LINETYPE] = parse_line;
    _handlers['t' + POLYGONTYPE] = parse_polygon;

    var _le_unpacker = function(data) {
        var self = this;

        self.data = data;
        self._pos = 0;

        this.uint32_unpack = function() {
            var bytes = self.data.slice(self._pos, self._pos + 4);
            self._pos += 4;
            return bytes[3] << 24 | bytes[2] << 16 | bytes[1] << 8 | bytes[0];
        }

        this.double_unpack = function() {
            var bytes = self.data.slice(self._pos, self._pos + 8);
            bytes.reverse();
            self._pos += 8;
            return sx.ieee754.unpackFloat64(bytes);
        }

        return self;
    }

    var _ge_unpacker = function(data) {
        var self = this;

        self.data = data;
        self._pos = 0;

        this.uint32_unpack = function() {
            var bytes = self.data.slice(self._pos, self._pos + 4);
            self._pos += 4;
            return bytes[0] << 24 | bytes[1] << 16 | bytes[2] << 8 | bytes[3];
        }

        this.double_unpack = function() {
            var bytes = self.data.slice(self._pos, self._pos + 8);
            self._pos += 8;
            return sx.ieee754.unpackFloat64(bytes);
        }

        return self;
    }

    function _parse_string(factory, s) {
        var bytes = sx.byteutils.unhexlify(s);
        return _parse_bytes(factory, bytes);
    }

    function _parse_bytes(factory, b) {
        var le = b[0];
        var unpacker = le ? new _le_unpacker(b.slice(1))
                          : new _be_unpacker(b.slice(1));

        return _parse_geometry(factory, unpacker);
    }

    function _parse_geometry(factory, unpacker) {
        var geotype = unpacker.uint32_unpack();

        var z_dimension = !!(geotype & WKBZOFFSET);
        var m_dimension = !!(geotype & WKBMOFFSET);
        var srid = geotype & WKBSRIDFLAG ? unpacker.uint32_unpack() : null;

        geotype &= 0x0000FFFF

        var handler = _handlers['t' + geotype];

        if (!handler) {
            throw new Error('unexpected geotype (' + geotype.toString() + ')'
                            + ' in EWKB data at offset '
                            + unpacker._pos.toString());
        }

        return handler(factory, unpacker, z_dimension, m_dimension, srid);
    };

    return {
        parse_string: _parse_string,
        parse_bytes: _parse_bytes
    };

})();
