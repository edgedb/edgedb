##
# Copyright (c) 2014 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.lang.javascript.tests.base import JSFunctionalTest


class TestWKBJS(JSFunctionalTest):
    def test_utils_gis_wkbjs_1(self):
        '''JS

        // %from metamagic.utils.gis.serialization import wkbjs

        var wkbt = '0101000020E61000009A99999999D945409A99999999D95340';

        var parsed = [];

        var factory = function(type, points, dimensions, srid) {
            parsed.push([
                type, points, dimensions, srid
            ]);
        };

        sx.wkb.parse_string(factory, wkbt);

        assert.equal(parsed.length, 1);

        assert.equal(parsed[0],
                     ['POINT', [43.7, 79.4], ['x', 'y', null, null], 4326]);
        '''
