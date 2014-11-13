##
# Copyright (c) 2014 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from jplus.tests import base as jpt_base


class TestWKBJP(jpt_base.BaseJPlusTest):
    def test_utils_gis_wkbjp_1(self):
        self.run_test(source='''

        from metamagic.utils.gis.serialization import wkbjp

        wkbt = '0101000020E61000009A99999999D945409A99999999D95340'

        parsed = []

        fn factory(type, points, dimensions, srid) {
            parsed.push([
                type, points, dimensions, srid
            ])
        }

        wkbjp.parseString(factory, wkbt)

        assert len(parsed) == 1

        assert parsed[0] ==
                     ['POINT', [43.7, 79.4], ['x', 'y', null, null], 4326]
        ''')
