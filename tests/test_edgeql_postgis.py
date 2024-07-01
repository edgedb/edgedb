#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import json
import functools
import os
import typing

import edgedb

from edb.testbase import server as tb
from edb.tools import test


class value(typing.NamedTuple):
    typename: str

    postgis: bool
    geometry: bool
    geography: bool
    box2d: bool
    box3d: bool


VALUES = {
    '<geometry>"point(0 1)"':
        value(typename='geometry',
              postgis=True,
              geometry=True, geography=False, box2d=False, box3d=False),

    '<geography>"point(0 1)"':
        value(typename='geography',
              postgis=True,
              geometry=False, geography=True, box2d=False, box3d=False),

    '<box2d>"box(0 0, 1 2)"':
        value(typename='box2d',
              postgis=True,
              geometry=False, geography=False, box2d=True, box3d=False),

    '<box3d>"BOX3D(0 0 0, 1 2 3)"':
        value(typename='box3d',
              postgis=True,
              geometry=False, geography=False, box2d=False, box3d=True),
}


@functools.lru_cache()
def get_test_values(**flags):
    res = []
    for val, desc in VALUES.items():
        if all((getattr(desc, fname) == fval)
               for fname, fval in flags.items()):
            res.append(val)
    return tuple(res)


@functools.lru_cache()
def get_test_items(**flags):
    res = []
    for val, desc in VALUES.items():
        if all((getattr(desc, fname) == fval)
               for fname, fval in flags.items()):
            res.append((val, desc))
    return tuple(res)


class TestEdgeQLPostgis(tb.QueryTestCase):
    EXTENSIONS = ['postgis']
    BACKEND_SUPERUSER = True

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'postgis.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'postgis_setup.edgeql')

    @classmethod
    def get_setup_script(cls):
        res = super().get_setup_script()

        # HACK: As a debugging cycle hack, when RELOAD is true, we reload the
        # extension package from the file, so we can test without a bootstrap.
        RELOAD = False

        if RELOAD:
            root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            with open(os.path.join(root, 'edb/lib/ext/postgis.edgeql')) as f:
                contents = f.read()
            to_add = '''
                drop extension package postgis version '3.4.2';
            ''' + contents
            splice = '__internal_testmode := true;'
            res = res.replace(splice, splice + to_add)

        return res

    async def test_edgeql_postgis_cast_01(self):
        # Basic casts to and from json and str.
        await self.assert_query_result(
            '''
                with module ext::postgis
                select <json><geometry>'point(0 1)';
            ''',
            ['POINT(0 1)'],
            json_only=True,
        )

        await self.assert_query_result(
            '''
                with module ext::postgis
                select <geometry>'point(0 1)' =
                       <geometry>to_json('"POINT(0 1)"');
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                with module ext::postgis
                select <str><geometry>to_json('"POINT(0 1)"');
            ''',
            ['POINT(0 1)'],
        )

    async def test_edgeql_postgis_cast_02(self):
        # Basic casts to and from json and str.
        await self.assert_query_result(
            '''
                with module ext::postgis
                select <json><geography>'point(0 1)';
            ''',
            ['POINT(0 1)'],
            json_only=True,
        )

        await self.assert_query_result(
            '''
                with module ext::postgis
                select <geography>'point(0 1)' =
                       <geography>to_json('"POINT(0 1)"');
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                with module ext::postgis
                select <str><geography>to_json('"POINT(0 1)"');
            ''',
            ['POINT(0 1)'],
        )

    async def test_edgeql_postgis_cast_03(self):
        # Basic casts to and from json and str.
        await self.assert_query_result(
            '''
                with module ext::postgis
                select <json><box2d>'box(0 0, 1 1)';
            ''',
            ['BOX(0 0,1 1)'],
            json_only=True,
        )

        await self.assert_query_result(
            '''
                with module ext::postgis
                select <box2d>'box(0 0, 1 1)' =
                       <box2d>to_json('"BOX(0 0, 1 1)"');
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                with module ext::postgis
                select <str><box2d>to_json('"BOX(0 0, 1 1)"');
            ''',
            ['BOX(0 0,1 1)'],
        )

    async def test_edgeql_postgis_cast_04(self):
        # Basic casts to and from json and str.
        await self.assert_query_result(
            '''
                with module ext::postgis
                select <json><box3d>'BOX3D(0 0 0, 1 2 3)';
            ''',
            ['BOX3D(0 0 0,1 2 3)'],
            json_only=True,
        )

        await self.assert_query_result(
            '''
                with module ext::postgis
                select <box3d>to_json('"BOX3D(0 0 0, 1 2 3)"');
            ''',
            ['BOX3D(0 0 0,1 2 3)'],
            json_only=True,
        )

        await self.assert_query_result(
            '''
                with module ext::postgis
                select <str><box3d>to_json('"BOX3D(0 0 0, 1 2 3)"');
            ''',
            ['BOX3D(0 0 0,1 2 3)'],
        )

    async def test_edgeql_postgis_cast_05(self):
        # All postgis types can be cast into geometry
        await self.assert_query_result(
            '''
                with module ext::postgis
                select (
                    <geometry>'srid=4326;point(0 1)' =
                        <geometry><geography>'point(0 1)',
                    <geometry>'POLYGON((0 0,0 2,1 2,1 0,0 0))' =
                        <geometry><box2d>'BOX(0 0, 1 2)',
                    <geometry>'POLYHEDRALSURFACE Z (
                        ((0 0 0,0 2 0,1 2 0,1 0 0,0 0 0)),
                        ((0 0 3,1 0 3,1 2 3,0 2 3,0 0 3)),
                        ((0 0 0,0 0 3,0 2 3,0 2 0,0 0 0)),
                        ((1 0 0,1 2 0,1 2 3,1 0 3,1 0 0)),
                        ((0 0 0,1 0 0,1 0 3,0 0 3,0 0 0)),
                        ((0 2 0,0 2 3,1 2 3,1 2 0,0 2 0))
                    )' =
                        <geometry><box3d>'BOX3D(0 0 0, 1 2 3)',
                );
            ''',
            [[True, True, True]],
        )

    async def test_edgeql_postgis_cast_06(self):
        # Geometry can be cast int any postgis type
        await self.assert_query_result(
            r'''
                with module ext::postgis
                select (
                    <geography><geometry>'point(0 1)' =
                        <geography>'point(0 1)',
                    <box2d><geometry>'LINESTRING(0 0, 1 2)' =
                        <box2d>'BOX(0 0, 1 2)',
                    # There's an extra `str` cast here because of an issue
                    # that prevents two box3d values being compared.
                    <str><box3d><geometry>'LINESTRING(0 0 0, 1 2 3)' =
                        <str><box3d>'BOX3D(0 0 0, 1 2 3)',
                );
            ''',
            [[True, True, True]],
        )

    async def test_edgeql_postgis_cast_07(self):
        # Box 2d and 3d can be cast into each other.
        await self.assert_query_result(
            '''
                with module ext::postgis
                select (
                    <box3d><box2d>'BOX(0 0, 1 2)',
                    <box2d><box3d>'BOX3D(0 0 0, 1 2 3)',
                );
            ''',
            [[
                'BOX3D(0 0 0,1 2 0)',
                'BOX(0 0,1 2)',
            ]],
            json_only=True,
        )

    async def test_edgeql_postgis_cast_08(self):
        # Geometry and geography can be cast into bytes and back.
        await self.assert_query_result(
            r'''
                with
                    module ext::postgis,
                    a := b'\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\
                           \x00\x00\x00\x00\x00\x00\x00\xf0\x3f',
                    b := b'\x01\x01\x00\x00\x20\xe6\x10\x00\x00\x00\x00\x00\
                           \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\
                           \x3f',
                select (
                    a = <bytes><geometry>'point(0 1)',
                    b = <bytes><geography>'point(0 1)',
                    <geometry>a = <geometry>'point(0 1)',
                    <geography>b = <geography>'point(0 1)',
                );
            ''',
            [[True, True, True, True]],
        )

    async def test_edgeql_postgis_op_01(self):
        await self.assert_query_result(
            '''
                with module ext::postgis
                select op_distance_centroid(
                    <geometry>'point(0 1)',
                    <geometry>'linestring(2 0, 2 2)',
                );
            ''',
            [2],
        )

    async def test_edgeql_postgis_op_02(self):
        await self.assert_query_result(
            '''
                with module ext::postgis
                select op_distance_centroid_nd(
                    <geometry>'point(0 1 3)',
                    <geometry>'linestring(2 0 3, 2 2 3)',
                );
            ''',
            [2],
        )

    async def test_edgeql_postgis_op_03(self):
        await self.assert_query_result(
            '''
                with module ext::postgis
                select op_distance_box(
                    <geometry>'point(0 1)',
                    <geometry>'linestring(2 0, 2 2)',
                );
            ''',
            [2],
        )

    async def test_edgeql_postgis_op_04(self):
        await self.assert_query_result(
            '''
                with module ext::postgis
                select op_distance_knn(
                    <geography>'point(0 1)',
                    <geography>'linestring(2 0, 2 2)',
                );
            ''',
            [222356.2746102745],
        )

    async def test_edgeql_postgis_op_05(self):
        # Validate that a whole bunch of geometry operators work.
        await self.assert_query_result(
            '''
                with
                    module ext::postgis,
                    a := <geometry>'point(0 0)',
                    b := <geometry>'point(1 2)',
                select (
                    op_overlaps(a, b),
                    op_same(a, b),
                    op_within(a, b),
                    op_contains(a, b),
                    op_left(a, b),
                    op_overleft(a, b),
                    op_below(a, b),
                    op_overbelow(a, b),
                    op_overright(a, b),
                    op_right(a, b),
                    op_overabove(a, b),
                    op_above(a, b),
                );
            ''',
            [[
                False,
                False,
                False,
                False,
                True,
                True,
                True,
                True,
                False,
                False,
                False,
                False,
            ]]
        )

    async def test_edgeql_postgis_op_06(self):
        # Validate that a whole bunch of geometry operators work.
        await self.assert_query_result(
            '''
                with
                    module ext::postgis,
                    a := <geometry>'point(0 0 0)',
                    b := <geometry>'point(1 2 3)',
                select (
                    op_overlaps_nd(a, b),
                    op_contains_nd(a, b),
                    op_within_nd(a, b),
                    op_same_nd(a, b),
                    op_overlaps_3d(a, b),
                    op_contains_3d(a, b),
                    op_contained_3d(a, b),
                    op_same_3d(a, b),
                );
            ''',
            [[
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
            ]]
        )

    async def test_edgeql_postgis_op_07(self):
        # Validate that a whole bunch of geometry operators work.
        await self.assert_query_result(
            '''
                with
                    module ext::postgis,
                    a := <box2d>'box(0 0, 1 2)',
                    b := <geometry>'point(2 3)',
                select (
                    op_contains_2d(a, b),
                    op_is_contained_2d(a, b),
                    op_overlaps_2d(a, b),
                );
            ''',
            [[
                False,
                False,
                False,
            ]]
        )

    async def test_edgeql_postgis_op_08(self):
        # Validate that a whole bunch of geometry operators work.
        await self.assert_query_result(
            '''
                with
                    module ext::postgis,
                    a := <geometry>'point(2 3)',
                    b := <box2d>'box(0 0, 1 2)',
                select (
                    op_contains_2d(a, b),
                    op_is_contained_2d(a, b),
                    op_overlaps_2d(a, b),
                );
            ''',
            [[
                False,
                False,
                False,
            ]]
        )

    async def test_edgeql_postgis_op_09(self):
        # Validate that a whole bunch of geometry operators work.
        await self.assert_query_result(
            '''
                with
                    module ext::postgis,
                    a := <box2d>'box(0 1, 3 4)',
                    b := <box2d>'box(0 0, 1 2)',
                select (
                    op_contains_2d(a, b),
                    op_is_contained_2d(a, b),
                    op_overlaps_2d(a, b),
                );
            ''',
            [[
                False,
                False,
                True,
            ]]
        )

    async def test_edgeql_postgis_op_09(self):
        # This is overlapping bounding box.
        await self.assert_query_result(
            '''
                with module ext::postgis,
                select op_overlaps(
                    <geography>'point(0 1)',
                    <geography>'linestring(0 0, 2 3)',
                );
            ''',
            [True]
        )

    async def _test_boolop(self, left, right, op, not_op, result):
        '''Test pairs of operators'''

        if isinstance(result, bool):
            # this operation should be valid and produce opposite
            # results for op and not_op
            await self.assert_query_result(
                f"""
                with module ext::postgis
                select {left} {op} {right};
                """,
                {result}
            )

            await self.assert_query_result(
                f"""
                with module ext::postgis
                select {left} {not_op} {right};
                """,
                {not result}
            )
        else:
            # operation is expected to be invalid
            for binop in [op, not_op]:
                query = f"""
                    with module ext::postgis
                    select {left} {binop} {right};
                """
                with self.assertRaisesRegex(edgedb.QueryError, result,
                                            msg=query):
                    async with self.con.transaction():
                        await self.con.query(query)

    async def test_edgeql_postgis_valid_eq_01(self):
        ops = [('=', '!='), ('?=', '?!='), ('IN', 'NOT IN')]

        for left, lt in get_test_items(postgis=True):
            for right, rt in get_test_items(postgis=True):
                for op, not_op in ops:
                    await self._test_boolop(
                        left, right, op, not_op,
                        # Can only compare same type
                        True
                        if rt.typename == lt.typename else
                        'cannot be applied to operands',
                    )

    async def test_edgeql_postgis_valid_comp_01(self):
        # cannot compare different postgis types
        ops = [('>=', '<'), ('<=', '>')]

        for left, lt in get_test_items(postgis=True):
            for right, rt in get_test_items(postgis=True):
                for op, not_op in ops:
                    await self._test_boolop(
                        left, right, op, not_op,
                        # Can only compare same type
                        True
                        if rt.typename == lt.typename else
                        'cannot be applied to operands',
                    )

    async def test_edgeql_postgis_query_01(self):
        await self.assert_query_result(
            '''
                with gis as module ext::postgis
                select GeoTest{
                    name,
                    geometry,
                    geography,
                }
                filter gis::op_is_contained_2d(
                    .geometry, <gis::box2d>'box(1 1, -1 -1)'
                )
            ''',
            [{
                'name': '1st',
                'geometry': 'POINT(0 1)',
                'geography': 'POINT(2 3)',
            }],
            json_only=True,
        )

    # XXX: I was thinking of using these to just extract signatures and run
    # whatever functions we have in postgis automatically, but because of the
    # various specific requirements for arguments (valid values, valid
    # geometry subtypes) this is not a simple task.
    async def _get_func_names_for_params(self, params):
        res = await self.con.query(
            '''
                with
                    F := schema::Function,
                    S := (
                        select F {
                            sig := array_agg((
                                select (F.params, F.params.type.name)
                                order by F.params@index
                            ).1)
                        } filter
                            .name like 'ext::postgis::%'
                            and
                            .sig = <array<str>>$params
                    ),
                select _ := distinct S.name
                order by _;
            ''',
            params=params,
        )

        return list(res)

    async def _get_all_signatures(self):
        res = await self.con.query(
            '''
                with
                    F := schema::Function,
                    S := (
                        select F {
                            sig := array_agg((
                                select (F.params, F.params.type.name)
                                order by F.params@index
                            ).1)
                        } filter .name like 'ext::postgis::%'
                    ),
                select _ := distinct S.sig
                order by _;
            ''',
        )

        return list(res)

    def _get_args(self, params):
        args = []
        for param in params:
            vals = get_test_values(typename=param.split('::')[-1])
            if not vals:
                raise Exception(
                    f"need a value of type {param} for bulk testing")
            args.append(vals[0])

        return args

    async def test_edgeql_postgis_bulk_01(self):
        # Test in bulk all postgis functions that take the same type of
        # arguments: (geometry)

        # These are all OK running with a 'point'
        for fname in [
            'area',
            'area2d',
            'asbinary',
            'asewkb',
            'asewkt',
            'ashexewkb',
            'astext',
            'boundary',
            'buildarea',
            'bytea',
            'centroid',
            'cleangeometry',
            'collectionextract',
            'collectionhomogenize',
            'convexhull',
            'coorddim',
            'dimension',
            'endpoint',
            'envelope',
            'exteriorring',
            'flipcoordinates',
            'force2d',
            'forcecollection',
            'forcecurve',
            'forcepolygonccw',
            'forcepolygoncw',
            'forcerhr',
            'forcesfs',
            'geometry_hash',
            'geometrytype',
            'hasarc',
            'isclosed',
            'iscollection',
            'isempty',
            'ispolygonccw',
            'ispolygoncw',
            'issimple',
            'isvalid',
            'isvalidreason',
            'isvalidtrajectory',
            'json',
            'length',
            'length2d',
            'length3d',
            'linemerge',
            'linetocurve',
            'm',
            'makevalid',
            'memsize',
            'minimumclearance',
            'minimumclearanceline',
            'multi',
            'ndims',
            'normalize',
            'npoints',
            'nrings',
            'numgeometries',
            'numinteriorring',
            'numinteriorrings',
            'numpatches',
            'numpoints',
            'orientedenvelope',
            'perimeter',
            'perimeter2d',
            'perimeter3d',
            'pointonsurface',
            'points',
            'postgis_addbbox',
            'postgis_dropbbox',
            'postgis_geos_noop',
            'postgis_getbbox',
            'postgis_hasbbox',
            'postgis_noop',
            'reverse',
            'shiftlongitude',
            'srid',
            'startpoint',
            'summary',
            'text',
            'to_box2d',
            'to_box3d',
            'to_geography',
            'x',
            'y',
            'z',
            'zmflag',
        ]:
            await self.con.query_json(
                f'''
                    with module ext::postgis
                    select {fname}(<geometry>'point(0 1)')
                ''',
            )

    async def test_edgeql_postgis_bulk_02(self):
        # Test in bulk all postgis functions that take the same type of
        # arguments: (geometry)

        # These are all OK running with a 'linestring'
        for fname in [
            'isring',
            'makepolygon',
            'node',
        ]:
            await self.con.query_json(
                f'''
                    with module ext::postgis
                    select {fname}(
                        <geometry>'linestring(0 1, 2 3, 4 5, 0 1)')
                ''',
            )

        await self.con.query_json(
            f'''
                with module ext::postgis
                select linefrommultipoint(
                    <geometry>'multipoint(0 1, 2 3, 4 5, 0 1)')
            ''',
        )

    async def test_edgeql_postgis_bulk_03(self):
        # this may require a specific GEOS version to run
        await self.con.query_json(
            f'''
                with module ext::postgis
                select triangulatepolygon(
                    <geometry>'polygon((0 1, 2 3, 4 5, 0 1))')
            ''',
        )

    async def test_edgeql_postgis_bulk_04(self):
        # Test in bulk all postgis functions that take the same type of
        # arguments: (geography)

        # These are all OK running with a 'point'
        for fname in [
            'asbinary',
            'asewkt',
            'astext',
            'bytea',
            'geometrytype',
            'srid',
            'summary',
            'to_geometry',
        ]:
            await self.con.query_json(
                f'''
                    with module ext::postgis
                    select {fname}(<geography>'point(0 1)')
                ''',
            )

    async def test_edgeql_postgis_bulk_05(self):
        # Test in bulk all postgis functions that take the same type of
        # arguments: (box2d)
        for fname in [
            'to_box3d',
            'to_geometry',
        ]:
            await self.con.query_json(
                f'''
                    with module ext::postgis
                    select {fname}(<box2d>'box(0 1, 2 3)')
                ''',
            )

    async def test_edgeql_postgis_bulk_06(self):
        # Test in bulk all postgis functions that take the same type of
        # arguments: (box3d)

        # These are all OK running with a 'BOX3D'
        for fname in [
            'to_box2d',
            'to_geometry',
            'xmax',
            'xmin',
            'ymax',
            'ymin',
            'zmax',
            'zmin',
        ]:
            await self.con.query_json(
                f'''
                    with module ext::postgis
                    select {fname}(<box3d>"BOX3D(0 0 0, 1 2 3)")
                ''',
            )

    async def test_edgeql_postgis_bulk_07(self):
        # Test in bulk all postgis functions that take the same type of
        # arguments: (geometry, geometry)

        # These are all OK running with 2 points
        for fname in [
            'angle',
            'azimuth',
            'closestpoint',
            'closestpoint3d',
            'collect',
            'contains',
            'containsproperly',
            'coveredby',
            'covers',
            'crosses',
            'disjoint',
            'distance',
            'distance3d',
            'distancesphere',
            'distancespheroid',
            'equals',
            'geometry_cmp',
            'hausdorffdistance',
            'intersects',
            'intersects3d',
            'longestline',
            'longestline3d',
            'makebox2d',
            'makebox3d',
            'makeline',
            'maxdistance',
            'maxdistance3d',
            'orderingequals',
            'overlaps',
            'relate',
            'scale',
            'shortestline',
            'shortestline3d',
            'symmetricdifference',
            'touches',
            'within',
        ]:
            await self.con.query_json(
                f'''
                    with module ext::postgis
                    select {fname}(
                        <geometry>'point(0 1)',
                        <geometry>'point(2 3)',
                    )
                ''',
            )

    # XXX: next batch to test for (geometry, geometry)
        # for fname in [
        #     'addpoint',
        #     'closestpointofapproach',
        #     'distancecpa',
        #     'interpolatepoint',
        #     'linecrossingdirection',
        #     'linelocatepoint',
        #     'scroll',
        #     'sharedpaths',
        #     'split',
        #     'union',
        # ]:

    # XXX: quick way to find all the functions that need special treatment in batch
    # async def test_edgeql_postgis_bulk_xxx(self):
    #     bad = []
    #     for fname in [
    #         'area',
    #         'area2d',
    #         'asbinary',
    #         'asewkb',
    #         'asewkt',
    #         'ashexewkb',
    #         'astext',
    #         'boundary',
    #         'buildarea',
    #         'bytea',
    #         'centroid',
    #         'cleangeometry',
    #         'collectionextract',
    #     ]:
    #         async with self._run_and_rollback():
    #             try:
    #                 await self.con.query_json(
    #                     f'''
    #                         with module ext::postgis
    #                         select {fname}(<geometry>'point(0 1)')
    #                     ''',
    #                 )
    #             except Exception:
    #                 bad.append(fname)

    #     if bad:
    #         raise Exception(bad)
