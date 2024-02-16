#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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
import os

import edgedb

from edb.testbase import server as tb
from edb.tools import test


class TestEdgeQLVector(tb.QueryTestCase):
    EXTENSIONS = ['pgvector']
    BACKEND_SUPERUSER = True

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'pgvector.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'pgvector_setup.edgeql')

    @classmethod
    def get_setup_script(cls):
        res = super().get_setup_script()

        # HACK: As a debugging cycle hack, when RELOAD is true, we reload the
        # extension package from the file, so we can test without a bootstrap.
        RELOAD = False

        if RELOAD:
            root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            with open(os.path.join(root, 'edb/lib/ext/pgvector.edgeql')) as f:
                contents = f.read()
            to_add = '''
                drop extension package pgvector version '0.4.0';
            ''' + contents
            splice = '__internal_testmode := true;'
            res = res.replace(splice, splice + to_add)

        return res

    async def test_edgeql_vector_cast_01(self):
        # Basic casts to and from json. Also a cast into an
        # array<float32>.
        await self.assert_query_result(
            '''
                select <json><ext::pgvector::vector>[1, 2, 3.5];
            ''',
            [[1, 2, 3.5]],
            json_only=True,
        )

        await self.assert_query_result(
            '''
                select <array<float32>><ext::pgvector::vector>
                  to_json('[1.5, 2, 3]');
            ''',
            [[1.5, 2, 3]],
        )

    async def test_edgeql_vector_cast_02(self):
        # Basic casts from json.
        await self.assert_query_result(
            '''
                select <array<float32>><ext::pgvector::vector>to_json((
                    select _ := Basic.p_str order by _
                ))
            ''',
            [[0, 1, 2.3], [1, 1, 10.11], [4.5, 6.7, 8.9]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>><ext::pgvector::vector>(
                    select _ := Basic.p_json order by _
                )
            ''',
            [[0, 1, 2.3], [1, 1, 10.11], [4.5, 6.7, 8.9]],
        )

    async def test_edgeql_vector_cast_03(self):
        # Casts from numeric array expressions.
        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::vector><array<int16>>[1, 2, 3];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::vector><array<int32>>[1, 2, 3];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::vector><array<int64>>[1.0, 2.0, 3.0];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::vector><array<float32>>[1.5, 2, 3];
            ''',
            [[1.5, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::vector><array<float64>>[1, 2, 3];
            ''',
            [[1, 2, 3]],
        )

    async def test_edgeql_vector_cast_04(self):
        # Casts from numeric array expressions.
        res = [0, 3, 4, 7]
        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::vector>array_agg(
                        Raw.p_int16 order by Raw.p_int16
                    );
            ''',
            [res],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::vector>array_agg(
                        Raw.p_int32 order by Raw.p_int32
                    );
            ''',
            [res],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::vector>array_agg(
                        Raw.p_int64 order by Raw.p_int64
                    );
            ''',
            [res],
        )

    async def test_edgeql_vector_cast_05(self):
        # Casts from numeric array expressions.
        res = [0, 3, 4.25, 6.75]
        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::vector>array_agg(
                        Raw.p_float32 order by Raw.p_float32
                    );
            ''',
            [res],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::vector>array_agg(
                        Raw.val order by Raw.val
                    );
            ''',
            [res],
        )

    async def test_edgeql_vector_cast_06(self):
        # Casts from literal numeric arrays.
        await self.assert_query_result(
            '''
                select <array<float32>><ext::pgvector::vector>[1, 2, 3];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>><ext::pgvector::vector>
                    [<int16>1, <int16>2, <int16>3];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>><ext::pgvector::vector>
                    [<int32>1, <int32>2, <int32>3];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>><ext::pgvector::vector>[1.5, 2, 3];
            ''',
            [[1.5, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>><ext::pgvector::vector>
                    [<float32>1.5, <float32>2, <float32>3];
            ''',
            [[1.5, 2, 3]],
        )

    async def test_edgeql_vector_cast_07(self):
        await self.assert_query_result(
            '''
                select <array<float32>><v3>[11, 3, 4];
            ''',
            [[11, 3, 4]],
        )

    async def test_edgeql_vector_cast_08(self):
        # Casts from arrays of derived types.
        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::vector><array<myf64>>[1, 2.3, 4.5];
            ''',
            [[1, 2.3, 4.5]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::vector><array<deepf64>>[1, 2.3, 4.5];
            ''',
            [[1, 2.3, 4.5]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::vector>array_agg(<myf64>{1, 2.3, 4.5});
            ''',
            [[1, 2.3, 4.5]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::vector>array_agg(<deepf64>{1, 2.3, 4.5});
            ''',
            [[1, 2.3, 4.5]],
        )

    async def test_edgeql_vector_cast_09(self):
        # Casts from arrays of derived types.
        res = [0, 3, 4.25, 6.75]
        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::vector>array_agg(
                        Raw.p_myf64 order by Raw.p_myf64
                    );
            ''',
            [res],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::vector>array_agg(
                        Raw.p_deepf64 order by Raw.p_deepf64
                    );
            ''',
            [res],
        )

    async def test_edgeql_vector_cast_10(self):
        # Arrays of vectors.
        await self.assert_query_result(
            '''
                with module ext::pgvector
                select [
                    <vector>[0, 1],
                    <vector>[2, 3],
                    <vector>[4, 5, 6],
                ]
            ''',
            [[[0, 1], [2, 3], [4, 5, 6]]],
            json_only=True,
        )

        # Arrays of vectors.
        await self.assert_query_result(
            '''
                with module ext::pgvector
                select <json>[
                    <vector>[0, 1],
                    <vector>[2, 3],
                    <vector>[4, 5, 6],
                ]
            ''',
            [[[0, 1], [2, 3], [4, 5, 6]]],
            json_only=True,
        )

    async def test_edgeql_vector_cast_11(self):
        # Vectors in tuples.
        await self.assert_query_result(
            '''
                with module ext::pgvector
                select (
                    <vector>[0, 1],
                    <vector>[2, 3],
                    <vector>[4, 5, 6],
                )
            ''',
            [[[0, 1], [2, 3], [4, 5, 6]]],
            json_only=True,
        )

    async def test_edgeql_vector_op_01(self):
        # Comparison operators.
        await self.assert_query_result(
            '''
                select <ext::pgvector::vector>[1, 2, 3] =
                    <ext::pgvector::vector>[0, 1, 1];
            ''',
            [False],
        )

        await self.assert_query_result(
            '''
                select <ext::pgvector::vector>[1, 2, 3] !=
                    <ext::pgvector::vector>[0, 1, 1];
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                select <ext::pgvector::vector>[1, 2, 3] ?=
                    <ext::pgvector::vector>{};
            ''',
            [False],
        )

        await self.assert_query_result(
            '''
                select <ext::pgvector::vector>[1, 2, 3] ?!=
                    <ext::pgvector::vector>{};
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                select <ext::pgvector::vector>{} ?=
                    <ext::pgvector::vector>{};
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                select <ext::pgvector::vector>[1, 2] <
                    <ext::pgvector::vector>[2, 3];
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                select <ext::pgvector::vector>[1, 2] <=
                    <ext::pgvector::vector>[2, 3];
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                select <ext::pgvector::vector>[1, 2] >
                    <ext::pgvector::vector>[2, 3];
            ''',
            [False],
        )

        await self.assert_query_result(
            '''
                select <ext::pgvector::vector>[1, 2] >=
                    <ext::pgvector::vector>[2, 3];
            ''',
            [False],
        )

    async def test_edgeql_vector_op_02(self):
        await self.assert_query_result(
            '''
                with module ext::pgvector
                select <vector>to_json('[3, 0]') in {
                    <vector>to_json('[1, 2]'),
                    <vector>to_json('[3, 4]'),
                };
            ''',
            [False],
        )

        await self.assert_query_result(
            '''
                with module ext::pgvector
                select <vector>to_json('[3, 0]') not in {
                    <vector>to_json('[1, 2]'),
                    <vector>to_json('[3, 4]'),
                };
            ''',
            [True],
        )

    @test.xerror("len will eventually be a trait function or something")
    async def test_edgeql_vector_func_01(self):
        await self.assert_query_result(
            '''
                with module ext::pgvector
                select len(
                    <vector>to_json('[1.2, 3.4, 5, 6]'),
                );
            ''',
            [4],
        )

        await self.assert_query_result(
            '''
                with module ext::pgvector
                select len(default::IVFFlat_L2.vec) limit 1;
            ''',
            [3],
        )

    async def test_edgeql_vector_func_02(self):
        await self.assert_query_result(
            '''
                with module ext::pgvector
                select euclidean_distance(
                    <vector>[3, 4],
                    <vector>[0, 0],
                );
            ''',
            [5],
        )

        await self.assert_query_result(
            '''
                with module ext::pgvector
                select euclidean_distance(
                    default::IVFFlat_L2.vec,
                    <vector>[0, 1, 0],
                );
            ''',
            {2.299999952316284, 10.159335266542493, 11.48694872607437},
        )

    async def test_edgeql_vector_func_03(self):
        await self.assert_query_result(
            '''
                with module ext::pgvector
                select euclidean_norm(<vector>[3, 4]);
            ''',
            [5],
        )

        await self.assert_query_result(
            '''
                with module ext::pgvector
                select euclidean_norm(default::IVFFlat_L2.vec);
            ''',
            {2.5079872331917934, 10.208432276239787, 12.014573942925704},
        )

    async def test_edgeql_vector_func_04(self):
        await self.assert_query_result(
            '''
                with module ext::pgvector
                select neg_inner_product(
                    <vector>[1, 2],
                    <vector>[3, 4],
                );
            ''',
            [-11],
        )

        await self.assert_query_result(
            '''
                with module ext::pgvector
                select neg_inner_product(
                    default::IVFFlat_IP.vec,
                    <vector>[3, 4, 1],
                );
            ''',
            {-6.299999952316284, -17.109999656677246, -49.19999885559082},
        )

    async def test_edgeql_vector_func_05(self):
        await self.assert_query_result(
            '''
                with module ext::pgvector
                select cosine_distance(
                    <vector>[3, 0],
                    <vector>[3, 4],
                );
            ''',
            [0.4],
        )

        await self.assert_query_result(
            '''
                with module ext::pgvector
                select cosine_distance(
                    default::IVFFlat_Cosine.vec,
                    <vector>[3, 4, 1],
                );
            ''',
            {0.5073612713543951, 0.6712965405380352, 0.19689922670600213},
        )

    @test.xerror("mean will eventually be a trait function or something")
    async def test_edgeql_vector_func_06(self):
        await self.assert_query_result(
            '''
                with module ext::pgvector
                select <array<float32>>
                    mean({
                        <vector>[3, 0],
                        <vector>[0, 4],
                    });
            ''',
            [[1.5, 2]],
        )

        await self.assert_query_result(
            '''
                with module ext::pgvector
                select <array<float32>>
                    mean(default::IVFFlat_L2.vec);
            ''',
            [[1.8333334, 2.8999999, 7.103333]],
        )

    async def test_edgeql_vector_insert_01(self):
        # Test assignment casts
        res = [0, 3, 4]
        async with self._run_and_rollback():
            await self.con.execute(r"""
                insert IVFFlat_L2 {
                    vec := array_agg(
                        Raw.p_int16 order by Raw.p_int16
                    )[:3]
                }
            """)
            await self.assert_query_result(
                '''
                    with res := <array<float32>>$res
                    select <array<float32>>(
                        select IVFFlat_L2
                        filter .vec = <ext::pgvector::vector>res
                    ).vec
                ''',
                [res],
                variables=dict(res=res),
            )

        async with self._run_and_rollback():
            await self.con.execute(r"""
                insert IVFFlat_L2 {
                    vec := array_agg(
                        Raw.p_int32 order by Raw.p_int32
                    )[:3]
                }
            """)
            await self.assert_query_result(
                '''
                    with res := <array<float32>>$res
                    select <array<float32>>(
                        select IVFFlat_L2
                        filter .vec = <ext::pgvector::vector>res
                    ).vec
                ''',
                [res],
                variables=dict(res=res),
            )

        async with self._run_and_rollback():
            await self.con.execute(r"""
                insert IVFFlat_L2 {
                    vec := array_agg(
                        Raw.p_int64 order by Raw.p_int64
                    )[:3]
                }
            """)
            await self.assert_query_result(
                '''
                    with res := <array<float32>>$res
                    select <array<float32>>(
                        select IVFFlat_L2
                        filter .vec = <ext::pgvector::vector>res
                    ).vec
                ''',
                [res],
                variables=dict(res=res),
            )

    async def test_edgeql_vector_insert_02(self):
        # Test assignment casts
        res = [0, 3, 4.25]
        async with self._run_and_rollback():
            await self.con.execute(r"""
                insert IVFFlat_L2 {
                    vec := array_agg(
                        Raw.p_float32 order by Raw.p_float32
                    )[:3]
                }
            """)
            await self.assert_query_result(
                '''
                    with res := <array<float32>>$res
                    select <array<float32>>(
                        select IVFFlat_L2
                        filter .vec = <ext::pgvector::vector>res
                    ).vec
                ''',
                [res],
                variables=dict(res=res),
            )

        async with self._run_and_rollback():
            await self.con.execute(r"""
                insert IVFFlat_L2 {
                    vec := array_agg(
                        Raw.val order by Raw.val
                    )[:3]
                }
            """)
            await self.assert_query_result(
                '''
                    with res := <array<float32>>$res
                    select <array<float32>>(
                        select IVFFlat_L2
                        filter .vec = <ext::pgvector::vector>res
                    ).vec
                ''',
                [res],
                variables=dict(res=res),
            )

    async def test_edgeql_vector_constraint_01(self):
        async with self._run_and_rollback():
            await self.con.execute(r"""
                insert Con {
                    vec := [1, 1, 2]
                }
            """)

        async with self.assertRaisesRegexTx(
            edgedb.errors.ConstraintViolationError, ''
        ):
            await self.con.execute(r"""
                insert Con {
                    vec := [1, 20, 1]
                }
            """)

    async def _assert_index_use(self, query, *args, index_type, index_op):
        def look(obj):
            if isinstance(obj, dict) and obj.get('plan_type') == "IndexScan":
                return any(
                    prop['title'] == 'index_name'
                    and f'pgvector::{index_type}_{index_op}' in prop['value']
                    for prop in obj.get('properties', [])
                )

            if isinstance(obj, dict):
                return any([look(v) for v in obj.values()])
            elif isinstance(obj, list):
                return any(look(v) for v in obj)
            else:
                return False

        plan = await self.con.query_json(f'analyze {query}', *args)
        if not look(json.loads(plan)):
            raise AssertionError(f'query did not use {index_type} index')

    async def _check_index(self, obj, func, index_type, index_op):
        # Test that we actually hit the indexes by looking at the query plans.

        obj_id = (await self.con.query_single(f"""
            insert {obj} {{
                vec := [1, 1, 2]
            }}
        """)).id
        await self.con.execute(f"""
            insert {obj} {{
                vec := [-1, -1, 2]
            }}
        """)
        embedding = [0.5, -0.1, 0.666]

        await self._assert_index_use(
            f'''
            with vec as module ext::pgvector,
                 base := (select {obj} filter .id = <uuid>$0),
            select {obj}
            filter {obj}.id != base.id
            order by vec::{func}(.vec, base.vec)
            empty last limit 5;
            ''',
            obj_id,
            index_type=index_type,
            index_op=index_op,
        )

        await self._assert_index_use(
            f'''
            with vec as module ext::pgvector
            select {obj}
            order by vec::{func}(.vec, <v3>to_json(<str>$0))
            empty last limit 5;
            ''',
            str(embedding),
            index_type=index_type,
            index_op=index_op,
        )

        await self._assert_index_use(
            f'''
            with vec as module ext::pgvector
            select {obj}
            order by vec::{func}(.vec, <v3><json>$0)
            empty last limit 5;
            ''',
            json.dumps(embedding),
            index_type=index_type,
            index_op=index_op,
        )

        await self._assert_index_use(
            f'''
            with vec as module ext::pgvector
            select {obj}
            order by vec::{func}(.vec, <v3><array<float32>>$0)
            empty last limit 5;
            ''',
            embedding,
            index_type=index_type,
            index_op=index_op,
        )

    async def test_edgeql_vector_index_01(self):
        await self._check_index(
            'IVFFlat_L2', 'euclidean_distance', 'ivfflat', 'euclidean')

    async def test_edgeql_vector_index_02(self):
        await self._check_index(
            'IVFFlat_Cosine', 'cosine_distance', 'ivfflat', 'cosine')

    async def test_edgeql_vector_index_03(self):
        await self._check_index(
            'IVFFlat_IP', 'neg_inner_product', 'ivfflat', 'ip')

    async def test_edgeql_vector_index_04(self):
        await self._check_index(
            'HNSW_L2', 'euclidean_distance', 'hnsw', 'euclidean')

    async def test_edgeql_vector_index_05(self):
        await self._check_index(
            'HNSW_Cosine', 'cosine_distance', 'hnsw', 'cosine')

    async def test_edgeql_vector_index_06(self):
        await self._check_index(
            'HNSW_IP', 'neg_inner_product', 'hnsw', 'ip')
