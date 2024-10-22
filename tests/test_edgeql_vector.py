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

    async def test_edgeql_vector_cast_12(self):
        # Vectors <-> bytes casts.
        await self.assert_query_result(
            r'''
                with module ext::pgvector
                select
                    <bytes><vector>[0, 1, 2]
                    = (
                        b'\x00\x03' ++
                        b'\x00\x00\x00\x00\x00\x00?\x80\x00\x00@\x00\x00\x00'
                    )
            ''',
            [True],
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
                select len(default::IVFFlat_vec_L2.vec) limit 1;
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
                    default::IVFFlat_vec_L2.vec,
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
                select euclidean_norm(default::IVFFlat_vec_L2.vec);
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
                    default::IVFFlat_vec_IP.vec,
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
                    default::IVFFlat_vec_Cosine.vec,
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
                    mean(default::IVFFlat_vec_L2.vec);
            ''',
            [[1.8333334, 2.8999999, 7.103333]],
        )

    async def test_edgeql_vector_func_07(self):
        await self.assert_query_result(
            '''
                with
                    module ext::pgvector,
                    x := <vector>[1, 3, 0, 6, 7]
                select (
                    <array<float32>>subvector(x, 3, 1) = [6],
                    <array<float32>>subvector(x, 3, 2) = [6, 7],
                    <array<float32>>subvector(x, 3, 20) = [6, 7],
                    <array<float32>>subvector(x, 0, 3) = [1, 3, 0],
                    <array<float32>>subvector(x, 1, 3) = [3, 0, 6],
                    <array<float32>>subvector(x, -2, 3) = [1],
                )
            ''',
            [[True, True, True, True, True, True]],
        )

    async def test_edgeql_vector_func_08(self):
        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidValueError,
            'must have at least 1 dimension',
        ):
            await self.con.execute(r"""
                with
                    module ext::pgvector,
                    x := <vector>[1, 3, 0, 6, 7]
                select <array<float32>>subvector(x, 3, 0)
            """)

        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidValueError,
            'must have at least 1 dimension',
        ):
            await self.con.execute(r"""
                with
                    module ext::pgvector,
                    x := <vector>[1, 3, 0, 6, 7]
                select <array<float32>>subvector(x, -2, 1)
            """)

        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidValueError,
            'must have at least 1 dimension',
        ):
            await self.con.execute(r"""
                with
                    module ext::pgvector,
                    x := <vector>[1, 3, 0, 6, 7]
                select <array<float32>>subvector(x, 20, 2)
            """)

    async def test_edgeql_vector_insert_01(self):
        # Test assignment casts
        res = [0, 3, 4]
        async with self._run_and_rollback():
            await self.con.execute(r"""
                insert IVFFlat_vec_L2 {
                    vec := array_agg(
                        Raw.p_int16 order by Raw.p_int16
                    )[:3]
                }
            """)
            await self.assert_query_result(
                '''
                    with res := <array<float32>>$res
                    select <array<float32>>(
                        select IVFFlat_vec_L2
                        filter .vec = <ext::pgvector::vector>res
                    ).vec
                ''',
                [res],
                variables=dict(res=res),
            )

        async with self._run_and_rollback():
            await self.con.execute(r"""
                insert IVFFlat_vec_L2 {
                    vec := array_agg(
                        Raw.p_int32 order by Raw.p_int32
                    )[:3]
                }
            """)
            await self.assert_query_result(
                '''
                    with res := <array<float32>>$res
                    select <array<float32>>(
                        select IVFFlat_vec_L2
                        filter .vec = <ext::pgvector::vector>res
                    ).vec
                ''',
                [res],
                variables=dict(res=res),
            )

        async with self._run_and_rollback():
            await self.con.execute(r"""
                insert IVFFlat_vec_L2 {
                    vec := array_agg(
                        Raw.p_int64 order by Raw.p_int64
                    )[:3]
                }
            """)
            await self.assert_query_result(
                '''
                    with res := <array<float32>>$res
                    select <array<float32>>(
                        select IVFFlat_vec_L2
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
                insert IVFFlat_vec_L2 {
                    vec := array_agg(
                        Raw.p_float32 order by Raw.p_float32
                    )[:3]
                }
            """)
            await self.assert_query_result(
                '''
                    with res := <array<float32>>$res
                    select <array<float32>>(
                        select IVFFlat_vec_L2
                        filter .vec = <ext::pgvector::vector>res
                    ).vec
                ''',
                [res],
                variables=dict(res=res),
            )

        async with self._run_and_rollback():
            await self.con.execute(r"""
                insert IVFFlat_vec_L2 {
                    vec := array_agg(
                        Raw.val order by Raw.val
                    )[:3]
                }
            """)
            await self.assert_query_result(
                '''
                    with res := <array<float32>>$res
                    select <array<float32>>(
                        select IVFFlat_vec_L2
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

        await self.con.query_single(
            'select _set_config("enable_seqscan", "off")'
        )

        plan = await self.con.query_json(f'analyze {query}', *args)
        if not look(json.loads(plan)):
            raise AssertionError(f'query did not use {index_type} index')

    async def _check_index(self, obj, func, index_type, index_op, vec_type):
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
            order by vec::{func}(.vec, <{vec_type}>to_json(<str>$0))
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
            order by vec::{func}(.vec, <{vec_type}><json>$0)
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
            order by vec::{func}(.vec, <{vec_type}><array<float32>>$0)
            empty last limit 5;
            ''',
            embedding,
            index_type=index_type,
            index_op=index_op,
        )

    async def test_edgeql_vector_index_01(self):
        await self._check_index(
            'IVFFlat_vec_L2', 'euclidean_distance', 'ivfflat', 'euclidean',
            'v3')

    async def test_edgeql_vector_index_02(self):
        await self._check_index(
            'IVFFlat_vec_Cosine', 'cosine_distance', 'ivfflat', 'cosine',
            'v3')

    async def test_edgeql_vector_index_03(self):
        await self._check_index(
            'IVFFlat_vec_IP', 'neg_inner_product', 'ivfflat', 'ip', 'v3')

    async def test_edgeql_vector_index_04(self):
        await self._check_index(
            'HNSW_vec_L2', 'euclidean_distance', 'hnsw', 'euclidean', 'v3')

    async def test_edgeql_vector_index_05(self):
        await self._check_index(
            'HNSW_vec_Cosine', 'cosine_distance', 'hnsw', 'cosine', 'v3')

    async def test_edgeql_vector_index_06(self):
        await self._check_index(
            'HNSW_vec_IP', 'neg_inner_product', 'hnsw', 'ip', 'v3')

    async def test_edgeql_vector_index_07(self):
        await self._check_index(
            'HNSW_vec_L1', 'taxicab_distance', 'hnsw', 'taxicab', 'v3')

    async def test_edgeql_vector_config(self):
        # We can't test the effects of config parameters easily, but we can
        # at least verify that they are updated as expected.

        # The pgvector configs don't seem to show up in
        # current_setting unless we've done something involving vector
        # on the connection...
        await self.con.query('''
            select <ext::pgvector::vector>[3, 4];
        ''')

        # probes
        await self.assert_query_result(
            'select <int64>_current_setting("ivfflat.probes")',
            [1],
        )

        await self.con.execute('''
            configure session
            set ext::pgvector::Config::probes := 23
        ''')

        await self.assert_query_result(
            'select <int64>_current_setting("ivfflat.probes")',
            [23],
        )

        await self.con.execute('''
            configure session
            reset ext::pgvector::Config::probes
        ''')

        await self.assert_query_result(
            'select <int64>_current_setting("ivfflat.probes")',
            [1],
        )

        # ef_search
        await self.assert_query_result(
            'select <int64>_current_setting("hnsw.ef_search")',
            [40],
        )

        await self.con.execute('''
            configure session
            set ext::pgvector::Config::ef_search := 23
        ''')

        await self.assert_query_result(
            'select <int64>_current_setting("hnsw.ef_search")',
            [23],
        )

        await self.con.execute('''
            configure session
            reset ext::pgvector::Config::ef_search
        ''')

        await self.assert_query_result(
            'select <int64>_current_setting("hnsw.ef_search")',
            [40],
        )

    async def test_edgeql_halfvec_cast_01(self):
        # Basic casts to and from json. Also a cast into an
        # array<float32>.
        await self.assert_query_result(
            '''
                select <json><ext::pgvector::halfvec>[1, 2, 3.5];
            ''',
            [[1, 2, 3.5]],
            json_only=True,
        )

        await self.assert_query_result(
            '''
                select <array<float32>><ext::pgvector::halfvec>
                  to_json('[1.5, 2, 3]');
            ''',
            [[1.5, 2, 3]],
        )

    async def test_edgeql_halfvec_cast_02(self):
        # Basic casts from json.
        await self.assert_query_result(
            '''
                select <array<float32>><ext::pgvector::halfvec>to_json((
                    select _ := Basic.p_str order by _
                ))
            ''',
            [[0, 1, 2.3], [1, 1, 10.11], [4.5, 6.7, 8.9]],
            # halfvecs will lose a fair bit of precision in convertions
            rel_tol=0.01,
        )

        await self.assert_query_result(
            '''
                select <array<float32>><ext::pgvector::halfvec>(
                    select _ := Basic.p_json order by _
                )
            ''',
            [[0, 1, 2.3], [1, 1, 10.11], [4.5, 6.7, 8.9]],
            # halfvecs will lose a fair bit of precision in convertions
            rel_tol=0.01,
        )

    async def test_edgeql_halfvec_cast_03(self):
        # Casts from numeric array expressions.
        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::halfvec><array<int16>>[1, 2, 3];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::halfvec><array<int32>>[1, 2, 3];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::halfvec><array<int64>>[1.0, 2.0, 3.0];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::halfvec><array<float32>>[1.5, 2, 3];
            ''',
            [[1.5, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::halfvec><array<float64>>[1, 2, 3];
            ''',
            [[1, 2, 3]],
        )

    async def test_edgeql_halfvec_cast_04(self):
        # Casts from numeric array expressions.
        res = [0, 3, 4, 7]
        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::halfvec>array_agg(
                        Raw.p_int16 order by Raw.p_int16
                    );
            ''',
            [res],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::halfvec>array_agg(
                        Raw.p_int32 order by Raw.p_int32
                    );
            ''',
            [res],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::halfvec>array_agg(
                        Raw.p_int64 order by Raw.p_int64
                    );
            ''',
            [res],
        )

    async def test_edgeql_halfvec_cast_05(self):
        # Casts from numeric array expressions.
        res = [0, 3, 4.25, 6.75]
        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::halfvec>array_agg(
                        Raw.p_float32 order by Raw.p_float32
                    );
            ''',
            [res],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::halfvec>array_agg(
                        Raw.val order by Raw.val
                    );
            ''',
            [res],
        )

    async def test_edgeql_halfvec_cast_06(self):
        # Casts from literal numeric arrays.
        await self.assert_query_result(
            '''
                select <array<float32>><ext::pgvector::halfvec>[1, 2, 3];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>><ext::pgvector::halfvec>
                    [<int16>1, <int16>2, <int16>3];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>><ext::pgvector::halfvec>
                    [<int32>1, <int32>2, <int32>3];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>><ext::pgvector::halfvec>[1.5, 2, 3];
            ''',
            [[1.5, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>><ext::pgvector::halfvec>
                    [<float32>1.5, <float32>2, <float32>3];
            ''',
            [[1.5, 2, 3]],
        )

    async def test_edgeql_halfvec_cast_07(self):
        await self.assert_query_result(
            '''
                select <array<float32>><v3>[11, 3, 4];
            ''',
            [[11, 3, 4]],
        )

    async def test_edgeql_halfvec_cast_08(self):
        # Casts from arrays of derived types.
        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::halfvec><array<myf64>>[1, 2.3, 4.5];
            ''',
            [[1, 2.3, 4.5]],
            # halfvecs will lose a fair bit of precision in convertions
            rel_tol=0.01,
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::halfvec><array<deepf64>>[1, 2.3, 4.5];
            ''',
            [[1, 2.3, 4.5]],
            # halfvecs will lose a fair bit of precision in convertions
            rel_tol=0.01,
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::halfvec>array_agg(<myf64>{1, 2.3, 4.5});
            ''',
            [[1, 2.3, 4.5]],
            # halfvecs will lose a fair bit of precision in convertions
            rel_tol=0.01,
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::halfvec>array_agg(<deepf64>{1, 2.3, 4.5});
            ''',
            [[1, 2.3, 4.5]],
            # halfvecs will lose a fair bit of precision in convertions
            rel_tol=0.01,
        )

    async def test_edgeql_halfvec_cast_09(self):
        # Casts from arrays of derived types.
        res = [0, 3, 4.25, 6.75]
        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::halfvec>array_agg(
                        Raw.p_myf64 order by Raw.p_myf64
                    );
            ''',
            [res],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <ext::pgvector::halfvec>array_agg(
                        Raw.p_deepf64 order by Raw.p_deepf64
                    );
            ''',
            [res],
        )

    async def test_edgeql_halfvec_cast_10(self):
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

    async def test_edgeql_halfvec_cast_11(self):
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

    async def test_edgeql_halfvec_cast_12(self):
        # Vectors <-> bytes casts.
        await self.assert_query_result(
            r'''
                with module ext::pgvector
                select
                    <bytes><halfvec>[0, 1, 2]
                    =
                    b'\x00\x03\x00\x00\x00\x00<\x00@\x00'
            ''',
            [True],
        )

    async def test_edgeql_halfvec_op_01(self):
        # Comparison operators.
        await self.assert_query_result(
            '''
                select <ext::pgvector::halfvec>[1, 2, 3] =
                    <ext::pgvector::halfvec>[0, 1, 1];
            ''',
            [False],
        )

        await self.assert_query_result(
            '''
                select <ext::pgvector::halfvec>[1, 2, 3] !=
                    <ext::pgvector::halfvec>[0, 1, 1];
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                select <ext::pgvector::halfvec>[1, 2, 3] ?=
                    <ext::pgvector::halfvec>{};
            ''',
            [False],
        )

        await self.assert_query_result(
            '''
                select <ext::pgvector::halfvec>[1, 2, 3] ?!=
                    <ext::pgvector::halfvec>{};
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                select <ext::pgvector::halfvec>{} ?=
                    <ext::pgvector::halfvec>{};
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                select <ext::pgvector::halfvec>[1, 2] <
                    <ext::pgvector::halfvec>[2, 3];
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                select <ext::pgvector::halfvec>[1, 2] <=
                    <ext::pgvector::halfvec>[2, 3];
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                select <ext::pgvector::halfvec>[1, 2] >
                    <ext::pgvector::halfvec>[2, 3];
            ''',
            [False],
        )

        await self.assert_query_result(
            '''
                select <ext::pgvector::halfvec>[1, 2] >=
                    <ext::pgvector::halfvec>[2, 3];
            ''',
            [False],
        )

    async def test_edgeql_halfvec_op_02(self):
        await self.assert_query_result(
            '''
                with module ext::pgvector
                select <halfvec>to_json('[3, 0]') in {
                    <halfvec>to_json('[1, 2]'),
                    <halfvec>to_json('[3, 4]'),
                };
            ''',
            [False],
        )

        await self.assert_query_result(
            '''
                with module ext::pgvector
                select <halfvec>to_json('[3, 0]') not in {
                    <halfvec>to_json('[1, 2]'),
                    <halfvec>to_json('[3, 4]'),
                };
            ''',
            [True],
        )

    async def test_edgeql_halfvec_func_01(self):
        await self.assert_query_result(
            '''
                with
                    module ext::pgvector,
                    x := <halfvec>[1, 3, 0, 6, 7]
                select (
                    <array<float32>>subvector(x, 3, 1) = [6],
                    <array<float32>>subvector(x, 3, 2) = [6, 7],
                    <array<float32>>subvector(x, 3, 20) = [6, 7],
                    <array<float32>>subvector(x, 0, 3) = [1, 3, 0],
                    <array<float32>>subvector(x, 1, 3) = [3, 0, 6],
                    <array<float32>>subvector(x, -2, 3) = [1],
                )
            ''',
            [[True, True, True, True, True, True]],
        )

    async def test_edgeql_halfvec_func_02(self):
        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidValueError,
            'must have at least 1 dimension',
        ):
            await self.con.execute(r"""
                with
                    module ext::pgvector,
                    x := <halfvec>[1, 3, 0, 6, 7]
                select <array<float32>>subvector(x, 3, 0)
            """)

        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidValueError,
            'must have at least 1 dimension',
        ):
            await self.con.execute(r"""
                with
                    module ext::pgvector,
                    x := <halfvec>[1, 3, 0, 6, 7]
                select <array<float32>>subvector(x, -2, 1)
            """)

        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidValueError,
            'must have at least 1 dimension',
        ):
            await self.con.execute(r"""
                with
                    module ext::pgvector,
                    x := <halfvec>[1, 3, 0, 6, 7]
                select <array<float32>>subvector(x, 20, 2)
            """)

    async def test_edgeql_halfvec_index_01(self):
        await self._check_index(
            'IVFFlat_hv_L2', 'euclidean_distance', 'ivfflat_hv', 'euclidean',
            'hv3')

    async def test_edgeql_halfvec_index_02(self):
        await self._check_index(
            'IVFFlat_hv_Cosine', 'cosine_distance', 'ivfflat_hv', 'cosine',
            'hv3')

    async def test_edgeql_halfvec_index_03(self):
        await self._check_index(
            'IVFFlat_hv_IP', 'neg_inner_product', 'ivfflat_hv', 'ip', 'hv3')

    async def test_edgeql_halfvec_index_04(self):
        await self._check_index(
            'HNSW_hv_L2', 'euclidean_distance', 'hnsw_hv', 'euclidean', 'hv3')

    async def test_edgeql_halfvec_index_05(self):
        await self._check_index(
            'HNSW_hv_Cosine', 'cosine_distance', 'hnsw_hv', 'cosine', 'hv3')

    async def test_edgeql_halfvec_index_06(self):
        await self._check_index(
            'HNSW_hv_IP', 'neg_inner_product', 'hnsw_hv', 'ip', 'hv3')

    async def test_edgeql_halfvec_index_07(self):
        await self._check_index(
            'HNSW_hv_L1', 'taxicab_distance', 'hnsw_hv', 'taxicab', 'hv3')

    async def test_edgeql_sparsevec_cast_01(self):
        # Basic casts to and from json.
        await self.assert_query_result(
            '''
                with module ext::pgvector
                select <json><sparsevec><vector>[0, 2, 3.5, 0];
            ''',
            [{"1": 2, "2": 3.5, "dim": 4}],
            json_only=True,
        )

        await self.assert_query_result(
            '''
                with module ext::pgvector
                select <array<float32>><vector><sparsevec>
                  to_json('{"dim": 4, "1": 2, "2": 3.5}');
            ''',
            [[0, 2, 3.5, 0]],
        )

        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidValueError,
            'object expected',
        ):
            await self.con.execute(r"""
                with module ext::pgvector
                select <sparsevec>to_json('[4,2]');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidValueError,
            'object expected',
        ):
            await self.con.execute(r"""
                with module ext::pgvector
                select <sparsevec>to_json('null');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidValueError,
            'missing "dim"',
        ):
            await self.con.execute(r"""
                with module ext::pgvector
                select <sparsevec>to_json('{"0": 4, "1": 2, "2": 3.5}');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidValueError,
            'unexpected key',
        ):
            await self.con.execute(r"""
                with module ext::pgvector
                select <sparsevec>to_json('{"z": 4, "dim": 2, "2": 3.5}');
            """)

    async def test_edgeql_sparsevec_cast_02(self):
        # str casts
        await self.assert_query_result(
            '''
                select <json><sv3>'{1:3.5}/3';
            ''',
            [{"1": 3.5, "dim": 3}],
            json_only=True,
        )

        await self.assert_query_result(
            '''
                select <str><sv3><ext::pgvector::vector>[0, 3.5, 0];
            ''',
            ['{1:3.5}/3'],
        )

    async def test_edgeql_sparsevec_cast_03(self):
        # str casts
        await self.assert_query_result(
            '''
                with module ext::pgvector
                select <json><sparsevec>
                    ' {   4     :   3.5e-6          }   /    5  ';
            ''',
            [{"4": 3.5e-6, "dim": 5}],
            json_only=True,
        )

        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidValueError,
            'invalid input syntax',
        ):
            await self.con.execute(r"""
                with module ext::pgvector
                select <sparsevec>'{4:2}';
            """)

        async with self.assertRaisesRegexTx(
            edgedb.errors.InvalidValueError,
            'invalid input syntax',
        ):
            await self.con.execute(r"""
                with module ext::pgvector
                select <sparsevec>'[4:2]';
            """)

    async def test_edgeql_sparsevec_cast_04(self):
        # Vectors <-> bytes casts.
        await self.assert_query_result(
            r'''
                with module ext::pgvector
                select
                    <bytes><sparsevec>'{1:5}/4'
                    = (
                        b'\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x00' ++
                        b'\x00\x00\x00\x01@\xa0\x00\x00'
                    )
            ''',
            [True],
        )

    async def _check_sv_index(self, obj, func, index_type, index_op):
        # Sparse vectors have differernt casts from regular vectors, so they
        # need different queries for this test

        obj_id = (await self.con.query_single(f"""
            insert {obj} {{
                vec := <sv3><v3>[1, 1, 0]
            }}
        """)).id
        await self.con.execute(f"""
            insert {obj} {{
                vec := <sv3><v3>[0, -1, 0]
            }}
        """)
        embedding = '{1:-0.1}/3'

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
            order by vec::{func}(.vec, <sv3><str>$0)
            empty last limit 5;
            ''',
            embedding,
            index_type=index_type,
            index_op=index_op,
        )

        await self._assert_index_use(
            f'''
            with vec as module ext::pgvector
            select {obj}
            order by vec::{func}(.vec, <sv3><json>$0)
            empty last limit 5;
            ''',
            json.dumps({'1': -0.1, 'dim': 3}),
            index_type=index_type,
            index_op=index_op,
        )

    async def test_edgeql_sparsevec_index_01(self):
        await self._check_sv_index(
            'HNSW_sv_L2', 'euclidean_distance', 'hnsw_sv', 'euclidean')

    async def test_edgeql_sparsevec_index_02(self):
        await self._check_sv_index(
            'HNSW_sv_Cosine', 'cosine_distance', 'hnsw_sv', 'cosine')

    async def test_edgeql_sparsevec_index_03(self):
        await self._check_sv_index(
            'HNSW_sv_IP', 'neg_inner_product', 'hnsw_sv', 'ip')

    async def test_edgeql_sparsevec_index_04(self):
        await self._check_sv_index(
            'HNSW_sv_L1', 'taxicab_distance', 'hnsw_sv', 'taxicab')


class TestEdgeQLVectorExtension(tb.QueryTestCase):
    BACKEND_SUPERUSER = True
    TRANSACTION_ISOLATION = False

    async def test_edgeql_vector_drop_extension_with_func_cache(self):
        await self.con.execute("create extension pgvector")
        try:
            # Run many times to wait for the func cache creation
            for _i in range(64):
                await self.con.query(
                    '''
                        select <ext::pgvector::vector>[4.2];
                    '''
                )
        finally:
            # this should drop the cache function of the query above as well
            await self.con.execute("drop extension pgvector")
