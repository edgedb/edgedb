#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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


from __future__ import annotations


import datetime
import decimal
import math
import pprint
import uuid
import unittest

import edgedb


class bag(list):
    """Wrapper for list that tells assert_query_result to ignore order"""
    def __repr__(self):
        return f'bag({list.__repr__(self)})'


def sort_results(results, sort):
    if sort is True:
        sort = lambda x: x
    # don't bother sorting empty things
    if results:
        # sort can be either a key function or a dict
        if isinstance(sort, dict):
            # the keys in the dict indicate the fields that
            # actually must be sorted
            for key, val in sort.items():
                # '.' is a special key referring to the base object
                if key == '.':
                    sort_results(results, val)
                else:
                    if isinstance(results, list):
                        for r in results:
                            sort_results(r[key], val)
                    else:
                        sort_results(results[key], val)

        else:
            results.sort(key=sort)


def assert_data_shape(
    data, shape, fail,
    message=None, from_sql=False, rel_tol=None, abs_tol=None,
):
    try:
        import asyncpg
        from asyncpg import types as pgtypes
    except ImportError:
        if from_sql:
            raise unittest.SkipTest(
                'SQL tests skipped: asyncpg not installed')

    base_fail = fail
    rel_tol = 1e-04 if rel_tol is None else rel_tol
    abs_tol = 1e-15 if abs_tol is None else abs_tol

    def fail(msg):
        base_fail(f'{msg}\nshape: {shape!r}\ndata: {data!r}')

    _void = object()

    def _format_path(path):
        if path:
            return 'PATH: ' + ''.join(str(p) for p in path)
        else:
            return 'PATH: <top-level>'

    def _assert_type_shape(path, data, shape):
        if shape in (int, float):
            if not isinstance(data, shape):
                fail(
                    f'{message}: expected {shape}, got {data!r} '
                    f'{_format_path(path)}')
        else:
            try:
                shape(data)
            except (ValueError, TypeError):
                fail(
                    f'{message}: expected {shape}, got {data!r} '
                    f'{_format_path(path)}')

    def _assert_dict_shape(path, data, shape):
        if not isinstance(data, dict):
            fail(
                f'{message}: expected dict '
                f'{_format_path(path)}')

        # TODO: should we also check that there aren't *extra* keys
        # (other than id, __tname__?)
        for sk, sv in shape.items():
            if not data or sk not in data:
                fail(
                    f'{message}: key {sk!r} '
                    f'is missing\n{pprint.pformat(data)} '
                    f'{_format_path(path)}')

            _assert_generic_shape(path + (f'["{sk}"]',), data[sk], sv)

    def _list_shape_iter(shape):
        last_shape = _void

        for item in shape:
            if item is Ellipsis:
                if last_shape is _void:
                    raise ValueError(
                        'invalid shape spec: Ellipsis cannot be the'
                        'first element')

                while True:
                    yield last_shape

            last_shape = item

            yield item

    def _assert_list_shape(path, data, shape):
        if not isinstance(data, (list, tuple)):
            fail(
                f'{message}: expected list got {type(data)} '
                f'{_format_path(path)}')

        if not data and shape:
            fail(
                f'{message}: expected non-empty list got {type(data)} '
                f'{_format_path(path)}')

        shape_iter = _list_shape_iter(shape)

        _data_count = 0
        for _data_count, el in enumerate(data):
            try:
                el_shape = next(shape_iter)
            except StopIteration:
                fail(
                    f'{message}: unexpected trailing elements in list '
                    f'{_format_path(path)}')

            _assert_generic_shape(
                path + (f'[{_data_count}]',),
                el,
                el_shape)

        if len(shape) > _data_count + 1:
            if shape[_data_count + 1] is not Ellipsis:
                fail(
                    f'{message}: expecting more elements in list '
                    f'{_format_path(path)}')

    def _assert_set_shape(path, data, shape):
        if not isinstance(data, (list, set)):
            fail(
                f'{message}: expected list or set '
                f'{_format_path(path)}')

        if not data and shape:
            fail(
                f'{message}: expected non-empty set '
                f'{_format_path(path)}')

        shape_iter = _list_shape_iter(sorted(shape))

        _data_count = 0
        for _data_count, el in enumerate(sorted(data)):
            try:
                el_shape = next(shape_iter)
            except StopIteration:
                fail(
                    f'{message}: unexpected trailing elements in set '
                    f'[path {_format_path(path)}]')

            _assert_generic_shape(
                path + (f'{{{_data_count}}}',), el, el_shape)

        if len(shape) > _data_count + 1:
            if Ellipsis not in shape:
                fail(
                    f'{message}: expecting more elements in set '
                    f'{_format_path(path)}')

    def _assert_bag_shape(path, data, shape):
        # A bag is treated like a set except that we want it to work
        # on objects, which can't be hashed or sorted.

        if not isinstance(data, (list, set)):
            fail(
                f'{message}: expected list or set '
                f'{_format_path(path)}')

        if Ellipsis in shape:
            raise ValueError(
                f"{message}: can't use ellipsis in set/bag shape")

        data = list(data)

        if len(data) > len(shape):
            fail(
                f'{message}: too many elements in list '
                f'{_format_path(path)}')

        # this is all very O(n^2) but n should be small
        for el_shape in shape:
            for data_count, el in enumerate(data):
                try:
                    _assert_generic_shape(
                        path + (f'[{data_count}]',),
                        el,
                        el_shape)
                except AssertionError:
                    # oh well
                    pass
                else:
                    data.pop(data_count)
                    break
            else:
                fail(
                    f'{message}: missing elements in list '
                    f'{_format_path(path)}: {el_shape!r}')

    def _assert_generic_shape(path, data, shape):
        if from_sql:
            if isinstance(shape, bag):
                return _assert_bag_shape(path, data, shape)
            elif isinstance(shape, list):
                # NULL is acceptable substitute for the empty set, so we'll
                # assume that in our tests None satisfies the [] expected
                # result.
                if data is not None or len(shape) > 0:
                    return _assert_list_shape(path, data, shape)
            elif isinstance(shape, tuple):
                assert isinstance(data, asyncpg.Record)
                return _assert_list_shape(
                    path, [d for d in data.values()], shape)
            elif isinstance(shape, set):
                return _assert_set_shape(path, data, shape)
            elif isinstance(shape, dict):
                assert isinstance(data, asyncpg.Record)

                # If the record has "target" pop the "id" from the expected
                # results as we expect it to be a "target" duplicate.
                rec = {k: v for k, v in data.items()}
                if 'target' in rec:
                    if 'id' in shape and shape['id'] == shape.get('target'):
                        shape.pop('id')

                return _assert_dict_shape(path, rec, shape)
            elif isinstance(shape, type):
                return _assert_type_shape(path, data, shape)
            elif isinstance(shape, float):
                if not math.isclose(data, shape,
                                    rel_tol=rel_tol, abs_tol=abs_tol):
                    fail(
                        f'{message}: not isclose({data}, {shape}) '
                        f'{_format_path(path)}')
            elif isinstance(shape, uuid.UUID):
                # If data comes from SQL, we expect UUID.
                if data != shape:
                    fail(
                        f'{message}: {data!r} != {shape!r} '
                        f'{_format_path(path)}')
            elif isinstance(shape, (str, int, bytes, datetime.timedelta,
                                    decimal.Decimal)):
                if data != shape:
                    fail(
                        f'{message}: {data!r} != {shape!r} '
                        f'{_format_path(path)}')
            elif isinstance(shape, edgedb.RelativeDuration):
                if data != datetime.timedelta(
                    days=shape.months * 30 + shape.days,
                    microseconds=shape.microseconds,
                ):
                    fail(
                        f'{message}: {data!r} != {shape!r} '
                        f'{_format_path(path)}')
            elif isinstance(shape, edgedb.DateDuration):
                if data != datetime.timedelta(
                    days=shape.months * 30 + shape.days,
                ):
                    fail(
                        f'{message}: {data!r} != {shape!r} '
                        f'{_format_path(path)}')
            elif isinstance(shape, edgedb.Range):
                if data != pgtypes.Range(
                    lower=shape.lower,
                    upper=shape.upper,
                    lower_inc=shape.inc_lower,
                    upper_inc=shape.inc_upper,
                    empty=shape.is_empty(),
                ):
                    fail(
                        f'{message}: {data!r} != {shape!r} '
                        f'{_format_path(path)}')
            elif isinstance(shape, edgedb.EnumValue):
                if data != str(shape):
                    fail(
                        f'{message}: {data!r} != {shape!r} '
                        f'{_format_path(path)}')
            elif shape is None:
                if data is not None:
                    fail(
                        f'{message}: {data!r} is expected to be None '
                        f'{_format_path(path)}')
            else:
                if data != shape:
                    fail(
                        f'{message}: ({type(data)}) {data!r} != '
                        f'({type(shape)}) {shape!r} '
                        f'{_format_path(path)}')

        else:
            if isinstance(shape, bag):
                return _assert_bag_shape(path, data, shape)
            elif isinstance(shape, (list, tuple)):
                return _assert_list_shape(path, data, shape)
            elif isinstance(shape, set):
                return _assert_set_shape(path, data, shape)
            elif isinstance(shape, dict):
                return _assert_dict_shape(path, data, shape)
            elif isinstance(shape, type):
                return _assert_type_shape(path, data, shape)
            elif isinstance(shape, float):
                if not math.isclose(data, shape,
                                    rel_tol=rel_tol, abs_tol=abs_tol):
                    fail(
                        f'{message}: not isclose({data}, {shape}) '
                        f'{_format_path(path)}')
            elif isinstance(shape, uuid.UUID):
                # We expect a str from JSON.
                if data != str(shape):
                    fail(
                        f'{message}: {data!r} != {shape!r} '
                        f'{_format_path(path)}')
            elif isinstance(shape, (str, int, bytes, datetime.timedelta,
                                    decimal.Decimal)):
                if data != shape:
                    fail(
                        f'{message}: {data!r} != {shape!r} '
                        f'{_format_path(path)}')
            elif isinstance(shape, edgedb.RelativeDuration):
                if data != shape:
                    fail(
                        f'{message}: {data!r} != {shape!r} '
                        f'{_format_path(path)}')
            elif isinstance(shape, edgedb.DateDuration):
                if data != shape:
                    fail(
                        f'{message}: {data!r} != {shape!r} '
                        f'{_format_path(path)}')
            elif shape is None:
                if data is not None:
                    fail(
                        f'{message}: {data!r} is expected to be None '
                        f'{_format_path(path)}')
            else:
                raise ValueError(f'unsupported shape type {shape}')

    message = message or 'data shape differs'
    return _assert_generic_shape((), data, shape)
