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


import decimal
import math
import pprint
import uuid

from datetime import timedelta

import edgedb


class bag(list):
    """Wrapper for list that tells assert_query_result to ignore order"""
    pass


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


def assert_data_shape(data, shape, fail, message=None):
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
                f'{message}: expected list '
                f'{_format_path(path)}')

        if not data and shape:
            fail(
                f'{message}: expected non-empty list '
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
            if not math.isclose(data, shape, rel_tol=1e-04, abs_tol=1e-15):
                fail(
                    f'{message}: not isclose({data}, {shape}) '
                    f'{_format_path(path)}')
        elif isinstance(shape, uuid.UUID):
            # since the data comes from JSON, it will only have a str
            if data != str(shape):
                fail(
                    f'{message}: {data!r} != {shape!r} '
                    f'{_format_path(path)}')
        elif isinstance(shape, (str, int, bytes, timedelta,
                                decimal.Decimal, edgedb.RelativeDuration)):
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
