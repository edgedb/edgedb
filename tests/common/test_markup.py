#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2011-present MagicStack Inc. and the EdgeDB authors.
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


import collections
import unittest

from edb.common import markup
from edb.common.markup.format import xrepr
from edb.common.struct import Field


class SpecialList(list):
    pass


class _SpecialListNode(markup.elements.base.Markup):
    pass


class SpecialListNode(_SpecialListNode):
    node = Field(_SpecialListNode, default=None)


@markup.serializer.serializer.register(SpecialList)
def serialize_special(obj, *, ctx):
    if obj and isinstance(obj[0], SpecialList):
        child = markup.serialize(obj[0], ctx=ctx)
        return SpecialListNode(node=child)
    else:
        return SpecialListNode()


class MarkupTests(unittest.TestCase):
    def _get_test_markup(self):
        def foobar():
            raise ValueError('foobar: spam ham!')

        exc = None

        try:
            foobar()
        except Exception as ex:
            exc = ex

        return markup.serialize(exc, ctx=markup.Context())

    def test_utils_markup_dumps(self):
        assert markup.dumps('123') == "'123'"

        expected = \
            "[\n    '123',\n    1,\n    1.1,\n    {\n        foo: ()\n    }\n]"
        expected = expected.replace(' ', '')
        assert markup.dumps(['123', 1, 1.1, {'foo': ()}]).replace(
            ' ', '') == expected

    def test_utils_markup_overflow(self):
        obj = a = []
        for _ in range(200):
            a.append([])
            a = a[0]

        result = markup.dumps(obj).replace(' ', '').replace('\n', '')

        # current limit is 100, so 2 chars per list - 200 + some space reserved
        # for the OverflowBarier markup element
        #
        assert len(result) < 220

    def test_utils_markup_overflow_deep_1(self):
        obj = a = []
        for _ in range(200):
            a.append([])
            a = a[0]

        result = markup.dumps(obj).replace(' ', '').replace('\n', '')

        # current limit is 100, so 2 chars per list - 200 + some space reserved
        # for the OverflowBarier markup element
        #
        assert len(result) < 220

    def test_utils_markup_overflow_deep_2(self):
        assert isinstance(
            markup.elements.base.OverflowBarier(),
            markup.elements.lang.TreeNode)
        assert issubclass(
            markup.elements.base.OverflowBarier, markup.elements.lang.TreeNode)
        assert isinstance(
            markup.elements.base.SerializationError(text='1', cls='1'),
            markup.elements.lang.TreeNode)
        assert issubclass(
            markup.elements.base.SerializationError,
            markup.elements.lang.TreeNode)
        assert not isinstance(
            markup.elements.base.Markup(), markup.elements.lang.TreeNode)
        assert not issubclass(
            markup.elements.base.Markup, markup.elements.lang.TreeNode)

        from edb.common.markup.serializer.base \
            import OVERFLOW_BARIER, Context

        def gen(deep):
            if deep > 0:
                return SpecialList([gen(deep - 1)])

        assert not str(
            markup.serialize(gen(OVERFLOW_BARIER - 1), ctx=Context())).count(
                'Overflow')
        assert str(markup.serialize(gen(OVERFLOW_BARIER + 10), ctx=Context(
        ))).count('Overflow') == 1
        assert not str(
            markup.serialize(gen(OVERFLOW_BARIER + 10), ctx=Context())).count(
                'SerializationError')

    def test_utils_markup_overflow_wide(self):
        obj3 = []
        for _ in range(10):
            obj2 = []
            for _ in range(10):
                obj1 = []
                for _ in range(10):
                    obj = []
                    for _ in range(20):
                        obj.append(list(1 for _ in range(10)))
                    obj1.append(obj)
                obj2.append(obj1)
            obj3.append(obj2)

        result = markup.dumps(obj3).replace(' ', '').replace('\n', '')
        assert len(result) < 13000

    def test_utils_markup_format_xrepr(self):
        a = '1234567890'

        assert xrepr(a) == repr(a)

        assert xrepr(a, max_len=5) == "''..."
        assert xrepr(a, max_len=7) == "'12'..."
        assert xrepr(a, max_len=12) == repr(a)

        assert repr(repr) == '<built-in function repr>'

        assert xrepr(repr) == repr(repr)
        assert xrepr(repr, max_len=10) == '<built>...'
        assert xrepr(repr, max_len=100) == repr(repr)

        assert len(xrepr(repr, max_len=10)) == 10

    def test_utils_markup_dump_ordereddict(self):
        obj = collections.OrderedDict([[1, 2], [2, 3], [3, 4], [5, 6]])
        result = ''.join(markup.dumps(obj).split())
        assert result == '{1:2,2:3,3:4,5:6}'
