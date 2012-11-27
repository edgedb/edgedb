##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import markup
from semantix.utils.markup.format import xrepr


from semantix.utils.datastructures import Field
class SpecialList(list): pass
class _SpecialListNode(markup.elements.base.Markup):
    pass
class SpecialListNode(_SpecialListNode):
    node = Field(_SpecialListNode, default=None)


@markup.serializer.serializer(handles=SpecialList)
def serialize_special(obj, *, ctx):
    if obj and isinstance(obj[0], SpecialList):
        child = markup.serialize(obj[0], ctx=ctx)
        return SpecialListNode(node=child)
    else:
        return SpecialListNode()


class TestUtilsMarkup:
    def _get_test_markup(self):
        def foobar():
            raise ValueError('foobar: spam ham!')

        exc = None

        try:
            foobar()
        except Exception as ex:
            exc = ex

        return markup.serialize(exc, ctx=markup.Context())

    def test_utils_markup_renderers_dhtml(self):
        from semantix.utils.markup.renderers import dhtml

        html = dhtml.render(self._get_test_markup())

        assert 'foobar: spam ham!' in html
        assert 'ValueError' in html

    def test_utils_markup_renderers_json(self):
        from semantix.utils.markup.renderers import json

        rendered = json.render(self._get_test_markup())
        assert 'foobar: spam ham!' in rendered
        assert 'ValueError' in rendered

        rendered = json.render(self._get_test_markup(), as_bytes=True)
        assert b'foobar: spam ham!' in rendered
        assert b'ValueError' in rendered

    def test_utils_markup_dumps(self):
        assert markup.dumps('123') == "'123'"

        expected = "[\n    '123',\n    1,\n    1.1,\n    {\n        foo: []\n    }\n]"
        expected = expected.replace(' ', '')
        assert markup.dumps(['123', 1, 1.1, {'foo': ()}]).replace(' ', '') == expected

    def test_utils_markup_overflow(self):
        obj = a = []
        for _ in range(200):
            a.append([])
            a = a[0]

        result = markup.dumps(obj).replace(' ', '').replace('\n', '')

        # current limit is 100, so 2 chars per list - 200 + some space reserved for
        # the OverflowBarier markup element
        #
        assert len(result) < 220

    def test_utils_markup_overflow_deep_1(self):
        obj = a = []
        for _ in range(200):
            a.append([])
            a = a[0]

        result = markup.dumps(obj).replace(' ', '').replace('\n', '')

        # current limit is 100, so 2 chars per list - 200 + some space reserved for
        # the OverflowBarier markup element
        #
        assert len(result) < 220

    def test_utils_markup_overflow_deep_2(self):
        assert isinstance(markup.elements.base.OverflowBarier(), markup.elements.lang.TreeNode)
        assert issubclass(markup.elements.base.OverflowBarier, markup.elements.lang.TreeNode)
        assert isinstance(markup.elements.base.SerializationError(text='1', cls='1'),
                          markup.elements.lang.TreeNode)
        assert issubclass(markup.elements.base.SerializationError, markup.elements.lang.TreeNode)
        assert not isinstance(markup.elements.base.Markup(), markup.elements.lang.TreeNode)
        assert not issubclass(markup.elements.base.Markup, markup.elements.lang.TreeNode)

        from semantix.utils.markup.serializer.base import OVERFLOW_BARIER, Context

        def gen(deep):
            if deep > 0:
                return SpecialList([gen(deep-1)])

        assert not str(markup.serialize(gen(OVERFLOW_BARIER-1), ctx=Context())).count('Overflow')
        assert str(markup.serialize(gen(OVERFLOW_BARIER+10), ctx=Context())).count('Overflow') == 1

    def test_utils_markup_overflow_wide(self):
        obj3 = []
        for i in range(10):
            obj2 = []
            for j in range(10):
                obj1 = []
                for k in range(10):
                    obj = []
                    for l in range(20):
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
