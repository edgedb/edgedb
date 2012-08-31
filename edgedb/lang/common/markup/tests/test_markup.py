##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import markup
from semantix.utils.markup.format import xrepr


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

    def test_utils_markup_overflow_deep(self):
        obj = a = []
        for _ in range(200):
            a.append([])
            a = a[0]

        result = markup.dumps(obj).replace(' ', '').replace('\n', '')

        # current limit is 100, so 2 chars per list - 200 + some space reserved for
        # the OverflowBarier markup element
        #
        assert len(result) < 220

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
