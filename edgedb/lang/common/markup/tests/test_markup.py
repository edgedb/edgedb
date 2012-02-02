##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import markup


class TestUtilsMarkup:
    def test_utils_markup_renderers_dhtml(self):
        from semantix.utils.markup.renderers import dhtml

        def foobar():
            raise ValueError('foobar: spam ham!')

        exc = None

        try:
            foobar()
        except Exception as ex:
            exc = ex


        html = dhtml.render(markup.serialize(exc))

        assert 'foobar: spam ham!' in html
        assert 'ValueError' in html

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
