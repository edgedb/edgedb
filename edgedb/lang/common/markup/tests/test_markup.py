##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import markup


class TestUtilsMarkup:
    def test_utils_markup_rendrers_dhtml(self):
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
