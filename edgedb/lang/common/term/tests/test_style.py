##
# Copyright (c) 2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.common.debug import assert_raises
from edgedb.lang.common.term import Style16, Style256


class TestUtilsIOTermStyle:
    def test_utils_term_style16(self):
        s = Style16(color='red', bgcolor='green', bold=True)

        assert s.color == 'red'
        assert s.bgcolor == 'green'

        assert s.bold
        s.bold = False
        assert not s.bold
        s.underline = True
        assert s.underline

        s.color = 'yellow'
        assert s.color == 'yellow'

        with assert_raises(ValueError, error_re='unknown color'):
            s.color = '#FFF'

        assert not s.empty
        assert Style16().empty

    def test_utils_term_style256(self):
        assert Style256(color='red')._color == 196
        assert Style256(color='#FF0000')._color == 196
        assert Style256(color='#FE0000')._color == 196
        assert Style256(color='darkmagenta')._color == 90

        with assert_raises(ValueError, error_re='Unknown color'):
            Style256(color='foooocolor')
