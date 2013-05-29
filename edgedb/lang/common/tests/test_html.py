##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils import html


def test_utils_html_unescape():
    assert html.unescape('') == ''
    assert html.unescape('foo & bar') == 'foo & bar'
    assert html.unescape('&lt;hello&gt;') == '<hello>'
