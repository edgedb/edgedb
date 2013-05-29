##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""HTML-related utility functions collection"""


import html.parser


def unescape(s):
    """Unescapes HTML-escaped strings:

    ..code-block:: pycon

        >>> html.unescape('&lt;hello&gt;')
        <hello>
    """

    return html.parser.HTMLParser.unescape(None, s)
