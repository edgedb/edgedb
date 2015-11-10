##
# Copyright (c) 2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""HTML-related utility functions collection"""


import html


try:
    unescape = html.unescape
except AttributeError:
    import html.parser

    def unescape(s):
        """Unescapes HTML-escaped strings:

        ..code-block:: pycon

            >>> html.unescape('&lt;hello&gt;')
            <hello>
        """

        return html.parser.HTMLParser.unescape(None, s)
