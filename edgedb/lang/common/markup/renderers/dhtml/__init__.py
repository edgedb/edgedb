##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os

from semantix.rendering.css import dumps as scss_dumps
from .. import json
from ... import serialize


__all__ = 'render',


#: Error page favicon
#: From Fugue Icons Set
#: (C) 2012 Yusuke Kamiyamane. All rights reserved.
#: The icon is licensed under a Creative Commons Attribution 3.0 License.
_FAVICON = ('data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAA'
            'AAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAkJJREFUeNqkUztv2l'
            'AUPjbG2FAesUoHpDIUoioZInVCShaydImSIWvV/ojMHTJ0ZuovaNUVVWJItkx9RM2Wwa'
            'oCIkoRBQQGbPD70XPMQ27UTrnSZ597zv2+c859MEEQwEMGQwIfGQZiOCEwAAXEMZq7iO'
            'Jy3R3iK6aqI7oeTgivkXtf4JAXxZNnOzvlwuamJGYyAsUNVTW7NzdK+/q6aRtGDcmNfw'
            'kcPsrlTisHB1sBx4mu64KiKGF6SZKA4zjwLMu4OjuT59PpKYmsBT4wTCEhCJ92j44qhu'
            'OIvu/DdDqFysVFKHC5vw/ZbBYYTMQFgfHj/PzStqxXb4Kgy8Kin+Pi9nZ5rGkiETVNA1'
            '3X1xtFNvlUVQXNNMUnpVLZW+wTcPRxAfbS+bw0XJZMYzabQb/fD0m37Tb4nhf6qZXHGx'
            'sScXD6fiXw1GdZgfr2HAe08Rj0+Rw6nQ7E43Fg0R9bKWPc1HWBONEKwMWATaVjFQFmi6'
            'OPyKlUCvjVwuWI4Vp3aYd+B+CXNhi8CIbDpBi5WHK1Gv4z9y6PY9smcchmlxV8GfR6oz'
            'T2l8R5cnGhoCrL8LLVCu1kBKphKMSJCtRvR6MWsKyRxKOiRQkEz/NhxkSEjEds9CyrSZ'
            'y1gI/Xc+55tW/DoYwBQ0SRPPq/l0ohyBYRNpKvZjNZ9/0acSC6N+ho/Mbd/jyZnGzxfP'
            'k5z0vFWEyg2MTzzJ+2rci23XSQjHvS+OsxvcOMNi1EaPiYvP88JjzKehoz56g9xNvVVX'
            '7I+CPAAPloMDN65yLPAAAAAElFTkSuQmCC')


_HTML_TPL_START = '''<!DOCTYPE html>
<!--
Copyright (c) 2011 Sprymix Inc.
All rights reserved.
-->

<html>
    <head>
        <link rel="shortcut icon" href="''' + _FAVICON + '''" >

        <style type="text/css">
            {styles}
        </style>

        <script type="text/javascript">
            {scripts}

            (function() {{
                var exc_info = ''';


_HTML_END = ''';
                sx.dom.on(window, 'load', function(exc_info) {
                    var spec = sx.Markup.Renderer.unpack_markup(exc_info);
                    var renderer = new sx.Markup.Renderer(spec);
                    renderer.render('body');
                    if (renderer.top_exc_title) {
                        document.title = renderer.top_exc_title;
                    }
                }, this, exc_info);
            })();
        </script>
    </head>

    <body>
        <div id="body">
        </div>
    </body>
</html>
'''


class Renderer:
    TPL_START = None

    @classmethod
    def _init(cls):
        from semantix.utils.lang import javascript
        with open(os.path.join(os.path.dirname(javascript.__file__), 'sx.js')) as f:
            scripts = f.read()

        with open(os.path.join(os.path.dirname(__file__), 'render.js')) as f:
            scripts += ';\n' + f.read()

        from . import styles
        rendered_styles = scss_dumps(styles)

        cls.TPL_START = _HTML_TPL_START.format(styles=rendered_styles, scripts=scripts)

    @classmethod
    def render(cls, markup, reload=False):
        if reload:
            cls._init()

        exc_info = json.render(markup)
        return ''.join((cls.TPL_START, exc_info, _HTML_END))


Renderer._init()

render = Renderer.render
