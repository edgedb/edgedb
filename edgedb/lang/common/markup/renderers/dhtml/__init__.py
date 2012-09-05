##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os

from semantix.rendering.css import dumps as scss_dumps, reload as scss_reload
from .. import json
from ... import serialize


__all__ = 'render',


_FAVICON = ('data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAAGXRFWHRTb'
            '2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAABhQTFRF6+np2VZWmVVVqxUV1qioxhMTuzw8////'
            'fKlS/gAAAAh0Uk5T/////////wDeg71ZAAAAbElEQVR42lyOCwrAMAhD47f3v/ES3aAsBY0PteL8hAl'
            'p3Zb4QLZJRAveWigFMB6TmqUa+IDuGcIhp4CQjIBVReSCFjC5C7gaPvksrargDiUtRcsCDDXfbkuRxh'
            '5G4jHI93QA6aOkXXDpPAIMAD0IA95480JWAAAAAElFTkSuQmCC')


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
        scss_reload(styles)
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
