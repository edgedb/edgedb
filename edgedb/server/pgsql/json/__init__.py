##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos.frontends.javascript import json_formats as js_json_formats
from . import _unpacker as _js_unpacker
js_json_formats.register_json_format('pgjson', _js_unpacker)


from metamagic.caos.frontends.jplus import json_formats as jp_json_formats
from . import _jp_unpacker
jp_json_formats.register_json_format('pgjson', _jp_unpacker)
