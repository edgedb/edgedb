##
# Copyright (c) 2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos.frontends.jplus import json_formats as jp_json_formats
from . import _jp_unpacker
jp_json_formats.register_json_format('pgjson', _jp_unpacker)
