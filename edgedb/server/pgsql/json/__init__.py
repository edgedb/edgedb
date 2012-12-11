##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos.frontends import javascript

from . import _unpacker

javascript.json_formats['pgjson'] = _unpacker
