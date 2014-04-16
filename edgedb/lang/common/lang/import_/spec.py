##
# Copyright (c) 2014 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


try:
    from importlib.util import find_spec
except ImportError:
    from ._spec import find_spec
