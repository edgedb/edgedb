##
# Copyright (c) 2011-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class settings:
    censor_sensitive_vars = True
    censor_list = ['secret', 'password']


from .base import serialize, serializer, serialize_traceback_point  # NOQA
from .base import Context  # NOQA
from .base import no_ref_detect  # NOQA
from .code import serialize_code  # NOQA
from . import logging  # NOQA
