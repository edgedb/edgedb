##
# Copyright (c) 2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils import config

class settings(config.Configurable):
    censor_sensitive_vars = config.cvalue(type=bool, default=True)
    censor_list = config.cvalue(type=list, default=['secret', 'password'])
del config


from .base import serialize, serializer, serialize_traceback_point, Context
from .code import serialize_code
from . import logging, ast, yaml
