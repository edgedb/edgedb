##
# Copyright (c) 2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class settings:
    censor_sensitive_vars = True
    censor_list = ['secret', 'password']


from .base import serialize, serializer, serialize_traceback_point, Context
from .code import serialize_code
from . import logging, ast, yaml
