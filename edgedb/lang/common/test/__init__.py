##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.config import configurable, cvalue
from .pytest_semantix import logging_off


@configurable
class Exceptions:
    traceback_style = cvalue(default='short', type=str,
                             validator=lambda arg: arg in ('short', 'long'),
                             doc='Style of exception traceback printout')
