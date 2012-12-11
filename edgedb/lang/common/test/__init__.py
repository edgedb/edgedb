##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.config import ConfigurableMeta, cvalue


class Exceptions(metaclass=ConfigurableMeta):
    traceback_style = cvalue(default='short', type=str,
                             validator=lambda arg: arg in ('short', 'long'),
                             doc='Style of exception traceback printout')
