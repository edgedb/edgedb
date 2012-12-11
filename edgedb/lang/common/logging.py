##
# Copyright (c) 2011-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import datetime
import logging
import os
import sys

from logging import *

from metamagic.bootstrap import MetamagicLogHandler as BootstrapLogHandler
from metamagic.utils import config, term


class MetamagicLogHandler(BootstrapLogHandler, metaclass=config.ConfigurableMeta):
    _enabled = config.cvalue(True, type=bool)

    _style_error = term.Style16(color='white', bgcolor='red')
    _style_other = term.Style16(color='white', bgcolor='blue')

    dump_exceptions = config.cvalue(True, type=bool)

    def emit(self, record):
        dt = datetime.datetime.fromtimestamp(record.created)
        str_dt = '@{}@'.format(dt)

        if self._enabled:
            if term.use_colors():
                style = self._style_other
                level = record.levelname
                if level == 'ERROR':
                    style = self._style_error

                print(style.apply(level), os.getpid(), str_dt, record.getMessage())
            else:
                print(record.levelname, os.getpid(), str_dt, record.getMessage())

            if record.exc_info and self.dump_exceptions:
                sys.excepthook(*record.exc_info)


if BootstrapLogHandler._installed:
    BootstrapLogHandler.uninstall()
    MetamagicLogHandler.install(BootstrapLogHandler._level)

