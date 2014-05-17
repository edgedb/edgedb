##
# Copyright (c) 2011-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import contextlib
import datetime
import logging
import os
import sys
import time

from logging import *

from metamagic.bootstrap import MetamagicLogHandler as BootstrapLogHandler
from metamagic.utils import config, term


class Dark16:
    error = term.Style16(color='white', bgcolor='red')
    default = term.Style16(color='white', bgcolor='blue')
    pid = date = term.Style16(color='black', bold=True)
    message = term.Style16()


class Dark256:
    error = term.Style256(color='#c6c6c6', bgcolor='#870000')
    warning = term.Style256(color='#c6c6c6', bgcolor='#5f00d7')
    info = term.Style256(color='#c6c6c6', bgcolor='#005f00')
    default = term.Style256(color='#c6c6c6', bgcolor='#000087')
    pid = date = term.Style256(color='#626262', bold=True)
    message = term.Style16()


class MetamagicLogHandler(BootstrapLogHandler, metaclass=config.ConfigurableMeta):
    _enabled = True

    dump_exceptions = config.cvalue(True, type=bool)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__styles = None

    def _init_styles(self):
        if not self.__styles:
            if term.max_colors() >= 255:
                self.__styles = Dark256()
            else:
                self.__styles = Dark16()

    def emit(self, record):
        if not self._enabled:
            return

        dt = datetime.datetime.fromtimestamp(record.created)
        str_dt = dt.strftime('%Y-%m-%d %H:%M:%S')

        if term.use_colors():
            self._init_styles()

            level = record.levelname
            print(getattr(self.__styles, level.lower(), self.__styles.default).apply(level),
                  self.__styles.pid.apply(str(os.getpid())),
                  self.__styles.date.apply(str_dt),
                  self.__styles.message.apply(record.getMessage()))
        else:
            print(record.levelname, os.getpid(), str_dt, record.getMessage())

        if record.exc_info and self.dump_exceptions:
            sys.excepthook(*record.exc_info)


@contextlib.contextmanager
def log_time(logger, msg, *, level=logging.DEBUG, **kwargs):
    """
    Use 'log_time' to time and log about some process:

    .. code-block:: python

        with log_time(my_logger, 'took {time.3f} seconds', level=logging.INFO):
            ...
    """
    started = time.time()
    try:
        yield
    finally:
        logger.log(level, msg.format(time=time.time() - started), **kwargs)


if BootstrapLogHandler._installed:
    BootstrapLogHandler.uninstall()
    MetamagicLogHandler.install(BootstrapLogHandler._level)

