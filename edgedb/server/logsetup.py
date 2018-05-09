#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2011-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import contextlib
import copy
import io
import logging
import logging.handlers
import sys
import warnings

from edgedb.lang.common import term


LOG_LEVELS = {
    'S': 'SILENT',
    'D': 'DEBUG',
    'I': 'INFO',
    'E': 'ERROR',
    'W': 'WARN',
    'WARN': 'WARN',
    'ERROR': 'ERROR',
    'INFO': 'INFO',
    'DEBUG': 'DEBUG',
    'SILENT': 'SILENT'
}


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


class EdgeDBLogFormatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__styles = None
        self._colorize = term.use_colors()

        if self._colorize:
            self._init_styles()

    def _init_styles(self):
        if not self.__styles:
            if term.max_colors() >= 255:
                self.__styles = Dark256()
            else:
                self.__styles = Dark16()

    def formatTime(self, record, datefmt=None):
        time = super().formatTime(record, datefmt=datefmt)
        if self._colorize:
            time = self.__styles.date.apply(time)
        return time

    def formatException(self, ei):
        sio = io.StringIO()
        with contextlib.redirect_stdout(sio):
            sys.excepthook(*ei)

        s = sio.getvalue()
        sio.close()
        if s[-1:] == "\n":
            s = s[:-1]

        return s

    def format(self, record):
        if self._colorize:
            record = copy.copy(record)

            level = record.levelname
            level_style = getattr(self.__styles, level.lower(),
                                  self.__styles.default)
            record.levelname = level_style.apply(level)
            record.process = self.__styles.pid.apply(str(record.process))
            record.message = self.__styles.message.apply(record.getMessage())

        return super().format(record)


class EdgeDBLogHandler(logging.StreamHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        fmt = EdgeDBLogFormatter(
            '{levelname} {process} {asctime} {name}: {message}',
            style='{')

        self.setFormatter(fmt)


def setup_logging(log_level, log_destination):
    log_level = log_level.upper()
    try:
        log_level = LOG_LEVELS[log_level]
    except KeyError:
        raise RuntimeError('Invalid logging level {!r}'.format(log_level))

    if log_level == 'SILENT':
        return

    if log_destination == 'syslog':
        fmt = logging.Formatter(
            '{processName}[{process}]: {name}: {message}',
            style='{')
        handler = logging.handlers.SysLogHandler(
            '/dev/log',
            facility=logging.handlers.SysLogHandler.LOG_DAEMON)
        handler.setFormatter(fmt)

    elif log_destination == 'stderr':
        handler = EdgeDBLogHandler()

    else:
        fmt = logging.Formatter(
            '{levelname} {process} {asctime} {name}: {message}',
            style='{')
        handler = logging.FileHandler(log_destination)
        handler.setFormatter(fmt)

    log_level = logging._checkLevel(log_level)

    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(handler)

    # Channel warnings into logging system
    logging.captureWarnings(True)

    # Show DeprecationWarnings by default
    warnings.simplefilter('default', category=DeprecationWarning)
