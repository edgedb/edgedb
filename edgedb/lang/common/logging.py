##
# Copyright (c) 2011-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import contextlib
import logging
import time


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
