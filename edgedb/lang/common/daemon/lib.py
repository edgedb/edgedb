##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import fcntl
import logging
import stat

from .exceptions import DaemonError


logger = logging.getLogger('semantix.utils.daemon')


def is_process_running(pid:int):
    '''Check if there is a running process with `pid`'''

    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


def lock_file(fileno:int):
    '''Locks file.  Returns ``True`` if succeeded, ``False`` otherwise'''

    try:
        # Try to lock file exclusively and in non-blocking fashion
        fcntl.flock(fileno, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        return False
    else:
        return True


def make_readonly(path:str):
    '''Makes a file read-only'''

    assert os.path.isfile(path)
    os.chmod(path, stat.S_IROTH | stat.S_IRUSR | stat.S_IRGRP)
