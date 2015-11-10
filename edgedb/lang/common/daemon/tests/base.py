##
# Copyright (c) 2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import errno
import os
import os.path
import functools
import random
import tempfile
import time

from metamagic.test import FunctionArgument


class BaseFNArg(FunctionArgument):
    scope = 'call'
    name = None

    def setup(self):
        sn = str(random.randint(0, 10**5)) + '.' + self.name
        path = os.path.join(tempfile.gettempdir(), sn)
        if os.path.exists(path):
            os.unlink(path)
        self.value = path

    def teardown(self):
        i = 0
        while True:
            i += 1
            try:
                os.unlink(self.value)
            except (OSError, IOError) as ex:
                if ex.errno == errno.ENOENT:
                    return
                else:
                    if i < 10:
                        time.sleep(0.5)
                        continue
                raise
            else:
                return

class Pid(BaseFNArg):
    name = 'metamagic.test.pid'

class Stdin(BaseFNArg):
    name = 'metamagic.test.stdin'

class Stdout(BaseFNArg):
    name = 'metamagic.test.stdout'

class Stderr(BaseFNArg):
    name = 'metamagic.test.stderr'

class Fn1(BaseFNArg):
    name = 'metamagic.test.fn1'

class Fn2(BaseFNArg):
    name = 'metamagic.test.fn2'


class BaseDaemonTestCase:
    pass
