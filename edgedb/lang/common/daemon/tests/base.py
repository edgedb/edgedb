##
# Copyright (c) 2012 Sprymix Inc.
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


def _finalize_funcarg(request, fn):
    i = 0
    while True:
        i += 1
        try:
            os.unlink(fn)
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


def _gen_base_fn_funcarg(sn):
    def funcarg(self, request, sn=sn):
        path = self.gen_tmp_filename(sn, randomize=True)
        if os.path.exists(path):
            os.unlink(path)
        request.addfinalizer(functools.partial(_finalize_funcarg, request, path))
        return path
    return funcarg


class BaseDaemonTestCase:
    def gen_tmp_filename(self, sn, randomize=False):
        if randomize:
            sn = str(random.randint(0, 10**5)) + '.' + sn
        return os.path.join(tempfile.gettempdir(), sn)

    pytest_funcarg__pid    = _gen_base_fn_funcarg('metamagic.test.pid')
    pytest_funcarg__stdin  = _gen_base_fn_funcarg('metamagic.test.stdin')
    pytest_funcarg__stdout = _gen_base_fn_funcarg('metamagic.test.stdout')
    pytest_funcarg__stderr = _gen_base_fn_funcarg('metamagic.test.stderr')
    pytest_funcarg__fn1    = _gen_base_fn_funcarg('metamagic.test.fn1')
    pytest_funcarg__fn2    = _gen_base_fn_funcarg('metamagic.test.fn2')
