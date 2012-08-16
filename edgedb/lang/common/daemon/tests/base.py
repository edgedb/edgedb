##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import os.path
import tempfile


class BaseDaemonTestCase:
    def gen_tmp_filename(self, sn):
        return os.path.join(tempfile.gettempdir(), sn)

    def _gen_base_fn_funcarg(sn):
        def funcarg(self, request, sn=sn):
            path = self.gen_tmp_filename(sn)
            if os.path.exists(path):
                os.unlink(path)
            return path
        return funcarg

    pytest_funcarg__pid    = _gen_base_fn_funcarg('semantix.test.pid')
    pytest_funcarg__stdin  = _gen_base_fn_funcarg('semantix.test.stdin')
    pytest_funcarg__stdout = _gen_base_fn_funcarg('semantix.test.stdout')
    pytest_funcarg__stderr = _gen_base_fn_funcarg('semantix.test.stderr')
    pytest_funcarg__fn1    = _gen_base_fn_funcarg('semantix.test.fn1')
    pytest_funcarg__fn2    = _gen_base_fn_funcarg('semantix.test.fn2')
