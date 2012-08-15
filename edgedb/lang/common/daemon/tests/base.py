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
    def pytest_funcarg__path(self, request):
        path = os.path.join(tempfile.gettempdir(), 'semantix.test.pid')
        if os.path.exists(path):
            os.unlink(path)
        return path
