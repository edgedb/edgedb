##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


__all__ = ('Error',)


class Error(Exception):
    def __init__(self, msg, *, code=None):
        super().__init__(msg)
        self.message = msg
        self.code = code
