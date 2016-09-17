##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.common import functional


def wrap(func):
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    functional.decorate(wrapper, func)
    return wrapper
