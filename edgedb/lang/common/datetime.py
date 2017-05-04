##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


def humanize_time_delta(t):
    f = 's'
    if t and not round(t):
        t *= 1000.0
        if round(t):
            f = 'ms'
        else:
            t *= 1000.0
            if round(t):
                f = 'us'
            else:
                t *= 1000.0
                f = 'ns'
    return f'{t:.2f}{f}'
