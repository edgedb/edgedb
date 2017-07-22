##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import click


__all__ = ()


marker_passed = lambda t: t
marker_errored = lambda t: click.style(t, fg='red', bold=True)
marker_skipped = lambda t: click.style(t, fg='yellow')
marker_failed = lambda t: click.style(t, fg='red', bold=True)
marker_xfailed = lambda t: t
marker_upassed = lambda t: click.style(t, fg='yellow')

status = lambda t: click.style(t, fg='white', bold=True)
warning = lambda t: click.style(t, fg='yellow')
