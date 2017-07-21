##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import sys
import unittest

import click

import edgedb
from edgedb.tools import etcommands

from . import loader
from . import runner


@etcommands.command()
@click.argument('files', nargs=-1, metavar='[file or directory]...')
@click.option('-v', '--verbose', is_flag=True,
              help='increase verbosity')
@click.option('-q', '--quiet', is_flag=True,
              help='decrease verbosity')
@click.option('-j', '--jobs', type=int,
              default=lambda: round(os.cpu_count() * 0.75),
              help='number of parallel processes to use')
@click.option('-k', '--include', type=str, multiple=True, metavar='REGEXP',
              help='only run tests which match the given regular expression')
@click.option('-e', '--exclude', type=str, multiple=True, metavar='REGEXP',
              help='do not run tests which match the given regular expression')
def test(*, files, jobs, include, exclude, verbose, quiet):
    """Run EdgeDB test suite.

    Discovers and runs tests in the specified files or directories.
    If no files or directories are specified, current directory is assumed.
    """
    if quiet:
        if verbose:
            click.secho(
                'Warning: both --quiet and --verbose are '
                'specified, assuming --quiet.', fg='yellow')
        verbosity = 0
    elif verbose:
        verbosity = 2
    else:
        verbosity = 1

    # When invoked without arguments, we need to do a bit
    # of a voodoo to make sure the discovered tests are
    # actually imported correctly.  To do this, we determine
    # if the current directory is under edgedb project root,
    # and if it is, we prepend edgedb project root to sys.path.
    if not files:
        cwd = os.path.abspath(os.getcwd())
        files = [cwd]
        top_level_dirs = list(edgedb.__path__)

        for top_level_dir in top_level_dirs:
            top_level_dir = os.path.dirname(top_level_dir)

            if os.path.commonpath([top_level_dir, cwd]) == top_level_dir:
                break
        else:
            top_level_dir = None
    else:
        top_level_dir = None

    if top_level_dir:
        sys.path.insert(0, top_level_dir)

    suite = unittest.TestSuite()

    total = 0
    total_unfiltered = 0

    if verbosity > 0:
        def _update_progress(n, unfiltered_n):
            nonlocal total, total_unfiltered
            total += n
            total_unfiltered += unfiltered_n
            click.secho(f'Collected {total}/{total_unfiltered} items.\r',
                        fg='white', bold=True, nl=False)
    else:
        _update_progress = None

    test_loader = loader.TestLoader(
        verbosity=verbosity, exclude=exclude, include=include,
        progress_cb=_update_progress)

    for file in files:
        if not os.path.exists(file) and verbosity > 0:
            click.secho(
                f'Warning: {file}: no such file or directory.',
                fg='yellow')

        if os.path.isdir(file):
            tests = test_loader.discover(
                file, top_level_dir=top_level_dir)
        else:
            tests = test_loader.loadTestsFromName(file)

        suite.addTest(tests)

    if verbosity > 0:
        click.echo()
        click.secho(f'Using up to {jobs} processes to run tests.',
                    fg='white', bold=True)

    test_runner = runner.ParallelTextTestRunner(
        verbosity=verbosity, num_workers=jobs)
    result = test_runner.run(suite)

    sys.exit(0 if result.wasSuccessful() else 1)
