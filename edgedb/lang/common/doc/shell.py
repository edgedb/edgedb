##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import sys
import webbrowser

from semantix.utils import shell


DOC_DIR = 'doc'
BUILD_SUBDIR = '_build'
DEFAULT_BUILDER = 'html'
INDEX_FILE = 'index.rst'
CONF_FILE = 'conf.py'


DEFAULT_CONF_PY = '''from semantix.utils.doc.sphinx.default_conf import *

project = 'PROJECT'
copyright = 'COPYRIGHT'
version = '0.0'
'''

DEFAULT_INDEX_FILE = '''
======================
Documentation Overview
======================

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
'''


class BaseSphinxCommand:
    def get_root(self):
        return os.path.dirname(sys.argv[0])

    def get_doc_root(self):
        return os.path.join(self.get_root(), DOC_DIR)

    def is_initialized_doc_root(self, doc_root):
        if os.path.isdir(doc_root) \
                    and os.path.exists(os.path.join(doc_root, CONF_FILE)) \
                    and os.path.isdir(os.path.join(doc_root, BUILD_SUBDIR)) \
                    and os.path.exists(os.path.join(doc_root, INDEX_FILE)):
            return True

    def get_sphinx(self):
        try:
            from sphinx.application import Sphinx
        except ImportError:
            raise SystemExit('Sphinx package is required to generate documentation. ' \
                             'Please install it.')

        return Sphinx


FS = (DOC_DIR, [
        (BUILD_SUBDIR, [
            ('.gitignore', '*\n')
        ]),

        (INDEX_FILE, DEFAULT_INDEX_FILE),
        (CONF_FILE, DEFAULT_CONF_PY),

        ('_static', []),
        ('_templates', [])
     ])


def init_fs(root, spec):
    """Creates file-system hierarchy - files and directories.

    Quick and dirty implementation.  Ideally, should validate specification for
    errors and conflicts and only after that begin any disk manipulations."""

    assert os.path.exists(root)

    def _fs(root, obj):
        assert isinstance(obj, tuple)
        assert len(obj) == 2
        assert isinstance(obj[0], str)

        if isinstance(obj[1], list): # directory
            subdir = os.path.join(root, obj[0])

            os.mkdir(subdir)

            for el in obj[1]:
                _fs(subdir, el)

        elif isinstance(obj[1], str): # text file
            file = os.path.join(root, obj[0])

            with open(file, 'w+') as f:
                f.write(obj[1])

        else:
            raise ValueError('expected list or str, got {} {!r}'. \
                             format(type(obj[1]).__name__, obj[1]))

    _fs(root, spec)


class DocInit(shell.Command, BaseSphinxCommand, name='init'):
    """Initialize Sphinx file-system structure for project documentation"""

    def __call__(self, args):
        doc_root = self.get_doc_root()
        if os.path.exists(doc_root):
            raise SystemError('Sphinx documentation files already exist.')

        init_fs(self.get_root(), FS)


class DocGen(shell.Command, BaseSphinxCommand, name='gen'):
    """Generate Sphinx Documentation"""

    def get_parser(self, subparsers, **kwargs):
        parser = super().get_parser(subparsers)

        parser.add_argument('--rebuild', dest='rebuild', action='store_true', default=False,
                            help="rebuild documentation without using any cached data")

        parser.add_argument('--open', dest='open', action='store_true', default=False,
                            help="open generated documentation after a successful build")

        return parser

    def require_doc_root(self):
        doc_root = self.get_doc_root()

        if os.path.isdir(doc_root) \
                    and os.path.exists(os.path.join(doc_root, CONF_FILE)) \
                    and os.path.isdir(os.path.join(doc_root, BUILD_SUBDIR)) \
                    and os.path.exists(os.path.join(doc_root, INDEX_FILE)):

            return doc_root

        raise SystemExit('Failed to locate project documentation directory.  ' \
                         'Should be a project top-level directory named "{}" with sphinx ' \
                         '"{}", "{}" files, and "{}" directory in it.'. \
                         format(DOC_DIR, CONF_FILE, INDEX_FILE, BUILD_SUBDIR))

    def __call__(self, args):
        Sphinx = self.get_sphinx()

        srcdir = confdir = self.require_doc_root()
        buildername = DEFAULT_BUILDER
        outdir = os.path.join(srcdir, BUILD_SUBDIR, buildername)
        doctreedir = os.path.join(outdir, '.doctrees')
        confoverrides = {}
        tags = []
        status = sys.stdout
        warning = sys.stderr
        force_all = freshenv = warningiserror = False
        filenames = []

        if args.rebuild:
            force_all = True
            freshenv = True

        app = Sphinx(srcdir, confdir, outdir, doctreedir, buildername,
                     confoverrides, status, warning, freshenv,
                     warningiserror, tags)
        app.build(force_all, filenames)

        if args.open:
            index = os.path.join(outdir, 'index.html')
            webbrowser.open('file://' + index)

        return app.statuscode


class DocCommands(shell.CommandGroup,
                   name='doc',
                   expose=True,
                   commands=(DocGen,DocInit)):
    pass
