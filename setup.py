##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from setuptools import setup


from edgedb.lang import build


RUNTIME_DEPS = [
    'asyncpg',
    'click',
    'graphql-core',
    'Parsing',
    'prompt-toolkit',
    'pygments',
    'setproctitle',
]


setup(
    setup_requires=[
        'setuptools_scm',
    ] + RUNTIME_DEPS,
    use_scm_version=True,
    name='edgedb-server',
    description='EdgeDB Server',
    author='MagicStack Inc.',
    author_email='hello@magic.io',
    packages=['edgedb'],
    provides=['edgedb'],
    include_package_data=True,
    cmdclass={
        'build': build.build
    },
    entry_points={
        'console_scripts': [
            'edgedb = edgedb.repl:main',
            'edgedb-server = edgedb.server.main:main',
            'edgedb-ctl = edgedb.server.ctl:main',
            'et = edgedb.tools.et:etcommands'
        ]
    },
    install_requires=RUNTIME_DEPS,
    test_suite='tests.suite',
)
