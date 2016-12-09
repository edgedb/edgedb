##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from setuptools import setup


from edgedb.lang import build


setup(
    setup_requires=[
        'setuptools_scm',
    ],
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
            'edgedb-ctl = edgedb.server.ctl:main'
        ]
    },
    install_requires=[
        'asyncpg',
        'importkit',
        'pyyaml',
        'pytest',
        'python-dateutil',
        'metamagic.json',
        'Parsing',
        'prompt-toolkit',
        'setproctitle',
    ]
)
