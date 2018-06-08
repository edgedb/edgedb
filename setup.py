#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from setuptools import setup


from edb.lang import build


RUNTIME_DEPS = [
    'asyncpg',
    'click',
    'graphql-core',
    'Parsing',
    'prompt_toolkit>=1.0.15,<2.0.0',
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
    packages=['edb'],
    provides=['edb'],
    include_package_data=True,
    cmdclass={
        'build': build.build
    },
    entry_points={
        'console_scripts': [
            'edgedb = edb.repl:main',
            'edgedb-server = edb.server.main:main',
            'edgedb-ctl = edb.server.ctl:main',
            'edb = edb.tools.edb:edbcommands'
        ]
    },
    install_requires=RUNTIME_DEPS,
    test_suite='tests.suite',
)
