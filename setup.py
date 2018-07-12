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


import os.path
import pathlib
import platform
import shutil
import subprocess

from distutils.command import build as distutils_build

from setuptools import setup
from setuptools.command import develop as setuptools_develop


RUNTIME_DEPS = [
    'asyncpg',
    'click',
    'graphql-core',
    'Parsing',
    'prompt_toolkit>=1.0.15,<2.0.0',
    'pygments',
    'setproctitle',
    'typing_inspect~=0.3.1',
]

EXTRA_DEPS = {
    'test': [
        'flake8~=3.5.0',
        'pycodestyle~=2.3.1',
    ]
}


def _compile_parsers(build_lib, inplace=False):
    import parsing

    import edb.lang.edgeql.parser.grammar.single as edgeql_spec
    import edb.lang.edgeql.parser.grammar.block as edgeql_spec2
    import edb.server.pgsql.parser.pgsql as pgsql_spec
    import edb.lang.schema.parser.grammar.declarations as schema_spec
    import edb.lang.graphql.parser.grammar.document as graphql_spec

    base_path = pathlib.Path(__file__).parent.resolve()

    for spec in (edgeql_spec, edgeql_spec2, pgsql_spec,
                 schema_spec, graphql_spec):
        spec_path = pathlib.Path(spec.__file__).parent
        subpath = pathlib.Path(str(spec_path)[len(str(base_path)) + 1:])
        pickle_name = spec.__name__.rpartition('.')[2] + '.pickle'
        pickle_path = subpath / pickle_name
        cache = build_lib / pickle_path
        cache.parent.mkdir(parents=True, exist_ok=True)
        parsing.Spec(spec, pickleFile=str(cache), verbose=True)
        if inplace:
            shutil.copy2(cache, base_path / pickle_path)


def _compile_postgres(build_base):

    proc = subprocess.run(
        ['git', 'submodule', 'status', 'postgres'],
        stdout=subprocess.PIPE, universal_newlines=True, check=True)
    status = proc.stdout
    if status[0] == '-':
        print(
            'postgres submodule not initialized, '
            'run `git submodule init; git submodule update`')
        exit(1)

    proc = subprocess.run(
        ['git', 'submodule', 'status', 'postgres'],
        stdout=subprocess.PIPE, universal_newlines=True, check=True)
    revision, _, _ = proc.stdout[1:].partition(' ')
    source_stamp = proc.stdout[0] + revision

    postgres_build = (build_base / 'postgres').resolve()
    postgres_src = (pathlib.Path(__file__).parent / 'postgres').resolve()
    postgres_build_stamp = postgres_build / 'stamp'

    if postgres_build_stamp.exists():
        with open(postgres_build_stamp, 'r') as f:
            build_stamp = f.read()
    else:
        build_stamp = None

    if source_stamp != build_stamp:
        system = platform.system()
        if system == 'Darwin':
            uuidlib = 'e2fs'
        elif system == 'Linux':
            uuidlib = 'e2fs'
        else:
            raise NotImplementedError('unsupported system: {}'.format(system))

        if postgres_build.exists():
            shutil.rmtree(postgres_build)
        build_dir = postgres_build / 'build'
        build_dir.mkdir(parents=True)
        subprocess.run([
            str(postgres_src / 'configure'),
            '--prefix=' + str(postgres_build / 'install'),
            '--with-uuid=' + uuidlib,
        ], check=True, cwd=str(build_dir))
        subprocess.run(
            ['make', '-j', str(max(os.cpu_count() - 1, 1))],
            cwd=str(build_dir), check=True)
        subprocess.run(
            ['make', '-C', 'contrib', '-j', str(max(os.cpu_count() - 1, 1))],
            cwd=str(build_dir), check=True)
        subprocess.run(
            ['make', 'install'],
            cwd=str(build_dir), check=True)
        subprocess.run(
            ['make', '-C', 'contrib', 'install'],
            cwd=str(build_dir), check=True)

        with open(postgres_build_stamp, 'w') as f:
            f.write(source_stamp)


def _compile_postgres_extensions(build_base):

    ext_base = (pathlib.Path(__file__).parent / 'ext').resolve()
    pg_config = (build_base / 'postgres' / 'install' /
                 'bin' / 'pg_config').resolve()

    for path in ext_base.iterdir():
        if path.is_dir():
            subprocess.run(
                ['make', 'PG_CONFIG=' + str(pg_config)],
                cwd=str(path), check=True)
            subprocess.run(
                ['make', 'PG_CONFIG=' + str(pg_config), 'install'],
                cwd=str(path), check=True)


class build(distutils_build.build):

    def run(self, *args, **kwargs):
        super().run(*args, **kwargs)
        _compile_parsers(pathlib.Path(self.build_lib))
        _compile_postgres(pathlib.Path(self.build_base))
        _compile_postgres_extensions(pathlib.Path(self.build_base))


class develop(setuptools_develop.develop):

    def run(self, *args, **kwargs):
        _compile_parsers(pathlib.Path('build/lib'), inplace=True)
        _compile_postgres(pathlib.Path('build').resolve())
        _compile_postgres_extensions(pathlib.Path('build').resolve())

        scripts = self.distribution.entry_points['console_scripts']
        patched_scripts = [s + '_dev' for s in scripts]
        patched_scripts.append('edb = edb.tools.edb:edbcommands')
        self.distribution.entry_points['console_scripts'] = patched_scripts

        super().run(*args, **kwargs)


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
        'build': build,
        'develop': develop,
    },
    entry_points={
        'console_scripts': [
            'edgedb = edb.repl:main',
            'edgedb-server = edb.server.main:main',
            'edgedb-ctl = edb.server.ctl:main',
        ]
    },
    install_requires=RUNTIME_DEPS,
    extras_require=EXTRA_DEPS,
    test_suite='tests.suite',
)
