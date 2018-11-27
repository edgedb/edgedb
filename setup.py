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
import textwrap

from distutils import extension as distutils_extension
from distutils.command import build as distutils_build
from distutils.command import build_ext as distutils_build_ext

import setuptools
from setuptools.command import develop as setuptools_develop


RUNTIME_DEPS = [
    'asyncpg',
    'click',
    'graphql-core~=2.1.0',
    'immutables>=0.9',
    'Parsing',
    'prompt_toolkit>=1.0.15,<2.0.0',
    'pygments',
    'setproctitle',
    'typing_inspect~=0.3.1',
    'uvloop~=0.12.0rc1',
]

CYTHON_DEPENDENCY = 'Cython==0.29.0'

BUILD_DEPS = [
    CYTHON_DEPENDENCY,
]

EXTRA_DEPS = {
    'test': [
        'flake8~=3.6.0',
        'pycodestyle~=2.4.0',
    ],

    'docs': [
        'Sphinx',
        'lxml',
        'requests-xml',
    ],
}

EXT_CFLAGS = ['-O2']
EXT_LDFLAGS = []


if platform.uname().system != 'Windows':
    EXT_CFLAGS.extend([
        '-fsigned-char', '-Wall', '-Wsign-compare', '-Wconversion'
    ])


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


def _compile_build_meta(build_lib, pg_config):
    content = textwrap.dedent('''\
        #
        # This source file is part of the EdgeDB open source project.
        #
        # Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
        #
        # Licensed under the Apache License, Version 2.0 (the "License");
        #
        # THIS FILE HAS BEEN AUTOMATICALLY GENERATED.
        #

        PG_CONFIG_PATH = {pg_config!r}
    ''').format(pg_config=pg_config)

    directory = build_lib / 'edb' / 'server'
    if not directory.exists():
        directory.mkdir(parents=True)

    with open(directory / '_buildmeta.py', 'w+t') as f:
        f.write(content)


def _compile_postgres(build_base, *,
                      force_build=False, fresh_build=True,
                      run_configure=True, build_contrib=True):

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

    is_outdated = source_stamp != build_stamp

    if is_outdated or force_build:
        system = platform.system()
        if system == 'Darwin':
            uuidlib = 'e2fs'
        elif system == 'Linux':
            uuidlib = 'e2fs'
        else:
            raise NotImplementedError('unsupported system: {}'.format(system))

        if fresh_build and postgres_build.exists():
            shutil.rmtree(postgres_build)
        build_dir = postgres_build / 'build'
        if not build_dir.exists():
            build_dir.mkdir(parents=True)

        if run_configure or fresh_build or is_outdated:
            subprocess.run([
                str(postgres_src / 'configure'),
                '--prefix=' + str(postgres_build / 'install'),
                '--with-uuid=' + uuidlib,
            ], check=True, cwd=str(build_dir))

        subprocess.run(
            ['make', '-j', str(max(os.cpu_count() - 1, 1))],
            cwd=str(build_dir), check=True)

        if build_contrib or fresh_build or is_outdated:
            subprocess.run(
                [
                    'make', '-C', 'contrib', '-j',
                    str(max(os.cpu_count() - 1, 1))
                ],
                cwd=str(build_dir), check=True)

        subprocess.run(
            ['make', 'install'],
            cwd=str(build_dir), check=True)

        if build_contrib or fresh_build or is_outdated:
            subprocess.run(
                ['make', '-C', 'contrib', 'install'],
                cwd=str(build_dir), check=True)

        with open(postgres_build_stamp, 'w') as f:
            f.write(source_stamp)


def _compile_postgres_extensions(build_base):
    postgres_build = (build_base / 'postgres').resolve()
    postgres_build_stamp_path = postgres_build / 'stamp'

    ext_build = (build_base / 'ext').resolve()
    ext_build_stamp_path = ext_build / 'stamp'

    if postgres_build_stamp_path.exists():
        with open(postgres_build_stamp_path, 'r') as f:
            postgres_build_stamp = f.read()
    else:
        raise RuntimeError('Postgres is not built, cannot build extensions')

    if ext_build_stamp_path.exists():
        with open(ext_build_stamp_path, 'r') as f:
            ext_build_stamp = f.read()
    else:
        ext_build_stamp = None

    ext_dir = (pathlib.Path(__file__).parent / 'ext').resolve()
    pg_config = (build_base / 'postgres' / 'install' /
                 'bin' / 'pg_config').resolve()

    if not ext_dir.exists():
        raise RuntimeError('missing Postgres extension directory')

    ext_make = ['make', '-C', str(ext_dir), 'PG_CONFIG=' + str(pg_config)]

    if ext_build_stamp != postgres_build_stamp:
        print('Extensions build stamp does not match Postgres build stamp. '
              'Rebuilding...')
        subprocess.run(ext_make + ['clean'], check=True)

    ext_build.mkdir(parents=True, exist_ok=True)

    subprocess.run(ext_make, check=True)
    subprocess.run(ext_make + ['install'], check=True)

    ext_build_stamp = postgres_build_stamp

    with open(ext_build_stamp_path, 'w') as f:
        f.write(ext_build_stamp)


class build(distutils_build.build):

    user_options = distutils_build.build.user_options + [
        ('pg-config=', None, 'path to pg_config to use with this build')
    ]

    def initialize_options(self):
        super().initialize_options()
        self.pg_config = None

    def finalize_options(self):
        super().finalize_options()

    def run(self, *args, **kwargs):
        super().run(*args, **kwargs)
        build_lib = pathlib.Path(self.build_lib)
        _compile_parsers(build_lib)
        if self.pg_config:
            _compile_build_meta(build_lib, self.pg_config)


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


class build_postgres(setuptools.Command):

    description = "build postgres"

    user_options = [
        ('configure', None, 'run ./configure'),
        ('build-contrib', None, 'build contrib'),
        ('fresh-build', None, 'rebuild from scratch'),
    ]

    def initialize_options(self):
        self.configure = False
        self.build_contrib = False
        self.fresh_build = False

    def finalize_options(self):
        pass

    def run(self, *args, **kwargs):
        _compile_postgres(
            pathlib.Path('build').resolve(),
            force_build=True,
            fresh_build=self.fresh_build,
            run_configure=self.configure,
            build_contrib=self.build_contrib)


class build_ext(distutils_build_ext.build_ext):

    user_options = distutils_build_ext.build_ext.user_options + [
        ('cython-annotate', None,
            'Produce a colorized HTML version of the Cython source.'),
        ('cython-directives=', None,
            'Cython compiler directives'),
    ]

    def initialize_options(self):
        # initialize_options() may be called multiple times on the
        # same command object, so make sure not to override previously
        # set options.
        if getattr(self, '_initialized', False):
            return

        super(build_ext, self).initialize_options()

        if os.environ.get('EDGEDB_DEBUG'):
            self.cython_always = True
            self.cython_annotate = True
            self.cython_directives = "linetrace=True"
            self.define = 'PG_DEBUG,CYTHON_TRACE,CYTHON_TRACE_NOGIL'
            self.debug = True
        else:
            self.cython_always = False
            self.cython_annotate = None
            self.cython_directives = None

    def finalize_options(self):
        # finalize_options() may be called multiple times on the
        # same command object, so make sure not to override previously
        # set options.
        if getattr(self, '_initialized', False):
            return

        import pkg_resources

        # Double check Cython presence in case setup_requires
        # didn't go into effect (most likely because someone
        # imported Cython before setup_requires injected the
        # correct egg into sys.path.
        try:
            import Cython
        except ImportError:
            raise RuntimeError(
                'please install {} to compile edgedb from source'.format(
                    CYTHON_DEPENDENCY))

        cython_dep = pkg_resources.Requirement.parse(CYTHON_DEPENDENCY)
        if Cython.__version__ not in cython_dep:
            raise RuntimeError(
                'edgedb requires {}, got Cython=={}'.format(
                    CYTHON_DEPENDENCY, Cython.__version__
                ))

        from Cython.Build import cythonize

        directives = {
            'language_level': '3'
        }

        if self.cython_directives:
            for directive in self.cython_directives.split(','):
                k, _, v = directive.partition('=')
                if v.lower() == 'false':
                    v = False
                if v.lower() == 'true':
                    v = True

                directives[k] = v

        self.distribution.ext_modules[:] = cythonize(
            self.distribution.ext_modules,
            compiler_directives=directives,
            annotate=self.cython_annotate,
            include_path=["edb/server2/pgproto/"])

        super(build_ext, self).finalize_options()


setuptools.setup(
    setup_requires=[
        'setuptools_scm',
    ] + RUNTIME_DEPS + BUILD_DEPS,
    use_scm_version=True,
    name='edgedb-server',
    description='EdgeDB Server',
    author='MagicStack Inc.',
    author_email='hello@magic.io',
    packages=['edb'],
    include_package_data=True,
    cmdclass={
        'build': build,
        'build_ext': build_ext,
        'develop': develop,
        'build_postgres': build_postgres,
    },
    entry_points={
        'console_scripts': [
            'edgedb = edb.repl:main',
            'edgedb-server = edb.server.main:main',
        ]
    },
    ext_modules=[
        distutils_extension.Extension(
            "edb.server2.pgproto.pgproto",
            ["edb/server2/pgproto/pgproto.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS),

        distutils_extension.Extension(
            "edb.server2.edgecon.edgecon",
            ["edb/server2/edgecon/edgecon.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS),

        distutils_extension.Extension(
            "edb.server2.pgcon.pgcon",
            ["edb/server2/pgcon/pgcon.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS),
    ],
    install_requires=RUNTIME_DEPS,
    extras_require=EXTRA_DEPS,
    test_suite='tests.suite',
)
