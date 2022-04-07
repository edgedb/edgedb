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


import binascii
import os
import os.path
import pathlib
import platform
import shutil
import subprocess
import textwrap

import setuptools
from setuptools import extension as setuptools_extension
from setuptools.command import build_ext as setuptools_build_ext
from setuptools.command import develop as setuptools_develop

import distutils
from distutils.command import build as distutils_build

try:
    import setuptools_rust
except ImportError:
    setuptools_rust = None

from typing import List


CYTHON_DEPENDENCY = 'Cython(>=0.29.24,<0.30.0)'

# Dependencies needed both at build- and run-time
COMMON_DEPS = [
    'edgedb==0.22.0',
    'parsing~=2.0',
]

RUNTIME_DEPS = [
    'asyncpg~=0.25.0',
    'httptools>=0.3.0',
    'immutables>=0.16',
    'uvloop~=0.16.0',

    'click~=7.1',
    'cryptography~=35.0',
    'graphql-core~=3.1.5',
    'psutil~=5.8',
    'setproctitle~=1.2',
    'wcwidth~=0.2',
] + COMMON_DEPS


DOCS_DEPS = [
    'docutils~=0.17.0',
    'lxml~=4.6.3',
    'Pygments~=2.10.0',
    'Sphinx~=4.2.0',
    'sphinxcontrib-asyncio~=0.3.0',
]

TEST_DEPS = [
    # Code QA
    'black~=21.7b0',
    'coverage~=5.5',
    'flake8~=3.9.2',
    'flake8-bugbear~=21.4.3',
    'pycodestyle~=2.7.0',
    'pyflakes~=2.3.1',

    # Needed for test_docs_sphinx_ext
    'requests-xml~=0.2.3',

    # For rebuilding GHA workflows
    'Jinja2~=2.11',
    'MarkupSafe~=1.1',
    'PyYAML~=5.4',

    'mypy==0.941',
    # mypy stub packages; when updating, you can use mypy --install-types
    # to install stub packages and then pip freeze to read out the specifier
    'types-click~=7.1',
    'types-docutils~=0.17.0,<0.17.6',  # incomplete nodes.document.__init__
    'types-Jinja2~=2.11',
    'types-MarkupSafe~=1.1',
    'types-pkg-resources~=0.1.3',
    'types-typed-ast~=1.4.2',
    'types-requests~=2.25.6',

    'prometheus_client~=0.11.0',
] + DOCS_DEPS

BUILD_DEPS = [
    CYTHON_DEPENDENCY,
    'packaging>=21.0',
    'setuptools-rust~=0.12.1',
    'wheel',  # needed by PyYAML and immutables, refs pypa/pip#5865
] + COMMON_DEPS

RUST_VERSION = '1.53.0'  # Also update docs/internal/dev.rst

EDGEDBCLI_REPO = 'https://github.com/edgedb/edgedb-cli'
# This can be a branch, tag, or commit
EDGEDBCLI_COMMIT = 'master'

EXTRA_DEPS = {
    'test': TEST_DEPS,
    'docs': DOCS_DEPS,
    'build': BUILD_DEPS,
}

EXT_CFLAGS = ['-O2']
EXT_LDFLAGS: List[str] = []

ROOT_PATH = pathlib.Path(__file__).parent.resolve()


if platform.uname().system != 'Windows':
    EXT_CFLAGS.extend([
        '-std=c99', '-fsigned-char', '-Wall', '-Wsign-compare', '-Wconversion'
    ])


def _compile_parsers(build_lib, inplace=False):
    import parsing

    import edb.edgeql.parser.grammar.single as edgeql_spec
    import edb.edgeql.parser.grammar.block as edgeql_spec2
    import edb.edgeql.parser.grammar.sdldocument as schema_spec

    for spec in (edgeql_spec, edgeql_spec2, schema_spec):
        spec_path = pathlib.Path(spec.__file__).parent
        subpath = pathlib.Path(str(spec_path)[len(str(ROOT_PATH)) + 1:])
        pickle_name = spec.__name__.rpartition('.')[2] + '.pickle'
        pickle_path = subpath / pickle_name
        cache = build_lib / pickle_path
        cache.parent.mkdir(parents=True, exist_ok=True)
        parsing.Spec(spec, pickleFile=str(cache), verbose=True)
        if inplace:
            shutil.copy2(cache, ROOT_PATH / pickle_path)


def _compile_build_meta(build_lib, version, pg_config, runstate_dir,
                        shared_dir, version_suffix):
    from edb.common import verutils

    parsed_version = verutils.parse_version(version)
    vertuple = list(parsed_version._asdict().values())
    vertuple[2] = int(vertuple[2])
    if version_suffix:
        vertuple[4] = tuple(version_suffix.split('.'))
    vertuple = tuple(vertuple)

    pg_config_path = pathlib.Path(pg_config)
    if not pg_config_path.is_absolute():
        pg_config_path = f"_ROOT / {str(pg_config_path)!r}"
    else:
        pg_config_path = repr(str(pg_config_path))

    if runstate_dir:
        runstate_dir_path = pathlib.Path(runstate_dir)
        if not runstate_dir_path.is_absolute():
            runstate_dir_path = f"_ROOT / {str(runstate_dir_path)!r}"
        else:
            runstate_dir_path = repr(str(runstate_dir_path))
    else:
        runstate_dir_path = "None  # default to <data-dir>"

    shared_dir_path = pathlib.Path(shared_dir)
    if not shared_dir_path.is_absolute():
        shared_dir_path = f"_ROOT / {str(shared_dir_path)!r}"
    else:
        shared_dir_path = repr(str(shared_dir_path))

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

        import pathlib

        _ROOT = pathlib.Path(__file__).parent

        PG_CONFIG_PATH = {pg_config_path}
        RUNSTATE_DIR = {runstate_dir_path}
        SHARED_DATA_DIR = {shared_dir_path}
        VERSION = {version!r}
    ''').format(
        version=vertuple,
        pg_config_path=pg_config_path,
        runstate_dir_path=runstate_dir_path,
        shared_dir_path=shared_dir_path,
    )

    directory = build_lib / 'edb'
    if not directory.exists():
        directory.mkdir(parents=True)

    with open(directory / '_buildmeta.py', 'w+t') as f:
        f.write(content)


def _get_env_with_openssl_flags():
    env = dict(os.environ)
    cflags = env.get('EDGEDB_BUILD_OPENSSL_CFLAGS')
    ldflags = env.get('EDGEDB_BUILD_OPENSSL_LDFLAGS')

    if not (cflags or ldflags) and platform.system() == 'Darwin':
        try:
            openssl_prefix = pathlib.Path(subprocess.check_output(
                ['brew', '--prefix', 'openssl'], text=True
            ).strip())
        except (FileNotFoundError, subprocess.CalledProcessError):
            openssl_prefix = None
        else:
            pc_path = str(openssl_prefix / 'lib' / 'pkgconfig')
            if 'PKG_CONFIG_PATH' in env:
                env['PKG_CONFIG_PATH'] += f':{pc_path}'
            else:
                env['PKG_CONFIG_PATH'] = pc_path
        try:
            cflags = subprocess.check_output(
                ['pkg-config', '--cflags', 'openssl'], text=True, env=env
            ).strip()
            ldflags = subprocess.check_output(
                ['pkg-config', '--libs', 'openssl'], text=True, env=env
            ).strip()
        except FileNotFoundError:
            # pkg-config is not installed
            if openssl_prefix:
                cflags = f'-I{openssl_prefix / "include"!s}'
                ldflags = f'-L{openssl_prefix / "lib"!s}'
            else:
                return env
        except subprocess.CalledProcessError:
            # Cannot find flags with pkg-config
            return env

    if cflags:
        if 'CPPFLAGS' in env:
            env['CPPFLAGS'] += f' {cflags}'
        elif 'CFLAGS' in env:
            env['CFLAGS'] += f' {cflags}'
        else:
            env['CPPFLAGS'] = cflags
    if ldflags:
        if 'LDFLAGS' in env:
            env['LDFLAGS'] += f' {ldflags}'
        else:
            env['LDFLAGS'] = ldflags
    return env


def _compile_postgres(build_base, *,
                      force_build=False, fresh_build=True,
                      run_configure=True, build_contrib=True):

    proc = subprocess.run(
        ['git', 'submodule', 'status', 'postgres'],
        stdout=subprocess.PIPE, universal_newlines=True, check=True)
    status = proc.stdout
    if status[0] == '-':
        print('postgres submodule not initialized, '
              'run `git submodule init; git submodule update`')
        exit(1)

    source_stamp = _get_pg_source_stamp()

    postgres_build = (build_base / 'postgres').resolve()
    postgres_src = ROOT_PATH / 'postgres'
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
            env = _get_env_with_openssl_flags()
            subprocess.run([
                str(postgres_src / 'configure'),
                '--prefix=' + str(postgres_build / 'install'),
                '--with-openssl',
                '--with-uuid=' + uuidlib,
            ], check=True, cwd=str(build_dir), env=env)

        subprocess.run(
            ['make', 'MAKELEVEL=0', '-j', str(max(os.cpu_count() - 1, 1))],
            cwd=str(build_dir), check=True)

        if build_contrib or fresh_build or is_outdated:
            subprocess.run(
                [
                    'make', '-C', 'contrib', 'MAKELEVEL=0', '-j',
                    str(max(os.cpu_count() - 1, 1))
                ],
                cwd=str(build_dir), check=True)

        subprocess.run(
            ['make', 'MAKELEVEL=0', 'install'],
            cwd=str(build_dir), check=True)

        if build_contrib or fresh_build or is_outdated:
            subprocess.run(
                ['make', '-C', 'contrib', 'MAKELEVEL=0', 'install'],
                cwd=str(build_dir), check=True)

        with open(postgres_build_stamp, 'w') as f:
            f.write(source_stamp)


def _check_rust():
    import packaging.version

    try:
        rustc_ver = (
            subprocess.check_output(["rustc", '-V'], text=True).split()[1]
            .rstrip("-nightly")
        )
        if (
            packaging.version.parse(rustc_ver)
            < packaging.version.parse(RUST_VERSION)
        ):
            raise RuntimeError(
                f'please upgrade Rust to {RUST_VERSION} to compile '
                f'edgedb from source')
    except FileNotFoundError:
        raise RuntimeError(
            f'please install rustc >= {RUST_VERSION} to compile '
            f'edgedb from source (see https://rustup.rs/)')


def _get_edgedbcli_rev(name):
    output = subprocess.check_output(
        ['git', 'ls-remote', EDGEDBCLI_REPO, name],
        universal_newlines=True,
    ).strip()
    if not output:
        return None
    rev, _ = output.split()
    return rev


def _get_pg_source_stamp():
    output = subprocess.check_output(
        ['git', 'submodule', 'status', 'postgres'], universal_newlines=True,
    )
    revision, _, _ = output[1:].partition(' ')
    # I don't know why we needed the first empty char, but we don't want to
    # force everyone to rebuild postgres either
    source_stamp = output[0] + revision
    return source_stamp


def _compile_cli(build_base, build_temp):
    _check_rust()
    rust_root = build_base / 'cli'
    env = dict(os.environ)
    env['CARGO_TARGET_DIR'] = str(build_temp / 'rust' / 'cli')
    env['PSQL_DEFAULT_PATH'] = build_base / 'postgres' / 'install' / 'bin'
    git_name = env.get("EDGEDBCLI_GIT_REV")
    if not git_name:
        git_name = EDGEDBCLI_COMMIT
    # The name can be a branch or tag, so we attempt to look it up
    # with ls-remote. If we don't find anything, we assume it's a
    # commit hash.
    git_rev = _get_edgedbcli_rev(git_name)
    if not git_rev:
        git_rev = git_name

    subprocess.run(
        [
            'cargo', 'install',
            '--verbose', '--verbose',
            '--git', EDGEDBCLI_REPO,
            '--rev', git_rev,
            '--bin', 'edgedb',
            '--root', rust_root,
            '--features=dev_mode',
            '--locked',
            '--debug',
        ],
        env=env,
        check=True,
    )

    cli_dest = ROOT_PATH / 'edb' / 'cli' / 'edgedb'
    # Delete the target first, to avoid "Text file busy" errors during
    # the copy if the CLI is currently running.
    try:
        cli_dest.unlink()
    except FileNotFoundError:
        pass

    shutil.copy(
        rust_root / 'bin' / 'edgedb',
        cli_dest,
    )


def _build_ui(build_base, build_temp):
    from edb import buildmeta

    ui_root = build_base / 'edgedb-studio'
    if not ui_root.exists():
        subprocess.run(
            [
                'git',
                'clone',
                'https://github.com/edgedb/edgedb-studio.git',
                ui_root,
            ],
            check=True
        )
    else:
        subprocess.run(
            ['git', 'pull'],
            check=True,
            cwd=ui_root,
        )

    dest = buildmeta.get_shared_data_dir_path() / 'ui'
    if dest.exists():
        shutil.rmtree(dest)

    # install deps
    subprocess.run(['yarn'], check=True, cwd=ui_root)

    # run build
    env = dict(os.environ)
    # With CI=true (set in GH CI) `yarn build` fails if there are any
    # warnings. We don't need this check in our build so we're disabling
    # this behavior.
    env['CI'] = ''
    subprocess.run(
        ['yarn', 'build'],
        check=True,
        cwd=ui_root / 'web',
        env=env
    )

    shutil.copytree(ui_root / 'web' / 'build', dest)


class build(distutils_build.build):

    user_options = distutils_build.build.user_options + [
        ('pg-config=', None, 'path to pg_config to use with this build'),
        ('runstatedir=', None, 'directory to use for the runtime state'),
        ('shared-dir=', None, 'directory to use for shared data'),
        ('version-suffix=', None, 'dot-separated local version suffix'),
    ]

    def initialize_options(self):
        super().initialize_options()
        self.pg_config = None
        self.runstatedir = None
        self.shared_dir = None
        self.version_suffix = None

    def finalize_options(self):
        super().finalize_options()
        if self.pg_config is None:
            self.pg_config = os.environ.get("EDGEDB_BUILD_PG_CONFIG")
        if self.runstatedir is None:
            self.runstatedir = os.environ.get("EDGEDB_BUILD_RUNSTATEDIR")
        if self.shared_dir is None:
            self.shared_dir = os.environ.get("EDGEDB_BUILD_SHARED_DIR")
        if self.version_suffix is None:
            self.version_suffix = os.environ.get("EDGEDB_BUILD_VERSION_SUFFIX")

    def run(self, *args, **kwargs):
        super().run(*args, **kwargs)
        build_lib = pathlib.Path(self.build_lib)
        _compile_parsers(build_lib)
        if (
            self.pg_config
            or self.runstatedir
            or self.shared_dir
            or self.version_suffix
        ):
            _compile_build_meta(
                build_lib,
                self.distribution.metadata.version,
                self.pg_config,
                self.runstatedir,
                self.shared_dir,
                self.version_suffix,
            )


class develop(setuptools_develop.develop):

    def run(self, *args, **kwargs):
        from edb import buildmeta
        from edb.common import devmode

        try:
            buildmeta.get_build_metadata_value("SHARED_DATA_DIR")
        except buildmeta.MetadataError:
            # buildmeta path resolution needs this
            devmode.enable_dev_mode()

        build = self.get_finalized_command('build')
        build_temp = pathlib.Path(build.build_temp).resolve()
        build_base = pathlib.Path(build.build_base).resolve()

        _compile_cli(build_base, build_temp)
        scripts = self.distribution.entry_points['console_scripts']
        patched_scripts = []
        for s in scripts:
            s = f'{s}_dev'
            patched_scripts.append(s)
        patched_scripts.append('edb = edb.tools.edb:edbcommands')
        patched_scripts.append('edgedb = edb.cli:rustcli')
        self.distribution.entry_points['console_scripts'] = patched_scripts

        super().run(*args, **kwargs)

        _compile_parsers(build_base / 'lib', inplace=True)
        _compile_postgres(build_base)
        _build_ui(build_base, build_temp)


class ci_helper(setuptools.Command):

    description = "echo specified hash or build info for CI"
    user_options = [
        ('type=', None,
         'one of: cli, rust, ext, parsers, postgres, bootstrap, '
         'build_temp, build_lib'),
    ]

    def run(self):
        import edb as _edb
        from edb.buildmeta import hash_dirs, get_cache_src_dirs

        build = self.get_finalized_command('build')
        pkg_dir = pathlib.Path(_edb.__path__[0])

        if self.type == 'parsers':
            parser_hash = hash_dirs(
                [(pkg_dir / 'edgeql/parser/grammar', '.py')],
                extra_files=[pkg_dir / 'edgeql-parser/src/keywords.rs'],
            )
            print(binascii.hexlify(parser_hash).decode())

        elif self.type == 'postgres':
            print(_get_pg_source_stamp().strip())

        elif self.type == 'bootstrap':
            bootstrap_hash = hash_dirs(
                get_cache_src_dirs(),
                extra_files=[pkg_dir / 'server/bootstrap.py'],
            )
            print(binascii.hexlify(bootstrap_hash).decode())

        elif self.type == 'rust':
            rust_hash = hash_dirs([
                (pkg_dir / 'edgeql-parser', '.rs'),
                (pkg_dir / 'edgeql-rust', '.rs'),
                (pkg_dir / 'graphql-rewrite', '.rs'),
            ], extra_files=[
                pkg_dir / 'edgeql-parser/Cargo.toml',
                pkg_dir / 'edgeql-rust/Cargo.toml',
                pkg_dir / 'graphql-rewrite/Cargo.toml',
            ])
            print(binascii.hexlify(rust_hash).decode())

        elif self.type == 'ext':
            ext_hash = hash_dirs([
                (pkg_dir, '.pyx'),
                (pkg_dir, '.pyi'),
                (pkg_dir, '.pxd'),
                (pkg_dir, '.pxi'),
            ])
            print(binascii.hexlify(ext_hash).decode())

        elif self.type == 'cli':
            print(_get_edgedbcli_rev(EDGEDBCLI_COMMIT) or EDGEDBCLI_COMMIT)

        elif self.type == 'build_temp':
            print(pathlib.Path(build.build_temp).resolve())

        elif self.type == 'build_lib':
            print(pathlib.Path(build.build_lib).resolve())

        else:
            raise RuntimeError(
                f'Illegal --type={self.type}; can only be: '
                'cli, rust, ext, postgres, bootstrap, parsers,'
                'build_temp or build_lib'
            )

    def initialize_options(self):
        self.type = None

    def finalize_options(self):
        pass


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
        build = self.get_finalized_command('build')
        _compile_postgres(
            pathlib.Path(build.build_base).resolve(),
            force_build=True,
            fresh_build=self.fresh_build,
            run_configure=self.configure,
            build_contrib=self.build_contrib)


class build_ext(setuptools_build_ext.build_ext):

    user_options = setuptools_build_ext.build_ext.user_options + [
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
            self.debug = False
        self.build_mode = os.environ.get('BUILD_EXT_MODE', 'both')

    def finalize_options(self):
        # finalize_options() may be called multiple times on the
        # same command object, so make sure not to override previously
        # set options.
        if getattr(self, '_initialized', False):
            return

        if self.build_mode not in {'both', 'py-only', 'rust-only', 'skip'}:
            raise RuntimeError(f'Illegal BUILD_EXT_MODE={self.build_mode}; '
                               f'can only be "both", "py-only" or "skip".')
        if self.build_mode not in {'both', 'py-only'}:
            super(build_ext, self).finalize_options()
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
            include_path=["edb/server/pgproto/"])

        super(build_ext, self).finalize_options()

    def run(self):
        if self.build_mode != 'skip':
            super().run()
        else:
            distutils.log.info(f'Skipping build_ext because '
                               f'BUILD_EXT_MODE={self.build_mode}')


class build_cli(setuptools.Command):

    description = "build the EdgeDB CLI"
    user_options: List[str] = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self, *args, **kwargs):
        build = self.get_finalized_command('build')
        _compile_cli(
            pathlib.Path(build.build_base).resolve(),
            pathlib.Path(build.build_temp).resolve(),
        )


class build_ui(setuptools.Command):

    description = "build EdgeDB UI"
    user_options: List[str] = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self, *args, **kwargs):
        from edb import buildmeta
        from edb.common import devmode

        try:
            buildmeta.get_build_metadata_value("SHARED_DATA_DIR")
        except buildmeta.MetadataError:
            # buildmeta path resolution needs this
            devmode.enable_dev_mode()

        build = self.get_finalized_command('build')
        _build_ui(
            pathlib.Path(build.build_base).resolve(),
            pathlib.Path(build.build_temp).resolve(),
        )


class build_parsers(setuptools.Command):

    description = "build the parsers"
    user_options = [
        ('inplace', None,
         'ignore build-lib and put compiled parsers into the source directory '
         'alongside your pure Python modules')]

    def initialize_options(self):
        self.inplace = None

    def finalize_options(self):
        pass

    def run(self, *args, **kwargs):
        build = self.get_finalized_command('build')
        if self.inplace:
            build_base = pathlib.Path(build.build_base).resolve()
            _compile_parsers(build_base / 'lib', inplace=True)
        else:
            build_lib = pathlib.Path(build.build_lib)
            _compile_parsers(build_lib)


COMMAND_CLASSES = {
    'build': build,
    'build_ext': build_ext,
    'develop': develop,
    'build_postgres': build_postgres,
    'build_cli': build_cli,
    'build_parsers': build_parsers,
    'build_ui': build_ui,
    'ci_helper': ci_helper,
}

if setuptools_rust is not None:
    rust_extensions = [
        setuptools_rust.RustExtension(
            "edb._edgeql_rust",
            path="edb/edgeql-rust/Cargo.toml",
            binding=setuptools_rust.Binding.RustCPython,
        ),
        setuptools_rust.RustExtension(
            "edb._graphql_rewrite",
            path="edb/graphql-rewrite/Cargo.toml",
            binding=setuptools_rust.Binding.RustCPython,
        ),
    ]

    class build_rust(setuptools_rust.build.build_rust):
        def run(self):
            _check_rust()
            build_ext = self.get_finalized_command("build_ext")
            if build_ext.build_mode not in {'both', 'rust-only'}:
                distutils.log.info(f'Skipping build_rust because '
                                   f'BUILD_EXT_MODE={build_ext.build_mode}')
                return
            self.plat_name = build_ext.plat_name
            copy_list = []
            if not build_ext.inplace:
                for ext in self.distribution.rust_extensions:
                    # Always build in-place because later stages of the build
                    # may depend on the modules having been built
                    dylib_path = pathlib.Path(
                        build_ext.get_ext_fullpath(ext.name))
                    build_ext.inplace = True
                    target_path = pathlib.Path(
                        build_ext.get_ext_fullpath(ext.name))
                    build_ext.inplace = False
                    copy_list.append((dylib_path, target_path))

                    # Workaround a bug in setuptools-rust: it uses
                    # shutil.copyfile(), which is not safe w.r.t mmap,
                    # so if the target module has been previously loaded
                    # bad things will happen.
                    if target_path.exists():
                        target_path.unlink()

                    target_path.parent.mkdir(parents=True, exist_ok=True)

            os.environ['CARGO_TARGET_DIR'] = str(
                pathlib.Path(build_ext.build_temp) / 'rust' / 'extensions',
            )
            super().run()

            for src, dst in copy_list:
                shutil.copyfile(src, dst)

    COMMAND_CLASSES['build_rust'] = build_rust
else:
    rust_extensions = []


def _version():
    from edb import buildmeta
    return buildmeta.get_version_from_scm(ROOT_PATH)


setuptools.setup(
    version=_version(),
    setup_requires=BUILD_DEPS,
    python_requires='>=3.10.0',
    name='edgedb-server',
    description='EdgeDB Server',
    author='MagicStack Inc.',
    author_email='hello@magic.io',
    packages=['edb'],
    include_package_data=True,
    cmdclass=COMMAND_CLASSES,
    entry_points={
        'console_scripts': [
            'edgedb-server = edb.server.main:main',
        ],
    },
    ext_modules=[
        setuptools_extension.Extension(
            "edb.server.cache.stmt_cache",
            ["edb/server/cache/stmt_cache.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS),

        setuptools_extension.Extension(
            "edb.protocol.protocol",
            ["edb/protocol/protocol.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS),

        setuptools_extension.Extension(
            "edb.server.pgproto.pgproto",
            ["edb/server/pgproto/pgproto.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS),

        setuptools_extension.Extension(
            "edb.server.dbview.dbview",
            ["edb/server/dbview/dbview.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS),

        setuptools_extension.Extension(
            "edb.server.protocol.binary",
            ["edb/server/protocol/binary.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS),

        setuptools_extension.Extension(
            "edb.server.protocol.notebook_ext",
            ["edb/server/protocol/notebook_ext.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS),

        setuptools_extension.Extension(
            "edb.server.protocol.ui_ext",
            ["edb/server/protocol/ui_ext.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS),

        setuptools_extension.Extension(
            "edb.server.protocol.edgeql_ext",
            ["edb/server/protocol/edgeql_ext.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS),

        setuptools_extension.Extension(
            "edb.server.protocol.protocol",
            ["edb/server/protocol/protocol.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS),

        setuptools_extension.Extension(
            "edb.server.pgcon.pgcon",
            ["edb/server/pgcon/pgcon.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS),

        setuptools_extension.Extension(
            "edb.graphql.extension",
            ["edb/graphql/extension.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS),
    ],
    rust_extensions=rust_extensions,
    install_requires=RUNTIME_DEPS,
    extras_require=EXTRA_DEPS,
)
