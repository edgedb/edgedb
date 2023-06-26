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
import importlib
import os
import os.path
import pathlib
import platform
import re
import shutil
import subprocess
import textwrap

import setuptools
from setuptools import extension as setuptools_extension
from setuptools.command import build as setuptools_build
from setuptools.command import build_ext as setuptools_build_ext

import distutils

import Cython.Build
import parsing
import setuptools_rust


EDGEDBCLI_REPO = 'https://github.com/edgedb/edgedb-cli'
# This can be a branch, tag, or commit
EDGEDBCLI_COMMIT = 'master'

EDGEDBGUI_REPO = 'https://github.com/edgedb/edgedb-studio.git'
# This can be a branch, tag, or commit
EDGEDBGUI_COMMIT = 'main'

PGVECTOR_REPO = 'https://github.com/pgvector/pgvector.git'
# This can be a branch, tag, or commit
PGVECTOR_COMMIT = 'v0.4.2'

ZOMBODB_REPO = 'https://github.com/zombodb/zombodb.git'
# This can be a branch, tag, or commit
ZOMBODB_COMMIT = 'v3000.1.20'

SAFE_EXT_CFLAGS: list[str] = []
if flag := os.environ.get('EDGEDB_OPT_CFLAG'):
    SAFE_EXT_CFLAGS += [flag]
else:
    SAFE_EXT_CFLAGS += ['-O2']

EXT_CFLAGS: list[str] = list(SAFE_EXT_CFLAGS)
EXT_LDFLAGS: list[str] = []

ROOT_PATH = pathlib.Path(__file__).parent.resolve()

EXT_INC_DIRS = [
    (ROOT_PATH / 'edb' / 'server' / 'pgproto').as_posix(),
    (ROOT_PATH / 'edb' / 'pgsql' / 'parser' / 'libpg_query').as_posix()
]

EXT_LIB_DIRS = [
    (ROOT_PATH / 'edb' / 'pgsql' / 'parser' / 'libpg_query').as_posix()
]


if platform.uname().system != 'Windows':
    EXT_CFLAGS.extend([
        '-std=c99', '-fsigned-char', '-Wall', '-Wsign-compare', '-Wconversion'
    ])


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

    directory = pathlib.Path(build_lib) / 'edb'
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
                      run_configure=True, build_contrib=True,
                      produce_compile_commands_json=False):

    proc = subprocess.run(
        ['git', 'submodule', 'status', 'postgres'],
        stdout=subprocess.PIPE,
        universal_newlines=True,
        check=True,
        cwd=ROOT_PATH,
    )
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
        if not run_configure:
            run_configure = not (build_dir / 'Makefile').exists()

        if run_configure or fresh_build or is_outdated:
            env = _get_env_with_openssl_flags()
            subprocess.run([
                str(postgres_src / 'configure'),
                '--prefix=' + str(postgres_build / 'install'),
                '--with-openssl',
                '--with-uuid=' + uuidlib,
            ], check=True, cwd=str(build_dir), env=env)

        if produce_compile_commands_json:
            make = ['bear', '--', 'make']
        else:
            make = ['make']

        subprocess.run(
            make + ['MAKELEVEL=0', '-j', str(max(os.cpu_count() - 1, 1))],
            cwd=str(build_dir), check=True)

        if build_contrib or fresh_build or is_outdated:
            subprocess.run(
                make + [
                    '-C', 'contrib', 'MAKELEVEL=0', '-j',
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

        if produce_compile_commands_json:
            shutil.copy(
                build_dir / "compile_commands.json",
                postgres_src / "compile_commands.json",
            )


def _compile_pgvector(build_base, build_temp):
    git_rev = _get_git_rev(PGVECTOR_REPO, PGVECTOR_COMMIT)

    pgv_root = (build_temp / 'pgvector').resolve()
    if not pgv_root.exists():
        subprocess.run(
            [
                'git',
                'clone',
                '--recursive',
                PGVECTOR_REPO,
                pgv_root,
            ],
            check=True
        )
    else:
        subprocess.run(
            ['git', 'fetch', '--all'],
            check=True,
            cwd=pgv_root,
        )

    subprocess.run(
        ['git', 'reset', '--hard', git_rev],
        check=True,
        cwd=pgv_root,
    )

    pg_config = (
        build_base / 'postgres' / 'install' / 'bin' / 'pg_config'
    ).resolve()

    cflags = os.environ.get("CFLAGS", "")
    cflags = f"{cflags} {' '.join(SAFE_EXT_CFLAGS)} -std=gnu99"

    subprocess.run(
        [
            'make',
            f'PG_CONFIG={pg_config}',
        ],
        cwd=pgv_root,
        check=True,
    )

    subprocess.run(
        [
            'make',
            'install',
            f'PG_CONFIG={pg_config}',
        ],
        cwd=pgv_root,
        check=True,
    )


def _compile_zombodb(build_base, build_temp):
    git_rev = _get_git_rev(ZOMBODB_REPO, ZOMBODB_COMMIT)

    zdb_root = (build_temp / 'zombodb').resolve()
    if not zdb_root.exists():
        subprocess.run(
            [
                'git',
                'clone',
                '--recursive',
                ZOMBODB_REPO,
                zdb_root,
            ],
            check=True
        )
    else:
        subprocess.run(
            ['git', 'fetch', '--all'],
            check=True,
            cwd=zdb_root,
        )

    subprocess.run(
        ['git', 'reset', '--hard', git_rev],
        check=True,
        cwd=zdb_root,
    )

    # Workaround Cargo's unwillingness to build a nested Cargo.toml
    with open(zdb_root / "Cargo.toml", "a") as f:
        f.write(f"\n[workspace]")

    pg_config = (
        build_base / 'postgres' / 'install' / 'bin' / 'pg_config'
    ).resolve()

    pg_ver_proc = subprocess.run(
        [
            str(pg_config),
            '--version',
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    pg_ver = pg_ver_proc.stdout.strip()

    if m := re.match(r"PostgreSQL (?P<major>\d+)\.(\d+).*", pg_ver):
        pg_major_ver = m.group("major")
    else:
        raise RuntimeError(f"could not parse Postgres version: {pg_ver!r}")

    env = dict(os.environ)

    env['CARGO_TARGET_DIR'] = str(build_temp / 'rust' / 'target')

    pgrx_dir = build_temp / 'rust' / 'cargo-pgrx'
    pgrx_home = build_temp / 'rust' / 'pgrx-config'
    pgrx_home.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            'cargo', 'install',
            '--verbose', '--verbose',
            '--root', str(pgrx_dir),
            'cargo-pgrx',
        ],
        env=env,
        check=True,
    )

    env["PATH"] = f"{pgrx_dir / 'bin'}{os.pathsep}{env['PATH']}"
    env["PGRX_HOME"] = str(pgrx_home)

    subprocess.run(
        [
            'cargo', 'pgrx', 'init',
            f'--pg{pg_major_ver}={pg_config}',
        ],
        env=env,
        check=True,
    )

    subprocess.run(
        [
            'cargo', 'pgrx', 'install',
            f'--pg-config={pg_config}',
        ],
        env=env,
        cwd=zdb_root,
        check=True,
    )


def _compile_libpg_query():
    dir = (ROOT_PATH / 'edb' / 'pgsql' / 'parser' / 'libpg_query').resolve()

    if not (dir / 'README.md').exists():
        print('libpg_query submodule has not been initialized, '
              'run `git submodule update --init --recursive`')
        exit(1)

    cflags = os.environ.get("CFLAGS", "")
    cflags = f"{cflags} {' '.join(SAFE_EXT_CFLAGS)} -std=gnu99"

    subprocess.run(
        [
            'make',
            'build',
            '-j',
            str(max(os.cpu_count() - 1, 1)),
            f'CFLAGS={cflags}',
        ],
        cwd=str(dir),
        check=True,
    )


def _get_git_rev(repo, ref):
    output = subprocess.check_output(
        ['git', 'ls-remote', repo, ref],
        universal_newlines=True,
    ).strip()

    if output:
        rev, _ = output.split()
        rev = rev.strip()
    else:
        rev = ''

    # The name can be a branch or tag, so we attempt to look it up
    # with ls-remote. If we don't find anything, we assume it's a
    # commit hash.
    return rev if rev else ref


def _get_pg_source_stamp():
    output = subprocess.check_output(
        ['git', 'submodule', 'status', 'postgres'],
        universal_newlines=True,
        cwd=ROOT_PATH,
    )
    revision, _, _ = output[1:].partition(' ')
    source_stamp = revision + '+' + PGVECTOR_COMMIT
    return source_stamp


def _compile_cli(build_base, build_temp):
    rust_root = build_base / 'cli'
    env = dict(os.environ)
    env['CARGO_TARGET_DIR'] = str(build_temp / 'rust' / 'target')
    env['PSQL_DEFAULT_PATH'] = build_base / 'postgres' / 'install' / 'bin'
    git_ref = env.get("EDGEDBCLI_GIT_REV") or EDGEDBCLI_COMMIT
    git_rev = _get_git_rev(EDGEDBCLI_REPO, git_ref)

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


class build(setuptools_build.build):

    user_options = setuptools_build.build.user_options

    sub_commands = (
        [
            ("build_libpg_query", lambda self: True),
            *setuptools_build.build.sub_commands,
            ("build_metadata", lambda self: True),
            ("build_parsers", lambda self: True),
            ("build_postgres", lambda self: True),
            ("build_cli", lambda self: True),
            ("build_ui", lambda self: True),
        ]
    )


class build_metadata(setuptools.Command):

    build_lib: str

    user_options = [
        ('pg-config=', None, 'path to pg_config to use with this build'),
        ('runstatedir=', None, 'directory to use for the runtime state'),
        ('shared-dir=', None, 'directory to use for shared data'),
        ('version-suffix=', None, 'dot-separated local version suffix'),
    ]

    def initialize_options(self):
        self.build_lib = None
        self.editable_mode = False
        self.pg_config = None
        self.runstatedir = None
        self.shared_dir = None
        self.version_suffix = None

    def finalize_options(self):
        self.set_undefined_options("build_py", ("build_lib", "build_lib"))
        if self.pg_config is None:
            self.pg_config = os.environ.get("EDGEDB_BUILD_PG_CONFIG")
        if self.runstatedir is None:
            self.runstatedir = os.environ.get("EDGEDB_BUILD_RUNSTATEDIR")
        if self.shared_dir is None:
            self.shared_dir = os.environ.get("EDGEDB_BUILD_SHARED_DIR")
        if self.version_suffix is None:
            self.version_suffix = os.environ.get("EDGEDB_BUILD_VERSION_SUFFIX")

    def has_build_metadata(self) -> bool:
        return bool(
            self.pg_config
            or self.runstatedir
            or self.shared_dir
            or self.version_suffix
        )

    def get_outputs(self) -> list[str]:
        if self.has_build_metadata():
            return [
                str(pathlib.Path(self.build_lib) / 'edb' / '_buildmeta.py'),
            ]
        else:
            return []

    def run(self, *args, **kwargs):
        if self.has_build_metadata():
            _compile_build_meta(
                self.build_lib,
                self.distribution.metadata.version,
                self.pg_config,
                self.runstatedir,
                self.shared_dir,
                self.version_suffix,
            )


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
                (pkg_dir / 'graphql-rewrite', '.rs'),
            ], extra_files=[
                pkg_dir / 'edgeql-parser/Cargo.toml',
                pkg_dir / 'edgeql-parser/edgeql-parser-python/Cargo.toml',
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
            print(_get_git_rev(EDGEDBCLI_REPO, EDGEDBCLI_COMMIT))

        elif self.type == 'ui':
            print(_get_git_rev(EDGEDBGUI_REPO, EDGEDBGUI_COMMIT))

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
        ('compile-commands', None, 'produce compile-commands.json using bear'),
    ]

    editable_mode: bool

    def initialize_options(self):
        self.editable_mode = False
        self.configure = False
        self.build_contrib = False
        self.fresh_build = False
        self.compile_commands = False

    def finalize_options(self):
        pass

    def run(self, *args, **kwargs):
        if os.environ.get("EDGEDB_BUILD_PACKAGE"):
            return
        build = self.get_finalized_command('build')
        _compile_postgres(
            pathlib.Path(build.build_base).resolve(),
            force_build=True,
            fresh_build=self.fresh_build,
            run_configure=self.configure,
            build_contrib=self.build_contrib,
            produce_compile_commands_json=self.compile_commands,
        )
        _compile_pgvector(
            pathlib.Path(build.build_base).resolve(),
            pathlib.Path(build.build_temp).resolve(),
        )
        _compile_zombodb(
            pathlib.Path(build.build_base).resolve(),
            pathlib.Path(build.build_temp).resolve(),
        )


class build_postgres_extensions(setuptools.Command):

    description = "build postgres extensions"

    user_options: list[str] = []

    editable_mode: bool

    def initialize_options(self):
        self.editable_mode = False

    def finalize_options(self):
        pass

    def run(self):
        build = self.get_finalized_command('build')
        # _compile_pgvector(
        #     pathlib.Path(build.build_base).resolve(),
        #     pathlib.Path(build.build_temp).resolve(),
        # )
        _compile_zombodb(
            pathlib.Path(build.build_base).resolve(),
            pathlib.Path(build.build_temp).resolve(),
        )


class build_libpg_query(setuptools.Command):

    description = "build libpg_query"

    user_options: list[str] = []

    editable_mode: bool

    def initialize_options(self):
        self.editable_mode = False

    def finalize_options(self):
        pass

    def run(self):
        _compile_libpg_query()


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

        self.distribution.ext_modules[:] = Cython.Build.cythonize(
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
    user_options: list[str] = []
    editable_mode: bool

    def initialize_options(self):
        self.editable_mode = False

    def finalize_options(self):
        pass

    def run(self, *args, **kwargs):
        if os.environ.get("EDGEDB_BUILD_PACKAGE"):
            return
        build = self.get_finalized_command('build')
        _compile_cli(
            pathlib.Path(build.build_base).resolve(),
            pathlib.Path(build.build_temp).resolve(),
        )


class build_ui(setuptools.Command):

    description = "build EdgeDB UI"
    user_options: list[str] = []
    editable_mode: bool
    build_lib: str

    def initialize_options(self):
        self.editable_mode = False
        self.build_lib = None

    def finalize_options(self):
        self.set_undefined_options("build_py", ("build_lib", "build_lib"))

    def run(self, *args, **kwargs):
        from edb import buildmeta
        from edb.common import devmode

        try:
            buildmeta.get_build_metadata_value("SHARED_DATA_DIR")
        except buildmeta.MetadataError:
            # buildmeta path resolution needs this
            devmode.enable_dev_mode()

        build = self.get_finalized_command('build')
        self._build_ui(pathlib.Path(build.build_base).resolve())

    def _build_ui(self, build_base: pathlib.Path) -> None:
        from edb import buildmeta

        git_ref = os.environ.get("EDGEDB_UI_GIT_REV") or EDGEDBGUI_COMMIT
        git_rev = _get_git_rev(EDGEDBGUI_REPO, git_ref)

        ui_root = build_base / 'edgedb-studio'
        if not ui_root.exists():
            subprocess.run(
                [
                    'git',
                    'clone',
                    '--recursive',
                    EDGEDBGUI_REPO,
                    ui_root,
                ],
                check=True
            )
        else:
            subprocess.run(
                ['git', 'fetch', '--all'],
                check=True,
                cwd=ui_root,
            )

        subprocess.run(
            ['git', 'reset', '--hard', git_rev],
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


class build_parsers(setuptools.Command):

    description = "build the parsers"

    build_lib: str
    target_root: pathlib.Path
    editable_mode: bool
    inplace: bool

    user_options = [
        ('inplace', None,
         'ignore build-lib and put compiled parsers into the source directory '
         'alongside your pure Python modules')]

    sources = [
        "edb.edgeql.parser.grammar.single",
        "edb.edgeql.parser.grammar.block",
        "edb.edgeql.parser.grammar.fragment",
        "edb.edgeql.parser.grammar.sdldocument",
        "edb.edgeql.parser.grammar.migration_body",
        "edb.edgeql.parser.grammar.extension_package_body",
    ]

    def initialize_options(self):
        self.editable_mode = False
        self.inplace = None
        self.build_lib = None
        self.target_root = None

    def finalize_options(self):
        self.set_undefined_options("build_py", ("build_lib", "build_lib"))
        if self.editable_mode:
            self.inplace = True
        if self.inplace:
            self.target_root = ROOT_PATH
        else:
            self.target_root = pathlib.Path(self.build_lib)

    def get_output_mapping(self) -> dict[str, str]:
        return {
            str(self.target_root / src.parent / f"{src.stem}.pickle"): str(src)
            for src in self._get_source_files()
        }

    def get_outputs(self) -> list[str]:
        return list(self.get_output_mapping())

    def get_source_files(self) -> list[str]:
        return [str(src) for src in self._get_source_files()]

    def run(self, *args, **kwargs):
        for src, dst in zip(self.sources, self.get_outputs()):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            spec_mod = importlib.import_module(src)
            parsing.Spec(spec_mod, pickleFile=dst, verbose=True)

    def _get_source_files(self) -> list[pathlib.Path]:
        return [
            pathlib.Path(src.replace(".", "/") + ".py")
            for src in self.sources
        ]


class build_rust(setuptools_rust.build.build_rust):
    def run(self):
        build_ext = self.get_finalized_command("build_ext")
        if build_ext.build_mode not in {'both', 'rust-only'}:
            distutils.log.info(
                f'Skipping build_rust because '
                f'BUILD_EXT_MODE={build_ext.build_mode}'
            )
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


def _version():
    from edb import buildmeta
    return buildmeta.get_version_from_scm(ROOT_PATH)


_entry_points = {
    'edgedb-server = edb.server.main:main',
    'edgedb = edb.cli:rustcli',
    'edb = edb.tools.edb:edbcommands',
}


setuptools.setup(
    version=_version(),
    entry_points={
        "console_scripts": _entry_points,
    },
    cmdclass={
        'build': build,
        'build_metadata': build_metadata,
        'build_ext': build_ext,
        'build_rust': build_rust,
        'build_postgres': build_postgres,
        'build_postgres_extensions': build_postgres_extensions,
        'build_cli': build_cli,
        'build_parsers': build_parsers,
        'build_ui': build_ui,
        'build_libpg_query': build_libpg_query,
        'ci_helper': ci_helper,
    },
    ext_modules=[
        setuptools_extension.Extension(
            "edb.common.turbo_uuid",
            ["edb/server/pgproto/uuid.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
        ),

        setuptools_extension.Extension(
            "edb.server.cache.stmt_cache",
            ["edb/server/cache/stmt_cache.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
        ),

        setuptools_extension.Extension(
            "edb.protocol.protocol",
            ["edb/protocol/protocol.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
        ),

        setuptools_extension.Extension(
            "edb.server.pgproto.pgproto",
            ["edb/server/pgproto/pgproto.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
        ),

        setuptools_extension.Extension(
            "edb.server.dbview.dbview",
            ["edb/server/dbview/dbview.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
        ),

        setuptools_extension.Extension(
            "edb.server.protocol.binary",
            ["edb/server/protocol/binary.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
        ),

        setuptools_extension.Extension(
            "edb.server.protocol.pg_ext",
            ["edb/server/protocol/pg_ext.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
        ),

        setuptools_extension.Extension(
            "edb.server.protocol.args_ser",
            ["edb/server/protocol/args_ser.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
        ),

        setuptools_extension.Extension(
            "edb.server.protocol.execute",
            ["edb/server/protocol/execute.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
        ),

        setuptools_extension.Extension(
            "edb.server.protocol.notebook_ext",
            ["edb/server/protocol/notebook_ext.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
        ),

        setuptools_extension.Extension(
            "edb.server.protocol.ui_ext",
            ["edb/server/protocol/ui_ext.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
        ),

        setuptools_extension.Extension(
            "edb.server.protocol.edgeql_ext",
            ["edb/server/protocol/edgeql_ext.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
        ),

        setuptools_extension.Extension(
            "edb.server.protocol.frontend",
            ["edb/server/protocol/frontend.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
        ),

        setuptools_extension.Extension(
            "edb.server.protocol.protocol",
            ["edb/server/protocol/protocol.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
        ),

        setuptools_extension.Extension(
            "edb.server.pgcon.pgcon",
            ["edb/server/pgcon/pgcon.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
        ),

        setuptools_extension.Extension(
            "edb.graphql.extension",
            ["edb/graphql/extension.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
        ),

        setuptools_extension.Extension(
            "edb.pgsql.parser.parser",
            ["edb/pgsql/parser/parser.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
            library_dirs=EXT_LIB_DIRS,
            libraries=['pg_query']
        ),
    ],
    rust_extensions=[
        setuptools_rust.RustExtension(
            "edb._edgeql_parser",
            path="edb/edgeql-parser/edgeql-parser-python/Cargo.toml",
            binding=setuptools_rust.Binding.RustCPython,
        ),
        setuptools_rust.RustExtension(
            "edb._graphql_rewrite",
            path="edb/graphql-rewrite/Cargo.toml",
            binding=setuptools_rust.Binding.RustCPython,
        ),
    ],
)
