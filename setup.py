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
import shlex
import shutil
import subprocess
import textwrap

import setuptools
from setuptools import extension as setuptools_extension
from setuptools.command import build as setuptools_build
from setuptools.command import build_ext as setuptools_build_ext

import distutils

import Cython.Build
import setuptools_rust


EDGEDBCLI_REPO = 'https://github.com/edgedb/edgedb-cli'
# This can be a branch, tag, or commit
EDGEDBCLI_COMMIT = 'master'

EDGEDBGUI_REPO = 'https://github.com/edgedb/edgedb-studio.git'
# This can be a branch, tag, or commit
EDGEDBGUI_COMMIT = 'main'

PGVECTOR_REPO = 'https://github.com/pgvector/pgvector.git'
# This can be a branch, tag, or commit
PGVECTOR_COMMIT = 'v0.7.4'

SAFE_EXT_CFLAGS: list[str] = []
if flag := os.environ.get('EDGEDB_OPT_CFLAG'):
    SAFE_EXT_CFLAGS += [flag]
else:
    SAFE_EXT_CFLAGS += ['-O2']

EXT_CFLAGS: list[str] = list(SAFE_EXT_CFLAGS)
# See also: https://github.com/cython/cython/issues/5240
EXT_CFLAGS += ['-Wno-error=incompatible-pointer-types']
EXT_LDFLAGS: list[str] = []

ROOT_PATH = pathlib.Path(__file__).parent.resolve()

EXT_INC_DIRS = [
    (ROOT_PATH / 'edb' / 'server' / 'pgproto').as_posix(),
    (ROOT_PATH / 'edb' / 'pgsql' / 'parser' / 'libpg_query').as_posix()
]

EXT_LIB_DIRS = [
    (ROOT_PATH / 'edb' / 'pgsql' / 'parser' / 'libpg_query').as_posix()
]
EDBSS_DIR = ROOT_PATH / 'edb_stat_statements'


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


def _compile_postgres(build_base, build_temp, *,
                      force_build=False, fresh_build=True,
                      run_configure=True, build_contrib=True,
                      produce_compile_commands_json=False,
                      run_tests=False):

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
            cmd = [
                str(postgres_src / 'configure'),
                '--prefix=' + str(postgres_build / 'install'),
                '--with-openssl',
                '--with-uuid=' + uuidlib,
            ]
            if os.environ.get('EDGEDB_DEBUG'):
                cmd += [
                    '--enable-tap-tests',
                    '--enable-debug',
                ]
                cflags = os.environ.get("CFLAGS", "")
                cflags = f"{cflags} -O0"
                env['CFLAGS'] = cflags
            subprocess.run(cmd, check=True, cwd=str(build_dir), env=env)

        if produce_compile_commands_json:
            make = ['bear', '--', 'make']
        else:
            make = ['make']

        make_args = ['MAKELEVEL=0', '-j', str(max(os.cpu_count() - 1, 1))]

        subprocess.run(
            make + make_args,
            cwd=str(build_dir), check=True)

        if build_contrib or fresh_build or is_outdated:
            subprocess.run(
                make + ['-C', 'contrib'] + make_args,
                cwd=str(build_dir), check=True)

        if run_tests:
            subprocess.run(
                make + ["check-world"],
                cwd=str(build_dir),
                check=True,
                env=os.environ | {"MAKELEVEL": "0"},
            )

        subprocess.run(
            ['make', 'MAKELEVEL=0', 'install'],
            cwd=str(build_dir), check=True)

        if build_contrib or fresh_build or is_outdated:
            subprocess.run(
                ['make', '-C', 'contrib', 'MAKELEVEL=0', 'install'],
                cwd=str(build_dir), check=True)

        pg_config = (
            build_base / 'postgres' / 'install' / 'bin' / 'pg_config'
        ).resolve()
        _compile_pgvector(pg_config, build_temp)
        _compile_edb_stat_statements(pg_config, build_temp)

        with open(postgres_build_stamp, 'w') as f:
            f.write(source_stamp)

        if produce_compile_commands_json:
            shutil.copy(
                build_dir / "compile_commands.json",
                postgres_src / "compile_commands.json",
            )


def _compile_pgvector(pg_config, build_temp):
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


def _compile_edb_stat_statements(pg_config, build_temp):
    subprocess.run(
        [
            'make',
            f'PG_CONFIG={pg_config}',
        ],
        cwd=EDBSS_DIR,
        check=True,
    )

    subprocess.run(
        [
            'make',
            'install',
            f'PG_CONFIG={pg_config}',
        ],
        cwd=EDBSS_DIR,
        check=True,
    )


def _get_env_with_protobuf_c_flags():
    env = dict(os.environ)
    cflags = env.get('EDGEDB_BUILD_PROTOBUFC_CFLAGS')
    ldflags = env.get('EDGEDB_BUILD_PROTOBUFC_LDFLAGS')

    if not (cflags or ldflags) and platform.system() == 'Darwin':
        try:
            prefix = pathlib.Path(subprocess.check_output(
                ['brew', '--prefix', 'protobuf-c'], text=True
            ).strip())
        except (FileNotFoundError, subprocess.CalledProcessError):
            prefix = None
        else:
            pc_path = str(prefix / 'lib' / 'pkgconfig')
            if 'PKG_CONFIG_PATH' in env:
                env['PKG_CONFIG_PATH'] += f':{pc_path}'
            else:
                env['PKG_CONFIG_PATH'] = pc_path
        try:
            cflags = subprocess.check_output(
                ['pkg-config', '--cflags', 'protobuf-c'], text=True, env=env
            ).strip()
            ldflags = subprocess.check_output(
                ['pkg-config', '--libs', 'protobuf-c'], text=True, env=env
            ).strip()
        except (FileNotFoundError, subprocess.CalledProcessError):
            # pkg-config is not installed or cannot find flags with pkg-config
            if not prefix:
                prefix = pathlib.Path("/opt/local")
            cflags = f'-I{prefix / "include"!s}'
            ldflags = f'-L{prefix / "lib"!s}'

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


def _compile_libpg_query():
    dir = (ROOT_PATH / 'edb' / 'pgsql' / 'parser' / 'libpg_query').resolve()

    if not (dir / 'README.md').exists():
        print('libpg_query submodule has not been initialized, '
              'run `git submodule update --init --recursive`')
        exit(1)

    cflags = os.environ.get("CFLAGS", "")
    cflags = f"{cflags} {' '.join(SAFE_EXT_CFLAGS)} -std=gnu99"

    env = _get_env_with_protobuf_c_flags()
    if "CFLAGS" in env:
        env["CFLAGS"] += f' {cflags}'
    else:
        env["CFLAGS"] = cflags

    subprocess.run(
        [
            'make',
            'build',
            '-j',
            str(max(os.cpu_count() - 1, 1)),
        ],
        cwd=str(dir),
        env=env,
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
    from edb.buildmeta import hash_dirs

    output = subprocess.check_output(
        ['git', 'submodule', 'status', '--cached', 'postgres'],
        universal_newlines=True,
        cwd=ROOT_PATH,
    )
    revision, _, _ = output[1:].partition(' ')
    edbss_dir = EDBSS_DIR.as_posix()
    edbss_hash = hash_dirs(
        [(edbss_dir, '.c'), (edbss_dir, '.sql')],
        extra_files=[
            EDBSS_DIR / 'Makefile',
            EDBSS_DIR / 'edb_stat_statements.control',
        ],
    )
    edbss = binascii.hexlify(edbss_hash).decode()
    stamp_list = [revision, PGVECTOR_COMMIT, edbss]
    if os.environ.get('EDGEDB_DEBUG'):
        stamp_list += ['debug']
    source_stamp = '+'.join(stamp_list)
    return source_stamp.strip()


def _get_libpg_query_source_stamp():
    output = subprocess.check_output(
        ['git', 'submodule', 'status', '--cached',
         'edb/pgsql/parser/libpg_query'],
        universal_newlines=True,
        cwd=ROOT_PATH,
    )
    revision, _, _ = output[1:].partition(' ')
    return revision.strip()


def _compile_cli(build_base, build_temp):
    rust_root = build_base / 'cli'
    env = dict(os.environ)
    env['CARGO_TARGET_DIR'] = str(build_temp / 'rust' / 'cli')
    env['PSQL_DEFAULT_PATH'] = build_base / 'postgres' / 'install' / 'bin'
    path = env.get("EDGEDBCLI_PATH")
    args = [
        'cargo', 'install',
        '--verbose', '--verbose',
        '--bin', 'edgedb',
        '--root', rust_root,
        '--features=dev_mode',
        '--locked',
        '--debug',
    ]
    if path:
        args.extend([
            '--path', path,
        ])
    else:
        git_ref = env.get("EDGEDBCLI_GIT_REV") or EDGEDBCLI_COMMIT
        git_rev = _get_git_rev(EDGEDBCLI_REPO, git_ref)
        args.extend([
            '--git', EDGEDBCLI_REPO,
            '--rev', git_rev,
        ])

    subprocess.run(
        args,
        env=env,
        check=True,
    )

    for dest in ('gel', 'edgedb'):
        cli_dest = ROOT_PATH / 'edb' / 'cli' / dest
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


_PYTHON_ONLY = os.environ.get("BUILD_EXT_MODE", "both") == "skip"


class build(setuptools_build.build):

    user_options = setuptools_build.build.user_options

    sub_commands = setuptools_build.build.sub_commands if _PYTHON_ONLY else [
        ("build_libpg_query", lambda self: True),
        *setuptools_build.build.sub_commands,
        ("build_metadata", lambda self: True),
        ("build_parsers", lambda self: True),
        ("build_postgres", lambda self: True),
        ("build_cli", lambda self: True),
        ("build_ui", lambda self: True),
    ]


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
         'one of: cli, rust, ext, parsers, postgres, libpg_query, bootstrap, '
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
                extra_files=[
                    pkg_dir / 'edgeql-parser/src/keywords.rs',
                    pkg_dir / 'edgeql-parser/edgeql-parser-python/src/parser.rs'
                ],
            )
            print(binascii.hexlify(parser_hash).decode())

        elif self.type == 'postgres':
            print(_get_pg_source_stamp())

        elif self.type == 'libpg_query':
            print(_get_libpg_query_source_stamp())

        elif self.type == 'bootstrap':
            bootstrap_hash = hash_dirs(
                get_cache_src_dirs(),
                extra_files=[
                    pkg_dir / 'server/bootstrap.py',
                    pkg_dir / 'buildmeta.py',
                ],
            )
            print(binascii.hexlify(bootstrap_hash).decode())

        elif self.type == 'rust':
            dirs = []
            # HACK: For annoying reasons, metapkg invokes setup.py
            # with an ancient version of Python, and that doesn't have
            # tomllib.  It doesn't invoke *this* code path, though, so
            # import it here.
            import tomllib

            # Read the list of Rust projects from Cargo.toml
            with open(pkg_dir.parent / 'Cargo.toml', 'rb') as f:
                root = tomllib.load(f)
                for member in root['workspace']['members']:
                    dirs.append(pkg_dir.parent / member)
            rust_hash = hash_dirs(
                [(dir, '.rs') for dir in dirs],
                extra_files=[dir / 'Cargo.toml' for dir in dirs] +
                  [pkg_dir.parent / 'Cargo.lock'])
            print(binascii.hexlify(rust_hash).decode())

        elif self.type == 'ext':
            import gel

            ext_hash = hash_dirs(
                [
                    (pkg_dir, '.pyx'),
                    (pkg_dir, '.pyi'),
                    (pkg_dir, '.pxd'),
                    (pkg_dir, '.pxi'),
                ],
                # protocol.pyx for tests links to edgedb-python binary
                extra_data=gel.__version__.encode(),
            )
            print(
                binascii.hexlify(ext_hash).decode() + '-'
                + _get_libpg_query_source_stamp()
            )

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
                'cli, rust, ext, postgres, libpg_query, bootstrap, parsers,'
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
        ('run-tests', None, 'run Postgres test suite after building'),
    ]

    editable_mode: bool

    def initialize_options(self):
        self.editable_mode = False
        self.configure = False
        self.build_contrib = False
        self.fresh_build = False
        self.compile_commands = False
        self.run_tests = False

    def finalize_options(self):
        pass

    def run(self, *args, **kwargs):
        if os.environ.get("EDGEDB_BUILD_PACKAGE"):
            return
        build = self.get_finalized_command('build')
        _compile_postgres(
            pathlib.Path(build.build_base).resolve(),
            pathlib.Path(build.build_temp).resolve(),
            force_build=True,
            fresh_build=self.fresh_build,
            run_configure=self.configure,
            build_contrib=self.build_contrib,
            produce_compile_commands_json=self.compile_commands,
            run_tests=self.run_tests,
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
        ('cython-extra-directives=', None,
            'Extra Cython compiler directives'),
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
            self.cython_extra_directives = "linetrace=True"
            self.define = 'PG_DEBUG,CYTHON_TRACE,CYTHON_TRACE_NOGIL'
            self.debug = True
        else:
            self.cython_always = False
            self.cython_annotate = None
            self.cython_extra_directives = None
            self.debug = False
        self.build_mode = os.environ.get('BUILD_EXT_MODE', 'both')

    def finalize_options(self) -> None:
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

        directives: dict[str, str | bool] = {
            'language_level': '3'
        }

        if self.cython_extra_directives:
            for directive in self.cython_extra_directives.split(','):
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

    description = "build and serialize the parser grammar spec"

    build_lib: str
    target_root: pathlib.Path
    editable_mode: bool
    inplace: bool

    user_options = [
        ('inplace', None,
         'ignore build-lib and put compiled parsers into the source directory '
         'alongside your pure Python modules')]

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

    def run(self, *args, **kwargs):
        # load grammar definitions and build the parser
        from edb.common import parsing
        from edb.edgeql.parser import grammar as qlgrammar
        spec = parsing.load_parser_spec(qlgrammar.start)
        spec_json = parsing.spec_to_json(spec)

        # prepare destination
        dst = str(self.target_root / 'edb' / 'edgeql' / 'grammar.bc')
        os.makedirs(os.path.dirname(dst), exist_ok=True)

        # serialize
        import edb._edgeql_parser as rust_parser
        rust_parser.save_spec(spec_json, str(dst))


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


_protobuf_c_flags = _get_env_with_protobuf_c_flags()
_protobuf_c_cflags = shlex.split(_protobuf_c_flags.get("CPPFLAGS", ""))


setuptools.setup(
    version=_version(),
    cmdclass={
        'build': build,
        'build_metadata': build_metadata,
        'build_ext': build_ext,
        'build_rust': build_rust,
        'build_postgres': build_postgres,
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
            "edb.server.protocol.auth_helpers",
            ["edb/server/protocol/auth_helpers.pyx"],
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
            extra_compile_args=EXT_CFLAGS + _protobuf_c_cflags,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
            library_dirs=EXT_LIB_DIRS,
            libraries=['pg_query']
        ),

        setuptools_extension.Extension(
            "edb.server.compiler.rpc",
            ["edb/server/compiler/rpc.pyx"],
            extra_compile_args=EXT_CFLAGS,
            extra_link_args=EXT_LDFLAGS,
            include_dirs=EXT_INC_DIRS,
        ),
    ],
    rust_extensions=[
        setuptools_rust.RustExtension(
            "edb._edgeql_parser",
            path="edb/edgeql-parser/edgeql-parser-python/Cargo.toml",
            features=["python_extension"],
            binding=setuptools_rust.Binding.PyO3,
        ),
        setuptools_rust.RustExtension(
            "edb._graphql_rewrite",
            path="edb/graphql-rewrite/Cargo.toml",
            features=["python_extension"],
            binding=setuptools_rust.Binding.PyO3,
        ),
        setuptools_rust.RustExtension(
            "edb.server._rust_native",
            path="edb/server/_rust_native/Cargo.toml",
            features=["python_extension"],
            binding=setuptools_rust.Binding.PyO3,
        ),
    ],
)
