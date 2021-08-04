#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


from __future__ import annotations
from typing import *

import datetime
import functools
import hashlib
import json
import logging
import os
import pathlib
import pickle
import re
import subprocess
import tempfile

import immutables as immu
from importlib.util import find_spec

import edb
from edb.common import devmode
from edb.common import verutils
from edb.common import debug

from . import defines

try:
    import setuptools_scm
except ImportError:
    setuptools_scm = None  # type: ignore


class MetadataError(Exception):
    pass


def get_build_metadata_value(prop: str) -> str:
    env_val = os.environ.get(f'_EDGEDB_BUILDMETA_{prop}')
    if env_val:
        return env_val

    try:
        from . import _buildmeta  # type: ignore
        return getattr(_buildmeta, prop)
    except (ImportError, AttributeError):
        raise MetadataError(
            f'could not find {prop} in build metadata') from None


def get_pg_config_path() -> pathlib.Path:
    if devmode.is_in_dev_mode():
        edb_path: os.PathLike = edb.server.__path__[0]  # type: ignore
        root = pathlib.Path(edb_path).parent.parent
        pg_config = (root / 'build' / 'postgres' /
                     'install' / 'bin' / 'pg_config').resolve()
        if not pg_config.is_file():
            try:
                pg_config = pathlib.Path(
                    get_build_metadata_value('PG_CONFIG_PATH'))
            except MetadataError:
                pass

        if not pg_config.is_file():
            raise MetadataError('DEV mode: Could not find PostgreSQL build, '
                                'run `pip install -e .`')

    else:
        pg_config = pathlib.Path(
            get_build_metadata_value('PG_CONFIG_PATH'))

        if not pg_config.is_file():
            raise MetadataError(
                f'invalid pg_config path: {pg_config!r}: file does not exist '
                f'or is not a regular file')

    return pg_config


def get_runstate_path(data_dir: pathlib.Path) -> pathlib.Path:
    if devmode.is_in_dev_mode():
        return data_dir
    else:
        return pathlib.Path(get_build_metadata_value('RUNSTATE_DIR'))


def get_shared_data_dir_path() -> pathlib.Path:
    if devmode.is_in_dev_mode():
        return devmode.get_dev_mode_cache_dir()  # type: ignore[return-value]
    else:
        return pathlib.Path(get_build_metadata_value('SHARED_DATA_DIR'))


def hash_dirs(
    dirs: Sequence[Tuple[str, str]],
    *,
    extra_files: Optional[Sequence[Union[str, pathlib.Path]]]=None
) -> bytes:
    def hash_dir(dirname, ext, paths):
        with os.scandir(dirname) as it:
            for entry in it:
                if entry.is_file() and entry.name.endswith(ext):
                    paths.append(entry.path)
                elif entry.is_dir():
                    hash_dir(entry.path, ext, paths)

    paths: List[str] = []
    for dirname, ext in dirs:
        hash_dir(dirname, ext, paths)

    if extra_files:
        for extra_file in extra_files:
            if isinstance(extra_file, pathlib.Path):
                extra_file = str(extra_file.resolve())
            paths.append(extra_file)

    h = hashlib.sha1()  # sha1 is the fastest one.
    for path in sorted(paths):
        with open(path, 'rb') as f:
            h.update(f.read())

    return h.digest()


def read_data_cache(
    cache_key: bytes,
    path: str,
    *,
    pickled: bool=True,
    source_dir: Optional[pathlib.Path] = None,
) -> Any:
    if source_dir is None:
        source_dir = get_shared_data_dir_path()
    full_path = source_dir / path

    if full_path.exists():
        with open(full_path, 'rb') as f:
            src_hash = f.read(len(cache_key))
            if src_hash == cache_key or debug.flags.bootstrap_cache_yolo:
                if pickled:
                    data = f.read()
                    try:
                        return pickle.loads(data)
                    except Exception:
                        logging.exception(f'could not unpickle {path}')
                else:
                    return f.read()


def write_data_cache(
    obj: Any,
    cache_key: bytes,
    path: str,
    *,
    pickled: bool = True,
    target_dir: Optional[pathlib.Path] = None,
):
    if target_dir is None:
        target_dir = get_shared_data_dir_path()
    full_path = target_dir / path

    try:
        with tempfile.NamedTemporaryFile(
                mode='wb', dir=full_path.parent, delete=False) as f:
            f.write(cache_key)
            if pickled:
                pickle.dump(obj, file=f, protocol=pickle.HIGHEST_PROTOCOL)
            else:
                f.write(obj)
    except Exception:
        try:
            os.unlink(f.name)
        except OSError:
            pass
        finally:
            raise
    else:
        os.rename(f.name, full_path)


def get_version() -> verutils.Version:
    if devmode.is_in_dev_mode():
        root = pathlib.Path(__file__).parent.parent.parent.resolve()
        if setuptools_scm is None:
            raise MetadataError(
                'cannot determine build version: no setuptools_scm module')
        version = setuptools_scm.get_version(
            root=str(root),
            version_scheme=functools.partial(scm_version_scheme, root),
            local_scheme=functools.partial(scm_local_scheme, root),
        )
        version = verutils.parse_version(version)
    else:
        vertuple: List[Any] = list(get_build_metadata_value('VERSION'))
        vertuple[2] = verutils.VersionStage(vertuple[2])
        version = verutils.Version(*vertuple)

    return version


_version_dict: Optional[immu.Map[str, Any]] = None


def get_version_dict() -> immu.Map[str, Any]:
    global _version_dict

    if _version_dict is None:
        ver = get_version()
        _version_dict = immu.Map({
            'major': ver.major,
            'minor': ver.minor,
            'stage': ver.stage.name.lower(),
            'stage_no': ver.stage_no,
            'local': tuple(ver.local) if ver.local else (),
        })

    return _version_dict


_version_json: Optional[str] = None


def get_version_json() -> str:
    global _version_json
    if _version_json is None:
        _version_json = json.dumps(dict(get_version_dict()))
    return _version_json


def scm_version_scheme(root, version):
    pretend = os.environ.get('SETUPTOOLS_SCM_PRETEND_VERSION')
    if pretend:
        return pretend

    posint = r'(0|[1-9]\d*)'
    pep440_version_re = re.compile(
        rf"""
        ^
        (?P<major>{posint})
        \.
        (?P<minor>{posint})
        (
            \.
            (?P<micro>{posint})
        )?
        (
            (?P<prekind>a|b|rc)
            (?P<preval>{posint})
        )?
        $
        """,
        re.X,
    )

    proc = subprocess.run(
        ['git', 'tag', '--list', 'v*'],
        stdout=subprocess.PIPE,
        universal_newlines=True,
        check=True,
        cwd=root,
    )
    versions = proc.stdout.split('\n')
    latest_version = max(versions)

    proc = subprocess.run(
        ['git', 'tag', '--points-at', 'HEAD'],
        stdout=subprocess.PIPE,
        universal_newlines=True,
        check=True,
        cwd=root,
    )
    tag_list = proc.stdout.strip()
    if tag_list:
        tags = tag_list.split('\n')
    else:
        tags = []

    exact_tags = set(versions) & set(tags)
    if exact_tags:
        return exact_tags.pop()[1:]

    proc = subprocess.run(
        ['git', 'rev-list', '--count', 'HEAD'],
        stdout=subprocess.PIPE,
        universal_newlines=True,
        check=True,
        cwd=root,
    )
    commits_on_branch = proc.stdout.strip()
    m = pep440_version_re.match(latest_version[1:])
    if not m:
        return f'{latest_version[1:]}.dev{commits_on_branch}'

    major = m.group('major')
    minor = m.group('minor')
    micro = m.group('micro') or ''
    microkind = '.' if micro else ''
    prekind = m.group('prekind') or ''
    preval = m.group('preval') or ''

    if prekind and preval:
        preval = str(int(preval) + 1)
    elif micro:
        micro = str(int(micro) + 1)
    else:
        minor = str(int(minor) + 1)

    incremented_ver = f'{major}.{minor}{microkind}{micro}{prekind}{preval}'
    return f'{incremented_ver}.dev{commits_on_branch}'


def scm_local_scheme(root, version):
    pretend = os.environ.get('SETUPTOOLS_SCM_PRETEND_VERSION')
    if pretend:
        return ''

    proc = subprocess.run(
        ['git', 'rev-parse', '--verify', '--quiet', 'HEAD'],
        stdout=subprocess.PIPE,
        universal_newlines=True,
        check=True,
        cwd=root,
    )

    curdate = datetime.datetime.now(tz=datetime.timezone.utc)
    curdate_str = curdate.strftime(r'%Y%m%d%H')
    commitish = proc.stdout.strip()
    catver = defines.EDGEDB_CATALOG_VERSION
    return f'+d{curdate_str}.g{commitish[:9]}.cv{catver}'


def get_cache_src_dirs():
    edgeql = pathlib.Path(find_spec('edb.edgeql').origin).parent
    return (
        (pathlib.Path(find_spec('edb.schema').origin).parent, '.py'),
        (edgeql / 'compiler', '.py'),
        (edgeql / 'parser', '.py'),
        (pathlib.Path(find_spec('edb.lib').origin).parent, '.edgeql'),
        (pathlib.Path(find_spec('edb.pgsql.metaschema').origin).parent, '.py'),
    )


def get_default_tenant_id() -> str:
    catver = defines.EDGEDB_CATALOG_VERSION
    return f'V{catver:x}'
