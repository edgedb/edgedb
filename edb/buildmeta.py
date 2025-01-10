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
from typing import (
    Any,
    Optional,
    Tuple,
    Union,
    Mapping,
    Sequence,
    List,
    NamedTuple,
    TypedDict,
    cast,
)

# DO NOT put any imports here other than from stdlib
# or modules from edb.common that themselves have only stdlib imports.

import base64
import datetime
import hashlib
import importlib.util
import json
import logging
import os
import pathlib
import pickle
import platform
import re
import subprocess
import sys
import tempfile

from edb.common import debug
from edb.common import devmode
from edb.common import verutils


# Increment this whenever the database layout or stdlib changes.
#
# WARNING: DO NOT INCREMENT THIS WHEN BACKPORTING CHANGES TO A RELEASE BRANCH.
# The merge conflict there is a nice reminder that you probably need
# to write a patch in edb/pgsql/patches.py, and then you should preserve
# the old value.
EDGEDB_CATALOG_VERSION = 2024_01_10_00_00
EDGEDB_MAJOR_VERSION = 7


class MetadataError(Exception):
    pass


class BackendVersion(NamedTuple):
    major: int
    minor: int
    micro: int
    releaselevel: str
    serial: int
    string: str


class VersionMetadata(TypedDict):
    build_date: datetime.datetime | None
    build_hash: str | None
    scm_revision: str | None
    source_date: datetime.datetime | None
    target: str | None


def get_build_metadata_value(prop: str) -> str:
    env_val = os.environ.get(f'_GEL_BUILDMETA_{prop}')
    if env_val:
        return env_val
    env_val = os.environ.get(f'_EDGEDB_BUILDMETA_{prop}')
    if env_val:
        return env_val

    try:
        from . import _buildmeta  # type: ignore
        return getattr(_buildmeta, prop)
    except (ImportError, AttributeError):
        raise MetadataError(
            f'could not find {prop} in Gel distribution metadata') from None


def _get_devmode_pg_config_path() -> pathlib.Path:
    root = pathlib.Path(__file__).parent.parent.resolve()
    pg_config = root / 'build' / 'postgres' / 'install' / 'bin' / 'pg_config'
    if not pg_config.is_file():
        try:
            pg_config = pathlib.Path(
                get_build_metadata_value('PG_CONFIG_PATH'))
        except MetadataError:
            pass

    if not pg_config.is_file():
        raise MetadataError('DEV mode: Could not find PostgreSQL build, '
                            'run `pip install -e .`')

    return pg_config


def get_pg_config_path() -> pathlib.Path:
    if devmode.is_in_dev_mode():
        pg_config = _get_devmode_pg_config_path()
    else:
        try:
            pg_config = pathlib.Path(
                get_build_metadata_value('PG_CONFIG_PATH'))
        except MetadataError:
            pg_config = _get_devmode_pg_config_path()
        else:
            if not pg_config.is_file():
                raise MetadataError(
                    f'invalid pg_config path: {pg_config!r}: file does not '
                    f'exist or is not a regular file')

    return pg_config


_pg_version_regex = re.compile(
    r"(Postgre[^\s]*)?\s*"
    r"(?P<major>[0-9]+)\.?"
    r"((?P<minor>[0-9]+)\.?)?"
    r"(?P<micro>[0-9]+)?"
    r"(?P<releaselevel>[a-z]+)?"
    r"(?P<serial>[0-9]+)?"
)


def parse_pg_version(version_string: str) -> BackendVersion:
    version_match = _pg_version_regex.search(version_string)
    if version_match is None:
        raise ValueError(
            f"malformed Postgres version string: {version_string!r}")
    version = version_match.groupdict()
    return BackendVersion(
        major=int(version["major"]),
        minor=0,
        micro=int(version.get("minor") or 0),
        releaselevel=version.get("releaselevel") or "final",
        serial=int(version.get("serial") or 0),
        string=version_string,
    )


_bundled_pg_version: Optional[BackendVersion] = None


def get_pg_version() -> BackendVersion:
    global _bundled_pg_version
    if _bundled_pg_version is not None:
        return _bundled_pg_version

    pg_config = subprocess.run(
        [get_pg_config_path()],
        capture_output=True,
        text=True,
        check=True,
    )

    for line in pg_config.stdout.splitlines():
        k, eq, v = line.partition('=')
        if eq and k.strip().lower() == 'version':
            v = v.strip()
            parsed_ver = parse_pg_version(v)
            _bundled_pg_version = BackendVersion(
                major=parsed_ver.major,
                minor=parsed_ver.minor,
                micro=parsed_ver.micro,
                releaselevel=parsed_ver.releaselevel,
                serial=parsed_ver.serial,
                string=v,
            )
            return _bundled_pg_version
    else:
        raise MetadataError(
            "could not find version information in pg_config output")


def get_runstate_path(data_dir: pathlib.Path) -> pathlib.Path:
    if devmode.is_in_dev_mode():
        return data_dir
    else:
        runstate_dir = get_build_metadata_value('RUNSTATE_DIR')
        if runstate_dir is not None:
            return pathlib.Path(runstate_dir)
        else:
            return data_dir


def get_shared_data_dir_path() -> pathlib.Path:
    if devmode.is_in_dev_mode():
        return devmode.get_dev_mode_cache_dir()  # type: ignore[return-value]
    else:
        return pathlib.Path(get_build_metadata_value('SHARED_DATA_DIR'))


def get_extension_dir_path() -> pathlib.Path:
    # TODO: Do we want a special metadata value??
    return get_shared_data_dir_path() / "extensions"


def hash_dirs(
    dirs: Sequence[Tuple[str, str]],
    *,
    extra_files: Optional[Sequence[Union[str, pathlib.Path]]]=None,
    extra_data: Optional[bytes] = None,
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
    h.update(str(sys.version_info[:2]).encode())
    if extra_data is not None:
        h.update(extra_data)
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
        root = pathlib.Path(__file__).parent.parent.resolve()
        version = verutils.parse_version(get_version_from_scm(root))
    else:
        vertuple: List[Any] = list(get_build_metadata_value('VERSION'))
        vertuple[2] = verutils.VersionStage(vertuple[2])
        version = verutils.Version(*vertuple)

    return version


_version_dict: Optional[Mapping[str, Any]] = None


def get_version_build_id(
    v: verutils.Version,
    short: bool = True,
) -> tuple[str, ...]:
    parts = []
    if v.local:
        if short:
            build_hash = None
            build_kind = None
            for segment in v.local:
                if segment[0] == "s":
                    build_hash = segment[1:]
                elif segment[0] == "b":
                    build_kind = segment[1:]

            if build_kind == "official":
                if build_hash:
                    parts.append(build_hash)
            elif build_kind:
                parts.append(build_kind)
        else:
            parts.extend(v.local)

    return tuple(parts)


def get_version_dict() -> Mapping[str, Any]:
    global _version_dict

    if _version_dict is None:
        ver = get_version()
        _version_dict = {
            'major': ver.major,
            'minor': ver.minor,
            'stage': ver.stage.name.lower(),
            'stage_no': ver.stage_no,
            'local': get_version_build_id(ver),
        }

    return _version_dict


_version_json: Optional[str] = None


def get_version_json() -> str:
    global _version_json
    if _version_json is None:
        _version_json = json.dumps(get_version_dict())
    return _version_json


def get_version_string(short: bool = True) -> str:
    v = get_version()
    string = f'{v.major}.{v.minor}'
    if v.stage is not verutils.VersionStage.FINAL:
        string += f'-{v.stage.name.lower()}.{v.stage_no}'
    build_id = get_version_build_id(v, short=short)
    if build_id:
        string += "+" + ".".join(build_id)
    return string


def get_version_metadata() -> VersionMetadata:
    v = get_version()
    pfx_map = {
        "b": "build_type",
        "r": "build_date",
        "s": "build_hash",
        "g": "scm_revision",
        "d": "source_date",
        "t": "target",
    }

    result = {}

    for segment in v.local:
        key = pfx_map.get(segment[0])
        if key:
            raw_val = segment[1:]
            val: str | datetime.datetime
            if key == "target":
                val = _decode_build_target(raw_val)
            elif key in {"build_date", "source_date"}:
                val = _decode_build_date(raw_val)
            else:
                val = raw_val

            result[key] = val

    return cast(VersionMetadata, result)


def _decode_build_target(val: str) -> str:
    return (
        base64.b32decode(val + "=" * (-len(val) % 8), casefold=True).decode()
    )


def _decode_build_date(val: str) -> datetime.datetime:
    return datetime.datetime.strptime(val, r"%Y%m%d%H%M").replace(
        tzinfo=datetime.timezone.utc)


def get_version_from_scm(root: pathlib.Path) -> str:
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
    all_tags = {
        v[1:]
        for v in proc.stdout.strip().split('\n')
        if pep440_version_re.match(v[1:])
    }

    proc = subprocess.run(
        ['git', 'tag', '--points-at', 'HEAD'],
        stdout=subprocess.PIPE,
        universal_newlines=True,
        check=True,
        cwd=root,
    )
    head_tags = {
        v[1:]
        for v in proc.stdout.strip().split('\n')
        if pep440_version_re.match(v[1:])
    }

    if all_tags & head_tags:
        tag = max(head_tags)
    else:
        tag = max(all_tags)

    m = pep440_version_re.match(tag)
    assert m is not None
    major = EDGEDB_MAJOR_VERSION
    minor = m.group('minor')
    micro = m.group('micro') or ''
    microkind = '.' if micro else ''
    prekind = m.group('prekind') or ''
    preval = m.group('preval') or ''

    if os.environ.get("EDGEDB_BUILD_IS_RELEASE"):
        # Release build.
        ver = f'{major}.{minor}{microkind}{micro}{prekind}{preval}'
    else:
        # Dev/nightly build.
        microkind = ''
        micro = ''
        minor = '0'

        incremented_ver = f'{major}.{minor}{microkind}{micro}'

        proc = subprocess.run(
            ['git', 'rev-list', '--count', 'HEAD'],
            stdout=subprocess.PIPE,
            universal_newlines=True,
            check=True,
            cwd=root,
        )
        commits_on_branch = proc.stdout.strip()
        ver = f'{incremented_ver}.dev{commits_on_branch}'

    proc = subprocess.run(
        ['git', 'rev-parse', '--verify', '--quiet', 'HEAD^{commit}'],
        stdout=subprocess.PIPE,
        universal_newlines=True,
        check=True,
        cwd=root,
    )
    commitish = proc.stdout.strip()

    env = dict(os.environ)
    env['TZ'] = 'UTC'
    proc = subprocess.run(
        ['git', 'show', '-s', '--format=%cd',
         '--date=format-local:%Y%m%d%H', commitish],
        stdout=subprocess.PIPE,
        universal_newlines=True,
        check=True,
        cwd=root,
        env=env,
    )
    rev_date = proc.stdout.strip()

    catver = EDGEDB_CATALOG_VERSION

    full_version = f'{ver}+d{rev_date}.g{commitish[:9]}.cv{catver}'

    build_target = os.environ.get("EDGEDB_BUILD_TARGET")
    if build_target:
        # Check that build target is encoded correctly
        _decode_build_target(build_target)
    else:
        plat = sys.platform
        if plat == "win32":
            plat = "windows"
        ident = [
            platform.machine(),
            "pc" if plat == "windows" else
            "apple" if plat == "darwin" else
            "unknown",
            plat,
        ]
        if hasattr(platform, "libc_ver"):
            libc, _ = platform.libc_ver()
            if libc == "glibc":
                ident.append("gnu")
            elif libc == "musl":
                ident.append("musl")
        build_target = base64.b32encode(
            "-".join(ident).encode()).decode().rstrip("=").lower()
    build_date = os.environ.get("EDGEDB_BUILD_DATE")
    if build_date:
        # Validate
        _decode_build_date(build_date)
    else:
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        build_date = now.strftime(r"%Y%m%d%H%M")
    version_line = f'{full_version}.r{build_date}.t{build_target}'
    if not os.environ.get("EDGEDB_BUILD_OFFICIAL"):
        build_type = "local"
    else:
        build_type = "official"
    version_line += f'.b{build_type}'
    version_hash = hashlib.sha256(version_line.encode("utf-8")).hexdigest()
    full_version = f"{version_line}.s{version_hash[:7]}"

    return full_version


def get_cache_src_dirs():
    find_spec = importlib.util.find_spec

    edgeql = pathlib.Path(find_spec('edb.edgeql').origin).parent
    return (
        (pathlib.Path(find_spec('edb.schema').origin).parent, '.py'),
        (edgeql / 'compiler', '.py'),
        (edgeql / 'parser', '.py'),
        (pathlib.Path(find_spec('edb.lib').origin).parent, '.edgeql'),
        (pathlib.Path(find_spec('edb.pgsql.metaschema').origin).parent, '.py'),
    )


def get_default_tenant_id() -> str:
    return 'E'


def get_version_line() -> str:
    ver_meta = get_version_metadata()

    extras = []
    source = ""
    if build_date := ver_meta["build_date"]:
        nice_date = build_date.strftime("%Y-%m-%dT%H:%MZ")
        source += f" on {nice_date}"
    if ver_meta["scm_revision"]:
        source += f" from revision {ver_meta['scm_revision']}"
        if source_date := ver_meta["source_date"]:
            nice_date = source_date.strftime("%Y-%m-%dT%H:%MZ")
            source += f" ({nice_date})"
    if source:
        extras.append(f", built{source}")
    if ver_meta["target"]:
        extras.append(f"for {ver_meta['target']}")

    return get_version_string() + " ".join(extras)
