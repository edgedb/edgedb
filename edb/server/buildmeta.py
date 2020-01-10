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
from typing import *  # NoQA

import enum
import os
import pathlib
from typing import *  # NoQA

import edb
from edb.common import devmode

try:
    import pkg_resources
except ImportError:
    pkg_resources = None  # type: ignore

try:
    import setuptools_scm
except ImportError:
    setuptools_scm = None  # type: ignore


class MetadataError(Exception):
    pass


def get_build_metadata_value(prop: str) -> str:
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


class VersionStage(enum.IntEnum):

    DEV = 0
    ALPHA = 10
    BETA = 20
    RC = 30
    FINAL = 40


class Version(NamedTuple):

    major: int
    minor: int
    stage: VersionStage
    stage_no: int
    local: Tuple[str, ...]

    def __str__(self):
        ver = f'{self.major}.{self.minor}'
        if self.stage is not VersionStage.FINAL:
            ver += (
                f'-{self.stage.name.lower()}.{self.stage_no}'
                f'{("+" + ".".join(self.local)) if self.local else ""}'
            )

        return ver


def parse_version(ver: Any) -> Version:
    v = ver._version
    local = []
    if v.pre:
        if v.pre[0] == 'a':
            stage = VersionStage.ALPHA
        elif v.pre[0] == 'b':
            stage = VersionStage.BETA
        elif v.pre[0] == 'c':
            stage = VersionStage.RC
        else:
            raise MetadataError(
                f'cannot determine release stage from {ver}')

        stage_no = v.pre[1]

        if v.dev:
            local.extend(['dev', str(v.dev[1])])
    elif v.dev:
        stage = VersionStage.DEV
        stage_no = v.dev[1]
    else:
        stage = VersionStage.FINAL
        stage_no = 0

    if v.local:
        local.extend(v.local)

    return Version(
        major=v.release[0],
        minor=v.release[1],
        stage=stage,
        stage_no=stage_no,
        local=tuple(local),
    )


def get_version() -> Version:
    if devmode.is_in_dev_mode():
        if pkg_resources is None:
            raise MetadataError(
                'cannot determine build version: no pkg_resources module')
        if setuptools_scm is None:
            raise MetadataError(
                'cannot determine build version: no setuptools_scm module')
        version = setuptools_scm.get_version(
            root='../..', relative_to=__file__)
        pv = pkg_resources.parse_version(version)
        version = parse_version(pv)
    else:
        vertuple: List[Any] = list(get_build_metadata_value('VERSION'))
        vertuple[2] = VersionStage(vertuple[2])
        version = Version(*vertuple)

    return version
