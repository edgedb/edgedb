#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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
from typing import Any, Tuple, NamedTuple

import enum
import re


VERSION_PATTERN = re.compile(r"""
    ^
    (?P<release>[0-9]+(?:\.[0-9]+)*)
    (?P<pre>
        [-\.]?
        (?P<pre_l>(a|b|c|rc|alpha|beta|dev))
        [\.]?
        (?P<pre_n>[0-9]+)?
    )?
    (?:\+(?P<local>[a-z0-9]+(?:[\.][a-z0-9]+)*))?
    $
""", re.X)


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
            ver += f'-{self.stage.name.lower()}.{self.stage_no}'
        if self.local:
            ver += f'{("+" + ".".join(self.local)) if self.local else ""}'

        return ver


def parse_version(ver: str) -> Version:
    v = VERSION_PATTERN.match(ver)
    if v is None:
        raise ValueError(f'cannot parse version: {ver}')
    local: list[str] = []
    if v.group('pre'):
        pre_l = v.group('pre_l')
        if pre_l in {'a', 'alpha'}:
            stage = VersionStage.ALPHA
        elif pre_l in {'b', 'beta'}:
            stage = VersionStage.BETA
        elif pre_l in {'c', 'rc'}:
            stage = VersionStage.RC
        elif pre_l in {'dev'}:
            stage = VersionStage.DEV
        else:
            raise ValueError(f'cannot determine release stage from {ver}')

        stage_no = int(v.group('pre_n'))
    else:
        stage = VersionStage.FINAL
        stage_no = 0
    if v.group('local'):
        local.extend(v.group('local').split('.'))

    release = [int(r) for r in v.group('release').split('.')]

    return Version(
        major=release[0],
        minor=release[1],
        stage=stage,
        stage_no=stage_no,
        local=tuple(local),
    )


def from_json(data: dict[str, Any]) -> Version:
    return Version(
        data['major'],
        data['minor'],
        VersionStage[data['stage'].upper()],
        data['stage_no'],
        tuple(data['local']),
    )
