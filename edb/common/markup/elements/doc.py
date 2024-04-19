#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2011-present MagicStack Inc. and the EdgeDB authors.
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

import difflib

from edb.common import checked
from edb.common.struct import Field
from . import base


class DocMarkup(base.Markup, ns='doc'):
    pass


class Marker(DocMarkup):
    text = Field(str)


class Section(DocMarkup):
    title = Field(str, coerce=True, default=None)
    body = Field(base.MarkupList, coerce=True)
    collapsed = Field(bool, coerce=True, default=False)


class SubNode(DocMarkup):
    body = Field(base.Markup)


class Text(DocMarkup):
    text = Field(str)


class SourceCode(DocMarkup):
    text = Field(str)


class Diff(DocMarkup):
    lines = Field(checked.CheckedList[str], coerce=True)

    @classmethod
    def get_diff(
        cls, a, b, fromfile='', tofile='', fromfiledate='', tofiledate='', n=10
    ):

        lines = difflib.unified_diff(
            a, b, fromfile, tofile, fromfiledate, tofiledate, n)
        lines = [line.rstrip() for line in lines]

        if lines:
            return cls(lines=lines)
        else:
            return Text(text='No differences')


class ValueDiff(DocMarkup):
    before = Field(str)
    after = Field(str)
    comment = Field(str, default=None)
