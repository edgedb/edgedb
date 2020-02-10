#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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
from typing_extensions import Literal

import math
import sys
import textwrap
import typing  # needed for linting TextIO

import edgedb


class ColumnSpec(NamedTuple):
    field: str
    title: str
    width: int
    align: Union[Literal['left'], Literal['center'], Literal['right']]


def render_table(
    *,
    title: str,
    columns: Sequence[ColumnSpec],
    data: Iterable[edgedb.Object],
    max_width: int = 0,
    file: typing.TextIO = sys.stdout,
) -> None:
    if max_width == 0:
        return

    # Find the amount of space available for text. Every column needs
    # whitespace padding of 2 (1 on each side) and 1 "|" column
    # separator (except for last column).
    avail_width = max(0, max_width - (3 * len(columns) - 1))
    total_flex = sum(col[2] for col in columns)

    # setup header row and separator
    hline_list: List[str] = []
    widths: List[int] = []
    # Look at columns backwards to calculate the column widths and to
    # pad the first column with all the extra width left over. The
    # assumption is that the first column probably contains highly
    # relevant information so it deserves a few extra characters if
    # they are available.
    for col in reversed(columns):
        width = math.floor(avail_width * (col.width / total_flex)) + 2
        # actual text needs to accommodate a space on each side and be
        # a minimum of 1
        width = max(3, width)
        widths.append(width - 2)
        hline_list.append('-' * width)

    # reverse things back
    widths.reverse()
    hline_list.reverse()

    # pad the first column if possible
    pad = avail_width - sum(widths)
    if pad > 0:
        widths[0] += pad
        hline_list[0] += '-' * pad

    # generate the horizontal separator line
    hline = '+'.join(hline_list)
    # merge the title and hline
    print(merge_centered_title(title, hline), file=file)

    # create the header row(s)
    headers = {col.field: col.title for col in columns}
    rows = get_table_row(headers, columns, widths)
    for row in rows:
        print(row, file=file)

    print(hline, file=file)

    # process data
    for item in data:
        rows = get_table_row(item, columns, widths)
        for row in rows:
            print(row, file=file)


def merge_centered_title(title: str, background: str) -> str:
    '''Embed the title in the center of the background string.'''
    bg_len = len(background)
    pos = max(0, (bg_len - len(title)) // 2)
    if pos == 0:
        # the title is expected to completely obscure the background
        result = title.ljust(bg_len)

    else:
        result = (background[:pos - 1] + f' {title} ' +
                  background[pos + 1 + len(title):])

    return result


def get_table_row(
    item: Any,
    columns: Sequence[ColumnSpec],
    widths: List[int]
) -> List[str]:
    # a single logical row may be split across multiple actual rows
    multirow: List[str] = []
    # first pass converts all column data into text that is
    # wrapped according to the column widths
    rdata: List[List[str]] = []
    numrows = 1
    for i, col in enumerate(columns):
        width = widths[i]
        # item could be either an object with attributes or a dict
        if isinstance(item, dict):
            text = str(item[col.field])
        else:
            text = str(getattr(item, col.field))

        rdata.append(textwrap.wrap(text, width))
        numrows = max(numrows, len(rdata[-1]))

    # iterate over the potentially multiple rows per logical row
    for r in range(numrows):
        row = []
        # build up each row from the respective pieces from each column
        for i, cdata in enumerate(rdata):
            width = widths[i]
            data = cdata[r:r + 1]
            if data:
                text = data[0]
            else:
                text = ''

            # determine if line continuation symbol is needed
            col_len = len(cdata)
            # if there are multiple rows in this column and the
            # current one is not the last
            if col_len > 1 and r < col_len - 1:
                cont = '+'
            else:
                cont = ' '

            # align the content
            if columns[i].align == 'right':
                text = text.rjust(width)
            elif columns[i].align == 'center':
                text = text.center(width)
            else:
                text = text.ljust(width)

            row.append(f' {text}{cont}')
        multirow.append('|'.join(row))

    return multirow
