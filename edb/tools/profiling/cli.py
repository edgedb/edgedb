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

"""An entry point to the aggregation capabilitities of the profiler.

See README.md in this package for more details.
"""

from __future__ import annotations
from typing import Optional, Union, List

import pathlib

import click

from edb.tools.edb import edbcommands

from . import profiler


@edbcommands.command()
@click.option(
    "--prefix",
    default=profiler.PREFIX,
    show_default=True,
    help="Input file prefix to match",
)
@click.option(
    "--suffix",
    default=profiler.PROF_SUFFIX,
    show_default=True,
    help="Input file suffix to match",
)
@click.option(
    "--sort-by",
    default="cumulative",
    show_default=True,
    help="Default sort, same values as pstats",
)
@click.option(
    "--out",
    default=profiler.EDGEDB_DIR,
    show_default=True,
    help="Output file or directory for the aggregations",
)
@click.option(
    "--width",
    default=1920,
    show_default=True,
    help="Width of the SVG flame graph in pixels",
)
@click.option(
    "--threshold",
    default=0.0001,
    show_default=True,
    help=(
        "Percentage of time spent in a function in relation to the total"
        " time spent below which the function is not going to appear on the"
        " SVG flame graph. 1.0 is 100%."
    ),
)
@click.argument("dirs", nargs=-1)  # one or zero
def perfviz(
    dirs: List[str],
    prefix: str,
    suffix: str,
    sort_by: str,
    out: Union[pathlib.Path, str],
    width: int,
    threshold: float,
) -> None:
    """Aggregate raw profiling traces into textual and graphical formats.

    Generates aggregate .prof and .singledispatch files, an aggregate textual
    .pstats file, as well as two SVG flame graphs.

    For more comprehensive documentation read edb/tools/profiling/README.md.
    """
    if len(dirs) > 1:
        raise click.UsageError("Specify at most one directory")

    dir: Optional[str] = dirs[0] if dirs else None
    prof = profiler.profile(
        dir=dir, prefix=prefix, suffix=suffix, save_every_n_calls=1
    )
    prof.aggregate(
        pathlib.Path(out), sort_by=sort_by, width=width, threshold=threshold
    )
