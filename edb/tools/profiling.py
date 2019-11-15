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

import atexit
import cProfile
import functools
import pathlib
import pstats
import sys
import tempfile

import click


# .parent.parent.parent removes the last three from
# edgedb/edb/tools/profiling.py, leaving just edgedb/
EDGEDB_DIR = pathlib.Path(__file__).resolve().parent.parent.parent
PREFIX = "edgedb_"
SUFFIX = ".pstats"


T = TypeVar("T", bound=Callable[..., Any])


class profile:
    """A decorator for profiling."""

    def __init__(
        self,
        *,
        prefix: str = PREFIX,
        suffix: str = SUFFIX,
        dir: Optional[str] = None,
        reuse: bool = True,
    ):
        """Create the decorator.

        If `reuse` is True, a single profiler is reused for the lifetime
        of the process and results are only dumped at exit.  Otherwise a new
        profile file is created on every function call.

        `dir`, `prefix`, and `suffix` after `tempfile.mkstemp`.
        """
        self.prefix = prefix
        self.suffix = suffix
        self.dir = dir
        self.reuse = reuse
        self.profiler = None
        if reuse:
            self.profiler = cProfile.Profile()
            atexit.register(self.dump_at_exit)

    def __call__(self, func: T) -> T:
        """Apply decorator to a function."""
        if self.reuse:

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                self.profiler.enable()
                try:
                    return func(*args, **kwargs)
                finally:
                    self.profiler.disable()

        else:

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                pr = cProfile.Profile()
                pr.enable()
                try:
                    return func(*args, **kwargs)
                finally:
                    pr.disable()
                    pr.dump_stats(self.make_dump_file())

        return cast(T, wrapper)

    def make_dump_file(self) -> str:
        """Return a path to a new, empty, existing named temporary file."""
        file = tempfile.NamedTemporaryFile(
            dir=self.dir, prefix=self.prefix, suffix=self.suffix, delete=False,
        )
        file.close()
        return file.name

    def dump_at_exit(self) -> None:
        # Note: this will also dump in case the profiler was never enabled
        # (the function was not called).  That's by design.  The presence of
        # the file lets us know the profiling scaffolding worked.
        assert self.profiler is not None
        self.profiler.dump_stats(self.make_dump_file())

    def aggregate(
        self, out_path: pathlib.Path, *, sort_by: str = ""
    ) -> Tuple[int, int]:
        """Read all pstats in `self.dir` and write a summary to `out_path`.

        `sort_by` after `pstats.sort_stats()`.  Files identified by `self.dir`,
        `self.prefix`, and `self.suffix`.

        Returns a tuple with number of successfully and unsucessfully
        aggregated files.
        """
        if not self.dir:
            with tempfile.NamedTemporaryFile() as tmp:
                directory = pathlib.Path(tmp.name).parent
        else:
            directory = pathlib.Path(self.dir)
        files = list(
            str(f) for f in directory.glob(self.prefix + "*" + self.suffix)
        )
        success = 0
        failure = 0
        with open(out_path, "w") as out:
            ps = pstats.Stats(stream=out)
            for file in files:
                try:
                    ps.add(file)
                except TypeError as te:
                    # Probably the profile file is empty.
                    print(te, file=sys.stderr)
                    failure += 1
                else:
                    success += 1
            if sort_by:
                ps.sort_stats(sort_by)
            ps.print_stats()
        print(
            f"Processed {success + failure} files, {failure} failed.",
            file=sys.stderr,
        )
        return success, failure


@click.command()
@click.option("--prefix", default=PREFIX)
@click.option("--suffix", default=SUFFIX)
@click.option("--sort-by", default="cumulative")
@click.option("--out", default=EDGEDB_DIR / "profile_analysis.out")
@click.argument("dirs", nargs=-1)
def cli(
    dirs: List[str],
    prefix: str,
    suffix: str,
    sort_by: str,
    out: Union[pathlib.Path, str],
) -> None:
    if len(dirs) > 1:
        raise click.UsageError("Specify at most one directory")

    dir: Optional[str] = dirs[0] if dirs else None
    prof = profile(dir=dir, prefix=prefix, suffix=suffix)
    prof.aggregate(pathlib.Path(out), sort_by=sort_by)


if __name__ == "__main__":
    cli()
