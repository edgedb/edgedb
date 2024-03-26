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

"""Provides a `profile()` decorator with aggregation capabilities.

See README.md in this package for more details.
"""

from __future__ import annotations
from typing import (
    Any,
    Callable,
    Optional,
    Tuple,
    TypeVar,
    Union,
    AbstractSet,
    Iterator,
    Sequence,
    Counter,
    Dict,
    List,
    Set,
    NamedTuple,
    cast,
    TYPE_CHECKING,
)

import ast
import atexit
import builtins
import cProfile
import dataclasses
import functools
import hashlib
import linecache
import os
import pathlib
import pickle
import pstats
import re
import sys
import tempfile
from xml.sax import saxutils

from edb.tools.profiling import tracing_singledispatch


CURRENT_DIR = pathlib.Path(__file__).resolve().parent
EDGEDB_DIR = CURRENT_DIR.parent.parent.parent
PROFILING_JS = CURRENT_DIR / "svg_helpers.js"
PREFIX = "edgedb_"
STAT_SUFFIX = ".pstats"
PROF_SUFFIX = ".prof"
SVG_SUFFIX = ".svg"
SINGLEDISPATCH_SUFFIX = ".singledispatch"


T = TypeVar("T", bound=Callable[..., Any])


if TYPE_CHECKING:
    ModulePath = str
    LineNo = int
    FunctionName = str
    FunctionID = Tuple[ModulePath, LineNo, FunctionName]
    LineID = Tuple[ModulePath, LineNo]
    # cc, nc, tt, ct, callers
    PrimitiveCallCount = int  # without recursion
    CallCount = int  # with recursion
    TotalTime = float
    CumulativeTime = float
    Stat = Tuple[PrimitiveCallCount, CallCount, TotalTime, CumulativeTime]
    StatWithCallers = Tuple[
        PrimitiveCallCount,
        CallCount,
        TotalTime,
        CumulativeTime,
        Dict[FunctionID, Stat],  # callers
    ]
    Stats = Dict[FunctionID, StatWithCallers]
    Caller = FunctionID
    Callee = FunctionID
    Call = Tuple[Caller, Optional[Callee]]
    CallCounts = Dict[Caller, Dict[Callee, CallCount]]


class profile:
    """A decorator for CPU profiling."""

    def __init__(
        self,
        *,
        prefix: str = PREFIX,
        suffix: str = PROF_SUFFIX,
        dir: Optional[str] = None,
        save_every_n_calls: int = 1,
    ):
        """Create the decorator.

        If `save_every_n_calls` is greater than 1, the profiler will not
        dump data to files on every call to the profiled function.  This speeds
        up the running program but risks incomplete data if the process is
        terminated non-gracefully.

        `dir`, `prefix`, and `suffix` after `tempfile.mkstemp`.
        """
        self.prefix = prefix
        self.suffix = suffix
        self.save_every_n_calls = save_every_n_calls
        self.n_calls = 0
        self._dir: Union[str, pathlib.Path, None] = dir
        self._profiler: Optional[cProfile.Profile] = None
        self._dump_file_path: Optional[str] = None
        self._profiler_enabled = False

    def __call__(self, func: T) -> T:
        """Apply decorator to a function."""

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tracing_singledispatch.profiling_in_progress.set()
            self.n_calls += 1
            profiler_was_enabled_here = False
            if not self._profiler_enabled:
                self.profiler.enable()
                self._profiler_enabled = True
                profiler_was_enabled_here = True
            try:
                return func(*args, **kwargs)
            finally:
                if profiler_was_enabled_here:
                    self.profiler.disable()
                if self.n_calls % self.save_every_n_calls == 0:
                    self.dump_stats()
                tracing_singledispatch.profiling_in_progress.clear()

        return cast(T, wrapper)

    @property
    def dir(self) -> pathlib.Path:
        if self._dir is None:
            with tempfile.NamedTemporaryFile() as tmp:
                self._dir = pathlib.Path(tmp.name).parent
        return pathlib.Path(self._dir)

    @property
    def profiler(self) -> cProfile.Profile:
        if self._profiler is None:
            self._profiler = cProfile.Profile()
            if self.save_every_n_calls > 1:
                # This is attached here so the registration is in the right
                # process (relevant for multiprocessing workers).  This is
                # still sadly flimsy, hence the `save every n calls`.
                atexit.register(self.dump_stats)
        return self._profiler

    @property
    def dump_file(self) -> str:
        """Return a path to a new, empty, existing named temporary file."""
        if self._dump_file_path is None:
            file = tempfile.NamedTemporaryFile(
                dir=self.dir,
                prefix=self.prefix,
                suffix=self.suffix,
                delete=False,
            )
            file.close()
            self._dump_file_path = file.name

        return self._dump_file_path

    def dump_stats(self) -> None:
        self.profiler.dump_stats(self.dump_file)
        try:
            done_dispatches = tracing_singledispatch.done_dispatches
        except AttributeError:
            return  # we're at program exit; `tracing_singledispatch` went away
        if done_dispatches:
            with open(self.dump_file + ".singledispatch", "wb") as sd_file:
                pickle.dump(done_dispatches, sd_file, pickle.HIGHEST_PROTOCOL)

    def aggregate(
        self,
        out_path: pathlib.Path,
        *,
        sort_by: str = "",
        width: int = 1920,
        threshold: float = 0.0001,  # 1.0 is 100%
        quiet: bool = False,
    ) -> Tuple[int, int]:
        """Read all pstats in `self.dir` and write a summary to `out_path`.

        `sort_by` after `pstats.sort_stats()`.  Files identified by `self.dir`,
        `self.prefix`, and `self.suffix`.

        `width` selects the width of the generated SVG.

        Functions whose runtime is below `threshold` percentage are not
        included.

        Returns a tuple with number of successfully and unsucessfully
        aggregated files.
        """
        print = builtins.print
        if quiet:
            print = lambda *args, **kwargs: None

        if out_path.is_dir():
            out_path = out_path / "profile_analysis"
        prof_path = out_path.with_suffix(PROF_SUFFIX)
        pstats_path = out_path.with_suffix(STAT_SUFFIX)
        call_svg_path = out_path.with_suffix(".call_stack" + SVG_SUFFIX)
        usage_svg_path = out_path.with_suffix(".usage" + SVG_SUFFIX)
        files = list(
            str(f) for f in self.dir.glob(self.prefix + "*" + self.suffix)
        )
        if not files:
            print(f"warning: no files to process", file=sys.stderr)
            return 0, 0

        success = 0
        failure = 0
        with open(pstats_path, "w") as out:
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
            ps.dump_stats(str(prof_path))
            if sort_by:
                ps.sort_stats(sort_by)
            ps.print_stats()
        singledispatch_traces = self.accumulate_singledispatch_traces()
        if singledispatch_traces:
            singledispatch_path = out_path.with_suffix(SINGLEDISPATCH_SUFFIX)
            with singledispatch_path.open("wb") as sd_file:
                pickle.dump(
                    singledispatch_traces, sd_file, pickle.HIGHEST_PROTOCOL
                )

        # Mypy is wrong below, `stats` is there on all pstats.Stats objects
        stats = ps.stats  # type: ignore
        filter_singledispatch_in_place(stats, singledispatch_traces)
        try:
            render_svg(
                stats,
                call_svg_path,
                usage_svg_path,
                width=width,
                threshold=threshold,
            )
        except ValueError as ve:
            print(f"Cannot display flame graph: {ve}", file=sys.stderr)
        print(
            f"Processed {success + failure} files, {failure} failed.",
            file=sys.stderr,
        )
        return success, failure

    def accumulate_singledispatch_traces(self) -> Dict[FunctionID, CallCounts]:
        result: Dict[FunctionID, CallCounts] = {}
        d = self.dir.glob(
            self.prefix + "*" + self.suffix + SINGLEDISPATCH_SUFFIX
        )
        for f in d:
            with open(str(f), "rb") as file:
                dispatches = pickle.load(file)
            for singledispatch_funcid, call_counts in dispatches.items():
                for caller, calls in call_counts.items():
                    for impl, call_count in calls.items():
                        r_d = result.setdefault(singledispatch_funcid, {})
                        c_d = r_d.setdefault(caller, {})
                        c_d[impl] = c_d.get(impl, 0) + call_count
        return result


def profile_memory(func: Callable[[], Any]) -> MemoryFrame:
    """Profile memory and return a tree of statistics.

    Feed those to `render_memory_svg()` to write an SVG.
    """
    import tracemalloc

    tracemalloc.start(1024)
    try:
        func()
    finally:
        snap = tracemalloc.take_snapshot()
        tracemalloc.stop()
    stats = snap.statistics("traceback")
    root = MemoryFrame(blocks=0, size=0)
    for stat in stats:
        blocks = stat.count
        size = stat.size
        callee = root
        callee.blocks += blocks
        callee.size += size
        for frame in stat.traceback:
            lineid = (frame.filename, frame.lineno)
            callee = callee.callers.setdefault(
                lineid, MemoryFrame(blocks=0, size=0)
            )
            callee.blocks += blocks
            callee.size += size
    while len(root.callers) == 1:
        root = next(iter(root.callers.values()))
    return root


@dataclasses.dataclass
class Function:
    id: FunctionID
    calls: List[FunctionID]
    calledby: List[FunctionID]
    stat: Stat


ROOT_ID: FunctionID = ("<root>", 0, "<root>")


class RGB(NamedTuple):
    r: int
    g: int
    b: int


def gen_colors(s: RGB, e: RGB, size: int) -> Iterator[RGB]:
    """Generate a gradient of `size` colors between `s` and `e`."""
    for i in range(size):
        yield RGB(
            s.r + (e.r - s.r) * i // size,
            s.g + (e.g - s.g) * i // size,
            s.b + (e.b - s.b) * i // size,
        )


COLORS = list(gen_colors(RGB(255, 240, 141), RGB(255, 65, 34), 7))
CCOLORS = list(gen_colors(RGB(44, 255, 210), RGB(113, 194, 0), 5))
ECOLORS = list(gen_colors(RGB(230, 230, 255), RGB(150, 150, 255), 5))
DCOLORS = list(gen_colors(RGB(190, 190, 190), RGB(240, 240, 240), 7))


def gradient_from_name(name: str) -> float:
    v = int(hashlib.sha1(name.encode("utf8")).hexdigest()[:8], base=16)
    return v / (0xFFFFFFFF + 1.0)


def calc_callers(
    stats: Stats,
    threshold: float,
) -> Tuple[Dict[FunctionID, Function], Dict[Call, Stat]]:
    """Calculate flattened stats of calls between functions."""
    roots: List[FunctionID] = []
    funcs: Dict[FunctionID, Function] = {}
    calls: Dict[Call, Stat] = {}
    for func, (cc, nc, tt, ct, callers) in stats.items():
        funcs[func] = Function(
            id=func, calls=[], calledby=[], stat=(cc, nc, tt, ct)
        )
        if not callers:
            roots.append(func)
            calls[ROOT_ID, func] = funcs[func].stat

    for func, (_, _, _, _, callers) in stats.items():
        for caller, t in callers.items():
            assert (caller, func) not in calls
            funcs[caller].calls.append(func)
            funcs[func].calledby.append(caller)
            calls[caller, func] = t

    total = sum(funcs[r].stat[3] for r in roots)
    ttotal = sum(funcs[r].stat[2] for r in funcs)

    # Try to find suitable root
    newroot = max(
        (r for r in funcs if r not in roots), key=lambda r: funcs[r].stat[3]
    )
    nstat = funcs[newroot].stat
    ntotal = total + nstat[3]
    if 0.8 < ntotal / ttotal < 1.2:
        roots.append(newroot)
        calls[ROOT_ID, newroot] = nstat
        total = ntotal
    else:
        total = ttotal

    funcs[ROOT_ID] = Function(
        id=ROOT_ID, calls=roots, calledby=[], stat=(1, 1, 0, total),
    )
    return funcs, calls


@dataclasses.dataclass
class Block:
    func: FunctionID
    call_stack: Tuple[FunctionID, ...]
    color: int
    level: int
    tooltip: str
    w: float
    x: float

    @property
    def id(self) -> str:
        return repr(self.func)

    @property
    def name(self) -> FunctionName:
        result = self.func[2]
        if result.startswith("<built-in method builtins."):
            result = result[len("<built-in method ") : -1]
        return result

    @property
    def module(self) -> str:
        result = self.func[0]
        edgedb = str(EDGEDB_DIR) + os.sep
        if result.startswith(edgedb):
            return result[len(edgedb):]

        parts = []
        maybe_stdlib = False
        for part in pathlib.Path(result).parts[::-1]:
            parts.append(part)
            if part in {"python3.6", "python3.7", "python3.8", "python3.9"}:
                maybe_stdlib = True
            elif maybe_stdlib:
                if part == "lib":
                    parts.pop()
                    return os.sep.join(parts[::-1])

                break

        return result

    @property
    def full_name(self) -> str:
        result = ""
        if self.func[0] not in {"~", "", None}:
            result += self.module
            result += ":"
        if self.func[1] not in (0, None):
            result += str(self.func[1])
            result += ":"
        result += self.name
        return f"{result} {self.tooltip}"


@dataclasses.dataclass
class MemoryFrame:
    """A node of a tree of calls.

    Leaves are were memory allocations actually happened.
    """
    blocks: int
    size: int  # in bytes
    callers: Dict[LineID, MemoryFrame] = dataclasses.field(
        default_factory=dict
    )


class ScopeRecorder(ast.NodeVisitor):
    """A nifty AST visitor that records all scope changes in the file."""

    # the value is a qualified name (without the module), e.g. "Class.method"
    scopes: Dict[LineNo, str]

    # internal stack for correct naming in the scope
    stack: List[FunctionName]

    def __init__(self):
        self.reset()
        self.visit_Functiondef = self.handle_scopes
        self.visit_AsyncFunctiondef = self.handle_scopes
        self.visit_Classdef = self.handle_scopes
        super().__init__()

    def reset(self) -> None:
        self.stack = []
        self.scopes = {}

    def handle_scopes(
        self, node: Union[ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef]
    ) -> None:
        self.stack.append(node.name)
        self.scopes[node.lineno] = ".".join(self.stack)
        self.generic_visit(node)
        self.stack.pop()


class ScopeCache:
    """Returns qualified names of scopes for a given module path and lineno.

    Caches both scopes from ScopeRecorder and queries about a given line number
    (which is likely *inside* the function body).
    """

    def __init__(self) -> None:
        self.recorder = ScopeRecorder()
        self.scopes: Dict[ModulePath, Dict[LineNo, str]] = {}
        self.cache: Dict[Tuple[ModulePath, LineNo], str] = {}

    def __getitem__(self, key: Tuple[ModulePath, LineNo]) -> str:
        if key not in self.cache:
            try:
                self.cache[key] = self._get_scope(key[0], key[1])
            except FileNotFoundError:
                self.cache[key] = ""
        return self.cache[key]

    def _get_scope(self, mod_path: ModulePath, wanted_lineno: LineNo) -> str:
        if mod_path not in self.scopes:
            with open(mod_path) as mod:
                self.recorder.visit(ast.parse(mod.read(), mod_path))
                self.scopes[mod_path] = self.recorder.scopes
                self.recorder.reset()
        last_scope = ""
        for lineno, scope in sorted(self.scopes[mod_path].items()):
            if lineno > wanted_lineno:
                return last_scope

            last_scope = scope

        return last_scope


def count_calls(funcs: Dict[FunctionID, Function]) -> Counter[Call]:
    call_counter: Counter[Call] = Counter()

    def _counts(caller: FunctionID, visited: Set[Call], level: int = 0) -> None:
        for callee in funcs[caller].calls:
            call = caller, callee
            call_counter[call] += 1
            if call_counter[call] < 2 and call not in visited:
                _counts(callee, visited | {call}, level + 1)

    _counts(ROOT_ID, set())
    return call_counter


def find_singledispatch_wrapper(
    stats: Stats, *, regular_location: bool = False
) -> FunctionID:
    """Returns the singledispatch wrapper function ID tuple.

    Raises LookupError if not found.
    """
    if regular_location:
        functools_path = re.compile(r"python3.\d+/functools.py$")
        dispatch_name = "dispatch"
        wrapper_name = "wrapper"
    else:
        functools_path = re.compile(r"profiling/tracing_singledispatch.py$")
        dispatch_name = "dispatch"
        wrapper_name = "sd_wrapper"

    for (modpath, _lineno, funcname), (_, _, _, _, callers) in stats.items():
        if funcname != dispatch_name:
            continue

        m = functools_path.search(modpath)
        if not m:
            continue

        # Using this opportunity, we're figuring out which `wrapper` from
        # functools in the trace is the singledispatch `wrapper` (there
        # are three more others in functools.py).
        for caller_modpath, caller_lineno, caller_funcname in callers:
            if caller_funcname == wrapper_name:
                m = functools_path.search(modpath)
                if not m:
                    continue

                return (caller_modpath, caller_lineno, caller_funcname)

        raise LookupError("singledispatch.dispatch without wrapper?")

    raise LookupError("No singledispatch use in provided stats")


def filter_singledispatch_in_place(
    stats: Stats,
    dispatches: Dict[FunctionID, CallCounts],
    regular_location: bool = False,
) -> None:
    """Removes singledispatch `wrapper` from the `stats.`

    Given that:
    - W is a wrapper function hiding original function O;
    - D is the internal dispatching function of singledispatch;
    - W calls D first to select which function to call;
    - then, W calls the concrete registered implementations F1, F2, F3, and
      rather rarely, O.

    This filter changes this ( -> means "calls"):

    A -> W -> F1
    A -> W -> D

    into this:

    A -> F1
    A -> D
    """

    try:
        wrapper = find_singledispatch_wrapper(
            stats, regular_location=regular_location
        )
    except LookupError:
        return

    # Delete the function from stats
    del stats[wrapper]

    # Fix up all "callers" stats
    singledispatch_functions = {d: (0, 0, 0, 0) for d in dispatches}
    for funcid, (_, _, _, _, callers) in stats.items():
        if wrapper not in callers:
            continue

        new_direct_calls = {}
        for call_counts in dispatches.values():
            for caller, calls in call_counts.items():
                if funcid not in calls:
                    continue

                new_direct_calls[caller] = calls[funcid]

        pcc, cc, tottime, cumtime = callers.pop(wrapper)
        all_calls = sum(new_direct_calls.values())
        if all_calls == 0:
            count = len(singledispatch_functions)
            for sdfid, old_stats in singledispatch_functions.items():
                cur_stats = (
                    round(pcc / count),
                    round(cc / count),
                    tottime / count,
                    cumtime / count,
                )
                callers[sdfid] = cur_stats
                new_stats = tuple(
                    old_stats[i] + cur_stats[i] for i in range(4)
                )
                singledispatch_functions[sdfid] = new_stats  # type: ignore

            continue

        factor = all_calls / cc
        pcc_fl = pcc * factor
        cc_fl = cc * factor
        tottime *= factor
        cumtime *= factor

        for caller, count in new_direct_calls.items():
            factor = count / cc_fl
            callers[caller] = (
                round(pcc_fl * factor),
                count,
                tottime * factor,
                cumtime * factor,
            )

    # Insert original single dispatch generic functions back
    for sdfid, sd_stats in singledispatch_functions.items():
        o_pcc, o_cc, o_tottime, o_cumtime, callers = stats.get(
            sdfid, (0, 0, 0, 0, {})
        )
        stats[sdfid] = (
            sd_stats[0] + o_pcc,
            sd_stats[1] + o_cc,
            sd_stats[2] + o_tottime,
            sd_stats[3] + o_cumtime,
            callers,
        )


def build_svg_blocks(
    funcs: Dict[FunctionID, Function],
    calls: Dict[Call, Stat],
    threshold: float,
) -> Tuple[List[Block], List[Block], float]:
    call_stack_blocks: List[Block] = []
    usage_blocks: List[Block] = []
    counts: Counter[Call] = count_calls(funcs)
    maxw = float(funcs[ROOT_ID].stat[3])

    def _build_blocks_by_call_stack(
        func: FunctionID,
        scaled_timings: Stat,
        *,
        visited: AbstractSet[Call] = frozenset(),
        level: int = 0,
        origin: float = 0,
        call_stack: Tuple[FunctionID, ...] = (),
        parent_call_count: int = 1,
        parent_block: Optional[Block] = None,
    ) -> None:
        _, _, func_tt, func_tc = scaled_timings
        pcc = parent_call_count
        fchildren = [
            (f, funcs[f], calls[func, f], max(counts[func, f], pcc))
            for f in funcs[func].calls
        ]
        fchildren.sort(key=lambda elem: elem[0])
        gchildren = [elem for elem in fchildren if elem[3] == 1]
        bchildren = [elem for elem in fchildren if elem[3] > 1]
        if bchildren:
            gchildren_tc_sum = sum(r[2][3] for r in gchildren)
            bchildren_tc_sum = sum(r[2][3] for r in bchildren)
            rest = func_tc - func_tt - gchildren_tc_sum
            if bchildren_tc_sum > 0:
                factor = rest / bchildren_tc_sum
            else:
                factor = 1
            # Round up and scale times and call counts.
            bchildren = [
                (
                    f,
                    ff,
                    (
                        round(cc * factor),
                        round(nc * factor),
                        tt * factor,
                        tc * factor,
                    ),
                    ccnt,
                )
                for f, ff, (cc, nc, tt, tc), ccnt in bchildren
            ]

        for child, _, (cc, nc, tt, tc), call_count in gchildren + bchildren:
            if tc / maxw < threshold:
                origin += tc
                continue

            child_call_stack = call_stack + (child,)
            tooltip = TOOLTIP.format(tc / maxw, cc, nc, tt, tc)
            block = Block(
                func=child,
                call_stack=child_call_stack,
                color=(parent_call_count == 1 and call_count > 1),
                level=level,
                tooltip=tooltip,
                w=tc,
                x=origin,
            )
            call_stack_blocks.append(block)
            call = func, child
            if call not in visited:
                _build_blocks_by_call_stack(
                    child,
                    (cc, nc, tt, tc),
                    level=level + 1,
                    origin=origin,
                    visited=visited | {call},
                    call_stack=child_call_stack,
                    parent_call_count=call_count,
                    parent_block=block,
                )
            origin += tc

    def _build_blocks_by_usage(
        ids: Sequence[FunctionID],
        *,
        level: int = 0,
        to: Optional[FunctionID] = None,
        origin: float = 0,
        visited: AbstractSet[Call] = frozenset(),
        parent_width: float = 0,
    ) -> None:
        factor = 1.0
        if ids and to is not None:
            calls_tottime = sum(calls[fid, to][3] for fid in ids)
            if calls_tottime:
                factor = parent_width / calls_tottime

        for fid in sorted(ids):
            call = fid, to
            if to is not None:
                cc, nc, tt, tc = calls[call]  # type: ignore
                ttt = tc * factor
            else:
                cc, nc, tt, tc = funcs[fid].stat
                ttt = tt * factor

            if ttt / maxw < threshold:
                origin += ttt
                continue

            tooltip = TOOLTIP.format(tt / maxw, cc, nc, tt, tc)
            block = Block(
                func=fid,
                call_stack=(),
                color=2 if level > 0 else not funcs[fid].calls,
                level=level,
                tooltip=tooltip,
                w=ttt,
                x=origin,
            )
            usage_blocks.append(block)
            if call not in visited:
                _build_blocks_by_usage(
                    funcs[fid].calledby,
                    level=level + 1,
                    to=fid,
                    origin=origin,
                    visited=visited | {call},
                    parent_width=ttt,
                )
            origin += ttt

    _build_blocks_by_call_stack(ROOT_ID, scaled_timings=(1, 1, maxw, maxw))
    _build_blocks_by_usage([fid for fid in funcs if fid != ROOT_ID])
    return call_stack_blocks, usage_blocks, maxw


def build_svg_blocks_by_memory(
    root: MemoryFrame,
    *,
    maxw: int,
    level: int = 0,
    x: int = 0,
    scope_cache: Optional[ScopeCache] = None,
) -> Iterator[Block]:
    if scope_cache is None:
        scope_cache = ScopeCache()
    for (mod_path, lineno), caller in root.callers.items():
        func_name = scope_cache[mod_path, lineno]
        line = linecache.getline(mod_path, lineno).strip()
        if len(caller.callers) == 0 or level == 0:
            color = 0
        elif len(caller.callers) >= 2:
            color = 1
        else:
            color = 2
        yield Block(
            func=(mod_path, lineno, func_name),
            call_stack=(),
            color=color,
            level=level,
            tooltip=(
                f"{caller.size / 1024:.2f} KiB / {caller.blocks}"
                f" blocks \N{RIGHTWARDS DOUBLE ARROW} {line}"
            ),
            w=caller.size,
            x=x,
        )
        yield from build_svg_blocks_by_memory(
            caller, maxw=maxw, level=level + 1, x=x, scope_cache=scope_cache,
        )
        x += caller.size


def render_svg_section(
    blocks: List[Block],
    maxw: float,
    colors: List[List[RGB]],
    block_height: int,
    font_size: int,
    width: int,
    javascript: str = "",
    invert: bool = False,
) -> str:
    maxlevel = max(r.level for r in blocks) + 1
    height = (maxlevel + 1) * block_height
    top = 0 if not invert else 3 * block_height
    content = []
    for b in blocks:
        x = b.x * width / maxw
        tx = block_height / 6
        y = b.level
        if invert:
            y = maxlevel - y
        y = top + height - y * block_height - block_height
        ty = block_height / 2
        w = max(1, b.w * width / maxw - 1)
        bcolors = colors[b.color]
        fill = bcolors[int(len(bcolors) * gradient_from_name(b.id))]
        content.append(
            ELEM.format(
                w=w,
                x=x,
                y=y,
                tx=tx,
                ty=ty,
                name=saxutils.escape(b.name),
                full_name=saxutils.escape(b.full_name),
                font_size=font_size,
                h=block_height - 1,
                fill=fill,
                upsidedown="true" if invert else "false",
            )
        )
    height += block_height
    content.append(
        DETAILS.format(
            font_size=font_size, y=2 * block_height if invert else height
        )
    )
    result = SVG.format(
        "\n".join(content),
        javascript=javascript,
        width=width,
        height=top + height + block_height,
        unzoom_button_x=width - 100,
        ui_font_size=1.33 * font_size,
    )
    return result


def render_svg(
    stats: Stats,
    call_out: Union[pathlib.Path, str],
    usage_out: Union[pathlib.Path, str],
    *,
    threshold: float = 0.00001,  # 1.0 is 100%
    width: int = 1920,  # in pixels
    block_height: int = 24,  # in pixels
    font_size: int = 12,
    raw: bool = False,
) -> None:
    """Render an SVG file to `call_out` and `usage_out`.

    Raises ValueError if rendering cannot be done with the given `stats`.

    Functions whose runtime is below `threshold` percentage are not included.
    Unless `raw` is True, functions are filtered to exclude common wrappers
    that make the resulting SVG too busy but are themselves harmless.
    """
    funcs, calls = calc_callers(stats, threshold)
    call_blocks, usage_blocks, maxw = build_svg_blocks(
        funcs, calls, threshold=threshold
    )
    with PROFILING_JS.open() as js_file:
        javascript = js_file.read()
    if call_blocks:
        call_svg = render_svg_section(
            call_blocks,
            maxw,
            [COLORS, CCOLORS],
            block_height=block_height,
            font_size=font_size,
            width=width,
            javascript=javascript,
        )
        with open(call_out, "w") as outf:
            outf.write(call_svg)
    if usage_blocks:
        usage_svg = render_svg_section(
            usage_blocks,
            maxw,
            [COLORS, ECOLORS, DCOLORS],
            block_height=block_height,
            font_size=font_size,
            width=width,
            javascript=javascript,
        )
        with open(usage_out, "w") as outf:
            outf.write(usage_svg)


def render_memory_svg(
    stats: MemoryFrame,
    out: Union[pathlib.Path, str],
    *,
    width: int = 1920,  # in pixels
    block_height: int = 24,  # in pixels
    font_size: int = 12,
):
    with PROFILING_JS.open() as js_file:
        javascript = js_file.read()
    maxw = stats.size
    mem_blocks = list(build_svg_blocks_by_memory(stats, maxw=maxw))
    mem_svg = render_svg_section(
        mem_blocks,
        maxw,
        [COLORS, CCOLORS, DCOLORS],
        block_height=block_height,
        font_size=font_size,
        width=width,
        javascript=javascript,
        invert=True,
    )
    with open(out, "w") as outf:
        outf.write(mem_svg)


SVG = """\
<?xml version="1.0" standalone="no"?>
<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" \
"http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">
<svg version="1.1" width="{width}" height="{height}"
 xmlns="http://www.w3.org/2000/svg"
 xmlns:xlink="http://www.w3.org/1999/xlink"
 onload="init(evt)"
 class="default">
<style type="text/css">
 .func_g {{ font-family: arial }}
 .func_g:hover {{ stroke:black; stroke-width:0.5; cursor:pointer; }}
</style>
<script type="text/ecmascript">
<![CDATA[
{javascript}
]]>
</script>
<text id="unzoom" onclick="unzoom()"
 text-anchor="" x="{unzoom_button_x}" y="24"
 font-size="{ui_font_size}" font-family="arial"
 fill="rgb(0,0,0)" style="opacity:0.0;cursor:pointer" >Reset Zoom</text>
<text id="search"
 onmouseover="searchover()" onmouseout="searchout()" onclick="search_prompt()"
 text-anchor="" x="10" y="24"
 font-size="{ui_font_size}" font-family="arial"
 fill="rgb(0,0,0)" style="opacity:0.1;cursor:pointer" >Search</text>
{}
</svg>"""

ELEM = """\
<svg class="func_g" x="{x}" y="{y}" width="{w}" height="{h}"
 onclick="zoom(this, {upsidedown})" onmouseover="s(this)" onmouseout="s()">
    <title>{full_name}</title>
    <rect height="100%" width="100%" fill="rgb({fill.r},{fill.g},{fill.b})"
     rx="2" ry="2" />
    <text text-anchor="" x="{tx}" y="{ty}"
     font-size="{font_size}px" fill="rgb(0,0,0)">{name}</text>
</svg>"""

DETAILS = """
<text id="details" text-anchor="" x="10.00" y="{y}"
 font-family="arial" font-size="{font_size}" font-weight="bold"
 fill="rgb(0,0,0)"> </text>
"""

TOOLTIP = "{0:.2%} (calls={1} pcalls={2} tottime={3:.2f} cumtime={4:.2f})"
