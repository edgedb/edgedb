from __future__ import annotations
from typing import Sequence
import edgedb

from ..data import data_ops as e
from .errors import FunCallErr


def indirection_slice_start_stop_impl(
    arg: Sequence[Sequence[e.Val]],
) -> Sequence[e.Val]:
    match arg:
        case [
            [e.ScalarVal(t1, s)],
            [e.ScalarVal(_, start)],
            [e.ScalarVal(_, end)],
        ]:
            return [e.ScalarVal(t1, s[start:end])]
        case [
            [e.ArrVal(val=arr)],
            [e.ScalarVal(_, start)],
            [e.ScalarVal(_, end)],
        ]:
            return [e.ArrVal(val=arr[start:end])]
    raise FunCallErr()


def indirection_slice_start_impl(
    arg: Sequence[Sequence[e.Val]],
) -> Sequence[e.Val]:
    match arg:
        case [[e.ScalarVal(t1, s)], [e.ScalarVal(_, start)]]:
            return [e.ScalarVal(t1, s[start:])]
        case [[e.ArrVal(val=arr)], [e.ScalarVal(_, start)]]:
            return [e.ArrVal(val=arr[start:])]
    raise FunCallErr()


def indirection_slice_stop_impl(
    arg: Sequence[Sequence[e.Val]],
) -> Sequence[e.Val]:
    match arg:
        case [[e.ScalarVal(t1, s)], [e.ScalarVal(_, end)]]:
            return [e.ScalarVal(t1, s[:end])]
        case [[e.ArrVal(val=arr)], [e.ScalarVal(_, end)]]:
            return [e.ArrVal(val=arr[:end])]
    raise FunCallErr()


def indirection_index_impl(arg: Sequence[Sequence[e.Val]]) -> Sequence[e.Val]:
    match arg:
        case [[e.ScalarVal(t1, s)], [e.ScalarVal(_, idx)]]:
            if idx < -len(s) or idx >= len(s):
                raise edgedb.InvalidValueError(f"index {idx} is out of bounds")
            return [e.ScalarVal(t1, s[idx])]
        case [[e.ArrVal(val=arr)], [e.ScalarVal(_, idx)]]:
            if idx < -len(arr) or idx >= len(arr):
                raise edgedb.InvalidValueError(
                    f"array index {idx} is out of bounds"
                )
            return [arr[idx]]
    raise FunCallErr()
