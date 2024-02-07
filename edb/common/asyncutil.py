#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
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

import asyncio


_T = TypeVar('_T')


async def deferred_shield(arg: Awaitable[_T]) -> _T:
    '''Wait for a future, deferring cancellation until it is complete.

    If you do
        await deferred_shield(something())

    it is approximately equivalent to
        await something()

    except that if the coroutine containing it is cancelled,
    something() is protected from cancellation, and *additionally*
    CancelledError is not raised in the caller until something()
    completes.

    This can be useful if something() contains something that
    shouldn't be interrupted but also can't be safely left running
    asynchronously.
    '''
    task = asyncio.ensure_future(arg)

    ex = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as cex:
            if ex is not None:
                cex.__context__ = ex
            ex = cex
        except Exception:
            if ex:
                raise ex from None
            raise

    if ex:
        raise ex
    return task.result()
