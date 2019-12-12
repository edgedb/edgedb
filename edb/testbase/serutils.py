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

import datetime
import decimal
import functools
import uuid

import edgedb


@functools.singledispatch
def serialize(o):
    raise TypeError(f'cannot serialiaze type {type(o)}')


@serialize.register
def _tuple(o: edgedb.Tuple):
    return [serialize(el) for el in o]


@serialize.register
def _namedtuple(o: edgedb.NamedTuple):
    return {attr: serialize(getattr(o, attr)) for attr in dir(o)}


@serialize.register
def _linkset(o: edgedb.LinkSet):
    return [serialize(el) for el in o]


@serialize.register
def _link(o: edgedb.Link):
    ret = {}

    for lprop in dir(o):
        if lprop in {'source', 'target'}:
            continue
        ret[f'@{lprop}'] = serialize(getattr(o, lprop))

    ret.update(_object(o.target))
    return ret


@serialize.register
def _object(o: edgedb.Object):
    ret = {}

    for attr in dir(o):
        try:
            link = o[attr]
        except KeyError:
            link = None

        if link:
            ret[attr] = serialize(link)
        else:
            ret[attr] = serialize(getattr(o, attr))

    return ret


@serialize.register(edgedb.Set)
@serialize.register(edgedb.Array)
def _set(o):
    return [serialize(el) for el in o]


@serialize.register(uuid.UUID)
def _stringify(o):
    return str(o)


@serialize.register(int)
@serialize.register(float)
@serialize.register(str)
@serialize.register(bool)
@serialize.register(type(None))
@serialize.register(decimal.Decimal)
@serialize.register(datetime.timedelta)
def _scalar(o):
    return o


@serialize.register
def _datetime(o: datetime.datetime):
    return o.isoformat()


@serialize.register
def _date(o: datetime.date):
    return o.isoformat()


@serialize.register
def _time(o: datetime.time):
    return o.isoformat()


@serialize.register
def _enum(o: edgedb.EnumValue):
    return str(o)
