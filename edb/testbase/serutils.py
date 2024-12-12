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

import dataclasses
import datetime
import decimal
import functools
import uuid

import edgedb


@functools.singledispatch
def serialize(o):
    raise TypeError(f'cannot serialize type {type(o)}')


@serialize.register
def _tuple(o: edgedb.Tuple):
    return [serialize(el) for el in o]


@serialize.register
def _namedtuple(o: edgedb.NamedTuple):
    return {attr: serialize(getattr(o, attr)) for attr in dir(o)}


@serialize.register
def _object(o: edgedb.Object):
    # We iterate over dataclasses.fields(o) (instead of dir(o))
    # because it contains both regular pointers and link properties,
    # and is I think the only current way to extract the names of all
    # the link properties
    attrs = [field.name for field in dataclasses.fields(o)]
    return {attr: serialize(getattr(o, attr)) for attr in attrs}


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
@serialize.register(bytes)
@serialize.register(bool)
@serialize.register(type(None))
@serialize.register(decimal.Decimal)
@serialize.register(datetime.timedelta)
@serialize.register(edgedb.RelativeDuration)
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


@serialize.register
def _record(o: edgedb.Record):
    return {k: serialize(v) for k, v in o.as_dict().items()}


@serialize.register
def _range(o: edgedb.Range):
    return {
        'lower': serialize(o.lower),
        'inc_lower': o.inc_lower,
        'upper': serialize(o.upper),
        'inc_upper': o.inc_upper,
        'empty': o.is_empty(),
    }


@serialize.register
def _multirane(o: edgedb.MultiRange):
    return [serialize(el) for el in o]


@serialize.register
def _cfg_memory(o: edgedb.ConfigMemory):
    return str(o)
