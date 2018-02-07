##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import datetime

from edgedb.lang.common import ast

from . import base as s_types


_add_impl = s_types.BaseTypeMeta.add_implementation
_add_map = s_types.BaseTypeMeta.add_mapping

_add_map(datetime.datetime, 'std::datetime')


class DateTime(datetime.datetime):
    pass


_add_map(DateTime, 'std::datetime')
_add_impl('std::datetime', DateTime)


class Date(datetime.date):
    pass


_add_impl('std::date', Date)
_add_map(Date, 'std::date')


_add_map(datetime.time, 'std::time')


class Time(datetime.time):
    pass


_add_impl('std::time', Time)
_add_map(Time, 'std::time')


_add_map(datetime.timedelta, 'std::timedelta')


class TimeDelta(datetime.timedelta):
    pass


_add_map(TimeDelta, 'std::timedelta')
_add_impl('std::timedelta', TimeDelta)

s_types.TypeRules.add_rule(ast.ops.ADD, (DateTime, DateTime), 'std::datetime')

s_types.TypeRules.add_rule(ast.ops.ADD, (DateTime, Time), 'std::datetime')
s_types.TypeRules.add_rule(ast.ops.ADD, (Time, DateTime), 'std::datetime')

s_types.TypeRules.add_rule(ast.ops.ADD, (TimeDelta, DateTime), 'std::datetime')
s_types.TypeRules.add_rule(ast.ops.ADD, (DateTime, TimeDelta), 'std::datetime')

s_types.TypeRules.add_rule(
    ast.ops.ADD, (TimeDelta, TimeDelta), 'std::timedelta')

s_types.TypeRules.add_rule(ast.ops.SUB, (DateTime, DateTime), 'std::timedelta')

s_types.TypeRules.add_rule(ast.ops.SUB, (DateTime, TimeDelta), 'std::datetime')

s_types.TypeRules.add_rule(
    ast.ops.SUB, (TimeDelta, TimeDelta), 'std::timedelta')
