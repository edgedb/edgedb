##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast

from . import base as s_types


class Bool(int):
    def __new__(cls, value=0):
        if value == 'False':
            value = 0
        elif value == 'True':
            value = 1
        elif value is None:
            value = 0

        return super().__new__(cls, value)

    def __repr__(self):
        return 'True' if self else 'False'

    __str__ = __repr__

    def __mm_serialize__(self):
        return bool(self)


s_types.BaseTypeMeta.add_implementation(
    'std.bool', Bool)
s_types.BaseTypeMeta.add_mapping(
    Bool, 'std.bool')
s_types.BaseTypeMeta.add_mapping(
    bool, 'std.bool')

s_types.TypeRules.add_rule(
    ast.ops.OR, (Bool, Bool), 'std.bool')
s_types.TypeRules.add_rule(
    ast.ops.AND, (Bool, Bool), 'std.bool')
s_types.TypeRules.add_rule(
    ast.ops.NOT, (Bool,), 'std.bool')
