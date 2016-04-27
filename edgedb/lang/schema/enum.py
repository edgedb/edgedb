##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import enum


class StrEnum(str, enum.Enum):
    def __str__(self):
        return self._value_
