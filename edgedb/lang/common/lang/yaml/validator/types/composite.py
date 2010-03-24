##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import copy

from .base import SchemaType
from ..error import SchemaValidationError

class CompositeType(SchemaType):
    __slots__ = ['checked']

    def __init__(self, schema):
        super(CompositeType, self).__init__(schema)
        self.checked = {}
