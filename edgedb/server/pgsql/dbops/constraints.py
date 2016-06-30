##
# Copyright (c) 2008-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .. import common
from . import base


class Constraint(base.DBObject):
    def __init__(self, subject_name, constraint_name=None):
        self._subject_name = subject_name
        self._constraint_name = constraint_name

    def get_type(self):
        return 'CONSTRAINT'

    def get_subject_type(self):
        raise NotImplementedError

    def get_subject_name(self, quote=True):
        if quote:
            return common.qname(*self._subject_name)
        else:
            return self._subject_name

    def get_id(self):
        return '{} ON {} {}'.format(self.constraint_name(),
                                    self.get_subject_type(),
                                    self.get_subject_name())

    def constraint_name(self, quote=True):
        if quote and self._constraint_name:
            return common.quote_ident(self._constraint_name)
        else:
            return self._constraint_name

    async def constraint_code(self):
        raise NotImplementedError
