##
# Copyright (c) 2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils import datastructures

from . import tables


class PgDescriptionTable(tables.Table):
    def __init__(self, name=None):
        super().__init__(name=('pg_catalog', 'pg_description'))

        self.__columns = datastructures.OrderedSet([
            tables.Column(name='objoid', type='oid', required=True),
            tables.Column(name='classoid', type='oid', required=True),
            tables.Column(name='objsubid', type='integer', required=True),
            tables.Column(name='description', type='text')
        ])

        self._columns = self.columns()
