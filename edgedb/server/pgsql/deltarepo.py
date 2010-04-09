##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.caos import backends

from . import common
from . import delta



class MetaDeltaRepository(backends.MetaDeltaRepository):
    def __init__(self, connection):
        self.connection = connection

    def resolve_delta_ref(self, ref):
        table = delta.DeltaRefTable()
        condition = delta.TableExists(table.name)
        have_deltaref = condition.execute(self.connection)

        result = []

        if have_deltaref:
            query = 'SELECT id FROM %s WHERE ref = $1' % common.qname(*table.name)

            ps = self.connection.prepare(query)

            result = ps.first(ref) or ref

            try:
                result = int(result, 16)
            except ValueError:
                result = None

            return result
        else:
            return None
