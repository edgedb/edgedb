##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.caos import backends

from semantix.caos import delta as base_delta

from . import common
from . import delta

from .datasources import deltalog


class MetaDeltaRepository(backends.MetaDeltaRepository):
    def __init__(self, connection):
        self.connection = connection

    def delta_ref_to_id(self, ref):
        table = delta.DeltaRefTable()
        condition = delta.TableExists(table.name)
        have_deltaref = condition.execute(delta.CommandContext(self.connection))

        result = []

        if have_deltaref:
            query = 'SELECT id FROM %s WHERE ref = $1' % common.qname(*table.name)

            ps = self.connection.prepare(query)

            result = ps.first(ref.ref) or ref.ref

            try:
                result = int(result, 16)
            except ValueError:
                result = None

            if ref.offset:
                rev_id = '%x' % result
                result = deltalog.DeltaLog(self.connection).fetch(rev_id=rev_id, offset=ref.offset)

                if not result:
                    raise base_delta.DeltaRefError('unknown revision: %s' % ref)
                result = int(result[0][0], 16)

            return result
        else:
            return None
