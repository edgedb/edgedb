##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.schema import deltarepo
from edgedb.lang.schema import delta as sd

from . import common
from . import dbops, deltadbops, delta

from .datasources import deltalog


class MetaDeltaRepository(deltarepo.MetaDeltaRepository):
    def __init__(self, connection):
        self.connection = connection

    async def delta_ref_to_id(self, ref):
        table = deltadbops.DeltaRefTable()
        condition = dbops.TableExists(table.name)
        have_deltaref = condition.execute(
            delta.CommandContext(self.connection))

        result = []

        if have_deltaref:
            query = 'SELECT id FROM %s WHERE ref = $1' % common.qname(
                *table.name)

            ps = self.connection.prepare(query)

            result = ps.first(ref.ref) or ref.ref

            try:
                result = int(result, 16)
            except ValueError:
                result = None

            if ref.offset:
                rev_id = '%x' % result
                result = await deltalog.fetch(
                    self.connection, rev_id=rev_id, offset=ref.offset)

                if not result:
                    raise sd.DeltaRefError('unknown revision: %s' % ref)
                result = int(result[0][0], 16)

            return result
        else:
            return None
