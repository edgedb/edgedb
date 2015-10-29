##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos.backends import deltarepo
from metamagic.utils.datastructures import OrderedIndex

from metamagic.caos import delta as delta_cmds


class MetaDeltaRepository(deltarepo.MetaDeltaRepository):
    def __init__(self):
        self.deltas = OrderedIndex(key=lambda i: i.id)
        self.refs = {}

    def write_delta(self, d):
        self.deltas.add(d)

    def write_delta_set(self, dset):
        for d in dset.deltas:
            self.write_delta(d)

    def load_delta(self, delta_id, compat_mode=False):
        return self.deltas[delta_id]

    def delta_ref_to_id(self, ref):
        if not ref.offset:
            return self.refs.get(ref.ref)
        else:
            deltas = list(self.deltas.keys())

            try:
                return deltas[deltas.index(ref.ref) - ref.offset]
            except IndexError:
                raise delta_cmds.DeltaRefError('unknown revision: %s' % ref)

    def update_delta_ref(self, ref, id):
        self.refs[ref] = id
