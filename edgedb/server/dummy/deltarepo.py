##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.caos import backends
from semantix.utils.datastructures import OrderedIndex


class MetaDeltaRepository(backends.MetaDeltaRepository):
    def __init__(self):
        self.deltas = OrderedIndex(key=lambda i: i.id)
        self.refs = {}

    def write_delta(self, d):
        self.deltas.add(d)

    def load_delta(self, delta_id):
        return self.deltas[delta_id]

    def resolve_delta_ref(self, ref):
        return self.refs.get(ref)

    def update_delta_ref(self, ref, id):
        self.refs[ref] = id
