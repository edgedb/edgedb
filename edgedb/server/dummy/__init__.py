##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.caos.backends import MetaBackend
from semantix.caos.proto import RealmMeta
from semantix.caos.delta import DeltaSet


class Backend(MetaBackend):
    def __init__(self, deltarepo):
        super().__init__(deltarepo())
        self.meta = RealmMeta(load_builtins=False)

    def apply_delta(self, delta):
        if isinstance(delta, DeltaSet):
            deltas = list(delta)
        else:
            deltas = [delta]
        delta.apply(self.meta)

        for d in deltas:
            self.deltarepo.write_delta(d)
        self.deltarepo.update_delta_ref('HEAD', deltas[-1].id)

    def getmeta(self):
        return self.meta
