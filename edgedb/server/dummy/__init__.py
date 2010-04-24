##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.caos.backends import MetaBackend
from semantix.caos.proto import RealmMeta
from semantix.caos.delta import DeltaSet
from semantix.caos import session


class Session(session.Session):
    def __init__(self, realm, entity_cache):
        super().__init__(realm, entity_cache)
        self.xact = []

    def in_transaction(self):
        return super().in_transaction() and bool(self.xact)

    def begin(self):
        super().begin()
        self.xact.append(True)

    def commit(self):
        super().commit()
        self.xact.pop()

    def rollback(self):
        super().rollback()
        if self.xact:
            self.xact.pop()

    def rollback_all(self):
        super().rollback_all()
        self.xact[:] = []


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

    def session(self, realm, entity_cache):
        return Session(realm, entity_cache=entity_cache)
