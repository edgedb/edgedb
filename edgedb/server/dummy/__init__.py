##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.caos.backends import MetaBackend, DataBackend
from semantix.caos.proto import RealmMeta
from semantix.caos.delta import DeltaSet
from semantix.caos import session


class SessionPool(session.SessionPool):
    def create(self):
        return Session(self.realm, pool=self)


class Transaction(session.Transaction):
    def _rollback_impl(self):
        pass

    def _commit_impl(self):
        pass


class Session(session.Session):
    def __init__(self, realm, pool):
        super().__init__(realm, entity_cache=session.WeakEntityCache, pool=pool)

    def _transaction(self, parent):
        return Transaction(self, parent)

    def load(self, id, concept=None):
        raise NotImplementedError

    def sequence_next(self, seqcls):
        raise NotImplementedError

    def _store_entity(self, entity):
        raise NotImplementedError

    def _delete_entities(self, entity):
        raise NotImplementedError

    def _store_links(self, source, targets, link_name, merge=False):
        raise NotImplementedError

    def _delete_links(self, source, targets, link_name):
        raise NotImplementedError

    def _load_link(self, link):
        raise NotImplementedError

    def load_link(self, link):
        raise NotImplementedError

    def start_batch(self, batch):
        raise NotImplementedError

    def commit_batch(self, batch):
        raise NotImplementedError

    def close_batch(self, batch):
        raise NotImplementedError

    def _store_entity_batch(self, entities, batch):
        raise NotImplementedError

    def _store_link_batch(self, links, batch):
        raise NotImplementedError

    def sync(self):
        raise NotImplementedError


class Backend(MetaBackend, DataBackend):
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

    def get_session_pool(self, realm):
        return SessionPool(realm)
