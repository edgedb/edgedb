##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import postgresql
import postgresql.protocol.xact3

from semantix.caos import session
from semantix.utils.debug import debug


class SessionPool(session.SessionPool):
    def __init__(self, backend, realm):
        super().__init__(realm)
        self.backend = backend

    def create(self):
        return Session(self.realm, self.backend, pool=self)


class AsyncSessionPool(SessionPool):
    def create(self):
        return AsyncSession(self.realm, self.backend, pool=self)


class Transaction(session.Transaction):
    def __init__(self, session, parent=None):
        super().__init__(session, parent=parent)
        self.xact = self.session.connection.xact()
        self._begin_impl()

    @debug
    def _begin_impl(self):
        """LOG [caos.sql]
        print('BEGIN %r' % self.xact)
        """
        self.xact.begin()

    @debug
    def _rollback_impl(self):
        """LOG [caos.sql]
        print('ROLLBACK %r' % self.xact)
        """
        self.xact.rollback()
        self.xact = None

    @debug
    def _commit_impl(self):
        """LOG [caos.sql]
        print('COMMIT %r' % self.xact)
        """
        self.xact.commit()
        self.xact = None


class Session(session.Session):
    def __init__(self, realm, backend, pool):
        super().__init__(realm, entity_cache=session.WeakEntityCache, pool=pool)
        self.backend = backend
        self.prepared_statements = {}
        self.init_connection()

    def init_connection(self):
        self.connection = self.backend.connection_pool(self)

    def get_connection(self):
        return self.connection

    def get_prepared_statement(self, query):
        ps = self.prepared_statements.get(self.connection)
        if ps:
            ps = ps.get(query)

        if not ps:
            ps = self.connection.prepare(query)
            self.prepared_statements.setdefault(self.connection, {})[query] = ps

        return ps

    def _transaction(self, parent):
        return Transaction(session=self, parent=parent)

    def _store_entity(self, entity):
        self.backend.store_entity(entity, self)

    def _delete_entities(self, entities):
        self.backend.delete_entities(entities, self)

    def _store_links(self, source, targets, link_name, merge=False):
        self.backend.store_links(source, targets, link_name, self, merge=merge)

    def _delete_links(self, source, targets, link_name):
        self.backend.delete_links(source, targets, link_name, self)

    def _load_link(self, link):
        return self.backend.load_link(link._instancedata.source, link._instancedata.target, link,
                                      self)

    def load(self, id, concept=None):
        if not concept:
            concept_name = self.backend.concept_name_from_id(id, session=self)
            if not concept_name:
                return None
            concept = self.schema.get(concept_name)
        else:
            concept_name = concept._metadata.name

        links = self.backend.load_entity(concept_name, id, session=self)

        if not links:
            return None

        return self._load(id, concept, links)

    def sequence_next(self, seqcls):
        return self.backend.sequence_next(seqcls)

    def start_batch(self, batch):
        super().start_batch(batch)
        self.backend.start_batch(self, id(batch))

    def commit_batch(self, batch):
        super().commit_batch(batch)
        self.backend.commit_batch(self, id(batch))

    def close_batch(self, batch):
        super().close_batch(batch)
        self.backend.close_batch(self, id(batch))

    def _store_entity_batch(self, entities, batch):
        self.backend.store_entity_batch(entities, self, id(batch))

    def _store_link_batch(self, links, batch):
        self.backend.store_link_batch(links, self, id(batch))

    def sync(self, skipbatch=False):
        self.do_sync(skipbatch=skipbatch)

    def interrupt(self):
        if self.connection.pq.xact and \
           self.connection.pq.xact.state != postgresql.protocol.xact3.Complete:
            try:
                self.connection.interrupt()
                self.connection.pq.complete()
            except postgresql.exceptions.QueryCanceledError:
                pass

    def _release_cleanup(self):
        self.connection.reset()

    def close(self):
        self.connection.release()
        self.connection = None
        self.prepared_statements.clear()
        super().close()


class AsyncSession(Session):
    def init_connection(self):
        self.connection = self.backend.async_connection_pool(self)
