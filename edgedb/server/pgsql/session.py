##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import postgresql.exceptions
import postgresql.protocol.xact3

from metamagic.caos import session
from metamagic.utils.algos import persistent_hash
from metamagic.utils.debug import debug


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
        self.xact = self.session.get_connection().xact()
        self._begin_impl()

    @debug
    def _begin_impl(self):
        """LOG [caos.sql]
        print('BEGIN %r[%d]' % (self.xact, len(list(self.chain()))))
        """
        self.xact.begin()

    @debug
    def _rollback_impl(self):
        """LOG [caos.sql]
        print('ROLLBACK %r[%d]' % (self.xact, len(list(self.chain()))))
        """
        if self.xact is not None:
            self.xact.rollback()
            self.xact = None

    @debug
    def _commit_impl(self):
        """LOG [caos.sql]
        print('COMMIT %r[%d]' % (self.xact, len(list(self.chain()))))
        """
        self.xact.commit()
        self.xact = None


class RecordInfo:
    def __init__(self, *, attribute_map, proto_class=None, proto_name=None, is_xvalue=False,
                          recursive_link=False, virtuals_map=None):
        self.attribute_map = attribute_map
        self.virtuals_map = virtuals_map
        self.proto_class = proto_class
        self.proto_name = proto_name
        self.is_xvalue = is_xvalue
        self.recursive_link = recursive_link
        self.id = str(persistent_hash.persistent_hash(self))

    def persistent_hash(self):
        return persistent_hash.persistent_hash((tuple(self.attribute_map),
                                                frozenset(self.virtuals_map.items())
                                                    if self.virtuals_map else None,
                                                self.proto_class,
                                                self.proto_name, self.is_xvalue,
                                                self.recursive_link))

    def __mm_serialize__(self):
        return dict(
            attribute_map=self.attribute_map,
            virtuals_map=self.virtuals_map,
            proto_class=self.proto_class,
            proto_name=self.proto_name,
            is_xvalue=self.is_xvalue,
            recursive_link=self.recursive_link,
            id=self.id
        )


class Session(session.Session):
    def __init__(self, realm, backend, pool, proto_schema=None, connection=None):
        super().__init__(realm, entity_cache=session.WeakEntityCache, pool=pool,
                                proto_schema=proto_schema)
        self.backend = backend
        self._connection = connection

    def init_connection(self):
        self._connection = self.backend.connection_pool(self)

    def get_connection(self):
        if self._connection is None:
            self.init_connection()
        return self._connection

    def get_prepared_statement(self, query, raw=True):
        connection = self.get_connection()
        return connection.get_prepared_statement(query, raw=raw)

    def _transaction(self, parent):
        return Transaction(session=self, parent=parent)

    def _get_query_adapter(self):
        return self.backend.caosqladapter(self)

    def _store_entity(self, entity):
        self.backend.store_entity(entity, self)

    def _delete_entities(self, entities):
        self.backend.delete_entities(entities, self)

    def _store_links(self, source, targets, link_name, merge=False):
        self.backend.store_links(source, targets, link_name, self, merge=merge)

    def _delete_links(self, link_name, endpoints):
        self.backend.delete_links(link_name, endpoints, self)

    def _load_link(self, link, pointers):
        return self.backend.load_link(link._instancedata.source, link._instancedata.target, link,
                                      pointers, self)

    def load(self, id, concept=None, access_control=True):
        if concept is None:
            concept_name = self.backend.concept_name_from_id(id, session=self)
            if not concept_name:
                return None
            concept = self.schema.get(concept_name)

        if not access_control:
            concept = concept.mark(access_control=False)

        return concept.get(concept.id == id)

    def sequence_next(self, seqcls):
        return self.backend.sequence_next(seqcls)

    def sync(self):
        self.do_sync()

    def interrupt(self):
        if self._connection is not None and self._connection.pq.xact and \
           self._connection.pq.xact.state != postgresql.protocol.xact3.Complete:
            try:
                self._connection.interrupt()
                self._connection.pq.complete()
            except postgresql.exceptions.QueryCanceledError:
                pass

    def _drop_connection(self):
        if self._connection is not None:
            try:
                self._connection.close()
            except (postgresql.exceptions.Error, IOError) as e:
                # The session is likely already dying horribly, quite likely due to a
                # backend/driver exception, so we must expect further breakage when releasing
                # the connection.
                msg = 'Unhandled backend exception while dropping session connection, masking'
                self.logger.error(msg, exc_info=(type(e), e, e.__traceback__))
            self._connection = None

    def _abortive_cleanup(self):
        self._drop_connection()

    def _release_cleanup(self):
        try:
            super()._release_cleanup()
            if self._connection is not None:
                self._connection.reset()
        except (postgresql.exceptions.Error, IOError) as e:
            # Backend could not release gracefully, abort and drop backend connection.
            msg = 'Unhandled backend exception in session.release(), attempting to recover'
            self.logger.error(msg, exc_info=(type(e), e, e.__traceback__))
            self._abortive_cleanup()
        except Exception:
            self._abortive_cleanup()
            raise

    def _close_cleanup(self):
        super()._close_cleanup()
        self._drop_connection()


class AsyncSession(Session):
    def init_connection(self):
        self._connection = self.backend.async_connection_pool(self)
