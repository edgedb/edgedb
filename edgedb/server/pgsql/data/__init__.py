import psycopg2
from semantix.caos.backends.data.base import BaseDataBackend
from semantix.caos.backends.meta.pgsql.common import DatabaseConnection, DatabaseTable

from .datasources import EntityLinks, ConceptLink
from .adapters.caosql import CaosQLQueryAdapter

from semantix.caos.concept import BaseConceptCollection

from semantix.config import settings

class CaosQLCursor(object):
    def __init__(self, connection):
        self.native_cursor = connection.cursor()
        self.adapter = CaosQLQueryAdapter()

    def prepare_query(self, query, vars):
        return self.adapter.adapt(query, vars)

    def execute_prepared(self, query):
        return self.native_cursor.execute(query.text, query.vars)

    def execute(self, query, vars=None):
        native_query = self.prepare_query(query, vars)
        return self.execute_prepared(native_query)

    def fetchall(self):
        return self.native_cursor.fetchall()

    def fetchmany(self, size=None):
        return self.native_cursor.fetchmany(size)

    def fetchone(self):
        return self.native_cursor.fetchone()


class PathCacheTable(DatabaseTable):
    def create(self):
        """
            CREATE TABLE caos.path_cache (
                id                  serial NOT NULL,

                entity_id           integer NOT NULL,
                parent_entity_id    integer,

                name_attribute      varchar(255),
                concept_name        varchar(255) NOT NULL,

                weight              integer,

                PRIMARY KEY (id),
                UNIQUE(entity_id, parent_entity_id),

                FOREIGN KEY (entity_id) REFERENCES caos.entity(id)
                    ON UPDATE CASCADE ON DELETE CASCADE,

                FOREIGN KEY (parent_entity_id) REFERENCES caos.entity(id)
                    ON UPDATE CASCADE ON DELETE CASCADE
            )
        """
        super(PathCacheTable, self).create()

    def insert(self, *dicts, **kwargs):
        """
            INSERT INTO
                caos.path_cache
                    (entity_id, parent_entity_id, name_attribute, concept_name, weight)

                VALUES(%(entity_id)s, %(parent_entity_id)s,
                       %(name_attribute)s, %(concept_name)s, %(weight)s)
            RETURNING entity_id
        """
        return super(PathCacheTable, self).insert(*dicts, **kwargs)


class DataBackend(BaseDataBackend):
    def __init__(self, connection):
        self.connection = DatabaseConnection(connection)
        self.entity_table = EntityTable(self.connection)
        self.entity_table.create()
        self.path_cache_table = PathCacheTable(self.connection)
        self.path_cache_table.create()

    def get_concept_from_entity(self, id):
        query = """SELECT
                            c.name
                        FROM
                            caos.entity e
                            INNER JOIN caos.concept c ON c.id = e.concept_id
                        WHERE
                            e.id = %d""" % id
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        if result is None:
            return None
        else:
            return result[0]

    def load_entity(self, concept, id):
        query = 'SELECT * FROM "caos"."%s_data" WHERE entity_id = %d' % (concept, id)
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()

        if result is not None:
            return dict((k, result[k]) for k in result.keys() if k != 'entity_id')
        else:
            return None

    def store_entity(self, entity):
        concept = entity.concept
        id = entity.id
        links = entity._links

        debug = 'caos' in settings.debug and 'sync' in settings.debug['caos'] \
                and settings.debug['caos']['sync']

        with self.connection as cursor:

            attrs = {n: v for n, v in links.items() if not isinstance(v, BaseConceptCollection)}
            attrs['entity_id'] = id

            if id is not None:
                query = 'UPDATE "caos"."%s_data" SET ' % concept
                query += ','.join(['%s = %%(%s)s' % (a, a) for a in attrs])
                query += ' WHERE entity_id = %d RETURNING entity_id' % id
            else:
                id = self.entity_table.insert({'concept': concept})[0]

                query = 'INSERT INTO "caos"."%s_data"' % concept
                query += '(' + ','.join(['"%s"' % a for a in attrs]) + ')'
                query += 'VALUES(' + ','.join(['%%(%s)s' % a for a in attrs]) + ') RETURNING entity_id'

            data = dict((k, str(attrs[k]) if attrs[k] is not None else None) for k in attrs)
            data['entity_id'] = id

            cursor.execute(query, data)
            id = cursor.fetchone()
            if id is None:
                raise Exception('failed to store entity')

            if debug:
                print()
                print('-' * 60)
                print('Merged entity %s[%s][%s]' % \
                        (concept, id[0], (data['name'] if 'name' in data else '')))
                print('-' * 60)

            id = id[0]

            entity.setid(id)
            entity.markclean()

            for link_type, link in links.items():
                if isinstance(link, BaseConceptCollection) and link.dirty:
                    self.store_links(concept, link_type, entity, link)
                    link.markclean()

        return id

    def load_links(self, this_concept, this_id, other_concepts=None, link_types=None, reverse=False):

        if link_types is not None and not isinstance(link_types, list):
            link_types = [link_types]

        if other_concepts is not None and not isinstance(other_concepts, list):
            other_concepts = [other_concepts]

        if not reverse:
            source_id = this_id
            target_id = None
            source_concepts = [this_concept]
            target_concepts = other_concepts
        else:
            source_id = None
            target_id = this_id
            target_concepts = [this_concept]
            source_concepts = other_concepts

        links = EntityLinks.fetch(source_id=source_id, target_id=target_id,
                                  target_concepts=target_concepts, source_concepts=source_concepts,
                                  link_types=link_types)

        return links


    def store_links(self, concept, link_type, source, targets):
        rows = []

        debug = 'caos' in settings.debug and 'sync' in settings.debug['caos'] \
                and settings.debug['caos']['sync']

        with self.connection as cursor:
            for target in targets:
                target.sync()

                if debug:
                    print()
                    print('-' * 60)
                    print('Merging link %s[%s][%s]---{%s}-->%s[%s][%s]' % \
                            (source.concept, source.id, (source.name if hasattr(source, 'name') else ''),
                             link_type,
                             target.concept, target.id, (target.name if hasattr(target, 'name') else '')))
                    print('-' * 60)

                # XXX: that's ugly
                targets = [c.concept for c in target.__class__.__mro__ if hasattr(c, 'concept')]

                lt = ConceptLink.fetch(source_concepts=[source.concept], target_concepts=targets,
                                       link_type=link_type)

                rows.append(cursor.mogrify('(%(source_id)s, %(target_id)s, %(link_type_id)s, %(weight)s)',
                                           {'source_id': source.id,
                                            'target_id': target.id,
                                            'link_type_id': lt[0]['id'],
                                            'weight': 0}))

            if len(rows) > 0:
                cursor.execute("""INSERT INTO caos.entity_map(source_id, target_id, link_type_id, weight)
                                    ((VALUES %s) EXCEPT (SELECT
                                                                *
                                                            FROM
                                                                caos.entity_map
                                                            WHERE
                                                                (source_id, target_id, link_type_id, weight) in (%s)))
                               """ % (",".join(rows), ",".join(rows)))

    def store_path_cache_entry(self, entity, parent_entity_id, weight):
        self.path_cache_table.insert(entity_id=entity.id,
                                 parent_entity_id=parent_entity_id,
                                 name_attribute=str(entity.attrs['name']) if 'name' in entity.attrs else None,
                                 concept_name=entity.name,
                                 weight=weight)

    def clear_path_cache(self):
        self.path_cache_table.create()
        with self.connection as cursor:
            cursor.execute('DELETE FROM caos.path_cache')


    def iter(self, concept):
        with self.connection as cursor:
            cursor.execute('''SELECT
                                    id
                                FROM
                                    caos.entity
                                WHERE
                                    concept_id = (SELECT id FROM caos.concept WHERE name = %(concept)s)''',
                            {'concept': concept})

            for row in cursor:
                id = row[0]
                yield id


    def caosqlcursor(self):
        return CaosQLCursor(self.connection)
