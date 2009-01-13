import psycopg2

class DatabaseConnection(object):

    def __init__(self, connection):
        self.connection = connection

    def cursor(self):
        return self.connection.cursor(cursor_factory = psycopg2.extras.DictCursor)

    def commit(self):
        return self.connection.commit()

    def rollback(self):
        return self.connection.rollback()

    def __enter__(self):
        return self.cursor()

    def __exit__(self, type, value, tb):
        if tb is None:
            self.commit()
        else:
            self.rollback()

class DatabaseTable(object):
    def __init__(self, connection):
        self.connection = connection

    def create(self):
        if self.create.__doc__ is None:
            raise Exception('missing table definition in docstring')

        with self.connection as cursor:
            try:
                cursor.execute(self.create.__doc__)
            except psycopg2.ProgrammingError:
                self.connection.rollback()

    def insert(self, *dicts, **kwargs):
        data = {}
        for d in dicts + (kwargs,):
            data.update(d)

        if self.insert.__doc__ is None:
            raise Exception('missing insert statement in docstring')
        with self.connection as cursor:
            try:
                cursor.execute(self.insert.__doc__, data)
            except psycopg2.ProgrammingError:
                self.connection.rollback()
                self.create()
                cursor.execute(self.insert.__doc__, data)

        data = cursor.fetchone()
        return data


class ConceptTable(DatabaseTable):
    def create(self):
        """
            CREATE TABLE "caos"."concept"(
                id serial NOT NULL,
                name text NOT NULL,

                PRIMARY KEY (id)
            )
        """
        super(ConceptTable, self).create()

    def insert(self, *dicts, **kwargs):
        """
            INSERT INTO "caos"."concept"(name) VALUES (%(name)s) RETURNING id
        """
        super(ConceptTable, self).insert(*dicts, **kwargs)

class ConceptMapTable(DatabaseTable):
    def create(self):
        """
            CREATE TABLE "caos"."concept_map"(
                id serial NOT NULL,
                source_id integer NOT NULL,
                target_id integer NOT NULL,
                link_type varchar(255) NOT NULL,
                mapping char(2) NOT NULL,

                PRIMARY KEY (id),
                FOREIGN KEY (source_id) REFERENCES "caos"."concept"(id) ON DELETE CASCADE,
                FOREIGN KEY (target_id) REFERENCES "caos"."concept"(id) ON DELETE CASCADE
            )
        """
        super(ConceptMapTable, self).create()

    def insert(self, *dicts, **kwargs):
        """
            INSERT INTO "caos"."concept_map"(source_id, target_id, link_type, mapping)
                VALUES (
                            (SELECT id FROM caos.concept WHERE name = %(source)s),
                            (SELECT id FROM caos.concept WHERE name = %(target)s),
                            %(link_type)s,
                            %(mapping)s
                ) RETURNING id
        """
        super(ConceptMapTable, self).insert(*dicts, **kwargs)


class EntityTable(DatabaseTable):
    def create(self):
        """
            CREATE TABLE "caos"."entity"(
                id serial NOT NULL,
                concept_id integer NOT NULL,

                PRIMARY KEY (id),
                FOREIGN KEY (concept_id) REFERENCES "caos"."concept"(id)
            )
        """
        super(EntityTable, self).create()

    def insert(self, *dicts, **kwargs):
        """
            INSERT INTO "caos"."entity"(concept_id) (SELECT id FROM caos.concept WHERE name = %(concept)s) RETURNING id
        """
        return super(EntityTable, self).insert(*dicts, **kwargs)


class EntityMapTable(DatabaseTable):
    def create(self):
        """
            CREATE TABLE "caos"."entity_map"(
                source_id integer NOT NULL,
                target_id integer NOT NULL,
                link_type_id integer NOT NULL,
                weight integer NOT NULL,

                PRIMARY KEY (source_id, target_id, link_type_id),
                FOREIGN KEY (source_id) REFERENCES "caos"."entity"(id) ON DELETE CASCADE,
                FOREIGN KEY (target_id) REFERENCES "caos"."entity"(id) ON DELETE CASCADE,
                FOREIGN KEY (link_type_id) REFERENCES "caos"."concept_map"(id) ON DELETE RESTRICT
            )
        """
        super(EntityMapTable, self).create()
