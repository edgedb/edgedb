import postgresql
from semantix.caos import Class, MetaError, ConceptLinkType

class BackendCommon(object):
    base_type_map = {
                        'integer': int,
                        'character': str,
                        'character varying': str,
                        'boolean': bool,
                        'numeric': int,
                        'double precision': float
                    }

    base_type_name_map = {
                                'str': 'character varying',
                                'int': 'numeric',
                                'bool': 'boolean',
                                'float': 'double precision'
                         }

    base_type_name_map_r = {
                                'character varying': 'str',
                                'character': 'str',
                                'text': 'str',
                                'integer': 'int',
                                'boolean': 'bool',
                                'numeric': 'int',
                                'double precision': 'float'
                           }


    typmod_types = ('character', 'character varying', 'numeric')
    fixed_length_types = {'character varying': 'character'}


    def demangle_concept_name(self, name):
        if name.endswith('_data'):
            name = name[:-5]

        return name


    def mangle_concept_name(self, name, quote=False):
        if quote:
            return '"caos"."%s_data"' % name
        else:
            return 'caos.%s_data' % name



    def mangle_domain_name(self, name, quote=False):
        if quote:
            return '"caos"."%s_domain"' % name
        else:
            return 'caos.%s_domain' % name

    def demangle_domain_name(self, name):
        name = name.split('.')[-1]

        if name.endswith('_domain'):
            name = name[:-7]

        return name

    def pg_type_from_atom_class(self, atom_cls):
        if (atom_cls.base is not None and atom_cls.base.name == 'str'
                and len(atom_cls.mods) == 1 and 'max-length' in atom_cls.mods):
            column_type = 'varchar(%d)' % atom_cls.mods['max-length']
        else:
            if atom_cls.name in self.base_type_name_map:
                column_type = self.base_type_name_map[atom_cls.name]
            else:
                column_type = self.mangle_domain_name(atom_cls.name, True)

        return column_type


class DatabaseConnection(object):

    def __init__(self, connection):
        self.connection = connection

    def cursor(self):
        return self.connection.cursor()

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
            except postgresql.exceptions.DuplicateTableError:
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
            except postgresql.exceptions.UndefinedTableError:
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
                required boolean NOT NULL DEFAULT FALSE,

                PRIMARY KEY (id),
                FOREIGN KEY (source_id) REFERENCES "caos"."concept"(id) ON DELETE CASCADE,
                FOREIGN KEY (target_id) REFERENCES "caos"."concept"(id) ON DELETE CASCADE
            )
        """
        super(ConceptMapTable, self).create()

    def insert(self, *dicts, **kwargs):
        """
            INSERT INTO "caos"."concept_map"(source_id, target_id, link_type, mapping, required)
                VALUES (
                            (SELECT id FROM caos.concept WHERE name = %(source)s),
                            (SELECT id FROM caos.concept WHERE name = %(target)s),
                            %(link_type)s,
                            %(mapping)s,
                            %(required)s
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
