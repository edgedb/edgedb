##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re
import hashlib
import base64

import postgresql
from postgresql.driver.dbapi20 import Cursor as CompatCursor

from semantix import caos

def quote_ident(text):
    """
    Quotes the identifier
    """
    result = postgresql.string.quote_ident(text)
    if result[0] != '"':
        return '"' + result + '"'
    return result

def qname(*parts):
    return '.'.join([quote_ident(q) for q in parts])

def caos_name_to_pg_colname(name):
    if len(name) > 63:
        hash = base64.b64encode(hashlib.md5(name.encode()).digest()).decode().rstrip('=')
        name = hash + ':' + name[-(63 - 1 - len(hash)):]
    return name


class DatabaseObject(object):
    def __init__(self, connection):
        self.connection = connection
        self.cursor = CompatCursor(connection)

    def create(self):
        if self.create.__doc__ is None:
            raise Exception('missing DDL statement in docstring')

        try:
            self.runquery(self.create.__doc__)
        except postgresql.exceptions.DuplicateTableError:
            pass


class DatabaseTable(DatabaseObject):
    def insert(self, *dicts, **kwargs):
        data = {}
        for d in dicts + (kwargs,):
            data.update(d)

        if self.insert.__doc__ is None:
            raise Exception('missing insert statement in docstring')

        result = self.runquery(self.insert.__doc__, data)

        return result

    def runquery(self, query, params=None):
        query, pxf, nparams = self.cursor._convert_query(query)
        ps = self.connection.prepare(query)
        if params:
            return ps(*pxf(params))
        else:
            return ps()


class MetaObjectTable(DatabaseTable):
    def create(self):
        """
            CREATE TABLE "caos"."metaobject"(
                id serial NOT NULL,
                name text NOT NULL,
                abstract boolean NOT NULL DEFAULT FALSE,
                title hstore,
                description hstore,

                PRIMARY KEY (id),
                UNIQUE (name)
            )
        """
        super().create()


class AtomTable(DatabaseTable):
    def create(self):
        """
            CREATE TABLE "caos"."atom"(
                automatic bool NOT NULL DEFAULT FALSE,

                PRIMARY KEY (id),
                UNIQUE (name)
            ) INHERITS ("caos"."metaobject")
        """
        super().create()

    def insert(self, *dicts, **kwargs):
        """
            INSERT INTO "caos"."atom"(id, name, title, description, automatic, abstract)
            VALUES (nextval('"caos"."metaobject_id_seq"'::regclass),
                    %(name)s, %(title)s::hstore, %(description)s::hstore, %(automatic)s, %(abstract)s)
            RETURNING id
        """
        return super().insert(*dicts, **kwargs)[0][0]


class ConceptTable(DatabaseTable):
    def create(self):
        """
            CREATE TABLE "caos"."concept"(
                PRIMARY KEY (id),
                UNIQUE (name)
            ) INHERITS ("caos"."metaobject")
        """
        super().create()

    def insert(self, *dicts, **kwargs):
        """
            INSERT INTO "caos"."concept"(id, name, title, description, abstract)
            VALUES (nextval('"caos"."metaobject_id_seq"'::regclass),
                    %(name)s, %(title)s::hstore, %(description)s::hstore, %(abstract)s)
            RETURNING id
        """
        return super().insert(*dicts, **kwargs)[0][0]


class LinkTable(DatabaseTable):
    def create(self):
        """
            CREATE TABLE "caos"."link"(
                source_id integer,
                target_id integer,
                mapping char(2) NOT NULL,
                required boolean NOT NULL DEFAULT FALSE,
                implicit boolean NOT NULL DEFAULT FALSE,
                atomic boolean NOT NULL DEFAULT FALSE,

                PRIMARY KEY (id)
            ) INHERITS("caos"."metaobject")
        """
        super().create()

    def insert(self, *dicts, **kwargs):
        """
            INSERT INTO "caos"."link"(id, source_id, target_id, name, mapping, required, title, description,
                                                                               implicit, atomic, abstract)
                VALUES (
                            nextval('"caos"."metaobject_id_seq"'::regclass),
                            (SELECT id FROM caos.metaobject WHERE name = %(source)s),
                            (SELECT id FROM caos.metaobject WHERE name = %(target)s),
                            %(name)s,
                            %(mapping)s,
                            %(required)s,
                            %(title)s::hstore,
                            %(description)s::hstore,
                            %(implicit)s,
                            %(atomic)s,
                            %(abstract)s
                ) RETURNING id
        """
        return super().insert(*dicts, **kwargs)[0][0]
