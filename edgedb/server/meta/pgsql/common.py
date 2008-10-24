from __future__ import with_statement
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
