# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import sys


__all__ = ('EdgeDBError', 'UnknownEdgeDBError', 'InterfaceError')


class EdgeDBMessageMeta(type):
    _message_map = {}
    _field_map = {
        'S': 'severity',
        'C': 'code',
        'M': 'message',
        'D': 'detail',
        'H': 'hint',
        'P': 'position',
        'Q': 'context',
        'T': 'traceback',
        'c': 'concept_name',
        'l': 'link_name',
        'a': 'atom_name',
        'n': 'constraint_name',
        'F': 'server_source_filename',
        'L': 'server_source_line',
        'R': 'server_source_function'
    }

    def __new__(mcls, name, bases, dct):
        cls = super().__new__(mcls, name, bases, dct)
        if cls.__module__ == mcls.__module__ and name == 'EdgeDBMessage':
            for f in mcls._field_map.values():
                setattr(cls, f, None)

        if (cls.__module__ == 'edgedb.client' or
                cls.__module__.startswith('edgedb.client.')):
            mod = sys.modules[cls.__module__]
            if hasattr(mod, name):
                raise RuntimeError('exception class redefinition: {}'.format(
                    name))

        code = dct.get('code')
        if code is not None:
            existing = mcls._message_map.get(code)
            if existing is not None:
                raise TypeError('{} has duplicate CODE, which is'
                                'already defined by {}'.format(
                                    name, existing.__name__))
            mcls._message_map[code] = cls

        return cls

    @classmethod
    def get_message_class_for_code(mcls, code):
        return mcls._message_map.get(code, UnknownEdgeDBError)


class EdgeDBMessage(metaclass=EdgeDBMessageMeta):
    @classmethod
    def new(cls, fields, query=None):
        errcode = fields.get('C')
        mcls = cls.__class__
        exccls = mcls.get_message_class_for_code(errcode)
        mapped = {
            'query': query
        }

        for k, v in fields.items():
            field = mcls._field_map.get(k)
            if field:
                mapped[field] = v

        e = exccls(mapped.get('message'))
        e.__dict__.update(mapped)

        return e


class EdgeDBError(Exception, EdgeDBMessage):
    """Base class for all EdgeDB errors."""

    def __str__(self):
        msg = self.message
        if self.detail:
            msg += '\nDETAIL:  {}'.format(self.detail)
        if self.hint:
            msg += '\nHINT:  {}'.format(self.hint)
        if self.context:
            msg += '\n' + self.context

        return msg


class UnknownEdgeDBError(EdgeDBError):
    """An error with an unknown CODE."""


class InterfaceError(Exception):
    """An error caused by improper use of EdgeDB API."""
