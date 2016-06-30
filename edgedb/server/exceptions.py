##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


__all__ = ()  # Will be completed by ErrorMeta


class ErrorMeta(type):
    _error_map = {}

    def __new__(mcls, name, bases, dct):
        global __all__

        cls = super().__new__(mcls, name, bases, dct)
        if cls.__module__ == 'edgedb.server.exceptions':
            __all__ += (name,)

        code = dct.get('code')
        if code is not None:
            mcls._error_map[code] = cls

        return cls

    @classmethod
    def get_error_for_code(mcls, code):
        return mcls._error_map.get(code, Error)


class Error(Exception, metaclass=ErrorMeta):
    def __init__(self, msg):
        super().__init__(msg)
        self._attrs = {}

    @property
    def attrs(self):
        return self._attrs


class FatalError(Error):
    pass


class IntergrityConstraintViolationError(Error):
    code = '23000'


class MissingRequiredPointerError(IntergrityConstraintViolationError):
    code = '23502'

    def __init__(self, msg, *, source_name=None, pointer_name=None):
        super().__init__(msg)
        self._attrs['source'] = source_name
        self._attrs['pointer'] = pointer_name


class UniqueConstraintViolationError(IntergrityConstraintViolationError):
    code = '23505'


class ConstraintViolationError(IntergrityConstraintViolationError):
    code = '23514'
