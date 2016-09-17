##
# Copyright (c) 2010-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import collections


def _get_contexts(ex, *, auto_init=False):
    try:
        return ex.__sx_error_contexts__
    except AttributeError:
        if auto_init:
            cs = ex.__sx_error_contexts__ = collections.OrderedDict()
            return cs
        else:
            return {}


def add_context(ex, context):
    assert isinstance(context, ExceptionContext)

    contexts = _get_contexts(ex, auto_init=True)

    cls = context.__class__
    if cls in contexts:
        raise ValueError(
            'context {}.{} is already present in '
            'exception'.format(cls.__module__, cls.__name__))

    contexts[cls] = context


def replace_context(ex, context):
    contexts = _get_contexts(ex, auto_init=True)
    contexts[context.__class__] = context


def get_context(ex, context_class):
    contexts = _get_contexts(ex)
    try:
        return contexts[context_class]
    except KeyError as ex:
        raise LookupError(
            '{} context class is not '
            'found'.format(context_class)) from ex


def iter_contexts(ex, ctx_class=None):
    contexts = _get_contexts(ex)
    if ctx_class is None:
        return iter(contexts.values())
    else:
        assert issubclass(ctx_class, ExceptionContext)
        return (
            context for context in contexts.values()
            if isinstance(context, ctx_class))


class EdgeDBErrorMeta(type):
    _error_map = {}

    def __new__(mcls, name, bases, dct):
        global __all__

        cls = super().__new__(mcls, name, bases, dct)
        if cls.__module__ == 'edgedb.server.exceptions':
            __all__ += (name, )

        code = dct.get('code')
        if code is not None:
            mcls._error_map[code] = cls

        return cls

    @classmethod
    def get_error_for_code(mcls, code):
        return mcls._error_map.get(code, EdgeDBError)


class EdgeDBError(Exception, metaclass=EdgeDBErrorMeta):
    def __init__(self, msg=None, *, hint=None, details=None, **kwargs):
        super().__init__(msg)
        self._attrs = {}
        if hint is not None:
            self._attrs['H'] = hint
        if details is not None:
            self._attrs['D'] = details
        for k, v in kwargs.items():
            if isinstance(v, ExceptionContext):
                add_context(self, v)

    @property
    def attrs(self):
        return self._attrs

    def as_text(self):
        buffer = ''

        for context in iter_contexts(self):
            buffer += context.as_text()

        return buffer


class MultiError(EdgeDBError):
    def __init__(self, msg=None, *, errors):
        assert errors
        assert all(isinstance(error, BaseException) for error in errors)
        self.errors = errors

        if msg is None:
            msg = '\n'.join(
                '{}: {}'.format(type(ex).__name__, ex) for ex in self.errors)

        super().__init__(msg)


class ExceptionContext:
    title = 'Exception Context'


class DefaultExceptionContext(ExceptionContext):
    title = 'Details'

    def __init__(self, hint=None, details=None):
        super().__init__()

        self.details = details
        self.hint = hint


class EdgeDBExceptionContext(ExceptionContext):
    pass
