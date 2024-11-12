#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2010-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations

import sys


def _get_contexts(ex, *, auto_init=False):
    try:
        return ex.__sx_error_contexts__
    except AttributeError:
        if auto_init:
            cs = ex.__sx_error_contexts__ = {}
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


class ExceptionContext:
    title = 'Exception Context'


class DefaultExceptionContext(ExceptionContext):
    title = 'Details'

    def __init__(self, hint=None, details=None):
        super().__init__()

        self.details = details
        self.hint = hint


_old_excepthook = sys.excepthook


def _is_internal_error(exc):
    if isinstance(exc, ExceptionGroup):
        return any(_is_internal_error(e) for e in exc.exceptions)
    # This is pretty cheesy but avoids needing to import our edgedb
    # exceptions or do anything elaborate with contexts.
    return type(exc).__name__ == 'InternalServerError'


def excepthook(exctype, exc, tb):
    try:
        from edb.common import markup
        markup.dump(exc, file=sys.stderr)

        if _is_internal_error(exc):
            # TODO(rename): change URL once we can
            print(
                f'This is most likely a bug in Gel. '
                f'Please consider opening an issue ticket '
                f'at https://github.com/edgedb/edgedb/issues/new'
                f'?template=bug_report.md'
            )

    except Exception as ex:
        print('!!! exception in edb.excepthook !!!', file=sys.stderr)

        # Attach the original exception as a context to top of the new chain,
        # but only if it's not already there.  Take some care to avoid looping
        # forever.
        visited = set()
        parent = ex
        while parent.__cause__ or (
                not parent.__suppress_context__ and parent.__context__):
            if (parent in visited or parent.__context__ is exc or
                    parent.__cause__ is exc):
                break
            visited.add(parent)
            parent = parent.__cause__ or parent.__context__
        parent.__context__ = exc
        parent.__cause__ = None

        _old_excepthook(type(ex), ex, ex.__traceback__)


def install_excepthook():
    sys.excepthook = excepthook


def uninstall_excepthook():
    sys.excepthook = _old_excepthook
