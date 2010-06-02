##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import sys
import importlib
import types

from semantix import SemantixError


def dump(stuff):
    if (not (isinstance(stuff, str) or isinstance(stuff, int)
             or isinstance(stuff, list) or isinstance(stuff, dict)
             or isinstance(stuff, tuple) or isinstance(stuff, float)
             or isinstance(stuff, complex))):

        buf = ['%r : %s' % (stuff, str(stuff))]

        for name in dir(stuff):
            attr = getattr(stuff, name)

            if not hasattr(attr, '__call__'):
                buf.append('  -> %s : %s' % (name, attr))

        print('\n'.join(buf) + '\n')

    else:
        import pprint
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(stuff)


def cleandir(path):
    import os

    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))

        for name in dirs:
            os.rmdir(os.path.join(root, name))


def dump_header(title):
    return '\n' + '=' * 80 + '\n' + title + '\n' + '=' * 80 + '\n'


def dump_relative_filename(filename):
    try:
        _main_path = os.path.dirname(sys.modules['__main__'].__file__)

    except (KeyError, AttributeError):
        _main_path = False

    if _main_path:
        new = os.path.relpath(filename, _main_path)
        if len(new) < len(filename):
            return new

    return filename


def get_function_class(func):
    """
    Tries to find in what class the func was defined.  Returns None in case of no luck.
    Digs through classmethods, staticmethods, semantix.functional.decorators.
    """

    from semantix.utils.functional import tools

    visited = {}

    def finder(obj, level=0):
        iter = None

        if (isinstance(obj, types.ModuleType) and level == 0) or isinstance(obj, type):
            iter = obj.__dict__.values()

        if isinstance(obj, dict):
            iter = obj.values()

        if iter:
            for item in iter:
                if item is func:
                    return obj

                if isinstance(item, staticmethod) or isinstance(item, classmethod):
                    if item.__func__ is func:
                        return obj

                if isinstance(item, types.FunctionType) and tools.isdecorated(item):
                    current = item
                    while current and tools.isdecorated(current):
                        current = current._func_
                    if current is func:
                        return obj

                try:
                    if item in visited:
                        continue
                except TypeError:
                    continue

                visited[item] = True
                result = finder(item, level+1)
                if result:
                    return result

    if isinstance(func, types.MethodType):
        if isinstance(func.__self__, type):
            # Class method?
            #
            return func.__self__

        else:
            return func.__self__.__class__

    result = finder(func.__globals__)
    if result is not None:
        return result

    try:
        module = get_object(func.__module__)
    except (ImportError, AttributeError):
        pass
    else:
        return finder(module)


def shorten_repr(rpr:str, max_len=120) -> str:
    if len(rpr) >= max_len:
        rpr = str(rpr[:(max_len-3)]) + '...'
    return rpr


def dump_code_context(filename, lineno, dump_range=4):
    with open(filename, 'r') as file:
        source = file.read().split('\n')

    source_snippet = ''
    for j in range(max(1, lineno-dump_range), min(len(source), lineno+dump_range+1)):
        line = source[j - 1] + '\n'

        if j == lineno:
            line = ' > ' + line
        else:
            line = ' | ' + line

        line = '{0:6}'.format(j) + line
        source_snippet += line

    return source_snippet


class ObjectImportError(SemantixError):
    pass


def get_object(cls):
    try:
        mod, _, name = cls.rpartition('.')
        return getattr(importlib.import_module(mod), name)
    except (ImportError, AttributeError) as e:
        raise ObjectImportError('could not load object %s' % cls) from e
