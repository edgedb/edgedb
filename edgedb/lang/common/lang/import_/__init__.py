##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import imp
import importlib
import os
import sys

from . import module as module_types
from .context import ImportContext

from .finder import install, update_finders


def reload(module):
    if isinstance(module, module_types.BaseProxyModule):
        sys.modules[module.__name__] = module.__wrapped__

        # XXX: imp.reload has a hardcoded check that fails on instances of module subclasses
        try:
            new_mod = module.__wrapped__.__loader__.load_module(module.__name__)

            if isinstance(new_mod, module_types.BaseProxyModule):
                module.__wrapped__ = new_mod.__wrapped__
            else:
                module.__wrapped__ = new_mod

        finally:
            sys.modules[module.__name__] = module

        return module

    else:
        return imp.reload(module)


def modules_from_import_statements(package, imports):
    modules = []

    for name, fromlist in imports:
        level = 0

        while level < len(name) and name[level] == '.':
            level += 1

        if level > 0:
            steps = package.rsplit('.', level - 1)
            if len(steps) < level:
                raise ValueError('relative import reaches beyond top-level package')

            suffix = name[level:]

            if suffix:
                fq_name = '{}.{}'.format(steps[0], name[level:])
            else:
                fq_name = steps[0]
        else:
            fq_name = name

        path = None
        steps = fq_name.split('.')

        add_package = True

        for i in range(len(steps)):
            modname = '.'.join(steps[:i + 1])

            loader = importlib.find_loader(modname, path=path)

            if loader is None:
                raise ValueError('could not find loader for module {}'.format(modname))

            if not loader.is_package(modname):
                break

            modfile = loader.get_filename(modname)
            # os.path.dirname(__file__) is a common importlib assumption for __path__
            path = [os.path.dirname(modfile)]
        else:
            if fromlist:
                add_package = False

                for entry in fromlist:
                    modname = '{}.{}'.format(fq_name, entry)
                    entry_loader = importlib.find_loader(modname, path=path)

                    if entry_loader is not None and entry_loader.path != loader.path:
                        modules.append(modname)
                    else:
                        add_package = True

        if add_package:
            modules.append(fq_name)

    return modules


class ObjectImportError(Exception):
    pass


def get_object(cls):
    modname, _, name = cls.rpartition('.')

    try:
        mod = importlib.import_module(modname)
    except ImportError as e:
        raise ObjectImportError('could not load object %s' % cls) from e
    else:
        try:
            result = getattr(mod, name)
        except AttributeError as e:
            raise ObjectImportError('could not load object %s' % cls) from e

        return result
