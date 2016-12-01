##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import abc

from . import objects as so
from . import pointers


_rewrite_hooks = {}


def register_rewrite_hook(cls, action, callback, type):
    if isinstance(cls, tuple):
        # (source, link) tuple
        scls = cls[0].__sx_class__
        ptr_class = cls[1].__sx_class__
        ptr_name = ptr_class.shortname
        source_class = scls
    else:
        if hasattr(cls, '__sx_class__'):
            scls = cls.__sx_class__

            if isinstance(scls, pointers.Pointer):
                scls = cls.__sx_class__
                ptr_name = scls.shortname
                source_class = scls.source
            else:
                source_class = scls
                ptr_name = None

        elif isinstance(cls, abc.ABCMeta):
            # Abstract base class
            source_class = [c.__sx_class__ for c in cls._abc_registry]
            ptr_name = None

        else:
            raise TypeError('unsupported object type: {!r}'.format(cls))

    if not isinstance(source_class, list):
        source_class = [source_class]

    for scls in source_class:
        classname = scls.name

        key = (classname, ptr_name) if ptr_name else classname

        try:
            cls_hooks = _rewrite_hooks[key, action, type]
        except KeyError:
            cls_hooks = _rewrite_hooks[key, action, type] = []

        cls_hooks.append(callback)


def register_access_control_hook(cls, action, callback):
    return register_rewrite_hook(cls, action, callback, type='filter')


def get_access_control_hooks(scls, action):
    if isinstance(scls, pointers.Pointer):
        source = scls.source
        pn = scls.shortname
    else:
        source = scls
        pn = None

    mro = source.get_mro()

    mro = [cls for cls in mro if isinstance(cls, so.Class)]

    hooks = []

    for scls in mro:
        if pn:
            key = (scls.name, pn)
        else:
            key = scls.name

        try:
            clshooks = _rewrite_hooks[key, action, 'filter']
        except KeyError:
            pass
        else:
            hooks.extend(clshooks)

    return hooks
