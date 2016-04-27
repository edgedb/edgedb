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
        proto = cls[0].__sx_prototype__
        ptr_proto = cls[1].__sx_prototype__
        ptr_name = ptr_proto.normal_name()
        source_proto = proto
    else:
        if hasattr(cls, '__sx_prototype__'):
            proto = cls.__sx_prototype__

            if isinstance(proto, pointers.Pointer):
                proto = cls.__sx_prototype__
                ptr_name = proto.normal_name()
                source_proto = proto.source
            else:
                source_proto = proto
                ptr_name = None

        elif isinstance(cls, abc.ABCMeta):
            # Abstract base class
            source_proto = [c.__sx_prototype__ for c in cls._abc_registry]
            ptr_name = None

        else:
            raise TypeError('unsupported object type: {!r}'.format(cls))

    if not isinstance(source_proto, list):
        source_proto = [source_proto]

    for proto in source_proto:
        proto_name = proto.name

        key = (proto_name, ptr_name) if ptr_name else proto_name

        try:
            cls_hooks = _rewrite_hooks[key, action, type]
        except KeyError:
            cls_hooks = _rewrite_hooks[key, action, type] = []

        cls_hooks.append(callback)


def register_access_control_hook(cls, action, callback):
    return register_rewrite_hook(cls, action, callback, type='filter')


def get_access_control_hooks(proto, action):
    if isinstance(proto, pointers.Pointer):
        source = proto.source
        pn = proto.normal_name()
    else:
        source = proto
        pn = None

    mro = source.get_mro()

    mro = [cls for cls in mro if isinstance(cls, so.ProtoObject)]

    hooks = []

    for proto in mro:
        if pn:
            key = (proto.name, pn)
        else:
            key = proto.name

        try:
            clshooks = _rewrite_hooks[key, action, 'filter']
        except KeyError:
            pass
        else:
            hooks.extend(clshooks)

    return hooks
