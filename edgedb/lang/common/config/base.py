##
# Copyright (c) 2011-2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import _thread
import threading

from .tree import *
from metamagic.utils.functional import decorate
from metamagic.utils import slots
from metamagic.utils.localcontext import HEAD as _HEAD


__all__ = 'inline',


def _set_head(val):
    _HEAD.set('__mm_config_head__', val)

def _get_head():
    return _HEAD.get('__mm_config_head__')


class ConfigRootLink(metaclass=slots.SlotsMeta):
    __slots__ = '_node', '_parent', '_cache'

    def __init__(self, node):
        self._node = node
        self._parent = None

    parent = property(lambda self: self._parent)
    node = property(lambda self: self._node)

    def __enter__(self):
        self._parent = _get_head()
        _set_head(self)

    def __exit__(self, exc_type, exc_value, exc_tb):
        _set_head(self._parent)
        self._parent = None

    def cache_get(self, key):
        try:
            return self._cache[key]
        except (KeyError, AttributeError):
            raise LookupError()

    def cache_set(self, key, value):
        try:
            self._cache[key] = value
        except AttributeError:
            self._cache = {key: value}


class ConfigValue(TreeValue):
    __slots__ = ()


class ConfigNode(TreeNode):
    __slots__ = ()


class ConfigRootNode(ConfigNode, TreeRootNode):
    __slots__ = ('__node_head_links__', '__node_ctx__')

    node_cls = ConfigNode
    value_cls = ConfigValue
    link_cls = ConfigRootLink

    def __init__(self, name):
        super().__init__(name)
        self.__node_head_links__ = {}
        self.__node_ctx__ = {}

    def __enter__(self):
        self.__class__.link_cls(self).__enter__()

    def __exit__(self, exc_type, exc_value, exc_tb):
        head = _get_head()
        if head is not None:
            # XXX This case is rather strange, but nevertheless, it
            # happens sometimes. To investigate this in the future.
            return head.__exit__(exc_type, exc_value, exc_tb)


def inline(values:dict):
    conf = ConfigRootNode('inline')
    for key, value in values.items():
        ConfigRootNode._set_value(conf, key, value, 'inline')
    return conf


_old_start_new_thread = _thread.start_new_thread
_old_thread = threading.Thread
_patched = False


def _patch_threading():
    global _patched
    assert not _patched
    _patched = True

    def start_new_thread(function, *args, **kwargs):
        current_head = _get_head()

        def wrapper(*args, **kwargs):
            _set_head(current_head)
            return function(*args, **kwargs)

        decorate(wrapper, function)
        return _old_start_new_thread(wrapper, *args, **kwargs)

    decorate(start_new_thread, _old_start_new_thread)
    _thread.start_new_thread = start_new_thread

    class Thread(_old_thread):
        def start(self):
            self.__config_head__ = _get_head()
            return super().start()

        def run(self):
            _set_head(self.__config_head__)
            return super().run()

    threading.Thread = Thread


def _unpatch_threading():
    global _patched
    assert _patched
    _patched = False

    _thread.start_new_thread = _old_start_new_thread
    threading.Thread = _old_thread
