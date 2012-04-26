##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import _thread
import threading

from .tree import *
from semantix.utils.functional import decorate
from semantix.utils import slots


__all__ = 'inline',


class _BaseHeadPointer(metaclass=slots.SlotsMeta):
    __slots__ = ()


class _HeadPointer(_BaseHeadPointer):
    __slots__ = ('head',)

    def __init__(self):
        super().__init__()
        self.head = None

    def set(self, head):
        self.head = head

    def get(self):
        return self.head


class _HeadManager:
    def __init__(self):
        self.local = threading.local()

    @property
    def head_pointers(self):
        try:
            return self.local._head_pointers
        except AttributeError:
            self.local._head_pointers = storage = [_HeadPointer()]
            return storage

    def add_head_pointer(self, pointer):
        self.head_pointers.append(pointer)

    def drop_head_pointer(self, pointer):
        self.head_pointers.remove(pointer)

    def set(self, head):
        self.head_pointers[-1].set(head)

    def get(self):
        for storage in reversed(self.head_pointers):
            head = storage.get()

            if head is not None:
                return head


HEAD = _HeadManager()


class ConfigRootLink(metaclass=slots.SlotsMeta):
    __slots__ = '_node', '_parent', '_thread_ident', '_cache'

    def __init__(self, node):
        self._node = node
        self._parent = None
        if __debug__:
            self._thread_ident = _thread.get_ident()

    parent = property(lambda self: self._parent)
    node = property(lambda self: self._node)

    def __enter__(self):
        self._parent = HEAD.get()
        HEAD.set(self)

    def __exit__(self, exc_type, exc_value, exc_tb):
        assert _thread.get_ident() == self._thread_ident
        HEAD.set(self._parent)
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
        return HEAD.get().__exit__(exc_type, exc_value, exc_tb)


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
        current_head = HEAD.get()

        def wrapper(*args, **kwargs):
            HEAD.set(current_head)
            return function(*args, **kwargs)

        decorate(wrapper, function)
        return _old_start_new_thread(wrapper, *args, **kwargs)

    decorate(start_new_thread, _old_start_new_thread)
    _thread.start_new_thread = start_new_thread

    class Thread(_old_thread):
        def start(self):
            self.__config_head__ = HEAD.get()
            return super().start()

        def run(self):
            HEAD.set(self.__config_head__)
            return super().run()

    threading.Thread = Thread


def _unpatch_threading():
    global _patched
    assert _patched
    _patched = False

    _thread.start_new_thread = _old_start_new_thread
    threading.Thread = _old_thread
