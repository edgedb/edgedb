##
# Copyright (c) 2011-2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import threading

from edgedb.lang.common import slots


__all__ = 'HEAD',


class _BaseHeadPointer(metaclass=slots.SlotsMeta):
    __slots__ = ()


class _HeadPointer(_BaseHeadPointer):
    __slots__ = ('heads',)

    def __init__(self):
        super().__init__()
        self.heads = None

    def set(self, ns, head):
        if self.heads is None:
            self.heads = {}
        self.heads[ns] = head

    def get(self, ns):
        if self.heads is None:
            return None
        return self.heads.get(ns)


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

    def set(self, ns, head):
        self.head_pointers[-1].set(ns, head)

    def get(self, ns):
        for storage in reversed(self.head_pointers):
            head = storage.get(ns)

            if head is not None:
                return head


HEAD = _HeadManager()
