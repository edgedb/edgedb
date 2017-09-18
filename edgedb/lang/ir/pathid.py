##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import objects as so


class PathId:
    """Unique identifier of a path in an expression."""

    def __init__(self, initializer=None, *, namespace=None):
        if isinstance(initializer, PathId):
            self._path = initializer._path
            self._norm_path = initializer._norm_path
            if namespace is not None:
                self._namespace = namespace
            else:
                self._namespace = initializer._namespace
        elif initializer is not None:
            if not isinstance(initializer, so.NodeClass):
                raise ValueError(
                    f'invalid PathId: bad source: {initializer!r}')
            self._path = (initializer,)
            self._norm_path = (initializer,)
            self._namespace = namespace
        else:
            self._path = ()
            self._norm_path = ()
            self._namespace = namespace

    def __hash__(self):
        return hash((self.__class__, self._norm_path, self._namespace))

    def __eq__(self, other):
        if not isinstance(other, PathId):
            return NotImplemented

        return (
            self._norm_path == other._norm_path and
            self._namespace == other._namespace
        )

    def __len__(self):
        return len(self._path)

    def __str__(self):
        result = ''

        if not self._path:
            return ''

        if self._namespace:
            result += f'{self._namespace}@@'

        path = self._norm_path

        result += f'({path[0].name})'

        for i in range(1, len(path) - 1, 2):
            ptr = path[i][0]
            ptrdir = path[i][1]
            tgt = path[i + 1]

            if tgt:
                lexpr = f'({ptr} [IS {tgt.name}])'
            else:
                lexpr = f'({ptr})'

            if isinstance(ptr, s_lprops.LinkProperty):
                step = '@'
            else:
                step = f'.{ptrdir}'

            result += f'{step}{lexpr}'

        if len(path) == 2:
            ptr = path[1][0]
            ptrdir = path[1][1]
            result += f'.{ptrdir}({ptr})'

        return result

    def __getitem__(self, n):
        if not isinstance(n, slice):
            return self._path[n]
        else:
            result = self.__class__()
            result._path = self._path[n]
            result._norm_path = self._norm_path[n]
            result._namespace = self._namespace
            return result

    __repr__ = __str__

    def rptr(self):
        if len(self) > 1:
            return self[self._ptr_offset()][0]
        else:
            return None

    def rptr_dir(self):
        if len(self) > 1:
            return self[self._ptr_offset()][1]
        else:
            return None

    def rptr_name(self):
        rptr = self.rptr()
        if rptr is not None:
            return rptr.shortname
        else:
            return None

    def src_path(self):
        if len(self) > 1:
            return self[:self._ptr_offset()]
        else:
            return None

    def iter_prefixes(self):
        yield self[:1]

        for i in range(1, len(self) - 1, 2):
            if self[i + 1]:
                yield self[:i + 2]
            else:
                break

    def starts_any_of(self, scope):
        for path_id in scope:
            if path_id.startswith(self):
                return True
        else:
            return False

    def is_in_scope(self, scope):
        for path_id in scope:
            if self.startswith(path_id):
                return True
        else:
            return False

    def is_concept_path(self):
        return isinstance(self._path[-1], s_concepts.Concept)

    def startswith(self, path_id):
        return self[:len(path_id)] == path_id

    def common_suffix_len(self, other):
        suffix_len = 0

        for i in range(min(len(self), len(other)), 0, -1):
            if self[i - 1] != other[i - 1]:
                break
            else:
                suffix_len += 1

        return suffix_len

    def replace_prefix(self, prefix, replacement):
        if self.startswith(prefix):
            prefix_len = len(prefix)
            if prefix_len < len(self):
                result = self.__class__()
                result._path = replacement._path + self._path[prefix_len:]
                result._norm_path = \
                    replacement._norm_path + self._norm_path[prefix_len:]
                result._namespace = self._namespace
                return result
            else:
                return replacement
        else:
            return self

    def extend(self, link, direction, target):
        if not self:
            raise ValueError('cannot extend empty PathId')

        if link.generic():
            raise ValueError('path id must contain specialized links')

        if self._is_dangling():
            raise ValueError('cannot extend link PathId')

        result = self.__class__()
        result._path = self._path + ((link, direction), target)
        result._norm_path = \
            self._norm_path + ((link.shortname, direction), target)
        result._namespace = self._namespace

        return result

    def _is_dangling(self):
        return not isinstance(self._path[-1], so.NodeClass)

    def _ptr_offset(self):
        return -1 if self._is_dangling() else -2
