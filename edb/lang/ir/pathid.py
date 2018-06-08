#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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


from edb.lang.schema import scalars as s_scalars
from edb.lang.schema import objtypes as s_objtypes
from edb.lang.schema import pointers as s_pointers
from edb.lang.schema import types as s_types


class PathId:
    """Unique identifier of a path in an expression."""

    __slots__ = ('_path', '_norm_path', '_namespace', '_is_ptr')

    def __init__(self, initializer=None, *, namespace=None):
        if isinstance(initializer, PathId):
            self._path = initializer._path
            self._norm_path = initializer._norm_path
            if namespace is not None:
                self._namespace = frozenset(namespace)
            else:
                self._namespace = initializer._namespace
            self._is_ptr = initializer._is_ptr
        elif initializer is not None:
            if not isinstance(initializer, s_types.Type):
                raise ValueError(
                    f'invalid PathId: bad source: {initializer!r}')
            self._path = (initializer,)
            if (initializer.is_view() and
                    initializer.peel_view().name == initializer.name):
                # The initializer is a view that aliases its base type.
                self._norm_path = (initializer.peel_view(),)
            else:
                self._norm_path = (initializer,)
            self._namespace = frozenset(namespace) if namespace else None
            self._is_ptr = False
        else:
            self._path = ()
            self._norm_path = ()
            self._namespace = frozenset(namespace) if namespace else None
            self._is_ptr = False

    def __hash__(self):
        return hash((
            self.__class__, self._norm_path, self._namespace, self._is_ptr))

    def __eq__(self, other):
        if not isinstance(other, PathId):
            return NotImplemented

        return (
            self._norm_path == other._norm_path and
            self._namespace == other._namespace and
            self._is_ptr == other._is_ptr
        )

    def __len__(self):
        return len(self._path)

    def __getitem__(self, n):
        if not isinstance(n, slice):
            return self._path[n]
        else:
            # Validate that slicing results in a
            # valid PathId, it must not produce a path ending
            # with a pointer spec.  Slicing off the start
            # is not allowed either.
            if (n.start != 0 and n.start is not None or
                    (n.step is not None and n.step != 1)):
                raise KeyError(f'invalid PathId slice: {n!r}')

            stop_point = self._path[n.stop - 1:n.stop]
            if stop_point and isinstance(stop_point[0], tuple):
                raise KeyError(f'invalid PathId slice: {n!r}')

            result = self.__class__()
            result._path = self._path[n]
            result._norm_path = self._norm_path[n]
            result._namespace = self._namespace

            if n.stop < len(self) and self._norm_path[n.stop][2]:
                # A link property ref has been chopped off.
                result._is_ptr = True

            return result

    def __str__(self):
        result = ''

        if not self._path:
            return ''

        if self._namespace:
            ns_str = []
            for item in sorted(self._namespace):
                if isinstance(item, WeakNamespace):
                    ns_str.append(f'[{item}]')
                else:
                    ns_str.append(item)

            result += f'{"@".join(ns_str)}@@'

        path = self._norm_path

        result += f'({path[0].name})'

        for i in range(1, len(path) - 1, 2):
            ptr = path[i][0]
            ptrdir = path[i][1]
            is_lprop = path[i][2]
            tgt = path[i + 1]

            if tgt:
                lexpr = f'({ptr})[IS {tgt.name}]'
            else:
                lexpr = f'({ptr})'

            if is_lprop:
                step = '@'
            else:
                step = f'.{ptrdir}'

            result += f'{step}{lexpr}'

        if self._is_ptr:
            result += '@'

        return result

    __repr__ = __str__

    def replace_namespace(self, namespace):
        result = self.__class__()
        result._path = self._path
        result._norm_path = self._norm_path
        result._namespace = frozenset(namespace) if namespace else None
        result._is_ptr = self._is_ptr
        return result

    def merge_namespace(self, namespace):
        if not self._namespace:
            new_namespace = namespace
        else:
            new_namespace = self._namespace | frozenset(namespace)

        return self.replace_namespace(new_namespace)

    def strip_weak_namespaces(self):
        stripped_ns = tuple(bit for bit in self._namespace
                            if not isinstance(bit, WeakNamespace))
        return self.replace_namespace(stripped_ns)

    def strip_namespace(self, namespace):
        if self._namespace and namespace:
            stripped_ns = self._namespace - set(namespace)
            return self.replace_namespace(stripped_ns)
        else:
            return self

    def iter_weak_namespace_prefixes(self):
        yield self

        if not self._namespace:
            return

        weak_nses = [ns for ns in self._namespace
                     if isinstance(ns, WeakNamespace)]

        for weak_ns in weak_nses:
            yield self.replace_namespace(self._namespace - {weak_ns})

    def pformat(self):
        """Pretty PathId format for user-visible messages."""
        result = ''

        if not self._path:
            return ''

        path = self._path

        result += f'{path[0].shortname.name}'

        for i in range(1, len(path) - 1, 2):
            ptr = path[i][0]
            ptrdir = path[i][1]
            is_lprop = ptr.is_link_property()

            lexpr = f'{ptr.shortname.name}'

            if is_lprop:
                step = '@'
            else:
                step = '.'
                if ptrdir == s_pointers.PointerDirection.Inbound:
                    step += ptrdir

            result += f'{step}{lexpr}'

        if self._is_ptr:
            result += '@'

        return result

    def rptr(self):
        if len(self) > 1:
            return self[-2][0]
        else:
            return None

    def rptr_dir(self):
        if len(self) > 1:
            return self[-2][1]
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
            return self[:-2]
        else:
            return None

    def tgt_path(self):
        if not self._is_ptr:
            return self
        else:
            result = self.__class__(self)
            result._is_ptr = False
            return result

    def iter_prefixes(self, include_ptr=False):
        yield self[:1]

        for i in range(1, len(self) - 1, 2):
            path_id = self[:i + 2]
            if path_id.is_ptr_path():
                yield path_id.tgt_path()
                if include_ptr:
                    yield path_id
            else:
                yield path_id

    def startswith(self, path_id):
        return self[:len(path_id)] == path_id

    def replace_prefix(self, prefix, replacement):
        if self.startswith(prefix):
            prefix_len = len(prefix)
            if prefix_len < len(self):
                result = self.__class__()
                result._path = replacement._path + self._path[prefix_len:]
                result._norm_path = \
                    replacement._norm_path + self._norm_path[prefix_len:]
                result._namespace = replacement._namespace
                return result
            else:
                return replacement
        else:
            return self

    def extend(self, link, direction=None, target=None):
        if not self:
            raise ValueError('cannot extend empty PathId')

        if link.generic():
            raise ValueError('path id must contain specialized links')

        if direction is None:
            direction = s_pointers.PointerDirection.Outbound

        if target is None:
            target = link.get_far_endpoint(direction)

        is_linkprop = link.is_link_property()
        if is_linkprop and not self._is_ptr:
            raise ValueError('link property path extension on a non-link path')

        result = self.__class__()
        result._path = self._path + ((link, direction), target)
        lnk = (link.shortname, direction, is_linkprop)
        norm_target = target.material_type()
        result._norm_path = self._norm_path + (lnk, norm_target)
        result._namespace = self._namespace

        return result

    def ptr_path(self):
        if self._is_ptr:
            return self
        else:
            result = self.__class__(self)
            result._is_ptr = True
            return result

    def is_objtype_path(self):
        return (
            not self.is_ptr_path() and
            isinstance(self._path[-1], s_objtypes.ObjectType)
        )

    def is_scalar_path(self):
        return (
            not self.is_ptr_path() and
            isinstance(self._path[-1], s_scalars.ScalarType)
        )

    def is_ptr_path(self):
        return self._is_ptr

    def is_linkprop_path(self):
        rptr = self.rptr()
        return rptr is not None and rptr.is_link_property()

    def is_type_indirection_path(self):
        rptr = self.rptr()
        if rptr is None:
            return False
        else:
            return rptr.shortname in (
                '__type__::indirection',
                '__type__::optindirection',
            )

    @property
    def namespace(self):
        return self._namespace


class WeakNamespace(str):
    pass
