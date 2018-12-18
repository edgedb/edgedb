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


from __future__ import annotations

from . import typeutils

from edb.lang.schema import abc as s_abc
from edb.lang.schema import name as sn
from edb.lang.schema import pointers as s_pointers
from edb.lang.schema import types as s_types


class PathId:
    """Unique identifier of a path in an expression."""

    __slots__ = ('_path', '_norm_path', '_namespace', '_prefix',
                 '_is_ptr', '_is_linkprop')

    def __init__(self, initializer=None, *, namespace=None, typename=None):
        if isinstance(initializer, PathId):
            self._path = initializer._path
            self._norm_path = initializer._norm_path
            if namespace is not None:
                self._namespace = frozenset(namespace)
            else:
                self._namespace = initializer._namespace
            self._is_ptr = initializer._is_ptr
            self._is_linkprop = initializer._is_linkprop
            self._prefix = initializer._prefix
        elif initializer is not None:
            raise TypeError('use PathId.from_type')
        else:
            self._path = ()
            self._norm_path = ()
            self._namespace = frozenset(namespace) if namespace else None
            self._prefix = None
            self._is_ptr = False
            self._is_linkprop = False

    @classmethod
    def from_type(cls, schema, initializer, *, namespace=None, typename=None):
        if not isinstance(initializer, s_abc.Type):
            raise ValueError(
                f'invalid PathId: bad source: {initializer!r}')

        initializer = typeutils.type_to_typeref(schema, initializer)
        return cls.from_typeref(initializer, namespace=namespace,
                                typename=typename)

    @classmethod
    def from_typeref(cls, initializer, *, namespace=None, typename=None):
        pid = cls()
        pid._path = (initializer,)
        if typename is None:
            typename = initializer.name
        pid._norm_path = (typename,)
        pid._namespace = frozenset(namespace) if namespace else None
        return pid

    def __hash__(self):
        return hash((
            self.__class__, self._norm_path,
            self._namespace, self._prefix, self._is_ptr))

    def __eq__(self, other):
        if not isinstance(other, PathId):
            return NotImplemented

        return (
            self._norm_path == other._norm_path and
            self._namespace == other._namespace and
            self._prefix == other._prefix and
            self._is_ptr == other._is_ptr
        )

    def __len__(self):
        return len(self._path)

    def get_prefix(self, size):
        # Validate that slicing results in a
        # valid PathId, it must not produce a path ending
        # with a pointer spec.
        stop_point = self._path[size - 1:size]
        if stop_point and isinstance(stop_point[0], tuple):
            raise KeyError(f'invalid PathId slice: {size!r}')

        return self._get_prefix(size)

    def _get_prefix(self, size):
        if size < 0:
            size = len(self._path) + size

        if size == len(self._path):
            return self

        if self._prefix is not None:
            prefix_len = len(self._prefix)
            if prefix_len == size:
                return self._prefix
            elif prefix_len > size:
                return self._prefix._get_prefix(size)

        result = self.__class__()
        result._path = self._path[0:size]
        result._norm_path = self._norm_path[0:size]
        result._prefix = self._prefix
        result._namespace = self._namespace

        if size < len(self._path) and self._norm_path[size][2]:
            # A link property ref has been chopped off.
            result._is_ptr = True

        return result

    def __str__(self):
        return self.pformat_internal(debug=False)

    __repr__ = __str__

    def replace_namespace(self, namespace):
        result = self.__class__(self)
        result._namespace = frozenset(namespace) if namespace else None
        return result

    def merge_namespace(self, namespace):
        if not self._namespace:
            new_namespace = namespace
        else:
            new_namespace = self._namespace | frozenset(namespace)

        if new_namespace != self._namespace:
            return self.replace_namespace(new_namespace)
        else:
            return self

    def _get_minimal_prefix(self, prefix):
        while prefix is not None:
            if prefix._namespace == self._namespace:
                prefix = prefix._prefix
            else:
                break

        return prefix

    def strip_weak_namespaces(self):
        if self._namespace is not None:
            stripped_ns = tuple(bit for bit in self._namespace
                                if not isinstance(bit, WeakNamespace))
            result = self.replace_namespace(stripped_ns)

            if result._prefix is not None:
                result._prefix = result._get_minimal_prefix(
                    result._prefix.strip_weak_namespaces())

        else:
            result = self

        return result

    def strip_namespace(self, namespace):
        if self._namespace and namespace:
            stripped_ns = self._namespace - set(namespace)
            result = self.replace_namespace(stripped_ns)

            if result._prefix is not None:
                result._prefix = result._get_minimal_prefix(
                    result._prefix.strip_namespace(namespace))

            return result
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

    def pformat_internal(self, debug=False):
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

        result += f'({path[0]})'

        for i in range(1, len(path) - 1, 2):
            if debug:
                ptr = path[i][0]
            else:
                ptr = sn.shortname_from_fullname(path[i][0])
            ptrdir = path[i][1]
            is_lprop = path[i][2]
            tgt = path[i + 1]

            if tgt:
                lexpr = f'({ptr})[IS {tgt}]'
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

    def pformat(self):
        """Pretty PathId format for user-visible messages."""
        result = ''

        if not self._path:
            return ''

        path = self._norm_path

        result += f'{path[0].name}'

        for i in range(1, len(path) - 1, 2):
            ptr_name = sn.shortname_from_fullname(path[i][0])
            ptrdir = path[i][1]
            is_lprop = path[i][2]

            if is_lprop:
                step = '@'
            else:
                step = '.'
                if ptrdir == s_pointers.PointerDirection.Inbound:
                    step += ptrdir

            result += f'{step}{ptr_name.name}'

        if self._is_ptr:
            result += '@'

        return result

    def rptr(self):
        if len(self._path) > 1:
            return self._path[-2][0]
        else:
            return None

    def rptr_dir(self):
        if len(self._path) > 1:
            return self._path[-2][1]
        else:
            return None

    def rptr_name(self):
        rptr = self.rptr()
        if rptr is not None:
            return rptr.shortname
        else:
            return None

    def src_path(self):
        if len(self._path) > 1:
            return self._get_prefix(-2)
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
        if self._prefix is not None:
            yield from self._prefix.iter_prefixes(include_ptr=include_ptr)
            start = len(self._prefix)
        else:
            yield self._get_prefix(1)
            start = 1

        for i in range(start, len(self._path) - 1, 2):
            path_id = self._get_prefix(i + 2)
            if path_id.is_ptr_path():
                yield path_id.tgt_path()
                if include_ptr:
                    yield path_id
            else:
                yield path_id

    def startswith(self, path_id):
        return self._get_prefix(len(path_id)) == path_id

    def replace_prefix(self, prefix, replacement):
        if self.startswith(prefix):
            prefix_len = len(prefix)
            if prefix_len < len(self):
                result = self.__class__()
                result._path = replacement._path + self._path[prefix_len:]
                result._norm_path = \
                    replacement._norm_path + self._norm_path[prefix_len:]
                result._namespace = replacement._namespace

                if self._prefix is not None and len(self._prefix) > prefix_len:
                    result._prefix = self._prefix.replace_prefix(
                        prefix, replacement)
                else:
                    result._prefix = replacement._prefix

                return result
            else:
                return replacement
        else:
            return self

    def extend(self, *, ptrcls, direction=None, target=None, ns=None, schema):
        if not self:
            raise ValueError('cannot extend empty PathId')

        if ptrcls.generic(schema):
            raise ValueError('path id must contain specialized pointers')

        if direction is None:
            direction = s_pointers.PointerDirection.Outbound

        if target is None:
            target = ptrcls.get_far_endpoint(schema, direction)

        if isinstance(target, s_types.Type):
            target_ref = typeutils.type_to_typeref(schema, target)
        else:
            target_ref = target

        if target_ref.material_type is not None:
            material_name = target_ref.material_type.name
        else:
            material_name = target_ref.name

        is_linkprop = ptrcls.is_link_property(schema)
        if is_linkprop:
            if not self._is_ptr:
                raise ValueError(
                    'link property path extension on a non-link path')

            ptr_ref = typeutils.ptrref_from_ptrcls(
                source_ref=None, target_ref=target_ref,
                parent_ptr=self.rptr(), ptrcls=ptrcls,
                direction=direction, schema=schema,
            )
        else:
            ptr_ref = typeutils.ptrref_from_ptrcls(
                source_ref=self.target, target_ref=target_ref,
                ptrcls=ptrcls, direction=direction, schema=schema,
            )

        result = self.__class__()
        result._path = self._path + ((ptr_ref, direction), target_ref)
        link_name = ptrcls.get_path_id_name(schema) or ptrcls.get_name(schema)
        lnk = (link_name, direction, is_linkprop)
        result._norm_path = (
            self._norm_path + (lnk, material_name)
        )
        result._is_linkprop = is_linkprop

        if ns:
            if self._namespace:
                result._namespace = self._namespace | frozenset(ns)
            else:
                result._namespace = frozenset(ns)
        else:
            result._namespace = self._namespace

        if self._namespace != result._namespace:
            result._prefix = self
        else:
            result._prefix = self._prefix

        return result

    def ptr_path(self):
        if self._is_ptr:
            return self
        else:
            result = self.__class__(self)
            result._is_ptr = True
            return result

    @property
    def target(self):
        return self._path[-1]

    @property
    def target_name(self):
        return self._norm_path[-1]

    def is_objtype_path(self):
        return not self.is_ptr_path() and typeutils.is_object(self.target)

    def is_scalar_path(self):
        return not self.is_ptr_path() and typeutils.is_scalar(self.target)

    def is_view_path(self):
        return not self.is_ptr_path() and typeutils.is_view(self.target)

    def is_tuple_path(self):
        return not self.is_ptr_path() and typeutils.is_tuple(self.target)

    def is_array_path(self):
        return not self.is_ptr_path() and typeutils.is_array(self.target)

    def is_collection_path(self):
        return not self.is_ptr_path() and typeutils.is_collection(self.target)

    def is_ptr_path(self):
        return self._is_ptr

    def is_linkprop_path(self):
        return self._is_linkprop

    def is_type_indirection_path(self):
        rptr_name = self.rptr_name()
        if rptr_name is None:
            return False
        else:
            return rptr_name in (
                '__type__::indirection',
                '__type__::optindirection',
            )

    @property
    def namespace(self):
        return self._namespace


class WeakNamespace(str):
    pass
