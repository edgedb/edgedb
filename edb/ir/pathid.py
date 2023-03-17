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
from typing import *

from . import typeutils

from edb.schema import name as s_name
from edb.schema import pointers as s_pointers
from edb.schema import types as s_types

from edb.ir import ast as irast

if TYPE_CHECKING:
    import uuid

    from edb.schema import schema as s_schema
    from edb.edgeql.compiler import context as qlcompiler_ctx


Namespace = str


class PathId:
    """A descriptor of a *variable* in an expression.

    ``PathId`` instances are used to identify and describe expressions
    in EdgeQL.  They are immutable, hashable and comparable.  Instances
    of ``PathId`` describing the same expression variable are equal.
    Another important aspect (and the reason for the class name) is that
    ``PathId`` instances describe *paths* in a structured way that allows
    walking the path to its root.

    ``PathId`` instances are normally directly created for a path root,
    and then PathIds representing the steps of a path are derived by
    calling ``extend()`` on the previous step.

    For example, for the expression ``Movie.reviews.author``
    the following would return a corresponding ``PathId`` (in pseudo-code):

        path_id = PathId.from_type(Movie).extend('reviews').extend('author')
    """

    __slots__ = ('_path', '_norm_path', '_namespace', '_prefix',
                 '_is_ptr', '_is_linkprop', '_hash')

    #: Actual path information.
    _path: Tuple[
        Union[
            irast.TypeRef,
            Tuple[irast.BasePointerRef, s_pointers.PointerDirection]
        ],
        ...
    ]

    #: Normalized path data, used for PathId hashing and comparisons.
    _norm_path: Tuple[
        Union[
            uuid.UUID,
            s_name.Name,
            Tuple[
                s_name.QualName, s_pointers.PointerDirection, bool
            ],
        ],
        ...
    ]

    #: A set of namespace identifiers which this PathId belongs to.
    _namespace: FrozenSet[str]

    #: If this PathId has a prefix from another namespace, this will
    #: contain said prefix.
    _prefix: Optional[PathId]

    #: True if this PathId represents the link portion of a link property path.
    _is_ptr: bool

    #: True if this PathId represents a link property path.
    _is_linkprop: bool

    def __init__(
        self,
        initializer: Optional[PathId] = None,
        *,
        namespace: AbstractSet[str] = frozenset(),
        typename: Optional[str] = None,
    ) -> None:
        if isinstance(initializer, PathId):
            self._path = initializer._path
            self._norm_path = initializer._norm_path
            if namespace:
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
            self._namespace = frozenset(namespace)
            self._prefix = None
            self._is_ptr = False
            self._is_linkprop = False

        self._hash = -1

    def __getstate__(self) -> Any:
        # We need to omit the cached _hash when we pickle because it won't
        # be correct in a different process.
        return tuple([
            getattr(self, k) if k != '_hash' else -1
            for k in PathId.__slots__
        ])

    def __setstate__(self, state: Any) -> None:
        for k, v in zip(PathId.__slots__, state):
            setattr(self, k, v)

    @classmethod
    def from_type(
        cls,
        schema: s_schema.Schema,
        t: s_types.Type,
        *,
        env: Optional[qlcompiler_ctx.Environment] = None,
        namespace: AbstractSet[Namespace] = frozenset(),
        typename: Optional[s_name.QualName] = None,
    ) -> PathId:
        """Return a ``PathId`` instance for a given :class:`schema.types.Type`

        The returned ``PathId`` instance describes a set variable of type *t*.
        The name of the passed type is used as the name for the variable,
        unless *typename* is specified, in which case it is used instead.

        Args:
            schema:
                A schema instance where the type *t* is defined.
            t:
                The type of the variable being defined.
            env:
                Optional EdgeQL compiler environment, used for caching.
            namespace:
                Optional namespace in which the variable is defined.
            typename:
                If specified, used as the name for the variable instead
                of the name of the type *t*.

        Returns:
            A ``PathId`` instance of type *t*.
        """
        if not isinstance(t, s_types.Type):
            raise ValueError(
                f'invalid PathId: bad source: {t!r}')

        cache = env.type_ref_cache if env is not None else None
        typeref = typeutils.type_to_typeref(
            schema, t, cache=cache, typename=typename
        )
        return cls.from_typeref(typeref, namespace=namespace,
                                typename=typename)

    @classmethod
    def from_pointer(
        cls,
        schema: s_schema.Schema,
        pointer: s_pointers.Pointer,
        *,
        namespace: AbstractSet[Namespace] = frozenset(),
    ) -> PathId:
        """Return a ``PathId`` instance for a given link or property.

        The specified *pointer* argument must be a concrete link or property.
        The returned ``PathId`` instance describes a set variable of all
        objects represented by the pointer (i.e, for a link, a set of all
        link targets).

        Args:
            schema:
                A schema instance where the type *t* is defined.
            pointer:
                An instance of a concrete link or property.
            namespace:
                Optional namespace in which the variable is defined.

        Returns:
            A ``PathId`` instance.
        """
        if pointer.generic(schema):
            raise ValueError(f'invalid PathId: {pointer} is not concrete')

        source = pointer.get_source(schema)
        if isinstance(source, s_pointers.Pointer):
            prefix = cls.from_pointer(schema, source, namespace=namespace)
            prefix = prefix.ptr_path()
        elif isinstance(source, s_types.Type):
            prefix = cls.from_type(schema, source, namespace=namespace)
        else:
            raise AssertionError(f'unexpected pointer source: {source!r}')

        ptrref = typeutils.ptrref_from_ptrcls(schema=schema, ptrcls=pointer)
        return prefix.extend(ptrref=ptrref)

    @classmethod
    def from_typeref(
        cls,
        typeref: irast.TypeRef,
        *,
        namespace: AbstractSet[Namespace] = frozenset(),
        typename: Optional[Union[s_name.Name, uuid.UUID]] = None,
    ) -> PathId:
        """Return a ``PathId`` instance for a given :class:`ir.ast.TypeRef`

        The returned ``PathId`` instance describes a set variable of type
        described by *typeref*.  The name of the passed type is used as
        the name for the variable, unless *typename* is specified, in
        which case it is used instead.

        Args:
            schema:
                A schema instance where the type *t* is defined.
            typeref:
                The descriptor of a type of the variable being defined.
            namespace:
                Optional namespace in which the variable is defined.
            typename:
                If specified, used as the name for the variable instead
                of the name of the type *t*.

        Returns:
            A ``PathId`` instance of type described by *typeref*.
        """
        pid = cls()
        pid._path = (typeref,)
        if typename is None:
            typename = typeref.id
        pid._norm_path = (typename,)
        pid._namespace = frozenset(namespace)
        return pid

    def __hash__(self) -> int:
        if self._hash == -1:
            self._hash = hash((
                self.__class__,
                self._norm_path,
                self._namespace,
                self._prefix,
                self._is_ptr,
            ))
        return self._hash

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, PathId):
            return NotImplemented

        return (
            self._norm_path == other._norm_path and
            self._namespace == other._namespace and
            self._prefix == other._prefix and
            self._is_ptr == other._is_ptr
        )

    def __len__(self) -> int:
        return len(self._path)

    def __str__(self) -> str:
        return self.pformat_internal(debug=False)

    __repr__ = __str__

    def extend(
        self,
        *,
        ptrref: irast.BasePointerRef,
        direction: s_pointers.PointerDirection = (
            s_pointers.PointerDirection.Outbound),
        ns: AbstractSet[Namespace] = frozenset(),
    ) -> PathId:
        """Return a new ``PathId`` that is a *path step* from this ``PathId``.

        For example, if you have a ``PathId`` that describes a variable ``A``,
        and you want to obtain a ``PathId`` for ``A.b``, you should call
        ``path_id_for_A.extend(ptrcls=pointer_object_b, schema=schema)``.

        Args:
            ptrref:
                A ``ir.ast.BasePointerRef`` instance that corresponds
                to the path step.  This may be a regular link or property
                object, or a pseudo-pointer, like a tuple or type intersection
                step.
            direction:
                The direction of the *ptrcls* pointer.  This makes sense
                only for reverse link traversal, all other path steps are
                always forward.
            namespace:
                Optional namespace in which the path extension is defined.
                If not specified, the namespace of the current PathId is
                used.
            schema:
                A schema instance.

        Returns:
            A new ``PathId`` instance representing a step extension of
            this ``PathId``.
        """
        if not self:
            raise ValueError('cannot extend empty PathId')

        if direction is s_pointers.PointerDirection.Outbound:
            target_ref = ptrref.out_target
        else:
            target_ref = ptrref.out_source

        is_linkprop = ptrref.source_ptr is not None
        if is_linkprop and not self._is_ptr:
            raise ValueError(
                'link property path extension on a non-link path')

        result = self.__class__()
        result._path = self._path + ((ptrref, direction), target_ref)
        link_name = ptrref.name
        lnk = (link_name, direction, is_linkprop)
        result._is_linkprop = is_linkprop

        if target_ref.material_type is not None:
            material_type = target_ref.material_type
        else:
            material_type = target_ref

        result._norm_path = (self._norm_path + (lnk, material_type.id))

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

    def replace_namespace(
        self,
        namespace: AbstractSet[Namespace],
    ) -> PathId:
        """Return a copy of this ``PathId`` with namespace set to *namespace*.
        """
        result = self.__class__(self)
        result._namespace = frozenset(namespace)

        if result._prefix is not None:
            result._prefix = result._get_minimal_prefix(
                result._prefix.replace_namespace(namespace))

        return result

    def merge_namespace(
        self,
        namespace: AbstractSet[Namespace],
        *,
        deep: bool=False,
    ) -> PathId:
        """Return a copy of this ``PathId`` that has *namespace* added to its
           namespace.
        """
        new_namespace = self._namespace | frozenset(namespace)

        if new_namespace != self._namespace or deep:
            result = self.__class__(self)
            result._namespace = new_namespace
            if deep and result._prefix is not None:
                result._prefix = result._prefix.merge_namespace(new_namespace)
            if result._prefix is not None:
                result._prefix = result._get_minimal_prefix(result._prefix)

            return result

        else:
            return self

    def strip_namespace(self, namespace: AbstractSet[Namespace]) -> PathId:
        """Return a copy of this ``PathId`` with a given portion of the
           namespace id removed."""
        if self._namespace and namespace:
            stripped_ns = self._namespace - set(namespace)
            result = self.replace_namespace(stripped_ns)

            if result._prefix is not None:
                result._prefix = result._get_minimal_prefix(
                    result._prefix.strip_namespace(namespace))

            return result
        else:
            return self

    def pformat_internal(self, debug: bool = False) -> str:
        """Verbose format for debugging purposes."""
        result = ''

        if not self._path:
            return ''

        if self._namespace:
            result += f'{"@".join(sorted(self._namespace))}@@'

        path = self._path

        result += f'({path[0].name_hint})'  # type: ignore

        for i in range(1, len(path) - 1, 2):
            ptrspec = cast(
                Tuple[irast.BasePointerRef, s_pointers.PointerDirection],
                path[i],
            )

            tgtspec = cast(
                irast.TypeRef,
                path[i + 1],
            )

            if debug:
                link_name = str(ptrspec[0].name)
                ptr = f'({link_name})'
            else:
                ptr = ptrspec[0].shortname.name
            ptrdir = ptrspec[1]
            is_lprop = ptrspec[0].source_ptr is not None

            if tgtspec.material_type is not None:
                mat_tgt = tgtspec.material_type
            else:
                mat_tgt = tgtspec
            tgt = mat_tgt.name_hint

            if tgt:
                lexpr = f'{ptr}[IS {tgt}]'
            else:
                lexpr = f'{ptr}'

            if is_lprop:
                step = '@'
            else:
                step = f'.{ptrdir}'

            result += f'{step}{lexpr}'

        if self._is_ptr:
            result += '@'

        return result

    def pformat(self) -> str:
        """Pretty PathId format for user-visible messages."""
        result = ''

        if not self._path:
            return ''

        path = self._path

        start_name = s_name.shortname_from_fullname(
            path[0].name_hint)  # type: ignore
        result += f'{start_name.name}'

        for i in range(1, len(path) - 1, 2):
            ptrspec = cast(
                Tuple[irast.BasePointerRef, s_pointers.PointerDirection],
                path[i],
            )

            ptr_name = ptrspec[0].shortname
            ptrdir = ptrspec[1]
            is_lprop = ptrspec[0].source_ptr is not None

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

    def rptr(self) -> Optional[irast.BasePointerRef]:
        """Return the descriptor of a pointer for the last path step, if any.

           If this PathId represents a non-path expression, ``rptr()``
           will return ``None``.
        """
        if len(self._path) > 1:
            return self._path[-2][0]  # type: ignore
        else:
            return None

    def rptr_dir(self) -> Optional[s_pointers.PointerDirection]:
        """Return the direction of a pointer for the last path step, if any.

           If this PathId represents a non-path expression, ``rptr_dir()``
           will return ``None``.
        """
        if len(self._path) > 1:
            return self._path[-2][1]  # type: ignore
        else:
            return None

    def rptr_name(self) -> Optional[s_name.QualName]:
        """Return the name of a pointer for the last path step, if any.

           If this PathId represents a non-path expression, ``rptr_name()``
           will return ``None``.
        """
        rptr = self.rptr()
        if rptr is not None:
            return rptr.shortname
        else:
            return None

    def src_path(self) -> Optional[PathId]:
        """Return a ``PathId`` instance representing an immediate path prefix
           of this ``PathId``, i.e
           ``PathId('Foo.bar.baz').src_path() == PathId('Foo.bar')``.

           If this PathId represents a non-path expression, ``src_path()``
           will return ``None``.
        """
        if len(self._path) > 1:
            return self._get_prefix(-2)
        else:
            return None

    def ptr_path(self) -> PathId:
        """Return a new ``PathId`` instance that is a "pointer prefix" of this
           ``PathId``.

           A pointer prefix is the common path prefix shared by paths to
           link properties of the same link, i.e

               common_path_id(Foo.bar@prop1, Foo.bar@prop2)
                   == PathId(Foo.bar).ptr_path()
        """
        if self._is_ptr:
            return self
        else:
            result = self.__class__(self)
            result._is_ptr = True
            return result

    def tgt_path(self) -> PathId:
        """If this is a pointer prefix, return the ``PathId`` representing
           the path to the target of the pointer.

           This is the inverse of :meth:`~PathId.ptr_path`.
        """
        if not self._is_ptr:
            return self
        else:
            result = self.__class__(self)
            result._is_ptr = False
            return result

    def iter_prefixes(self, include_ptr: bool = False) -> Iterator[PathId]:
        """Return an iterator over all prefixes of this ``PathId``.

           The order of prefixes is from longest to shortest, i.e
           ``PathId(A.b.c.d).iter_prefixes()`` will yield
           [PathId(A.b.c.d), PathId(A.b.c), PathId(A.b), PathId(A)].

           If *include_ptr* is ``True``, then pointer prefixes for each
           step are also included.
        """
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

    def startswith(
            self, path_id: PathId, permissive_ptr_path: bool=False) -> bool:
        """Return true if this ``PathId`` has *path_id* as a prefix."""
        base = self._get_prefix(len(path_id))
        return base == path_id or (
            permissive_ptr_path and base.tgt_path() == path_id)

    @property
    def target(self) -> irast.TypeRef:
        """Return the type descriptor for this PathId."""
        return self._path[-1]  # type: ignore

    @property
    def target_name_hint(self) -> s_name.Name:
        """Return the name of the type for this PathId."""
        if self.target.material_type is not None:
            material_type = self.target.material_type
        else:
            material_type = self.target
        return material_type.name_hint

    def is_objtype_path(self) -> bool:
        """Return True if this PathId represents an expression of object
           type.
        """
        return not self.is_ptr_path() and typeutils.is_object(self.target)

    def is_scalar_path(self) -> bool:
        """Return True if this PathId represents an expression of scalar
           type.
        """
        return not self.is_ptr_path() and typeutils.is_scalar(self.target)

    def is_view_path(self) -> bool:
        """Return True if this PathId represents an expression that is a view.
        """
        return not self.is_ptr_path() and typeutils.is_view(self.target)

    def is_tuple_path(self) -> bool:
        """Return True if this PathId represents an expression of an tuple
           type.
        """
        return not self.is_ptr_path() and typeutils.is_tuple(self.target)

    def is_tuple_indirection_path(self) -> bool:
        """Return True if this PathId represents a tuple element indirection
           expression.
        """
        src_path = self.src_path()
        return src_path is not None and src_path.is_tuple_path()

    def is_array_path(self) -> bool:
        """Return True if this PathId represents an expression of an array
           type.
        """
        return not self.is_ptr_path() and typeutils.is_array(self.target)

    def is_range_path(self) -> bool:
        """Return True if this PathId represents an expression of a range
           type.
        """
        return not self.is_ptr_path() and typeutils.is_range(self.target)

    def is_collection_path(self) -> bool:
        """Return True if this PathId represents an expression of a collection
           type.
        """
        return not self.is_ptr_path() and typeutils.is_collection(self.target)

    def is_ptr_path(self) -> bool:
        """Return True if this PathId represents a link prefix of the path.

        Immediate prefix of a link property ``PathId`` will return True here.
        """
        return self._is_ptr

    def is_linkprop_path(self) -> bool:
        """Return True if this PathId represents a link property path
           expression, i.e ``Foo.bar@prop``."""
        return self._is_linkprop

    def is_type_intersection_path(self) -> bool:
        """Return True if this PathId represents a type intersection
           expression, i.e ``Foo[IS Bar]``."""
        rptr_name = self.rptr_name()
        if rptr_name is None:
            return False
        else:
            return str(rptr_name) in (
                '__type__::indirection',
                '__type__::optindirection',
            )

    @property
    def namespace(self) -> FrozenSet[str]:
        """The namespace of this ``PathId``"""
        return self._namespace

    def _get_prefix(self, size: int) -> PathId:
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
        if rptr := result.rptr():
            result._is_linkprop = rptr.source_ptr is not None

        if size < len(self._path) and self._norm_path[size][2]:  # type: ignore
            # A link property ref has been chopped off.
            result._is_ptr = True

        return result

    def _get_minimal_prefix(
        self,
        prefix: Optional[PathId],
    ) -> Optional[PathId]:
        while prefix is not None:
            if prefix._namespace == self._namespace:
                prefix = prefix._prefix
            else:
                break

        return prefix
