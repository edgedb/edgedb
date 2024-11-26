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

import copy
import collections.abc
import functools
import re
import sys
from typing import (
    Any,
    Callable,
    TypeVar,
    Dict,
    List,
    Set,
    FrozenSet,
    cast,
    get_type_hints,
    TYPE_CHECKING,
    AbstractSet  # NoQA
)

from edb.common import debug
from edb.common import markup
from edb.common import typing_inspect


T = TypeVar('T')


class ASTError(Exception):
    pass


class _Field:
    def __init__(
        self,
        name,
        type_,
        default,
        factory,
        field_hidden=False,
        field_meta=False,
    ):
        self.name = name
        self.type = type_
        self.default = default
        self.factory = factory
        self.hidden = field_hidden
        self.meta = field_meta


class _FieldSpec:
    def __init__(self, factory):
        self.factory = factory


def field(*, factory: Callable[[], T]) -> T:
    return cast(T, _FieldSpec(factory=factory))


def _check_type_passthrough(type_, value, raise_error):
    pass


def _check_type_real(type_, value, raise_error):
    if type_ is None:
        return

    if typing_inspect.is_union_type(type_):
        for t in typing_inspect.get_args(type_, evaluate=True):
            try:
                _check_type(t, value, raise_error)
            except TypeError:
                pass
            else:
                break
        else:
            raise_error(str(type_), value)

    elif typing_inspect.is_tuple_type(type_):
        _check_tuple_type(type_, value, raise_error, tuple)

    elif typing_inspect.is_generic_type(type_):
        ot = typing_inspect.get_origin(type_)

        if ot in (list, List, collections.abc.Sequence):
            _check_container_type(type_, value, raise_error, list)

        elif ot in (set, Set):
            _check_container_type(type_, value, raise_error, set)

        elif ot in (frozenset, FrozenSet):
            _check_container_type(type_, value, raise_error, frozenset)

        elif ot in (dict, Dict):
            _check_mapping_type(type_, value, raise_error, dict)

        elif ot is not None:
            raise TypeError(f'unsupported typing type: {type_!r}')

    elif type_ is not Any:
        if value is not None and not isinstance(value, type_):
            raise_error(type_.__name__, value)


if debug.flags.typecheck:
    _check_type = _check_type_real
else:
    _check_type = _check_type_passthrough


class AST:
    # These use type comments because type annotations are interpreted
    # by the AST system and so annotating them would interfere!
    __ast_frozen_fields__ = frozenset()  # type: AbstractSet[str]

    # Class setup stuff:
    @classmethod
    def _collect_direct_fields(cls):
        dct = cls.__dict__
        cls.__abstract_node__ = bool(dct.get('__abstract_node__'))
        cls.__rust_ignore__ = bool(dct.get('__rust_ignore__'))

        if '__annotations__' not in dct:
            cls._direct_fields = []
            return cls

        globalns = sys.modules[cls.__module__].__dict__.copy()
        globalns[cls.__name__] = cls

        try:
            while True:
                try:
                    annos = get_type_hints(cls, globalns)
                except NameError as e:
                    # Forward type declaration.  Generally, we try
                    # to avoid these as much as possible, but when
                    # there's a cycle it's better to have correct
                    # static type analysis even though the runtime
                    # validation infrastructure does not support
                    # cyclic references.
                    # XXX: This is a horrible hack, need to find
                    # a better way.
                    m = re.match(r"name '(\w+)' is not defined", e.args[0])
                    if not m:
                        raise
                    globalns[m.group(1)] = AST
                else:
                    break

        except Exception:
            raise RuntimeError(
                f'unable to resolve type annotations for '
                f'{cls.__module__}.{cls.__qualname__}')

        if annos:
            annos = {k: v for k, v in annos.items()
                     if k in dct['__annotations__']}

            hidden = ()
            if '__ast_hidden__' in dct:
                hidden = set(dct['__ast_hidden__'])

            meta = ()
            if '__ast_meta__' in dct:
                meta = set(dct['__ast_meta__'])

            fields = []
            for f_name, f_type in annos.items():
                if f_type is object:
                    f_type = None

                factory = None
                if f_name in dct:
                    f_default = dct[f_name]
                    if isinstance(f_default, _FieldSpec):
                        factory = f_default.factory
                        f_default = None
                        delattr(cls, f_name)
                else:
                    f_default = None

                f_hidden = f_name in hidden
                f_meta = f_name in meta

                fields.append(_Field(
                    f_name, f_type, f_default, factory, f_hidden, f_meta
                ))

            cls._direct_fields = fields

        return cls

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        cls._collect_direct_fields()

        fields = collections.OrderedDict()

        for parent in reversed(cls.__mro__):
            lst = getattr(parent, '_direct_fields', [])
            for field in lst:
                fields[field.name] = field

        cls._fields = fields
        cls._field_factories = tuple(
            (k, v.factory) for k, v in fields.items()
            if v.factory and not isinstance(getattr(cls, k, None), property)
        )

        # Push the default values down in the MRO
        for k, v in cls._fields.items():
            if (
                not v.factory
                and not isinstance(getattr(cls, k, None), property)
                and k not in cls.__dict__
            ):
                setattr(cls, k, v.default)

    @classmethod
    def get_field(cls, name):
        return cls._fields.get(name)

    # Actual object level code
    def __init__(self, **kwargs):
        if type(self).__abstract_node__:
            raise ASTError(
                f'cannot instantiate abstract AST node '
                f'{self.__class__.__name__!r}')

        # Make kwargs directly into our __dict__
        for field_name, factory in self._field_factories:
            if field_name not in kwargs:
                kwargs[field_name] = factory()

        should_check_types = __debug__ and _check_type is _check_type_real
        if should_check_types:
            for k, v in kwargs.items():
                self.check_field_type(self._fields[k], v)

        self.__dict__ = kwargs

    def __copy__(self):
        copied = self._init_copy()
        for field, value in iter_fields(self, include_meta=True):
            try:
                object.__setattr__(copied, field, value)
            except AttributeError:
                # don't mind not setting getter_only attrs.
                continue
        return copied

    def __deepcopy__(self, memo):
        copied = self._init_copy()
        for field, value in iter_fields(self, include_meta=True):
            object.__setattr__(copied, field, copy.deepcopy(value, memo))
        return copied

    def _init_copy(self):
        return self.__class__()

    def replace(self: T, **changes) -> T:
        copied = copy.copy(self)
        for field, value in changes.items():
            object.__setattr__(copied, field, value)
        return copied

    def _checked_setattr(self, name, value):
        super().__setattr__(name, value)
        field = self._fields.get(name)
        if field:
            self.check_field_type(field, value)
            if name in self.__ast_frozen_fields__:
                raise TypeError(f'cannot set immutable {name} on {self!r}')

    if __debug__ and _check_type is _check_type_real:
        __setattr__ = _checked_setattr

    def check_field_type(self, field, value):
        def raise_error(field_type_name, value):
            raise TypeError(
                '%s.%s.%s: expected %s but got %s' % (
                    self.__class__.__module__, self.__class__.__name__,
                    field.name, field_type_name, value.__class__.__name__))

        _check_type(field.type, value, raise_error)

    def dump(self, *, meta=True):
        markup.dump(self, _ast_include_meta=meta)


class ImmutableASTMixin:
    __frozen = False
    # This uses type comments because type annotations are interpreted
    # by the AST system and so annotating them would interfere!
    __ast_mutable_fields__ = frozenset()  # type: AbstractSet[str]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__frozen = True

    # mypy gets mad about this if there isn't a __setattr__ in AST.
    # I don't know why.
    if not TYPE_CHECKING:

        def __setattr__(self, name, value):
            if self.__frozen and name not in self.__ast_mutable_fields__:
                raise TypeError(f'cannot set {name} on immutable {self!r}')
            else:
                super().__setattr__(name, value)


@markup.serializer.serializer.register(AST)
def serialize_to_markup(ast, *, ctx):
    node = markup.elements.lang.TreeNode(id=id(ast), name=type(ast).__name__)
    include_meta = ctx.kwargs.get('_ast_include_meta', True)
    exclude_unset = ctx.kwargs.get('_ast_exclude_unset', True)

    fields = iter_fields(
        ast, include_meta=include_meta, exclude_unset=exclude_unset)
    for fieldname, field in fields:
        if ast._fields[fieldname].hidden:
            continue
        if field is None:
            if ast._fields[fieldname].meta:
                continue
        node.add_child(label=fieldname, node=markup.serialize(field, ctx=ctx))

    return node


@functools.lru_cache(1024)
def _is_ast_node_type(cls):
    return issubclass(cls, AST)


def is_ast_node(obj):
    return _is_ast_node_type(obj.__class__)


_marker = object()


def iter_fields(node, *, include_meta=True, exclude_unset=False):
    exclude_meta = not include_meta
    for field_name, field in node._fields.items():
        if exclude_meta and field.meta:
            continue
        field_val = getattr(node, field_name, _marker)
        if field_val is _marker:
            continue
        if exclude_unset:
            if field.factory:
                default = field.factory()
            else:
                default = field.default
            if field_val == default:
                continue
        yield field_name, field_val


def _is_optional(type_):
    return (typing_inspect.is_union_type(type_) and
            type(None) in typing_inspect.get_args(type_, evaluate=True))


def _check_container_type(type_, value, raise_error, instance_type):
    if not isinstance(value, instance_type):
        raise_error(str(type_), value)

    type_args = typing_inspect.get_args(type_, evaluate=True)
    eltype = type_args[0]
    for el in value:
        _check_type(eltype, el, raise_error)


def _check_tuple_type(type_, value, raise_error, instance_type):
    if not isinstance(value, instance_type):
        raise_error(str(type_), value)

    eltype = None
    ellipsis = False
    type_args = typing_inspect.get_args(type_, evaluate=True)

    for i, el in enumerate(value):
        if not ellipsis:
            new_eltype = type_args[i]
            if new_eltype is Ellipsis:
                ellipsis = True
            else:
                eltype = new_eltype
        if eltype is not None:
            _check_type(eltype, el, raise_error)


def _check_mapping_type(type_, value, raise_error, instance_type):
    if not isinstance(value, instance_type):
        raise_error(str(type_), value)

    type_args = typing_inspect.get_args(type_, evaluate=True)
    ktype = type_args[0]
    vtype = type_args[1]
    for k, v in value.items():
        _check_type(ktype, k, raise_error)
        if not k and not _is_optional(ktype):
            raise RuntimeError('empty key in map')
        _check_type(vtype, v, raise_error)
