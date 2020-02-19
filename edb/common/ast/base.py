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
from typing import *

import typing_inspect

from edb.common import debug
from edb.common import markup
from edb.common import typeutils


class ASTError(Exception):
    pass


class Field:
    def __init__(
            self, name, type_, default, traverse, child_traverse=None,
            field_hidden=False, field_meta=False):
        self.name = name
        self.type = type_
        self.default = default
        self.traverse = traverse
        self.child_traverse = \
            child_traverse if child_traverse is not None else traverse
        self.hidden = field_hidden
        self.meta = field_meta


class MetaAST(type):
    def __new__(mcls, name, bases, dct):
        cls = super().__new__(mcls, name, bases, dct)

        cls.__abstract_node__ = bool(dct.get('__abstract_node__'))

        if '__annotations__' not in dct:
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
                    # cyclic rerefences.
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

            module_name = cls.__module__
            fields_attrname = f'_{name}__fields'

            if fields_attrname in dct:
                raise RuntimeError(
                    'cannot combine class annotations and '
                    'legacy __fields attribute in '
                    f'{cls.__module__}.{cls.__qualname__}')

            hidden = ()
            if '__ast_hidden__' in dct:
                hidden = set(dct['__ast_hidden__'])

            meta = ()
            if '__ast_meta__' in dct:
                meta = set(dct['__ast_meta__'])

            fields = []
            for f_name, f_type in annos.items():
                f_fullname = f'{module_name}.{cls.__qualname__}.{f_name}'

                if f_type is object:
                    f_type = None

                if f_name in dct:
                    f_default = dct[f_name]
                    delattr(cls, f_name)
                else:
                    f_default = None

                f_default = _check_annotation(f_type, f_fullname, f_default)

                f_hidden = f_name in hidden
                f_meta = f_name in meta

                fields.append((f_name, f_type, f_default,
                               True, None, f_hidden, f_meta))

            setattr(cls, fields_attrname, fields)

        return cls

    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)
        fields = collections.OrderedDict()

        for parent in cls.__mro__:
            lst = getattr(cls, '_' + parent.__name__ + '__fields', [])
            for field in lst:
                field_name = field
                field_type = None
                field_default = None
                field_traverse = True
                field_child_traverse = None
                field_hidden = False
                field_meta = False

                if isinstance(field, tuple):
                    field_name = field[0]

                    if len(field) > 1:
                        field_type = field[1]
                    if len(field) > 2:
                        field_default = field[2]
                    else:
                        field_default = field_type

                    if len(field) > 3:
                        field_traverse = field[3]

                    if len(field) > 4:
                        field_child_traverse = field[4]

                    if len(field) > 5:
                        field_hidden = field[5]

                    if len(field) > 6:
                        field_meta = field[6]

                if field_name not in fields:
                    fields[field_name] = Field(
                        field_name, field_type, field_default, field_traverse,
                        field_child_traverse, field_hidden, field_meta)

        cls._fields = fields

    def get_field(cls, name):
        return cls._fields.get(name)


class AST(object, metaclass=MetaAST):
    __fields = []
    __ast_frozen_fields__ = frozenset()

    def __init__(self, **kwargs):
        if type(self).__abstract_node__:
            raise ASTError(
                f'cannot instantiate abstract AST node '
                f'{self.__class__.__name__!r}')

        should_check_types = __debug__ and _check_type is _check_type_real
        for field_name, field in self.__class__._fields.items():
            if field_name in kwargs:
                value = kwargs[field_name]
            elif field.default is not None:
                if callable(field.default):
                    value = field.default()
                else:
                    value = field.default
            else:
                value = None

            if should_check_types:
                self.check_field_type(field, value)

            # Bypass overloaded setattr
            try:
                object.__setattr__(self, field_name, value)
            except AttributeError:
                # Field overriden as a property in a subclass.
                pass

    def __copy__(self):
        copied = self.__class__()
        for field, value in iter_fields(self, include_meta=False):
            setattr(copied, field, value)
        return copied

    def __deepcopy__(self, memo):
        copied = self.__class__()
        for field, value in iter_fields(self, include_meta=False):
            setattr(copied, field, copy.deepcopy(value, memo))
        return copied

    if __debug__:
        def __setattr__(self, name, value):
            super().__setattr__(name, value)
            field = self._fields.get(name)
            if field:
                self.check_field_type(field, value)
                if name in self.__ast_frozen_fields__:
                    raise TypeError(f'cannot set immutable {name} on {self!r}')

    def check_field_type(self, field, value):
        def raise_error(field_type_name, value):
            raise TypeError(
                '%s.%s.%s: expected %s but got %s' % (
                    self.__class__.__module__, self.__class__.__name__,
                    field.name, field_type_name, value.__class__.__name__))

        _check_type(field.type, value, raise_error)

    def dump(self):
        markup.dump(self)


class ImmutableASTMixin:
    __frozen = False
    __ast_mutable_fields__ = frozenset()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__frozen = True

    def __setattr__(self, name, value):
        if self.__frozen and name not in self.__ast_mutable_fields__:
            raise TypeError(f'cannot set {name} on immutable {self!r}')
        else:
            super().__setattr__(name, value)


@markup.serializer.serializer.register(AST)
def _serialize_to_markup(ast, *, ctx):
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
            if callable(field.default):
                default = field.default()
            else:
                default = field.default
            if field_val == default:
                continue
        yield field_name, field_val


def _is_optional(type_):
    return (typing_inspect.is_union_type(type_) and
            type(None) in typing_inspect.get_args(type_, evaluate=True))


def _check_annotation(f_type, f_fullname, f_default):
    if typing_inspect.is_tuple_type(f_type):
        if f_default is not None:
            raise RuntimeError(
                f'invalid type annotation on {f_fullname}: '
                f'default is defined for tuple type')

        f_default = tuple

    elif typing_inspect.is_union_type(f_type):
        for t in typing_inspect.get_args(f_type, evaluate=True):
            _check_annotation(t, f_fullname, f_default)

    elif typing_inspect.is_generic_type(f_type):
        if f_default is not None:
            raise RuntimeError(
                f'invalid type annotation on {f_fullname}: '
                f'default is defined for container type '
                f'{f_type!r}')

        ot = typing_inspect.get_origin(f_type)
        if ot is None:
            raise RuntimeError(
                f'cannot find origin of a generic type {f_type}')

        if ot in (list, List, collections.abc.Sequence):
            f_default = list
        elif ot in (set, Set):
            f_default = set
        elif ot in (frozenset, FrozenSet):
            f_default = frozenset
        elif ot in (dict, Dict):
            f_default = dict
        else:
            raise RuntimeError(
                f'invalid type annotation on {f_fullname}: '
                f'{f_type!r} is not supported')

    elif f_type is not None:
        if f_type is Any:
            f_type = object

        if not isinstance(f_type, type):
            raise RuntimeError(
                f'invalid type annotation on {f_fullname}: '
                f'{f_type!r} is not a type')

        if typeutils.is_container_type(f_type):
            if f_default is not None:
                raise RuntimeError(
                    f'invalid type annotation on {f_fullname}: '
                    f'default is defined for container type '
                    f'{f_type!r}')
            f_default = f_type

    return f_default


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
