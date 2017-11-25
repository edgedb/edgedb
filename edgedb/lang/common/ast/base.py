##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import copy
import collections.abc
import functools
import typing

from edgedb.lang.common import markup


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
        if '__annotations__' in dct:
            module_name = dct['__module__']
            fields_attrname = f'_{name}__fields'

            if fields_attrname in dct:
                raise RuntimeError(
                    'cannot combine class annotations and '
                    'legacy __fields attribute in '
                    f'{dct["__module__"]}.{dct["__qualname__"]}')

            hidden = ()
            if '__ast_hidden__' in dct:
                hidden = set(dct['__ast_hidden__'])

            meta = ()
            if '__ast_meta__' in dct:
                meta = set(dct['__ast_meta__'])

            fields = []
            for f_name, f_type in dct['__annotations__'].items():
                f_fullname = f'{module_name}.{dct["__qualname__"]}.{f_name}'

                if f_type is object:
                    f_type = None

                if f_name in dct:
                    f_default = dct.pop(f_name)
                else:
                    f_default = None

                f_default = _check_annotation(f_type, f_fullname, f_default)

                f_hidden = f_name in hidden
                f_meta = f_name in meta

                fields.append((f_name, f_type, f_default,
                               True, None, f_hidden, f_meta))

            dct[fields_attrname] = fields

        return super().__new__(mcls, name, bases, dct)

    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)
        fields = collections.OrderedDict()

        for parent in reversed(cls.__mro__):
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

    def __init__(self, **kwargs):
        object.__setattr__(self, 'parent', None)
        self._init_fields(kwargs)

        # XXX: use weakref here
        for arg, value in kwargs.items():
            if hasattr(self, arg):
                if is_ast_node(value):
                    object.__setattr__(value, 'parent', self)
                elif isinstance(value, list):
                    for v in value:
                        if is_ast_node(v):
                            object.__setattr__(v, 'parent', self)
                elif isinstance(value, dict):
                    for v in value.values():
                        if is_ast_node(v):
                            object.__setattr__(v, 'parent', self)
            else:
                raise ASTError(
                    'cannot set attribute "%s" in ast class "%s"' %
                    (arg, self.__class__.__name__))

        if 'parent' in kwargs:
            object.__setattr__(self, 'parent', kwargs['parent'])

    def _init_fields(self, values):
        for field_name, field in self.__class__._fields.items():
            if field_name in values:
                value = values[field_name]
            elif field.default is not None:
                if callable(field.default):
                    value = field.default()
                else:
                    value = field.default
            else:
                value = None

            if __debug__:
                self.check_field_type(field, value)

            # Bypass overloaded setattr
            object.__setattr__(self, field_name, value)

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

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__frozen = True

    def __setattr__(self, name, value):
        if self.__frozen:
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


@functools.lru_cache(1024)
def _is_container_type(cls):
    return (
        issubclass(cls, (collections.abc.Container)) and
        not issubclass(cls, (str, bytes, bytearray, memoryview))
    )


@functools.lru_cache(1024)
def _is_iterable_type(cls):
    return (
        issubclass(cls, collections.abc.Iterable)
    )


def is_container(obj):
    cls = obj.__class__
    return _is_container_type(cls) and _is_iterable_type(cls)


def is_container_type(type_):
    return isinstance(type_, type) and _is_container_type(type_)


def fix_parent_links(node):
    for field, value in iter_fields(node):
        if is_container(value):
            for n in value:
                if is_ast_node(n):
                    n.parent = node
                    fix_parent_links(n)

        elif isinstance(value, dict):
            for n in value.values():
                if is_ast_node(n):
                    n.parent = node
                    fix_parent_links(n)

        elif is_ast_node(value):
            value.parent = node
            fix_parent_links(value)

    return node


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


def _is_union(type_):
    return type_.__class__ is typing.Union.__class__


def _is_typing(type_):
    return _is_union(type_) or isinstance(type_, typing.TypingMeta)


def _check_annotation(f_type, f_fullname, f_default):
    if _is_typing(f_type):
        if _is_union(f_type):
            for t in f_type.__args__:
                _check_annotation(t, f_fullname, f_default)

        else:
            if (issubclass(f_type, typing.Container) and
                    f_default is not None):
                raise RuntimeError(
                    f'invalid type annotation on {f_fullname}: '
                    f'default is defined for container type '
                    f'{f_type!r}')

            if issubclass(f_type, typing.List):
                f_default = list
            elif issubclass(f_type, typing.Tuple):
                f_default = tuple
            elif issubclass(f_type, typing.Set):
                f_default = set
            elif issubclass(f_type, typing.FrozenSet):
                f_default = frozenset
            elif issubclass(f_type, typing.Dict):
                f_default = dict
            else:
                raise RuntimeError(
                    f'invalid type annotation on {f_fullname}: '
                    f'{f_type!r} is not supported')

    elif f_type is not None:
        if not isinstance(f_type, type):
            raise RuntimeError(
                f'invalid type annotation on {f_fullname}: '
                f'{f_type!r} is not a type')

        if is_container_type(f_type):
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

    eltype = type_.__args__[0]
    for el in value:
        _check_type(eltype, el, raise_error)


def _check_tuple_type(type_, value, raise_error, instance_type):
    if not isinstance(value, instance_type):
        raise_error(str(type_), value)

    eltype = None

    for i, el in enumerate(value):
        new_eltype = type_.__args__[i]
        if new_eltype is not Ellipsis:
            eltype = new_eltype
        if eltype is not None:
            _check_type(eltype, el, raise_error)


def _check_mapping_type(type_, value, raise_error, instance_type):
    if not isinstance(value, instance_type):
        raise_error(str(type_), value)

    ktype = type_.__args__[0]
    vtype = type_.__args__[1]
    for k, v in value.items():
        _check_type(ktype, k, raise_error)
        if not k:
            raise RuntimeError('empty key in map')
        _check_type(vtype, v, raise_error)


def _check_type(type_, value, raise_error):
    if type_ is None:
        return

    if not _is_typing(type_):
        if value is not None and not isinstance(value, type_):
            raise_error(type_.__name__, value)

    else:
        if _is_union(type_):
            for t in type_.__args__:
                try:
                    _check_type(t, value, raise_error)
                except TypeError as e:
                    pass
                else:
                    break
            else:
                raise_error(str(type_), value)

        elif issubclass(type_, typing.List):
            _check_container_type(type_, value, raise_error, list)

        elif issubclass(type_, typing.Tuple):
            _check_tuple_type(type_, value, raise_error, tuple)

        elif issubclass(type_, typing.Set):
            _check_container_type(type_, value, raise_error, set)

        elif issubclass(type_, typing.FrozenSet):
            _check_container_type(type_, value, raise_error, frozenset)

        elif issubclass(type_, typing.Dict):
            _check_mapping_type(type_, value, raise_error, dict)

        elif issubclass(type_, typing.Meta):
            raise TypeError(f'unsupported typing type: {type_!r}')
