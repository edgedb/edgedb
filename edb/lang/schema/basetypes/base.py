#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2012-present MagicStack Inc. and the EdgeDB authors.
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


import itertools
import inspect
import types

from edb.lang.common import ast
from edb.lang.schema import error as s_err
from edb.lang.schema import objects as s_obj
from edb.lang.schema import pointers as s_pointers
from edb.lang.schema import types as s_types


class TypeRules:
    rules = {}

    @classmethod
    def add_rule(cls, op, args, result):
        try:
            oprules = cls.rules[op]
        except KeyError:
            oprules = cls.rules[op] = {}

        arglen = len(args)

        try:
            argrules = oprules[arglen]
        except KeyError:
            argrules = oprules[arglen] = {}

        args = tuple(args)
        argrules[tuple(args)] = result

    @classmethod
    def _issubclass(cls, schema, arg, sig_arg):
        if sig_arg is Ellipsis:
            return True

        is_subclass = isinstance(arg, type) and issubclass(arg, sig_arg)

        if not is_subclass and isinstance(arg, s_obj.Object):
            if not isinstance(sig_arg, s_obj.Object):
                sig_arg_typ = BaseTypeMeta.type_to_edgedb_builtin(sig_arg)
                if not sig_arg_typ:
                    return False
                else:
                    sig_arg = schema.get(sig_arg_typ)

            is_subclass = arg.issubclass(sig_arg)

        return is_subclass

    @classmethod
    def get_result(cls, op, args, schema):
        match = None
        rules = cls.rules.get(op)

        if rules:
            rules = rules.get(len(args))

        if rules:
            for sig, result in rules.items():
                for i, arg in enumerate(args):
                    if arg is not None:
                        if hasattr(arg, 'get_implementation_type'):
                            arg_impl = arg.get_implementation_type()
                        else:
                            arg_impl = arg
                    else:
                        arg_impl = None

                    sig_arg = sig[i]

                    if not cls._issubclass(schema, arg_impl, sig_arg):
                        break
                else:
                    match = result
                    break

        if match and match.__class__ is str:
            match = schema.get(match)

        return match


class TypeInfoMeta(type):
    def __new__(mcls, name, bases, dct, *, type):
        result = super().__new__(mcls, name, bases, dct)

        if type is not None:
            for name, proc in dct.items():
                if not isinstance(proc, types.FunctionType):
                    continue

                astop = ast.ops.Operator.funcname_to_op(name)

                if astop:
                    args = inspect.getfullargspec(proc)

                    argtypes = []

                    allargs = itertools.chain(args.args, args.kwonlyargs)
                    for i, arg in enumerate(allargs):
                        if i == 0:
                            argtypes.append((type,))
                        else:
                            argtype = args.annotations[arg]
                            if not isinstance(argtype, tuple):
                                argtype = (argtype,)
                            argtypes.append(argtype)

                    result = args.annotations['return']

                    if not issubclass(result, SchemaObject):
                        raise TypeError(
                            'type operation return annotations must '
                            'be SchemaObject annotations')

                    for argt in itertools.product(*argtypes):
                        TypeRules.add_rule(astop, argt, result.schema_name)

        return result

    def __init__(cls, name, bases, dct, *, type):
        super().__init__(name, bases, dct)


class TypeInfo(metaclass=TypeInfoMeta, type=None):
    pass


class FunctionMeta(type):
    function_map = {}

    def __new__(mcls, name, bases, dct):
        result = super().__new__(mcls, name, bases, dct)

        get_signature = getattr(result, 'get_signature', None)

        signature = None
        if get_signature:
            signature = get_signature()

        if signature:
            args = signature[1]
            if args is not None:
                if not isinstance(args, tuple):
                    args = (args,)
            else:
                args = ()

            signature = (signature[0], args, signature[2])
            TypeRules.add_rule(*signature)

        get_canonical_name = getattr(result, 'get_canonical_name', None)

        if get_canonical_name:
            canonical_name = get_canonical_name()
            mcls.function_map[canonical_name] = result

        return result

    @classmethod
    def get_function_class(mcls, name):
        return mcls.function_map.get(name)


class BaseTypeMeta:
    base_type_map = {}
    implementation_map = {}
    meta_implementation_map = {}
    mixin_map = {}

    @classmethod
    def add_mapping(cls, type, edgedb_builtin_name):
        cls.base_type_map[type] = edgedb_builtin_name

    @classmethod
    def add_implementation(cls, edgedb_name, type):
        existing = cls.implementation_map.get(edgedb_name)
        if existing is not None:
            msg = ('cannot set {!r} as implementation: {!r} is already ' +
                   'implemented by {!r}').format(type, edgedb_name, existing)
            raise ValueError(msg)

        cls.implementation_map[edgedb_name] = type

    @classmethod
    def add_meta_implementation(cls, schema_type, type):
        existing = cls.meta_implementation_map.get(schema_type)
        if existing is not None:
            msg = ('cannot set {!r} as implementation: {!r} is already ' +
                   'implemented by {!r}').format(type, schema_type, existing)
            raise ValueError(msg)

        cls.meta_implementation_map[schema_type] = type

    @classmethod
    def add_mixin(cls, edgedb_name, type):
        try:
            mixins = cls.mixin_map[edgedb_name]
        except KeyError:
            mixins = cls.mixin_map[edgedb_name] = []

        mixins.append(type)

    @classmethod
    def type_to_edgedb_builtin(cls, type):
        for t in type.__mro__:
            try:
                return cls.base_type_map[t]
            except KeyError:
                continue

    @classmethod
    def get_implementation(cls, edgedb_name):
        return cls.implementation_map.get(edgedb_name)

    @classmethod
    def get_meta_implementation(cls, schema_type):
        return cls.meta_implementation_map.get(schema_type)

    @classmethod
    def get_mixins(cls, edgedb_name):
        mixins = cls.mixin_map.get(edgedb_name)
        return tuple(mixins) if mixins else tuple()


class SchemaObjectMeta(type):
    def __new__(mcls, clsname, bases, dct, *, name=None):
        cls = super().__new__(mcls, clsname, bases, dct)
        cls.schema_name = name
        return cls


class SchemaObject(metaclass=SchemaObjectMeta):
    pass


def classname_from_type(typ):
    """Return canonical Object name for a given type.

    Arguments:

    - type             -- Type to normalize

    Result:

    Canonical Object name.
    """
    is_composite = isinstance(typ, tuple)

    if is_composite:
        container_type = typ[0]
        item_type = typ[1]
    else:
        item_type = typ

    classname = None

    if isinstance(item_type, s_types.Type):
        classname = item_type.name

    elif isinstance(item_type, s_pointers.Pointer):
        classname = item_type.name

    elif isinstance(item_type, s_obj.ObjectMeta):
        classname = item_type

    else:
        classname = BaseTypeMeta.type_to_edgedb_builtin(item_type)

    if classname is None:
        raise s_err.SchemaError(
            'could not find matching schema item for %r' % typ)

    if is_composite:
        result = (container_type, classname)
    else:
        result = classname

    return result


def normalize_type(type, schema):
    """Normalize provided type description into a canonical Object form.

    Arguments:

    - type             -- Type to normalize
    - schema     -- Schema to use for Object lookups

    Result:

    Normalized type.
    """

    classname = classname_from_type(type)
    if classname is None:
        raise s_err.SchemaError(
            'could not find matching schema item for %r' % type)

    is_composite = isinstance(classname, tuple)

    if is_composite:
        container_type = classname[0]
        item_class_name = classname[1]
    else:
        item_class_name = classname

    if isinstance(item_class_name, s_obj.ObjectMeta):
        item_class = item_class_name
    else:
        item_class = schema.get(item_class_name)

    if is_composite:
        result = (container_type, item_class)
    else:
        result = item_class

    return result
