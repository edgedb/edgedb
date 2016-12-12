##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import builtins
import collections

from edgedb.lang.common.persistent_hash import persistent_hash
from edgedb.lang.common.ordered import OrderedSet

from edgedb.lang.edgeql import ast as qlast

from . import delta as sd
from .error import SchemaError
from . import name as sn
from . import named
from . import objects as so


class ModuleCommandContext(sd.CommandContextToken):
    def __init__(self, op, module=None):
        super().__init__(op)
        self.module = module


class ModuleCommand(named.NamedClassCommand):
    context_class = ModuleCommandContext

    classname = so.Field(str)

    @classmethod
    def _classname_from_ast(cls, astnode, context):
        if astnode.name.module:
            classname = sn.Name(module=astnode.name.module,
                                name=astnode.name.name)
        else:
            classname = astnode.name.name

        return classname

    @classmethod
    def _get_metaclass(cls):
        return Module


class CreateModule(named.CreateNamedClass, ModuleCommand):
    astnode = qlast.CreateModuleNode

    def apply(self, schema, context):
        props = self.get_struct_properties(schema)
        self.module = self.metaclass(**props)
        schema.add_module(self.module)
        return self.module


class AlterModule(named.CreateOrAlterNamedClass, ModuleCommand):
    astnode = qlast.AlterModuleNode

    def apply(self, schema, context):
        self.module = schema.get_module(self.classname)

        props = self.get_struct_properties(schema)
        for name, value in props.items():
            setattr(self.module, name, value)

        return self.module


class DeleteModule(ModuleCommand):
    astnode = qlast.DropModuleNode

    def apply(self, schema, context):
        self.module = schema.get_module(self.classname)
        schema.delete_module(self.module)
        return self.module


class Module(named.NamedClass):
    # Override 'name' to str type, since modules don't have
    # fully-qualified names.
    name = so.Field(str)

    imports = so.Field(so.ClassSet, so.ClassSet)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.index_by_name = collections.OrderedDict()
        self.index_by_type = {}
        self.index_derived = set()

    def copy(self):
        result = self.__class__(name=self.name, imports=self.imports)
        for obj in self:
            result.add(obj.copy())
        return result

    def add(self, obj):
        if obj in self:
            err = '{!r} is already present in the schema'.format(obj.name)
            raise SchemaError(err)

        self.index_by_name[obj.name] = obj

        idx_by_type = self.index_by_type.setdefault(obj._type, OrderedSet())
        idx_by_type.add(obj.name)

        if getattr(obj, 'is_derived', None):
            self.index_derived.add(obj.name)

    def discard(self, obj):
        existing = self.index_by_name.pop(obj.name, None)
        if existing is not None:
            self._delete(existing)
        return existing

    def delete(self, obj):
        existing = self.discard(obj)
        if existing is None:
            err = 'object {!r} is not present in the schema'.format(obj.name)
            raise SchemaError(err)

        return existing

    def replace(self, obj):
        existing = self.discard(obj)
        if existing:
            self._delete(existing)
        self.add(obj)

    def _delete(self, obj):
        self.index_by_name.pop(obj.name, None)
        self.index_by_type[obj.__class__._type].remove(obj.name)
        self.index_derived.discard(obj.name)

    def lookup_qname(self, name):
        return self.index_by_name.get(name)

    def get(self, name, default=SchemaError, *,
            module_aliases=None, type=None,
            include_pyobjects=False, index_only=True,
            implicit_builtins=True):

        fail_cause = None

        if isinstance(type, tuple):
            for typ in type:
                try:
                    scls = self.get(name, module_aliases=module_aliases,
                                    type=typ,
                                    include_pyobjects=include_pyobjects,
                                    index_only=index_only, default=None)
                except SchemaError:
                    pass
                else:
                    if scls is not None:
                        return scls
        else:
            scls = None

            fq_name = '{}::{}'.format(self.name, name)
            scls = self.lookup_qname(fq_name)

            if type is not None and issubclass(type, so.Class):
                type = type.get_canonical_class()

        raise_ = None

        if scls is None:
            if default is not None:
                raise_ = (isinstance(default, Exception) or
                          (isinstance(default, builtins.type) and
                           issubclass(default, Exception)))

            if raise_:
                msg = ('reference to non-existent schema class: '
                       '{}::{}'.format(self.name, name))
                if fail_cause is not None:
                    raise default(msg) from fail_cause
                else:
                    raise default(msg)
            else:
                scls = default

        if (type is not None and
                isinstance(scls, so.Class) and
                not isinstance(scls, type)):
            if default is not None:
                raise_ = (isinstance(default, Exception) or
                          (isinstance(default, builtins.type) and
                           issubclass(default, Exception)))
            if raise_:
                if isinstance(type, tuple):
                    typname = ' or '.join(typ.__name__ for typ in type)
                else:
                    typname = type.__name__

                msg = '{} exists but is not {}'.format(name, typname)

                if fail_cause is not None:
                    raise default(msg) from fail_cause
                else:
                    raise default(msg)
            else:
                scls = default
        return scls

    def reorder(self, new_order):
        name_order = [p.name for p in new_order]

        def sortkey(item):
            try:
                return name_order.index(item)
            except ValueError:
                return -1

        # The new_order may be partial, as the most common source
        # is schema enumeration, which may filter out certain objects.
        names = OrderedSet(self.index_by_name)
        names = OrderedSet(sorted(names, key=sortkey))

        self.index_by_name = collections.OrderedDict(
            (name, self.index_by_name[name]) for name in names)

        for typeindex in self.index_by_type.values():
            sortedindex = sorted(typeindex, key=sortkey)
            typeindex.clear()
            typeindex.update(sortedindex)

    def get_objects(self, *, type=None, include_derived=False):
        return SchemaIterator(self, type, include_derived=include_derived)

    def get_checksum(self):
        if self.index_by_name:
            objects = frozenset(self)
            checksum = persistent_hash(objects)
        else:
            checksum = persistent_hash(None)

        return checksum


class SchemaIterator:
    def __init__(self, module, type, include_derived=False):
        self.module = module
        self.type = type
        self.include_derived = include_derived
        self.iter = self._make_iter()

    def _make_iter(self):
        names = OrderedSet(self.module.index_by_name)
        if self.type is not None:
            typ = self.type
            if isinstance(self.type, so.MetaClass):
                typ = self.type._type

            itertyp = self.module.index_by_type.get(typ)
            if itertyp:
                names = itertyp
            else:
                names = OrderedSet()

        if not self.include_derived:
            names -= self.module.index_derived

        return iter(names)

    def __iter__(self):
        return self

    def __next__(self):
        return self.module.index_by_name[next(self.iter)]
