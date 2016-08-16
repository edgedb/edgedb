##
# Copyright (c) 2008-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import builtins
import collections
import re

from edgedb.lang.common.algos.persistent_hash import persistent_hash
from edgedb.lang.common.datastructures import OrderedSet

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


class ModuleCommand(named.NamedPrototypeCommand):
    context_class = ModuleCommandContext

    prototype_name = so.Field(str)

    @classmethod
    def _protoname_from_ast(cls, astnode, context):
        if astnode.name.module:
            prototype_name = sn.Name(module=astnode.name.module,
                                     name=astnode.name.name)
        else:
            prototype_name = astnode.name.name

        return prototype_name

    @classmethod
    def _get_prototype_class(cls):
        return ProtoModule


class CreateModule(named.CreateNamedPrototype, ModuleCommand):
    astnode = qlast.CreateModuleNode

    def apply(self, schema, context):
        props = self.get_struct_properties(schema)
        self.module = self.prototype_class(**props)
        schema.add_module(self.module)
        return self.module


class AlterModule(named.CreateOrAlterNamedPrototype, ModuleCommand):
    astnode = qlast.AlterModuleNode

    def apply(self, schema, context):
        self.module = schema.get_module(self.prototype_name)

        props = self.get_struct_properties(schema)
        for name, value in props.items():
            setattr(self.module, name, value)

        return self.module


class DeleteModule(ModuleCommand):
    astnode = qlast.DropModuleNode

    def apply(self, schema, context):
        self.module = schema.get_module(self.prototype_name)
        schema.delete_module(self.module)
        return self.module


class ProtoModule(named.NamedPrototype):
    name = so.Field(str)
    imports = so.Field(frozenset, frozenset)

    def get_object_key(self, obj):
        return obj.__class__.get_canonical_class(), obj.name

    def get_object_by_key(self, key):
        cls, name = key
        obj = self.lookup_qname(name)
        assert obj, 'Could not find "%s" object named "%s"' % (cls, name)
        return obj

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.index = OrderedSet()
        self.index_by_name = collections.OrderedDict()
        self.index_by_type = {}
        self.index_derived = OrderedSet()

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

        key = self.get_object_key(obj)
        self.index.add(key)

        idx_by_type = self.index_by_type.setdefault(obj._type, OrderedSet())
        idx_by_type.add(key)

        key = self.get_object_key(obj)

        if getattr(obj, 'is_derived', None):
            self.index_derived.add(key)

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
        key = self.get_object_key(obj)
        self.index.remove(key)
        self.index_by_type[obj.__class__._type].remove(key)
        self.index_derived.discard(key)

    def lookup_qname(self, name):
        return self.index_by_name.get(name)

    def get(self, name, default=SchemaError, module_aliases=None, type=None,
                  include_pyobjects=False, index_only=True,
                  implicit_builtins=True):

        fail_cause = None

        if isinstance(type, tuple):
            for typ in type:
                try:
                    prototype = self.get(name, module_aliases=module_aliases,
                                         type=typ,
                                         include_pyobjects=include_pyobjects,
                                         index_only=index_only, default=None)
                except SchemaError:
                    pass
                else:
                    if prototype is not None:
                        return prototype
        else:
            prototype = None

            fq_name = '{}::{}'.format(self.name, name)
            prototype = self.lookup_qname(fq_name)

            if type is not None and issubclass(type, so.ProtoObject):
                type = type.get_canonical_class()

        raise_ = None

        if prototype is None:
            if default is not None:
                raise_ = (isinstance(default, Exception) or
                            (isinstance(default, builtins.type) and
                             issubclass(default, Exception)))

            if raise_:
                msg = 'reference to non-existent schema prototype: {}::{}'. \
                        format(self.name, name)
                if fail_cause is not None:
                    raise default(msg) from fail_cause
                else:
                    raise default(msg)
            else:
                prototype = default

        if (type is not None and
                isinstance(prototype, so.ProtoObject) and
                not isinstance(prototype, type)):
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
                prototype = default
        return prototype

    def match(self, name, module_aliases=None, type=None):
        name, module, nqname = sn.split_name(name)

        result = []

        if '%' in nqname:
            module = self.resolve_module(module, module_aliases)
            if not module:
                return None

            pattern = re.compile(re.escape(nqname).replace('\%', '.*'))
            index = self.index_by_name

            for name, obj in index.items():
                if pattern.match(name):
                    if type and isinstance(obj, type):
                        result.append(obj)

        else:
            result = self.get(name, module_aliases=module_aliases, type=type,
                              default=None)
            if result:
                result = [result]

        return result

    def reorder(self, new_order):
        name_order = [self.get_object_key(p) for p in new_order]

        # The new_order may be partial, as the most common source
        # is schema enumeration, which may filter out certain objects.
        #
        def sortkey(item):
            try:
                return name_order.index(item)
            except ValueError:
                return -1

        self.index = OrderedSet(sorted(self.index, key=sortkey))
        for typeindex in self.index_by_type.values():
            sortedindex = sorted(typeindex, key=sortkey)
            typeindex.clear()
            typeindex.update(sortedindex)

    def __contains__(self, obj):
        return obj in self.index

    def __iter__(self):
        return ProtoSchemaIterator(self, None)

    def __call__(self, type=None, include_derived=False):
        return ProtoSchemaIterator(self, type, include_derived=include_derived)

    def normalize(self, imports):
        "Revert reference reductions made by __getstate__ methods of prototype"

        modules = {m.__name__: m.__sx_prototypes__ for m in imports
                   if hasattr(m, '__sx_prototypes__')}

        modules[self.name] = self

        objects = {}

        _resolve = lambda name: modules[name.module].get(name.name)
        for obj in self(include_derived=True):
            obj._finalize_setstate(objects, _resolve)

    def get_checksum(self):
        if self.index:
            objects = frozenset(self)
            checksum = persistent_hash(str(persistent_hash(objects)))
        else:
            checksum = persistent_hash(None)

        return checksum


class ProtoSchemaIterator:
    def __init__(self, index, type, include_derived=False):
        self.index = index
        self.type = type
        self.include_derived = include_derived

        sourceset = self.index.index

        if type is not None:
            if isinstance(type, so.PrototypeClass):
                type = type._type

            itertype = index.index_by_type.get(type)

            if itertype:
                sourceset = itertype
            else:
                sourceset = OrderedSet()

        filtered = self.filter_set(sourceset)
        self.iter = iter(filtered)

    def filter_set(self, sourceset):
        filtered = sourceset
        if not self.include_derived:
            filtered -= self.index.index_derived
        return filtered

    def __iter__(self):
        return self

    def __next__(self):
        result = self.index.get_object_by_key(next(self.iter))
        return result
