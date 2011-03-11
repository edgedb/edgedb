##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import builtins
import collections
import importlib
import operator
import re

from semantix import SemantixError
from semantix.utils import lang
from semantix.utils.datastructures import OrderedSet, ExtendedSet
from semantix.utils import abc
from semantix.utils.functional import hybridmethod

from .error import SchemaError
from .name import SchemaName


class ImportContext(lang.ImportContext):
    def __new__(cls, name, *, protoschema=None, toplevel=False, builtin=False, private=None):
        result = super(ImportContext, cls).__new__(cls, name)
        result.protoschema = protoschema if protoschema else cls.new_schema(builtin)
        result.protoschema.add_module(name, name)
        result.toplevel = toplevel
        result.builtin = builtin
        result.private = private
        return result

    def __init__(self, name, *, protoschema=None, toplevel=False, builtin=False, private=None):
        pass

    @classmethod
    def from_parent(cls, name, parent):
        if parent and isinstance(parent, ImportContext):
            result = cls(name, protoschema=parent.protoschema, toplevel=False, builtin=parent.builtin)
            result.protoschema.add_module(name, name)
        else:
            result = cls(name, toplevel=True)
        return result

    @classmethod
    def copy(cls, name, other):
        if isinstance(other, ImportContext):
            result = cls(other, protoschema=other.protoschema, toplevel=other.toplevel,
                                builtin=other.builtin, private=other.private)
        else:
            result = cls(other)
        return result

    @classmethod
    def new_schema(cls, builtin):
        return (BuiltinProtoSchema() if builtin else ProtoSchema())


default_err = object()


class PrototypeSet(ExtendedSet):
    def __init__(self, *args, key=operator.attrgetter('name'), **kwargs):
        super().__init__(*args, key=key, **kwargs)


class PrototypeClass(type):
    pass


class ProtoObject(metaclass=PrototypeClass):
    @classmethod
    def get_canonical_class(cls):
        return cls

    @classmethod
    def is_prototype(cls, name):
        if isinstance(name, ProtoObject):
            return True
        elif isinstance(name, (str, SchemaName)):
            mod, _, name = str(name).rpartition('.')
            try:
                getattr(importlib.import_module(mod), name)
            except (ImportError, AttributeError):
                return True

            return False


class Namespace:
    def __init__(self, index):
        self.index_by_name = collections.OrderedDict()
        self.index_by_module = collections.OrderedDict()
        self.index = index

    def __contains__(self, obj):
        return obj.name in self.index_by_name

    def __iter__(self):
        return iter(self.index_by_name.items())

    def add(self, obj):
        idx_by_mod = self.index_by_module.setdefault(obj.name.module, collections.OrderedDict())
        idx_by_mod[obj.name.name] = obj

        self.index_by_name[obj.name] = obj

    def discard(self, obj):
        existing = self.index_by_name.pop(obj.name, None)
        if existing:
            self.index_by_module[existing.name.module].pop(existing.name.name, None)
        return existing

    def match(self, module, nqname, type):
        result = []
        pattern = re.compile(re.escape(nqname).replace('\%', '.*'))
        index = self.index_by_module.get(module)

        for name, obj in index.items():
            if pattern.match(name):
                if type and isinstance(obj, type):
                    result.append(obj)
        return result

    def lookup_name(self, name, module_aliases=None, default=default_err):
        name = self.normalize_name(name, module_aliases, default=default)
        if name:
            return self.lookup_qname(name)

    def lookup_qname(self, name):
        return self.index_by_name.get(name)

    def normalize_name(self, name, module_aliases=None, default=default_err,
                             include_pyobjects=False):
        name, module, nqname = self.split_name(name)
        norm_name = None

        if module is None:
            object = None
            default_module = self.index.resolve_module(module, module_aliases)

            if default_module:
                fq_name = self.index.SchemaName(name=nqname, module=default_module)
                object = self.lookup_qname(fq_name)
            if not object:
                fq_name = self.index.SchemaName(name=nqname, module=self.index.builtins_module)
                object = self.lookup_qname(fq_name)
            if object:
                norm_name = object.name
        else:
            object = self.lookup_qname(name)

            if object:
                norm_name = object.name
            else:
                fullmodule = self.index.resolve_module(module, module_aliases)
                if not fullmodule:
                    if module in self.index.rmodules or \
                                 (module_aliases and module in module_aliases.values()):
                        fullmodule = module

                if fullmodule:
                    norm_name = self.index.SchemaName(name=nqname, module=fullmodule)

        if not norm_name and include_pyobjects and module:
            try:
                getattr(importlib.import_module(module), nqname)
                norm_name = name
            except (ImportError, AttributeError) as e:
                pass

        if norm_name:
            return norm_name
        else:
            if default is default_err:
                default = self.index.SchemaError

            if default and issubclass(default, Exception):
                raise default('could not normalize caos name %s' % name)
            else:
                return default

    @hybridmethod
    def split_name(self, name):
        _SchemaName = self.index.SchemaName if isinstance(self, Namespace) else SchemaName

        if isinstance(name, SchemaName):
            module = name.module
            nqname = name.name
        elif isinstance(name, tuple):
            module = name[0]
            nqname = name[1]
            name = module + '.' + nqname if module else nqname
        elif SchemaName.is_qualified(name):
            name = _SchemaName(name)
            module = name.module
            nqname = name.name
        else:
            module = None
            nqname = name

        return name, module, nqname


class ProtoSchemaIterator:
    def __init__(self, index, type, include_builtin=False):
        self.index = index
        self.type = type
        self.include_builtin = include_builtin

        sourceset = self.index.index

        if type is not None:
            itertype = index.index_by_type.get(type)

            if itertype:
                sourceset = itertype
            else:
                sourceset = OrderedSet()

        filtered = self.filter_set(sourceset)
        self.iter = iter(filtered)

    def filter_set(self, sourceset):
        filtered = sourceset
        if not self.include_builtin:
            filtered = filtered - self.index.index_builtin

        return filtered

    def __iter__(self):
        return self

    def __next__(self):
        result = self.index.get_object_by_key(next(self.iter))
        return result


class ProtoSchema:

    def get_builtins_module(self):
        raise NotImplementedError

    def get_schema_error(self):
        return SchemaError

    def get_import_context(self):
        return ImportContext

    def get_schema_name(self):
        return SchemaName

    def get_object_key(self, obj):
        return obj.__class__.get_canonical_class(), obj.name

    def get_object_by_key(self, key):
        cls, name = key
        obj = self.get_namespace(cls).lookup_qname(name)
        assert obj, 'Could not find "%s" object named "%s"' % (cls, name)
        return obj

    def __init__(self, load_builtins=True, main_module=None):
        self.index = OrderedSet()
        self.index_by_type = {}
        self.index_builtin = OrderedSet()

        self.namespaces = {}
        self.modules = {}

        self.main_module = main_module
        self.rmodules = set()
        self.checksum = None
        self.SchemaError = self.get_schema_error()
        self.builtins_module = self.get_builtins_module()
        self.ImportContext = self.get_import_context()
        self.SchemaName = self.get_schema_name()

        if load_builtins and self.builtins_module:
            self._init_builtin()

    def _init_builtin(self):
        module = self.ImportContext(name=self.builtins_module, toplevel=True, builtin=True)
        builtins = importlib.import_module(module)
        for key in builtins._index_.index:
            self.add(builtins._index_.get_object_by_key(key))

    def add(self, obj):
        ns = self.get_namespace(obj)
        if obj in ns:
            raise self.SchemaError('object named "%s" is already present in the schema' % obj.name)

        # Invalidate checksum
        self.checksum = None

        ns.add(obj)

        key = self.get_object_key(obj)
        self.index.add(key)

        idx_by_type = self.index_by_type.setdefault(obj._type, OrderedSet())
        idx_by_type.add(key)

        if obj.name.module == self.builtins_module:
            self.index_builtin.add(key)

    def discard(self, obj):
        existing = self.get_namespace(obj).discard(obj)
        if existing:
            self._delete(existing)
        return existing

    def delete(self, obj):
        existing = self.discard(obj)
        if existing is None:
            raise self.SchemaError('object "%s" is not present in the index' % obj.name)

        return existing

    def replace(self, obj):
        existing = self.get_namespace(obj).discard(obj)
        if existing:
            self._delete(existing)
        self.add(obj)

    def _delete(self, obj):
        key = self.get_object_key(obj)
        self.index.remove(key)
        self.index_builtin.discard(key)
        self.index_by_type[obj.__class__._type].remove(key)

    def add_module(self, module, alias):
        existing = self.modules.get(alias)
        if existing and existing != module:
            raise self.SchemaError('Alias %s is already bound to module %s' %
                                   (alias, self.modules[alias]))
        else:
            self.modules[alias] = module
            self.rmodules.add(module)

    def get(self, name, default=default_err, module_aliases=None, type=None, include_pyobjects=False):
        if isinstance(type, tuple):
            for typ in type:
                ns = self.get_namespace(typ)
                obj = ns.lookup_name(name, module_aliases, default=None)
                if obj:
                    break
        else:
            ns = self.get_namespace(type)
            obj = ns.lookup_name(name, module_aliases, default=None)

        if not obj and include_pyobjects:
            module_name, attrname = str(name).rpartition('.')[::2]
            if module_name:
                try:
                    obj = getattr(importlib.import_module(module_name), attrname)
                except (ImportError, AttributeError):
                    pass

        if default is default_err:
            default = self.SchemaError

        if not obj:
            raise_ = default and (isinstance(default, Exception) or \
                     (isinstance(default, builtins.type) and issubclass(default, Exception)))
            if raise_:
                raise default('reference to a non-existent schema prototype: %s' % name)
            else:
                obj = default

        if type and isinstance(obj, ProtoObject) and not isinstance(obj, type):
            raise_ = default and (isinstance(default, Exception) or \
                     (isinstance(default, builtins.type) and issubclass(default, Exception)))
            if raise_:
                if isinstance(type, tuple):
                    typname = ' or '.join(typ.__name__ for typ in type)
                else:
                    typname = type.__name__
                raise default('reference to a non-existent %s: %s' % (typname, name))
            else:
                obj = default
        return obj

    def match(self, name, module_aliases=None, type=None):
        name, module, nqname = Namespace.split_name(name)

        result = []

        if '%' in nqname:
            module = self.resolve_module(module, module_aliases)
            if not module:
                return None

            ns = self.get_namespace(type)
            result = ns.match(module, nqname, type)

        else:
            result = self.get(name, module_aliases=module_aliases, type=type, default=None)
            if result:
                result = [result]

        return result

    def __contains__(self, obj):
        return obj in self.index

    def __iter__(self):
        return ProtoSchemaIterator(self, None)

    def __call__(self, type=None, include_builtin=False):
        return ProtoSchemaIterator(self, type, include_builtin)


    def resolve_module(self, module, module_aliases):
        if module_aliases:
            if module:
                parts = str(module).split('.')
                aliased = module_aliases.get(parts[0])

                if aliased and len(parts) > 1:
                    aliased += '.' + '.'.join(parts[1:])
            else:
                aliased = module_aliases.get(module)

            if aliased:
                module = aliased
            else:
                module = self.modules.get(module)
        else:
            module = self.modules.get(module)
        return module

    def get_namespace(self, obj):
        if obj is None:
            ns = None
        elif getattr(obj, '_separate_namespace', False):
            ns = obj.get_canonical_class()
        else:
            ns = None
        return self.namespaces.setdefault(ns, Namespace(self))


class BuiltinProtoSchema(ProtoSchema):
    def _init_builtin(self):
        pass
