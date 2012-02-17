##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import builtins
import collections
import importlib
import itertools
import operator
import re
import sys
import types

from semantix import SemantixError
from semantix.utils import lang
from semantix.utils.datastructures import OrderedSet, ExtendedSet
from semantix.utils import abc
from semantix.utils.functional import hybridmethod
from semantix.utils.datastructures.struct import Struct, StructMeta, Field
from semantix.utils.datastructures import Void
from semantix.utils import helper

from .error import SchemaError, NoPrototypeError
from .name import SchemaName


class ImportContext(lang.ImportContext):
    pass


_schemas = {}


def get_loaded_proto_schema(module_class):
    try:
        schema = _schemas[module_class]
    except KeyError:
        schema = _schemas[module_class] = module_class.get_schema_class()()

    return schema


def drop_loaded_proto_schema(module_class, unload_modules=True):
    try:
        schema = _schemas[module_class]
    except KeyError:
        pass
    else:
        for module in schema.iter_modules():
            try:
                del sys.modules[module]
            except KeyError:
                pass

        schema.clear()


class SchemaModule(types.ModuleType):
    def __sx_finalize_load__(self):
        schema = get_loaded_proto_schema(self.__class__)
        populate_proto_modules(schema, self)

    def __getattr__(self, name):
        if name.startswith('__') and name.endswith('__'):
            return super().__getattr__(name)

        protoname = name
        nsname = None

        if name.startswith('_ns_') and len(name) > 4:
            try:
                prefix_len_end = name.index('_', 4)
            except ValueError:
                pass
            else:
                prefix_len = name[4:prefix_len_end]

                try:
                    prefix_len = int(prefix_len)
                except ValueError:
                    pass
                else:
                    protoname = name[prefix_len_end + prefix_len + 2:]
                    nsname = name[prefix_len_end + 1:prefix_len_end + prefix_len + 1]

        proto = self.__sx_prototypes__.get(protoname, nsname=nsname)
        schema = get_loaded_proto_schema(self.__class__)
        cls = proto(schema, cache=False)
        setattr(self, name, cls)
        return cls

    @classmethod
    def get_schema_class(cls):
        return ProtoSchema


default_err = object()


class attrgetter:
    def __new__(cls, *attrs):
        result = super().__new__(cls)
        result.getter = operator.attrgetter(*attrs)
        result.attrs = attrs
        return result

    def __getstate__(self):
        return {'attrs': self.attrs}

    def __getnewargs__(self):
        return self.attrs

    def __call__(self, obj):
        return self.getter(obj)


class PrototypeSet(ExtendedSet):
    def __init__(self, *args, key=attrgetter('name'), **kwargs):
        super().__init__(*args, key=key, **kwargs)


class PrototypeClass(type):
    pass


class ObjectClass(type):
    pass


class ProtoObject(metaclass=PrototypeClass):
    @classmethod
    def get_canonical_class(cls):
        return cls

    @classmethod
    def _get_prototype(cls, obj):
        try:
            proto = object.__getattribute__(obj, '__sx_prototype__')
        except AttributeError as e:
            raise NoPrototypeError('{!r} does not have a valid prototype'.format(obj)) from e
        else:
            if not isinstance(proto, ProtoObject):
                raise NoPrototypeError('{!r} does not have a valid prototype'.format(obj))
        return proto

    @classmethod
    def load_prototype(cls, proto_schema, obj_name):
        mod, _, name = str(obj_name).rpartition('.')
        ns = proto_schema.get_namespace(cls)
        name = ns.prefix_name(name, dir(types.ModuleType))

        try:
            obj = getattr(importlib.import_module(mod), name)
        except (ImportError, AttributeError) as e:
            raise NoPrototypeError('could not load {}'.format(obj_name)) from e
        else:
            return cls._get_prototype(obj)

    @classmethod
    def is_prototype(cls, proto_schema, name):
        if isinstance(name, ProtoObject):
            return True
        else:
            obj = proto_schema.get(name, include_pyobjects=True, index_only=False)
            return isinstance(obj, ProtoObject)


class PrototypeMeta(PrototypeClass, StructMeta):
    pass


class Prototype(Struct, ProtoObject, metaclass=PrototypeMeta):
    pass


class Namespace:
    def __init__(self, index, name=None):
        self.index_by_name = collections.OrderedDict()
        self.index_by_module = collections.OrderedDict()
        self.index = index
        self.name = name

    def __contains__(self, obj):
        return obj.name in self.index_by_name

    def __iter__(self):
        return iter(self.index_by_name.items())

    def iter_modules(self):
        return iter(self.index_by_module)

    def has_module(self, module):
        return module in self.index_by_module

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

    def lookup_qname(self, name):
        return self.index_by_name.get(name)

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

    def prefix_name(self, name, reserved_names=None):
        if reserved_names is not None:
            name = helper.get_safe_attrname(name, reserved_names)

        if self.name:
            return '_ns_{}_{}_{}'.format(len(self.name), self.name, name)
        else:
            return name


class ProtoSchemaIterator:
    def __init__(self, index, type):
        self.index = index
        self.type = type

        sourceset = self.index.index

        if type is not None:
            if isinstance(type, PrototypeClass):
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
        return filtered

    def __iter__(self):
        return self

    def __next__(self):
        result = self.index.get_object_by_key(next(self.iter))
        return result


class ProtoModule:
    def get_schema_error(self):
        return SchemaError

    def get_import_context(self):
        return ImportContext

    def get_object_key(self, obj):
        return obj.__class__.get_canonical_class(), obj.name

    def get_object_by_key(self, key):
        cls, name = key
        obj = self.get_namespace(cls).lookup_qname(name)
        assert obj, 'Could not find "%s" object named "%s"' % (cls, name)
        return obj

    def __init__(self, name):
        self.name = name

        self.index = OrderedSet()
        self.index_by_type = {}

        self.namespaces = {}

        self.checksum = None

        self.SchemaError = self.get_schema_error()
        self.ImportContext = self.get_import_context()

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
        self.index_by_type[obj.__class__._type].remove(key)

    def get(self, name, default=default_err, module_aliases=None, type=None,
                  include_pyobjects=False, index_only=True, implicit_builtins=True,
                  nsname=None):
        if isinstance(type, tuple):
            for typ in type:
                try:
                    prototype = self.get(name, module_aliases=module_aliases,
                                         type=typ, include_pyobjects=include_pyobjects,
                                         index_only=index_only, default=None)
                except self.SchemaError:
                    pass
                else:
                    if prototype is not None:
                        break
        else:
            ns = self.get_namespace(type, name=nsname)

            fail_cause = None
            prototype = None

            fq_name = '{}.{}'.format(self.name, name)
            prototype = ns.lookup_qname(fq_name)

        if default is default_err:
            default = self.SchemaError

        raise_ = None

        if prototype is None:
            if default is not None:
                raise_ = (isinstance(default, Exception) or \
                            (isinstance(default, builtins.type) and issubclass(default, Exception)))

            if raise_:
                msg = 'reference to a non-existent schema prototype: {}.{}'.format(self.name, name)
                if fail_cause is not None:
                    raise default(msg) from fail_cause
                else:
                    raise default(msg)
            else:
                prototype = default

        if type is not None and isinstance(prototype, ProtoObject) and not isinstance(prototype, type):
            if default is not None:
                raise_ = (isinstance(default, Exception) or \
                          (isinstance(default, builtins.type) and issubclass(default, Exception)))
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

    def reorder(self, new_order):
        name_order = [self.get_object_key(p) for p in new_order]

        self.index = OrderedSet(sorted(self.index, key=name_order.index))
        for typeindex in self.index_by_type.values():
            sortedindex = sorted(typeindex, key=name_order.index)
            typeindex.clear()
            typeindex.update(sortedindex)

    def __contains__(self, obj):
        return obj in self.index

    def __iter__(self):
        return ProtoSchemaIterator(self, None)

    def __call__(self, type=None):
        return ProtoSchemaIterator(self, type)

    def get_namespace(self, obj, name=None):
        if obj is None:
            ns = None
        elif getattr(obj, '_separate_namespace', False):
            ns = obj.get_canonical_class()
        else:
            ns = None

        if name is not None:
            return self.namespaces[name]

        if ns is not None:
            nsname = ns.__name__
        else:
            nsname = None

        try:
            return self.namespaces[ns]
        except KeyError:
            result = Namespace(self, name=nsname)
            self.namespaces[ns] = result
            self.namespaces[nsname] = result
            return result

    def iter_modules(self):
        return itertools.chain.from_iterable(ns.iter_modules() for ns in self.namespaces.values())

    def has_module(self, module):
        for ns in self.namespaces.values():
            if ns.has_module(module):
                return True


class ProtoSchema:
    """ProtoSchema is a collection of ProtoModules"""

    @classmethod
    def get_schema_name(cls):
        return SchemaName

    @classmethod
    def get_builtins_module(cls):
        raise NotImplementedError

    @classmethod
    def get_schema_error(cls):
        return SchemaError

    def __init__(self):
        self.modules = collections.OrderedDict()
        self.foreign_modules = collections.OrderedDict()
        self.module_aliases = {}
        self.module_aliases_r = {}

        self.SchemaName = self.get_schema_name()
        self.SchemaError = self.get_schema_error()
        self.builtins_module = self.get_builtins_module()

    def add_module(self, proto_module, alias=Void):
        """Add a module to the schema

        :param ProtoModule proto_module: A module that should be added to the schema
        :param str alias: An optional alias for this module to use when resolving names
        """

        if isinstance(proto_module, ProtoModule):
            name = proto_module.name
            self.modules[name] = proto_module
        else:
            name = proto_module.__name__
            self.foreign_modules[name] = proto_module

        if alias is not Void:
            self.set_module_alias(name, alias)

    def set_module_alias(self, module_name, alias):
        self.module_aliases[alias] = module_name
        self.module_aliases_r[module_name] = alias

    def get_module(self, module):
        return self.modules[module]

    def delete_module(self, proto_module):
        """Remove a module from the schema

        :param proto_module: Either a string name of the module or a ProtoModule object
                             thet should be dropped from the schema.
        """
        if isinstance(proto_module, str):
            module_name = proto_module
        else:
            module_name = proto_module

        del self.modules[module_name]

        try:
            alias = self.module_aliases_r[module_name]
        except KeyError:
            pass
        else:
            del self.module_aliases_r[module_name]
            del self.module_aliases[alias]

    def add(self, obj):
        try:
            module = self.modules[obj.name.module]
        except KeyError as e:
            raise self.SchemaError('module {} is not in this schema'.format(obj.name.module)) from e

        module.add(obj)

    def discard(self, obj):
        try:
            module = self.modules[obj.name.module]
        except KeyError:
            return

        return module.discard(obj)

    def delete(self, obj):
        try:
            module = self.modules[obj.name.module]
        except KeyError as e:
            raise self.SchemaError('module {} is not in this schema'.format(obj.name.module)) from e

        return module.delete(obj)

    def clear(self):
        self.modules.clear()
        self.foreign_modules.clear()
        self.module_aliases.clear()
        self.module_aliases_r.clear()

    def reorder(self, new_order):
        by_module = {}

        for item in new_order:
            try:
                module_order = by_module[item.name.module]
            except KeyError:
                module_order = by_module[item.name.module] = []
            module_order.append(item)

        for module_name, module_order in by_module.items():
            module = self.modules[module_name]
            module.reorder(module_order)

    def module_name_by_alias(self, module, module_aliases):
        aliased = None

        if module:
            parts = str(module).split('.')
            aliased = module_aliases.get(parts[0])

            if aliased and len(parts) > 1:
                aliased += '.' + '.'.join(parts[1:])
        else:
            aliased = module_aliases.get(module)

        return aliased

    def get(self, name, default=default_err, module_aliases=None, type=None,
                  include_pyobjects=False, index_only=True, implicit_builtins=True,
                  nsname=None):

        name, module, nqname = Namespace.split_name(name)

        fq_module = None

        if module_aliases is not None:
            fq_module = self.module_name_by_alias(module, module_aliases)

        if fq_module is None:
            fq_module = self.module_name_by_alias(module, self.module_aliases)

        if fq_module is not None:
            module = fq_module

        if default is default_err:
            default = self.SchemaError
            default_raise = True
        else:
            if default is not None and \
                    (isinstance(default, Exception) or \
                     (isinstance(default, builtins.type) and issubclass(default, Exception))):
                default_raise = True
            else:
                default_raise = False

        errmsg = 'reference to a non-existent schema prototype: {}'.format(name)

        proto_module = None

        try:
            proto_module = self.modules[module]
        except KeyError as e:
            module_err = e

            if include_pyobjects:
                try:
                    proto_module = self.foreign_modules[module]
                except KeyError as e:
                    module_err = e


            if proto_module is None:
                if default_raise:
                    raise default(errmsg) from module_err
                else:
                    return default

        if isinstance(proto_module, ProtoModule):
            if default_raise:
                try:
                    result = proto_module.get(nqname, default=default, type=type,
                                              index_only=index_only,
                                              nsname=nsname)
                except default:
                    if not implicit_builtins:
                        raise
                    else:
                        proto_module = self.modules[self.get_builtins_module()]
                        result = proto_module.get(nqname, default=None, type=type,
                                                  index_only=index_only,
                                                  nsname=nsname)
                        if result is None:
                            raise
            else:
                result = proto_module.get(nqname, default=default, type=type,
                                          include_pyobjects=include_pyobjects, index_only=index_only,
                                          nsname=nsname)
        else:
            try:
                result = getattr(proto_module, nqname)
            except AttributeError as e:
                if default_raise:
                    raise default(errmsg) from e
                else:
                    result = default

        return result

    def iter_modules(self):
        return iter(self.modules)

    def has_module(self, module):
        return module in self.modules


def populate_proto_modules(schema, module):
    if not schema.has_module(module.__name__):
        try:
            proto_module = module.__sx_prototypes__
        except AttributeError:
            schema.add_module(module)
        else:
            schema.add_module(proto_module)

    try:
        subimports = module.__sx_imports__
    except AttributeError:
        pass
    else:
        for subimport in subimports:
            submodule = sys.modules[subimport]
            populate_proto_modules(schema, submodule)
