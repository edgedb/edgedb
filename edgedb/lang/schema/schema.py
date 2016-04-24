##
# Copyright (c) 2008-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import builtins
import collections
import re
import sys
import types

import importkit
from importkit.import_ import module as module_types

from metamagic.caos import classfactory

from metamagic.exceptions import MetamagicError
from metamagic.utils.datastructures import OrderedSet
from metamagic.utils.datastructures.struct import MixedStruct, MixedStructMeta
from metamagic.utils.datastructures import Void
from metamagic.utils.algos.persistent_hash import persistent_hash

from .error import SchemaError, NoPrototypeError
from . import name as schema_name


class ImportContext(importkit.ImportContext):
    @classmethod
    def new_schema(cls, builtin):
        return ProtoSchema()


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
            return super().__getattribute__(name)

        try:
            proto = self.__sx_prototypes__.get(name)
        except SchemaError:
            raise AttributeError('{!r} object has no attribute {!r}'.
                                    format(self, name))

        schema = get_loaded_proto_schema(self.__class__)

        try:
            cls = proto(schema, cache=False)
        except Exception as e:
            err = 'could not create class from prototype'
            raise MetamagicError(err) from e

        setattr(self, name, cls)
        return cls

    @classmethod
    def get_schema_class(cls):
        return ProtoSchema


default_err = object()


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
    def is_prototype(cls, proto_schema, name):
        if isinstance(name, ProtoObject):
            return True
        else:
            obj = proto_schema.get(name, include_pyobjects=True, index_only=False)
            return isinstance(obj, ProtoObject)


class PrototypeMeta(PrototypeClass, MixedStructMeta):
    pass


class Prototype(MixedStruct, ProtoObject, metaclass=PrototypeMeta):
    pass


class ProtoSchemaIterator:
    def __init__(self, index, type, include_derived=False):
        self.index = index
        self.type = type
        self.include_derived = include_derived

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
        if not self.include_derived:
            filtered -= self.index.index_derived
        return filtered

    def __iter__(self):
        return self

    def __next__(self):
        result = self.index.get_object_by_key(next(self.iter))
        return result


class ProtoModule:
    def get_object_key(self, obj):
        return obj.__class__.get_canonical_class(), obj.name

    def get_object_by_key(self, key):
        cls, name = key
        obj = self.lookup_qname(name)
        assert obj, 'Could not find "%s" object named "%s"' % (cls, name)
        return obj

    def __init__(self, name):
        self.name = name
        self.index = OrderedSet()
        self.index_by_name = collections.OrderedDict()
        self.index_by_type = {}
        self.index_derived = OrderedSet()

    def copy(self):
        result = self.__class__()
        for obj in self:
            result.add(obj.copy())

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
            raise SchemaError('object "%s" is not present in the index' % obj.name)

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

    def get(self, name, default=default_err, module_aliases=None, type=None,
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

            fq_name = '{}.{}'.format(self.name, name)
            prototype = self.lookup_qname(fq_name)

            if type is not None and issubclass(type, ProtoObject):
                type = type.get_canonical_class()

        if default is default_err:
            default = SchemaError

        raise_ = None

        if prototype is None:
            if default is not None:
                raise_ = (isinstance(default, Exception) or
                            (isinstance(default, builtins.type) and
                             issubclass(default, Exception)))

            if raise_:
                msg = 'reference to non-existent schema prototype: {}.{}'. \
                        format(self.name, name)
                if fail_cause is not None:
                    raise default(msg) from fail_cause
                else:
                    raise default(msg)
            else:
                prototype = default

        if (type is not None and isinstance(prototype, ProtoObject) and
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
        name, module, nqname = schema_name.split_name(name)

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
        "Revert reference reductions made by __getstate__ methods of prototypes"

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


class DummyModule(types.ModuleType):
    def __getattr__(self, name):
        return type


class ProtoSchema(classfactory.ClassCache, classfactory.ClassFactory):
    global_dep_order = ('action', 'event', 'attribute', 'constraint',
                        'atom', 'link_property', 'link', 'concept')

    """ProtoSchema is a collection of ProtoModules"""

    @classmethod
    def get_builtins_module(cls):
        return 'metamagic.caos.builtins'

    def __init__(self):
        classfactory.ClassCache.__init__(self)

        self.modules = collections.OrderedDict()
        self.foreign_modules = collections.OrderedDict()
        self.module_aliases = {}
        self.module_aliases_r = {}

        self.builtins_module = self.get_builtins_module()

        self._policy_schema = None
        self._virtual_inheritance_cache = {}
        self._inheritance_cache = {}

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
            self.foreign_modules[name] = module_types.AutoloadingLightProxyModule(name, proto_module)

        if alias is not Void:
            self.set_module_alias(name, alias)

        self._policy_schema = None

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
            module_name = proto_module.name

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
            raise SchemaError('module {} is not in this schema'.format(obj.name.module)) from e

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
            raise SchemaError('module {} is not in this schema'.format(obj.name.module)) from e

        return module.delete(obj)

    def clear(self):
        self.modules.clear()
        self.foreign_modules.clear()
        self.module_aliases.clear()
        self.module_aliases_r.clear()
        self.clear_class_cache()
        self._virtual_inheritance_cache.clear()
        self._inheritance_cache.clear()
        self._policy_schema = None

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
                  include_pyobjects=False, index_only=True,
                  implicit_builtins=True):

        name, module, nqname = schema_name.split_name(name)

        fq_module = None

        if module_aliases is not None:
            fq_module = self.module_name_by_alias(module, module_aliases)

        if fq_module is None:
            fq_module = self.module_name_by_alias(module, self.module_aliases)

        if fq_module is not None:
            module = fq_module

        if default is default_err:
            default = SchemaError
            default_raise = True
        else:
            if default is not None and \
                    (isinstance(default, Exception) or \
                     (isinstance(default, builtins.type) and issubclass(default, Exception))):
                default_raise = True
            else:
                default_raise = False

        errmsg = 'reference to a non-existent schema prototype: {}'.format(name)

        if module is None:
            if implicit_builtins:
                proto_module = self.modules[self.get_builtins_module()]
                result = proto_module.get(nqname, default=None, type=type,
                                          index_only=index_only)
                if result is not None:
                    return result

            if default_raise:
                raise default(errmsg)
            else:
                return default

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
                else:
                    try:
                        proto_module = sys.modules[proto_module.__name__]
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
                    result = proto_module.get(nqname, default=default,
                                              type=type,
                                              index_only=index_only)
                except default:
                    if not implicit_builtins:
                        raise
                    else:
                        proto_module = self.modules[self.get_builtins_module()]
                        result = proto_module.get(nqname, default=None,
                                                  type=type,
                                                  index_only=index_only)
                        if result is None:
                            raise
            else:
                result = proto_module.get(nqname, default=default, type=type,
                                          include_pyobjects=include_pyobjects,
                                          index_only=index_only)
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

    def update_virtual_inheritance(self, proto, children):
        try:
            proto_children = self._virtual_inheritance_cache[proto.name]
        except KeyError:
            proto_children = self._virtual_inheritance_cache[proto.name] = set()

        proto_children.update(c.name for c in children if c is not proto)
        proto._virtual_children = set(children)

    def drop_inheritance_cache(self, proto):
        self._inheritance_cache.pop(proto.name, None)

    def drop_inheritance_cache_for_child(self, proto):
        bases = getattr(proto, 'bases', ())

        for base in bases:
            try:
                children = self._inheritance_cache[base.name]
            except KeyError:
                pass
            else:
                children.discard(proto.name)

    def _get_descendants(self, proto, *, max_depth=None, depth=0):
        result = set()

        try:
            children = proto._virtual_children
        except AttributeError:
            try:
                child_names = self._inheritance_cache[proto.name]
            except KeyError:
                child_names = self._inheritance_cache[proto.name] = \
                                    self._find_children(proto)
        else:
            child_names = [c.name for c in children]

        canonical_class = proto.get_canonical_class()
        children = {self.get(n, type=canonical_class) for n in child_names}

        if max_depth is not None and depth < max_depth:
            for child in children:
                result.update(self._get_descendants(
                        child, max_depth=max_depth, depth=depth+1))

        result.update(children)
        return result

    def _find_children(self, proto):
        flt = lambda p: p.issubclass(proto) and proto is not p
        return {c.name for c in filter(flt, self(proto._type))}

    def get_root_class(self, cls):
        from metamagic import caos
        import metamagic.caos.types

        if issubclass(cls, caos.types.ProtoConcept):
            name = 'metamagic.caos.builtins.BaseObject'
        elif issubclass(cls, caos.types.ProtoLink):
            name = 'metamagic.caos.builtins.link'
        elif issubclass(cls, caos.types.ProtoLinkProperty):
            name = 'metamagic.caos.builtins.link_property'
        else:
            assert False, 'get_root_class: unexpected object type: %r' % type

        return self.get(name, type=cls)

    def get_class(self, name, module_aliases=None):
        proto = self.get(name, module_aliases=module_aliases)
        return proto(self)

    def get_event_policy(self, subject_proto, event_proto):
        from metamagic.caos import proto

        if self._policy_schema is None:
            self._policy_schema = proto.PolicySchema()

            for policy in self('policy'):
                self._policy_schema.add(policy)

            for link in self('link'):
                link.materialize_policies(self)

            for concept in self('concept'):
                concept.materialize_policies(self)

        return self._policy_schema.get(subject_proto, event_proto)

    def get_checksum(self):
        c = []
        for n, m in self.modules.items():
            c.append((n, m.get_checksum()))

        return persistent_hash(frozenset(c))

    def get_checksum_details(self):
        objects = list(sorted(self, key=lambda e: e.name))
        return [(str(o.name), persistent_hash(o)) for o in objects]

    def __iter__(self):
        yield from self()

    def __call__(self, type=None):
        for mod in self.modules.values():
            for proto in mod(type=type):
                yield proto

    def __eq__(self, other):
        if not isinstance(other, ProtoSchema):
            return NotImplemented

        return self.get_checksum() == other.get_checksum()

    def __hash__(self):
        return self.get_checksum()


def populate_proto_modules(schema, module):
    process_subimports = True

    if not schema.has_module(module.__name__):
        try:
            proto_module = module.__sx_prototypes__
        except AttributeError:
            schema.add_module(module)
            process_subimports = False
        else:
            schema.add_module(proto_module)

    if process_subimports:
        try:
            subimports = module.__sx_imports__
        except AttributeError:
            pass
        else:
            for subimport in subimports:
                submodule = sys.modules[subimport]
                populate_proto_modules(schema, submodule)


def get_global_proto_schema():
    return get_loaded_proto_schema(SchemaModule)
