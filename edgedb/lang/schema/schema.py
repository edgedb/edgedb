##
# Copyright (c) 2008-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import builtins
import collections
import sys

from importkit.import_ import module as module_types

from metamagic.caos import classfactory

from metamagic.utils.datastructures import Void
from metamagic.utils.algos.persistent_hash import persistent_hash

from .error import SchemaError
from . import modules as schema_module
from . import name as schema_name


class ObjectClass(type):
    pass


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

        if isinstance(proto_module, schema_module.ProtoModule):
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

    def get(self, name, default=SchemaError, module_aliases=None, type=None,
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

        if default is not None and \
                (isinstance(default, Exception) or
                 (isinstance(default, builtins.type) and
                  issubclass(default, Exception))):
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

        if isinstance(proto_module, schema_module.ProtoModule):
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
        from . import concepts, lproperties, links

        if issubclass(cls, concepts.Concept):
            name = 'metamagic.caos.builtins.BaseObject'
        elif issubclass(cls, links.Link):
            name = 'metamagic.caos.builtins.link'
        elif issubclass(cls, lproperties.LinkProperty):
            name = 'metamagic.caos.builtins.link_property'
        else:
            assert False, 'get_root_class: unexpected object type: %r' % type

        return self.get(name, type=cls)

    def get_class(self, name, module_aliases=None):
        proto = self.get(name, module_aliases=module_aliases)
        return proto(self)

    def get_event_policy(self, subject_proto, event_proto):
        from . import policy as spol

        if self._policy_schema is None:
            self._policy_schema = spol.PolicySchema()

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
