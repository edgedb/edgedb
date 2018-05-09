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


import collections
import typing

from edgedb.lang.common.persistent_hash import persistent_hash

from . import error as s_err
from . import modules as s_modules
from . import name as schema_name


_void = object()


class TypeContainer:
    pass


class Schema(TypeContainer):
    global_dep_order = ('action', 'event', 'attribute', 'constraint',
                        'ScalarType', 'link_property', 'link', 'ObjectType')

    """Schema is a collection of ProtoModules."""

    def __init__(self):
        self.modules = collections.OrderedDict()
        self.deltas = collections.OrderedDict()

        self._policy_schema = None
        self._virtual_inheritance_cache = {}
        self._inheritance_cache = {}

    def copy(self):
        result = type(self)()
        result.modules = collections.OrderedDict((
            (name, mod.copy()) for name, mod in self.modules.items()))
        result.deltas = self.deltas.copy()
        return result

    def add_module(self, class_module):
        """Add a module to the schema

        :param Module class_module: A module that should be added
                                    to the schema.
        """

        name = class_module.name
        self.modules[name] = class_module
        self._policy_schema = None

    def get_module(self, module):
        return self.modules[module]

    def get_modules(self):
        return self.modules.values()

    def delete_module(self, class_module):
        """Remove a module from the schema

        :param class_module: Either a string name of the module or a Module
                             object that should be dropped from the schema.
        """
        if isinstance(class_module, str):
            module_name = class_module
        else:
            module_name = class_module.name

        del self.modules[module_name]

    def add_delta(self, delta):
        """Add a delta to the schema.

        :param Delta delta: Delta object to add to the schema.
        """
        name = delta.name
        self.deltas[name] = delta

    def get_delta(self, name):
        return self.deltas[name]

    def delete_delta(self, delta):
        """Remove the delta from the schema.

        :param name: Either a string name of the delta or a Delta object
                     thet should be dropped from the schema.
        """
        if isinstance(delta, str):
            delta_name = delta
        else:
            delta_name = delta.name

        del self.deltas[delta_name]

    def add(self, obj):
        try:
            module = self.modules[obj.name.module]
        except KeyError as e:
            raise s_err.SchemaModuleNotFoundError(
                f'module {obj.name.module!r} is not in this schema') from e

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
            raise s_err.SchemaModuleNotFoundError(
                f'module {obj.name.module} is not in this schema') from e

        return module.delete(obj)

    def clear(self):
        self.modules.clear()
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

    def _resolve_module(self, module_name) -> typing.List[s_modules.Module]:
        modules = []

        if module_name is not None:
            module = self.modules.get(module_name)
            if module is not None:
                modules.append(module)

        return modules

    def _get(self, name, *, getter, default, module_aliases):
        name, module, nqname = schema_name.split_name(name)
        implicit_builtins = module is None

        if module_aliases is not None:
            # Alias has a priority over `self.modules` lookup.
            fq_module = module_aliases.get(module)
            if fq_module is not None:
                module = fq_module

        class_modules = self._resolve_module(module)

        if class_modules:
            result = getter(class_modules, nqname)
            if result is not None:
                return result

        if implicit_builtins:
            std = self.modules['std']
            result = getter([std], nqname)
            if result is not None:
                return result

        return default

    def get_functions(self, name, default=_void, *, module_aliases=None):
        def getter(modules, name):
            for module in modules:
                result = module.get_functions(name)
                if result:
                    return result

        funcs = self._get(name,
                          getter=getter,
                          module_aliases=module_aliases,
                          default=default)

        if funcs is not _void:
            return funcs

        raise s_err.ItemNotFoundError(
            f'reference to a non-existent function: {name}')

    def get(self, name, default=_void, *, module_aliases=None, type=None):
        def getter(modules, name):
            for module in modules:
                result = module.get(name, default=None, type=type)
                if result is not None:
                    return result

        obj = self._get(name,
                        getter=getter,
                        module_aliases=module_aliases,
                        default=default)

        if obj is not _void:
            return obj

        raise s_err.ItemNotFoundError(
            f'reference to a non-existent schema item: {name}')

    def has_module(self, module):
        return module in self.modules

    def has_class(self, name):
        return self.get(name, default=None) is not None

    def update_virtual_inheritance(self, scls, children):
        try:
            class_children = self._virtual_inheritance_cache[scls.name]
        except KeyError:
            class_children = self._virtual_inheritance_cache[scls.name] = set()

        class_children.update(c.name for c in children if c is not scls)
        scls._virtual_children = set(children)

    def drop_inheritance_cache(self, scls):
        self._inheritance_cache.pop(scls.name, None)

    def drop_inheritance_cache_for_child(self, scls):
        bases = getattr(scls, 'bases', ())

        for base in bases:
            self._inheritance_cache.pop(base.name, None)

    def _get_descendants(self, scls, *, max_depth=None, depth=0):
        result = set()

        try:
            children = scls._virtual_children
        except AttributeError:
            try:
                child_names = self._inheritance_cache[scls.name]
                raise KeyError
            except KeyError:
                child_names = self._find_children(scls)
                self._inheritance_cache[scls.name] = child_names
        else:
            child_names = [c.material_type().name for c in children]

        canonical_class = scls.get_canonical_class()
        children = {self.get(n, type=canonical_class) for n in child_names}

        if max_depth is not None and depth < max_depth:
            for child in children:
                result.update(self._get_descendants(
                    child, max_depth=max_depth, depth=depth + 1))

        result.update(children)
        return result

    def _find_children(self, scls):
        flt = lambda p: scls in p.bases
        it = self.get_objects(type=scls._type)
        return {c.name for c in filter(flt, it)}

    def get_event_policy(self, subject_class, event_class):
        from . import policy as spol

        if self._policy_schema is None:
            self._policy_schema = spol.PolicySchema()

            for policy in self.get_objects(type='policy'):
                self._policy_schema.add(policy)

            for link in self.get_objects(type='link'):
                link.materialize_policies(self)

            for objtype in self.get_objects(type='ObjectType'):
                objtype.materialize_policies(self)

        return self._policy_schema.get(subject_class, event_class)

    def get_checksum(self):
        c = []
        for n, m in self.modules.items():
            c.append((n, m.get_checksum()))

        return persistent_hash(frozenset(c))

    def get_checksum_details(self):
        objects = list(sorted(self, key=lambda e: e.name))
        return [(str(o.name), persistent_hash(o)) for o in objects]

    def get_objects(self, *, type=None, include_derived=False):
        for mod in self.get_modules():
            for scls in mod.get_objects(type=type,
                                        include_derived=include_derived):
                yield scls

    def get_overlay(self, extra=None):
        return SchemaOverlay(self, extra=extra)


class SchemaOverlay(Schema):
    def __init__(self, schema, extra=None):
        self.schema = schema
        self.local_modules = collections.OrderedDict()
        self.modules = collections.ChainMap(self.local_modules, schema.modules)
        self.deltas = collections.OrderedDict()

        self._policy_schema = None
        self._local_vic = {}
        self._virtual_inheritance_cache = collections.ChainMap(
            self._local_vic, schema._virtual_inheritance_cache)
        self._local_ic = {}
        self._inheritance_cache = collections.ChainMap(
            self._local_ic, schema._inheritance_cache)

        if extra:
            for v in extra.values():
                if hasattr(v, '_type'):
                    self.add(v)

    def has_module(self, module):
        return module in self.modules

    def get_modules(self):
        yield from self.local_modules.values()
        yield from self.schema.get_modules()

    def add(self, obj):
        if obj.name.module not in self.local_modules:
            self.local_modules[obj.name.module] = s_modules.Module(
                name=obj.name.module)
        super().add(obj)

    def _resolve_module(self, module_name) -> typing.List[s_modules.Module]:
        modules = []
        if module_name is not None:
            local_module = self.local_modules.get(module_name)
            if local_module is not None:
                modules.append(local_module)

            their_modules = self.schema._resolve_module(module_name)
            if their_modules:
                for their_module in their_modules:
                    if their_module is not local_module:
                        modules.append(their_module)

        return modules
