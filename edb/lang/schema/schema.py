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

from . import error as s_err
from . import modules as s_modules
from . import name as schema_name


_void = object()


class TypeContainer:
    pass


class Schema(TypeContainer):
    global_dep_order = ('attribute', 'constraint',
                        'ScalarType', 'link_property',
                        'link', 'ObjectType')

    """Schema is a collection of ProtoModules."""

    def __init__(self):
        self.modules = collections.OrderedDict()
        self.deltas = collections.OrderedDict()

        self._policy_schema = None
        self._virtual_inheritance_cache = {}
        self._inheritance_cache = {}
        self._garbage = set()
        self.index_by_id = {}

    def add_module(self, mod) -> 'Schema':
        """Add a module to the schema

        :param Module mod: A module that should be added
                           to the schema.
        """

        name = mod.get_name(self)
        self.modules[name] = mod
        self._policy_schema = None
        return self

    def get_module(self, module):
        return self.modules.get(module)

    def get_modules(self):
        return self.modules.values()

    def delete_module(self, mod):
        """Remove a module from the schema

        :param mod: Either a string name of the module or a Module
                    object that should be dropped from the schema.
        """
        if isinstance(mod, str):
            module_name = mod
        else:
            module_name = mod.get_name(self)

        del self.modules[module_name]
        return self

    def _add_delta(self, delta) -> 'Schema':
        """Add a delta to the schema.

        :param Delta delta: Delta object to add to the schema.
        """
        name = delta.get_name(self)
        self.deltas[name] = delta
        return self

    def get_delta(self, name):
        return self.deltas[name]

    def delete_delta(self, delta) -> 'Schema':
        """Remove the delta from the schema.

        :param name: Either a string name of the delta or a Delta object
                     thet should be dropped from the schema.
        """
        if isinstance(delta, str):
            delta_name = delta
        else:
            delta_name = delta.get_name(self)

        del self.deltas[delta_name]

        return self

    def _rename(self, obj, oldname):
        # XXX temporary method, will go away when schema.Module
        # is refactored.

        oldmod = self.modules[oldname.module]
        oldmod.index_by_name.pop(oldname, None)
        oldmod.index_derived.discard(oldname)
        oldmod.index_by_type[obj._type].discard(oldname)

        newname = obj.get_name(self)
        newmod = self.modules[newname.module]
        newmod.index_by_name[newname] = obj
        newmod.index_derived.add(newname)
        newmod.index_by_type[obj._type].add(newname)

        return self

    def add(self, name, obj, *, _nameless=False) -> 'Schema':
        from . import deltas as s_deltas

        if isinstance(obj, s_modules.Module):
            self.modules[name] = obj
            return self
        elif isinstance(obj, s_deltas.Delta):
            self._add_delta(obj)
            return self

        self.index_by_id[obj.id] = obj

        if _nameless:
            return self
        else:
            try:
                module = self.modules[name.module]
            except KeyError as e:
                raise s_err.SchemaModuleNotFoundError(
                    f'module {name.module!r} is not in this schema') from e

            return module._add(self, name, obj)

    def mark_as_garbage(self, obj) -> 'Schema':
        try:
            module = self.modules[obj.get_name(self).module]
        except KeyError:
            return

        schema = module._discard(self, obj)
        self._garbage.add(obj.id)

        return schema

    def discard(self, obj) -> 'Schema':
        self.index_by_id.pop(obj.id, None)

        try:
            module = self.modules[obj.get_name(self).module]
        except KeyError:
            return

        schema = module._discard(self, obj)
        return schema

    def delete(self, obj) -> 'Schema':
        self.index_by_id.pop(obj.id, None)

        try:
            module = self.modules[obj.get_name(self).module]
        except KeyError as e:
            raise s_err.SchemaModuleNotFoundError(
                f'module {obj.get_name(self).module} '
                f'is not in this schema') from e

        schema = module._delete(self, obj)
        return schema

    def reorder(self, new_order) -> 'Schema':
        by_module = {}

        for item in new_order:
            try:
                module_order = by_module[item.get_name(self).module]
            except KeyError:
                module_order = by_module[item.get_name(self).module] = []
            module_order.append(item)

        schema = self
        for module_name, module_order in by_module.items():
            module = self.modules[module_name]
            schema = module.reorder(schema, module_order)

        return schema

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
            std = self._resolve_module('std')
            result = getter(std, nqname)
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

    def get_operators(self, name, default=_void, *, module_aliases=None):
        def getter(modules, name):
            for module in modules:
                result = module.get_operators(name)
                if result:
                    return result

        funcs = self._get(name,
                          getter=getter,
                          module_aliases=module_aliases,
                          default=default)

        if funcs is not _void:
            return funcs

        raise s_err.ItemNotFoundError(
            f'reference to a non-existent operator: {name}')

    def get_by_id(self, item_id, default=_void):
        try:
            return self.index_by_id[item_id]
        except KeyError:
            if default is _void:
                raise s_err.ItemNotFoundError(
                    f'reference to a non-existent schema item: {item_id}'
                    f' in schema {self!r}'
                ) from None
            else:
                return default

    def get(self, name, default=_void, *, module_aliases=None, type=None):
        def getter(modules, name):
            for module in modules:
                result = module.get(self, name, default=None, type=type)
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

    def drop_inheritance_cache(self, scls) -> 'Schema':
        self._inheritance_cache.pop(scls.get_name(self), None)
        return self

    def drop_inheritance_cache_for_child(self, scls) -> 'Schema':
        bases = scls.get_bases(self)

        for base in bases.objects(self):
            self._inheritance_cache.pop(base.get_name(self), None)

        return self

    def _get_descendants(self, scls, *, max_depth=None, depth=0):
        result = set()

        try:
            children = scls._virtual_children
        except AttributeError:
            try:
                child_names = self._inheritance_cache[scls.get_name(self)]
                raise KeyError
            except KeyError:
                child_names = self._find_children(scls)
                self._inheritance_cache[scls.get_name(self)] = child_names
        else:
            child_names = [c.material_type(self).get_name(self)
                           for c in children]

        children = {self.get(n, type=type(scls)) for n in child_names}

        if max_depth is not None and depth < max_depth:
            for child in children:
                result.update(self._get_descendants(
                    child, max_depth=max_depth, depth=depth + 1))

        result.update(children)
        return result

    def _find_children(self, scls):
        flt = lambda p: scls in p.get_bases(self).objects(self)
        it = self.get_objects(type=scls._type)
        return {c.get_name(self) for c in filter(flt, it)}

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

        self.index_by_id = {}
        self._garbage = set()
        self._policy_schema = None
        self._local_vic = {}
        self._virtual_inheritance_cache = collections.ChainMap(
            self._local_vic, schema._virtual_inheritance_cache)
        self._local_ic = {}
        self._inheritance_cache = collections.ChainMap(
            self._local_ic, schema._inheritance_cache)

        if extra:
            for n, v in extra.items():
                if hasattr(v, '_type'):
                    self.add(n, v)

    def has_module(self, module):
        return module in self.modules

    def get_modules(self):
        yield from self.local_modules.values()
        yield from self.schema.get_modules()

    def add(self, name, obj=None, *, _nameless=False):
        if not _nameless:
            if isinstance(obj, s_modules.Module):
                self.local_modules[name] = obj
                return self

            if obj is None:
                obj = name
                name = obj.get_name(self)
                import warnings
                warnings.warn(
                    f'Deprecated call to schema.add()',
                    RuntimeWarning, stacklevel=2)

            if obj.get_name(self).module not in self.local_modules:
                s_modules.Module.create_in_schema(
                    self,
                    name=obj.get_name(self).module)

        return super().add(name, obj, _nameless=_nameless)

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

    def get_by_id(self, item_id, default=_void):
        item = self.index_by_id.get(item_id)
        if item is None:
            return self.schema.get_by_id(item_id, default=default)
        else:
            return item
