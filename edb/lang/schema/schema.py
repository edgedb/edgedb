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


class Schema:

    def __init__(self):
        self._modules = {}
        self._deltas = {}

        self._inheritance_cache = {}
        self._garbage = set()
        self._nameless_ids = {}

        self._id_to_data = {}
        self._id_to_type = {}
        self._name_to_id = {}

    def _get_obj_field(self, obj_id, field):
        try:
            d = self._id_to_data[obj_id]
        except KeyError:
            return None

        return d.get(field)

    def _set_obj_field(self, obj_id, field, value):
        try:
            d = self._id_to_data[obj_id]
        except KeyError:
            d = self._id_to_data[obj_id] = {}

        if field == 'name' and obj_id not in self._nameless_ids:
            oldname = d['name']
            self._name_to_id.pop(oldname)
            self._name_to_id[value] = obj_id

        d[field] = value
        return self

    def _unset_obj_field(self, obj_id, field):
        try:
            d = self._id_to_data[obj_id]
        except KeyError:
            return

        d.pop(field, None)
        return self

    def get_module(self, module):
        obj_id = self._modules.get(module)
        if obj_id is not None:
            return s_modules.Module._create_from_id(obj_id)

    def get_modules(self):
        for mod_id in self._modules.values():
            module = self.get_by_id(mod_id)
            if module is not None:
                yield module

    def delete_module(self, mod):
        """Remove a module from the schema

        :param mod: Either a string name of the module or a Module
                    object that should be dropped from the schema.
        """
        if isinstance(mod, str):
            module_name = mod
        else:
            module_name = mod.get_name(self)

        mod_id = self._modules.pop(module_name)
        self._id_to_data.pop(mod_id, None)
        return self

    def get_delta(self, name):
        from . import deltas as s_deltas
        delta_id = self._deltas[name]
        return s_deltas.Delta._create_from_id(delta_id)

    def delete_delta(self, delta) -> 'Schema':
        """Remove the delta from the schema.

        :param name: Either a string name of the delta or a Delta object
                     thet should be dropped from the schema.
        """
        if isinstance(delta, str):
            delta_name = delta
        else:
            delta_name = delta.get_name(self)

        delta_id = self._deltas.pop(delta_name)
        self._id_to_data.pop(delta_id, None)

        return self

    def _add(self, id, scls, data, *, _nameless=False) -> 'Schema':
        from . import deltas as s_deltas

        if _nameless:
            self._nameless_ids[id] = True

        self._id_to_data[id] = data
        self._id_to_type[id] = scls

        name = data['name']

        if isinstance(scls, s_modules.Module):
            self._modules[name] = id
            return self
        elif isinstance(scls, s_deltas.Delta):
            self._deltas[name] = id
            return self

        if not _nameless:
            try:
                self._modules[name.module]
            except KeyError:
                raise s_err.SchemaModuleNotFoundError(
                    f'module {name.module!r} is not in this schema') from None

            if name in self._name_to_id:
                err = f'{name!r} is already present in the schema {self!r}'
                raise s_err.SchemaError(err)

            self._name_to_id[name] = id

        return self

    def mark_as_garbage(self, obj) -> 'Schema':
        self._garbage.add(obj.id)
        self._name_to_id.pop(obj.get_name(self), None)
        return self

    def discard(self, obj) -> 'Schema':
        self._name_to_id.pop(obj.get_name(self), None)
        self._id_to_data.pop(obj.id, None)

        if obj.id in self._nameless_ids:
            del self._nameless_ids[obj.id]
            return self

        return self

    def delete(self, obj) -> 'Schema':
        data = self._id_to_data.pop(obj.id, None)
        self._name_to_id.pop(data['name'], None)

        if obj.id in self._nameless_ids:
            del self._nameless_ids[obj.id]
            return self

        try:
            self._modules[data['name'].module]
        except KeyError:
            raise s_err.SchemaModuleNotFoundError(
                f'module {data["name"].module} '
                f'is not in this schema') from None

        return self

    def _resolve_module(self, module_name) -> typing.List[s_modules.Module]:
        modules = []

        if module_name is not None:
            module_id = self._modules.get(module_name)
            if module_id is not None:
                modules.append((self, module_name))

        return modules

    def _get(self, name, *, getter, default, module_aliases):
        name, module, shortname = schema_name.split_name(name)
        implicit_builtins = module is None

        if module_aliases is not None:
            # Alias has a priority over `self.modules` lookup.
            fq_module = module_aliases.get(module)
            if fq_module is not None:
                module = fq_module

        class_modules = self._resolve_module(module)
        for schema, modname in class_modules:
            lname = schema_name.SchemaName(shortname, modname)
            result = getter(schema, lname)
            if result is not None:
                return result

        if implicit_builtins:
            for schema, modname in self._resolve_module('std'):
                lname = schema_name.SchemaName(shortname, modname)
                result = getter(schema, lname)
                if result is not None:
                    return result

        return default

    def get_functions(self, name, default=_void, *, module_aliases=None):
        from . import functions as s_func

        def getter(schema, name):
            ret = []
            for obj_id, obj in schema._id_to_type.items():
                if (isinstance(obj, s_func.Function) and
                        obj.get_shortname(schema) == name and
                        obj.id not in schema._nameless_ids):
                    ret.append(obj)
            if ret:
                return ret

        funcs = self._get(name,
                          getter=getter,
                          module_aliases=module_aliases,
                          default=default)

        if funcs is not _void:
            return funcs

        raise s_err.ItemNotFoundError(
            f'reference to a non-existent function: {name}')

    def get_operators(self, name, default=_void, *, module_aliases=None):
        from . import operators as s_oper

        def getter(schema, name):
            ret = []
            for obj_id, obj in schema._id_to_type.items():
                if (isinstance(obj, s_oper.Operator) and
                        obj.get_shortname(schema) == name and
                        obj.id not in schema._nameless_ids):
                    ret.append(obj)
            if ret:
                return ret

        funcs = self._get(name,
                          getter=getter,
                          module_aliases=module_aliases,
                          default=default)

        if funcs is not _void:
            return funcs

        raise s_err.ItemNotFoundError(
            f'reference to a non-existent operator: {name}')

    def get_by_id(self, obj_id, default=_void):
        try:
            return self._id_to_type[obj_id]
        except KeyError:
            if default is _void:
                raise s_err.ItemNotFoundError(
                    f'reference to a non-existent schema item: {obj_id}'
                    f' in schema {self!r}'
                ) from None
            else:
                return default

    def _get_by_name(self, name, *, type=None):
        if isinstance(type, tuple):
            for typ in type:
                scls = self._get_by_name(name, type=typ)
                if scls is not None:
                    return scls
            return None

        obj_id = self._name_to_id.get(name)
        if obj_id is None:
            return None

        obj = self._id_to_type[obj_id]
        if type is not None and not isinstance(obj, type):
            return None

        return obj

    def get(self, name, default=_void, *, module_aliases=None, type=None):
        def getter(schema, name):
            return schema._get_by_name(name, type=type)

        obj = self._get(name,
                        getter=getter,
                        module_aliases=module_aliases,
                        default=default)

        if obj is not _void:
            return obj

        raise s_err.ItemNotFoundError(
            f'reference to a non-existent schema item: {name}')

    def has_module(self, module):
        return module in self._modules

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

        _virtual_children = scls.get__virtual_children(self)

        if _virtual_children is None:
            try:
                child_names = self._inheritance_cache[scls.get_name(self)]
                raise KeyError
            except KeyError:
                child_names = self._find_children(scls)
                self._inheritance_cache[scls.get_name(self)] = child_names
        else:
            child_names = [c.material_type(self).get_name(self)
                           for c in _virtual_children.objects(self)]

        children = {self.get(n, type=type(scls)) for n in child_names}

        if max_depth is not None and depth < max_depth:
            for child in children:
                result.update(self._get_descendants(
                    child, max_depth=max_depth, depth=depth + 1))

        result.update(children)
        return result

    def _find_children(self, scls):
        flt = lambda p: scls in p.get_bases(self).objects(self)
        it = self.get_objects(type=type(scls))
        return {c.get_name(self) for c in filter(flt, it)}

    def get_objects(self, *, modules=None, type=None):
        return SchemaIterator(self, modules=modules, type=type)

    def get_overlay(self, extra=None):
        return SchemaOverlay(self, extra=extra)


class SchemaIterator:
    def __init__(
            self,
            schema, *,
            modules: typing.Optional[typing.Iterable[str]],
            type=None) -> None:

        filters = [
            lambda obj:
                obj.id not in schema._nameless_ids and
                obj.id not in schema._garbage
        ]

        if modules is not None:
            modules = frozenset(modules)
            filters.append(
                lambda obj: obj.get_name(schema).module in modules)

        if type is not None:
            filters.append(
                lambda obj: isinstance(obj, type))

        self._filters = filters
        self._schema = schema

    def __iter__(self):
        filters = self._filters

        for obj_id, obj in tuple(self._schema._id_to_type.items()):
            if isinstance(obj, s_modules.Module):
                continue
            if all(f(obj) for f in filters):
                yield obj


class SchemaOverlay(Schema):
    def __init__(self, schema, extra=None):
        self.schema = schema
        self.local_modules = {}
        self._modules = collections.ChainMap(self.local_modules,
                                             schema._modules)
        self._deltas = {}

        self._local_id_to_data = {}
        self._id_to_data = collections.ChainMap(self._local_id_to_data,
                                                schema._id_to_data)
        self._local_id_to_type = {}
        self._id_to_type = collections.ChainMap(self._local_id_to_type,
                                                schema._id_to_type)

        self._name_to_id = {}
        self._garbage = set()
        self._local_ic = {}
        self._inheritance_cache = collections.ChainMap(
            self._local_ic, schema._inheritance_cache)
        self._local_nameless_ids = dict()
        self._nameless_ids = collections.ChainMap(
            self._local_nameless_ids, schema._nameless_ids)

        if extra:
            for n, v in extra.items():
                self._add(n, v)

    def _get_obj_field(self, obj_id, field):
        try:
            d = self._local_id_to_data[obj_id]
        except KeyError:
            pass
        else:
            val = d.get(field)
            if val is not None:
                if val is ...:
                    return None
                else:
                    return val

        return self.schema._get_obj_field(obj_id, field)

    def _set_obj_field(self, obj_id, field, value):
        try:
            d = self._local_id_to_data[obj_id]
        except KeyError:
            d = self._local_id_to_data[obj_id] = {}

        d[field] = value
        return self

    def _unset_obj_field(self, obj_id, field):
        try:
            d = self._local_id_to_data[obj_id]
        except KeyError:
            d = self._local_id_to_data[obj_id] = {}

        d[field] = ...
        return self

    def has_module(self, module):
        return module in self._modules

    def get_modules(self):
        yield from self.local_modules.values()
        yield from self.schema.get_modules()

    def _add(self, id, scls, data, *, _nameless=False):
        if not _nameless:
            name = data['name']

            if isinstance(scls, s_modules.Module):
                self.local_modules[name] = id
                return self

            if name.module not in self.local_modules:
                s_modules.Module.create_in_schema(
                    self,
                    name=name.module)

        return super()._add(id, scls, data, _nameless=_nameless)

    def _resolve_module(self, module_name) -> typing.List[s_modules.Module]:
        modules = []

        if module_name is not None:
            if module_name in self.local_modules:
                modules.append((self, module_name))

            modules.extend(self.schema._resolve_module(module_name))

        return modules
