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


import typing

import immutables as immu

from . import error as s_err
from . import modules as s_modules
from . import name as schema_name


_void = object()


class Schema:

    def __init__(self):
        self._modules = immu.Map()
        self._garbage = immu.Map()
        self._id_to_data = immu.Map()
        self._id_to_type = immu.Map()
        self._name_to_id = immu.Map()

    def _replace(self, *, id_to_data=None, id_to_type=None,
                 name_to_id=None, modules=None, garbage=None):
        new = Schema.__new__(Schema)

        if modules is None:
            new._modules = self._modules
        else:
            new._modules = modules

        if id_to_data is None:
            new._id_to_data = self._id_to_data
        else:
            new._id_to_data = id_to_data

        if id_to_type is None:
            new._id_to_type = self._id_to_type
        else:
            new._id_to_type = id_to_type

        if name_to_id is None:
            new._name_to_id = self._name_to_id
        else:
            new._name_to_id = name_to_id

        if garbage is None:
            new._garbage = self._garbage
        else:
            new._garbage = garbage

        return new

    def _get_obj_field(self, obj_id, field):
        try:
            d = self._id_to_data[obj_id]
        except KeyError:
            return None

        return d.get(field)

    def _set_obj_field(self, obj_id, field, value):
        try:
            data = self._id_to_data[obj_id]
        except KeyError:
            data = immu.Map()

        name_to_id = None
        if field == 'name':
            old_name = data.get('name')

            name_to_id = self._name_to_id
            if old_name is not None:
                name_to_id = name_to_id.delete(old_name)

            name_to_id = name_to_id.set(value, obj_id)

        data = data.set(field, value)
        id_to_data = self._id_to_data.set(obj_id, data)

        return self._replace(name_to_id=name_to_id, id_to_data=id_to_data)

    def _unset_obj_field(self, obj_id, field):
        try:
            data = self._id_to_data[obj_id]
        except KeyError:
            return self

        try:
            data = data.delete(field)
        except KeyError:
            return self

        id_to_data = self._id_to_data.set(obj_id, data)
        return self._replace(id_to_data=id_to_data)

    def _add(self, id, scls, data) -> 'Schema':
        name = data['name']

        if name in self._name_to_id:
            err = f'{name!r} is already present in the schema {self!r}'
            raise s_err.SchemaError(err)

        data = immu.Map(data)

        updates = dict(
            id_to_data=self._id_to_data.set(id, data),
            id_to_type=self._id_to_type.set(id, scls),
            name_to_id=self._name_to_id.set(name, id),
        )

        if isinstance(scls, s_modules.Module):
            updates['modules'] = self._modules.set(name, id)
        elif name.module not in self._modules:
            raise s_err.SchemaModuleNotFoundError(
                f'module {name.module!r} is not in this schema')

        return self._replace(**updates)

    def _delete(self, obj):
        data = self._id_to_data.get(obj.id)
        if data is None:
            raise s_err.ItemNotFoundError(
                f'cannot delete {obj!r}: not in this schema')

        name = data['name']

        updates = dict(
            name_to_id=self._name_to_id.delete(name),
            id_to_data=self._id_to_data.delete(obj.id),
            id_to_type=self._id_to_type.delete(obj.id),
        )

        if isinstance(obj, s_modules.Module):
            updates['modules'] = self._modules.delete(name)

        return self._replace(**updates)

    def discard(self, obj) -> 'Schema':
        if obj.id in self._id_to_data:
            return self._delete(obj)
        else:
            return self

    def delete(self, obj) -> 'Schema':
        return self._delete(obj)

    def _get(self, name, *, getter, default, module_aliases):
        name, module, shortname = schema_name.split_name(name)
        implicit_builtins = module is None

        if module_aliases is not None:
            fq_module = module_aliases.get(module)
            if fq_module is not None:
                module = fq_module

        if module is not None:
            fqname = schema_name.SchemaName(shortname, module)
            result = getter(self, fqname)
            if result is not None:
                return result
        else:
            # Some items have unqualified names, like modules
            # themselves.
            result = getter(self, shortname)
            if result is not None:
                return result

        if implicit_builtins:
            fqname = schema_name.SchemaName(shortname, 'std')
            result = getter(self, fqname)
            if result is not None:
                return result

        return default

    def get_functions(self, name, default=_void, *, module_aliases=None):
        from . import functions as s_func

        def getter(schema, name):
            ret = []
            for obj_id, obj in schema._id_to_type.items():
                if (isinstance(obj, s_func.Function) and
                        obj.get_shortname(schema) == name):
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
                        obj.get_shortname(schema) == name):
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

    def _get_descendants(self, scls, *, max_depth=None, depth=0):
        result = set()

        _virtual_children = scls.get__virtual_children(self)

        if _virtual_children is None:
            child_names = self._find_children(scls)
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

    def mark_as_garbage(self, obj) -> 'Schema':
        garbage = self._garbage.set(obj.id, True)

        name = obj.get_name(self)
        name_to_id = self._name_to_id
        if name in self._name_to_id:
            name_to_id = name_to_id.delete(name)

        return self._replace(garbage=garbage, name_to_id=name_to_id)


class SchemaIterator:
    def __init__(
            self,
            schema, *,
            modules: typing.Optional[typing.Iterable[str]],
            type=None) -> None:

        filters = [
            lambda obj:
                obj.id not in schema._garbage
        ]

        if type is not None:
            filters.append(
                lambda obj: isinstance(obj, type))

        if modules is not None:
            modules = frozenset(modules)
            filters.append(
                lambda obj:
                    not isinstance(obj, s_modules.Module) and
                    obj.get_name(schema).module in modules)

        self._filters = filters
        self._schema = schema

    def __iter__(self):
        filters = self._filters
        index = self._schema._id_to_type

        for obj_id, obj in index.items():
            if all(f(obj) for f in filters):
                yield obj
