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


import builtins
import collections

from edb.lang.common.ordered import OrderedSet

from edb.lang.edgeql import ast as qlast

from . import delta as sd
from . import functions as fu
from . import error as s_err
from . import name as sn
from . import named
from . import objects as so
from . import operators as s_opers


class Module(named.NamedObject):
    # Override 'name' to str type, since modules don't have
    # fully-qualified names.
    name = so.Field(str)

    imports = so.Field(so.ObjectSet, so.ObjectSet)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.funcs_by_name = collections.defaultdict(list)
        self.opers_by_name = collections.defaultdict(list)
        self.index_by_name = collections.OrderedDict()
        self.index_by_type = {}
        self.index_derived = set()

    def add(self, schema, obj):
        if obj in self:
            err = '{!r} is already present in the schema'.format(obj.name)
            raise s_err.SchemaError(err)

        if isinstance(obj, fu.Function):
            self.funcs_by_name[obj.shortname.name].append(obj)
        elif isinstance(obj, s_opers.Operator):
            self.opers_by_name[obj.shortname.name].append(obj)

        self.index_by_name[obj.name] = obj

        idx_by_type = self.index_by_type.setdefault(obj._type, OrderedSet())
        idx_by_type.add(obj.name)

        if getattr(obj, 'is_derived', None):
            self.index_derived.add(obj.name)

        return schema

    def discard(self, schema, obj):
        existing = self.index_by_name.pop(obj.name, None)
        if existing is not None:
            self._delete(existing)
        return schema

    def delete(self, schema, obj):
        existing = self.index_by_name.pop(obj.name, None)
        if existing is None:
            err = 'object {!r} is not present in the schema'.format(obj.name)
            raise s_err.ItemNotFoundError(err)
        else:
            self._delete(existing)
        return schema

    def _delete(self, obj):
        self.index_by_name.pop(obj.name, None)
        self.index_by_type[obj.__class__._type].remove(obj.name)
        self.index_derived.discard(obj.name)

    def lookup_qname(self, name):
        return self.index_by_name.get(name)

    def get_functions(self, name):
        return self.funcs_by_name.get(name)

    def get_operators(self, name):
        return self.opers_by_name.get(name)

    def get(self, name, default=s_err.ItemNotFoundError, *,
            module_aliases=None, type=None,
            implicit_builtins=True):

        fail_cause = None

        if isinstance(type, tuple):
            for typ in type:
                try:
                    scls = self.get(name, module_aliases=module_aliases,
                                    type=typ, default=None)
                except s_err.SchemaError:
                    pass
                else:
                    if scls is not None:
                        return scls
        else:
            scls = None

            fq_name = '{}::{}'.format(self.name, name)
            scls = self.lookup_qname(fq_name)

            if type is not None and issubclass(type, so.Object):
                type = type.get_canonical_class()

        raise_ = None

        if scls is None:
            if default is not None:
                raise_ = (isinstance(default, Exception) or
                          (isinstance(default, builtins.type) and
                           issubclass(default, Exception)))

            if raise_:
                msg = ('reference to non-existent schema item: '
                       '{}::{}'.format(self.name, name))
                if fail_cause is not None:
                    raise default(msg) from fail_cause
                else:
                    raise default(msg)
            else:
                scls = default

        if (type is not None and
                isinstance(scls, so.Object) and
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

    def reorder(self, schema, new_order):
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

        return schema

    def get_objects(self, *, type=None, include_derived=False):
        return SchemaIterator(self, type, include_derived=include_derived)


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
            if isinstance(self.type, so.ObjectMeta):
                typ = self.type._type

            itertyp = self.module.index_by_type.get(typ)
            if itertyp:
                names = itertyp
            else:
                names = OrderedSet()

        if not self.include_derived:
            names = names - self.module.index_derived

        return iter(names)

    def __iter__(self):
        return self

    def __next__(self):
        return self.module.index_by_name[next(self.iter)]


class ModuleCommandContext(sd.CommandContextToken):
    def __init__(self, op, module=None):
        super().__init__(op)
        self.module = module


class ModuleCommand(named.NamedObjectCommand, schema_metaclass=Module,
                    context_class=ModuleCommandContext):

    classname = so.Field(str)

    @classmethod
    def _classname_from_ast(cls, astnode, context, schema):
        if astnode.name.module:
            classname = sn.Name(module=astnode.name.module,
                                name=astnode.name.name)
        else:
            classname = astnode.name.name

        return classname


class CreateModule(named.CreateNamedObject, ModuleCommand):
    astnode = qlast.CreateModule

    def apply(self, schema, context):
        props = self.get_struct_properties(schema)
        metaclass = self.get_schema_metaclass()
        self.module = metaclass(**props)
        if schema.get_module(self.module.name) is not None:
            raise s_err.SchemaError(
                f'module {self.module.name!r} already exists',
                context=self.source_context)
        schema = schema.add_module(self.module)
        return schema, self.module


class AlterModule(named.CreateOrAlterNamedObject, ModuleCommand):
    astnode = qlast.AlterModule

    def apply(self, schema, context):
        self.module = schema.get_module(self.classname)

        props = self.get_struct_properties(schema)
        for name, value in props.items():
            setattr(self.module, name, value)

        return schema, self.module


class DeleteModule(ModuleCommand):
    astnode = qlast.DropModule

    def apply(self, schema, context):
        self.module = schema.get_module(self.classname)
        schema = schema.delete_module(self.module)
        return schema, self.module
