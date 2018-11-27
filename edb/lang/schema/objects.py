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
import collections.abc
import itertools
import typing
import uuid

from edb import errors

from edb.lang.common import markup
from edb.lang.common import ordered
from edb.lang.common import parsing
from edb.lang.common import struct
from edb.lang.common import topological
from edb.lang.common import typed
from edb.lang.common import uuidgen

from . import abc as s_abc
from . import name as sn
from . import _types


def get_known_type_id(typename, default=...):
    try:
        return _types.TYPE_IDS[typename]
    except KeyError:
        pass

    if default is ...:
        raise errors.SchemaError(
            f'failed to lookup named type id for {typename!r}')

    return default


def default_field_merge(target: 'Object', sources: typing.List['Object'],
                        field_name: str, *, schema) -> object:
    ours = target.get_explicit_field_value(schema, field_name, None)
    if ours is None:
        for source in sources:
            theirs = source.get_explicit_field_value(schema, field_name, None)
            if theirs is not None:
                return theirs
    else:
        return ours


class ComparisonContextWrapper:
    def __init__(self, context, pair):
        self.context = context
        self.pair = pair

    def __enter__(self):
        self.context.push(self.pair)

    def __exit__(self, exc_type, exc_value, traceback):
        self.context.pop()


class ComparisonContext:
    def __init__(self):
        self.stacks = collections.defaultdict(list)
        self.ptrs = []
        self.memo = {}

    def push(self, pair):
        obj = pair[1] if pair[0] is None else pair[0]
        cls = type(obj)

        if not issubclass(cls, Object):
            raise ValueError(
                f'invalid argument type {cls!r} for comparison context')

        self.stacks[cls].append(pair)
        self.ptrs.append(cls)

    def pop(self, cls=None):
        cls = cls or self.ptrs.pop()
        return self.stacks[cls].pop()

    def get(self, cls):
        stack = self.stacks[cls]
        if stack:
            return stack[-1]

    def __call__(self, left, right):
        return ComparisonContextWrapper(self, (left, right))


class NoDefault:
    pass


class Field(struct.ProtoField):  # derived from ProtoField for validation

    __slots__ = ('name', 'type', 'coerce',
                 'compcoef', 'inheritable', 'simpledelta',
                 'merge_fn', 'ephemeral', 'introspectable', 'allow_ddl_set')

    def __init__(self, type_, *, coerce=False,
                 compcoef=None, inheritable=True,
                 simpledelta=True, merge_fn=None, ephemeral=False,
                 introspectable=True, **kwargs):
        """Schema item core attribute definition.

        """
        if not isinstance(type_, type):
            raise ValueError(f'{type_!r} is not a type')

        self.type = type_
        self.coerce = coerce
        self.allow_ddl_set = False

        self.compcoef = compcoef
        self.inheritable = inheritable
        self.simpledelta = simpledelta
        self.introspectable = introspectable

        if merge_fn is not None:
            self.merge_fn = merge_fn
        elif callable(getattr(self.type, 'merge_values', None)):
            self.merge_fn = self.type.merge_values
        else:
            self.merge_fn = default_field_merge

        self.ephemeral = ephemeral

    def coerce_value(self, schema, value):
        ftype = self.type

        if value is None or isinstance(value, ftype):
            return value

        if not self.coerce:
            raise TypeError(
                f'{self.name} field: expected {ftype} but got {value!r}')

        if issubclass(ftype, (typed.AbstractTypedSequence,
                              typed.AbstractTypedSet)):
            casted_value = []
            for v in value:
                if v is not None and not isinstance(v, ftype.type):
                    v = ftype.type(v)
                casted_value.append(v)
            return ftype(casted_value)

        if issubclass(ftype, typed.AbstractTypedMapping):
            casted_value = {}
            for k, v in value.items():
                if k is not None and not isinstance(k, ftype.keytype):
                    k = ftype.keytype(k)
                if v is not None and not isinstance(v, ftype.valuetype):
                    v = ftype.valuetype(v)
                casted_value[k] = v
            return ftype(casted_value)

        if issubclass(ftype, ObjectCollection):
            return ftype.create(schema, value)

        try:
            return ftype(value)
        except Exception:
            raise TypeError(
                f'cannot coerce {self.name!r} value {value!r} to {ftype}')

    @property
    def required(self):
        return True

    @property
    def is_schema_field(self):
        return False

    def __get__(self, instance, owner):
        if instance is not None:
            return None
        else:
            return self

    def __repr__(self):
        return (
            f'<{type(self).__name__} name={self.name!r} '
            f'type={self.type} {id(self):#x}>'
        )


class SchemaField(Field):

    __slots__ = ('default', 'hashable')

    def __init__(self, type, *,
                 default=NoDefault, hashable=True,
                 allow_ddl_set=False, **kwargs):
        super().__init__(type, **kwargs)
        self.default = default
        self.hashable = hashable
        self.allow_ddl_set = allow_ddl_set

    @property
    def required(self):
        return self.default is NoDefault

    @property
    def is_schema_field(self):
        return True

    def __get__(self, instance, owner):
        if instance is not None:
            raise FieldValueNotFoundError(self.name)
        else:
            return self


class RefDict(struct.Struct):

    local_attr = struct.Field(str, frozen=True)
    attr = struct.Field(str, frozen=True)
    non_inheritable_attr = struct.Field(str, default=None, frozen=True)
    backref_attr = struct.Field(str, default='subject', frozen=True)
    requires_explicit_inherit = struct.Field(bool, default=False, frozen=True)
    ref_cls = struct.Field(type, frozen=True)


class ObjectMeta(type):

    _schema_metaclasses = []
    _schema_types = set()

    def __new__(mcls, name, bases, clsdict):
        fields = {}
        myfields = {}
        refdicts = collections.OrderedDict()
        mydicts = {}

        if '__slots__' in clsdict:
            raise TypeError(
                f'cannot create {name} class: __slots__ are not supported')

        for k, v in tuple(clsdict.items()):
            if isinstance(v, RefDict):
                mydicts[k] = v
                continue
            if not isinstance(v, struct.ProtoField):
                continue
            if not isinstance(v, Field):
                raise TypeError(
                    f'cannot create {name} class: schema.objects.Field '
                    f'expected, got {type(v)}')

            v.name = k
            myfields[k] = v

            if v.is_schema_field:
                getter_name = f'get_{v.name}'
                if getter_name in clsdict:
                    raise TypeError(
                        f'cannot create {name} class: schema field getter '
                        f'{getter_name}() is already defined')
                clsdict[getter_name] = (
                    lambda self, schema, *, _fn=v.name:
                        self._get_schema_field_value(schema, _fn)
                )

        try:
            cls = super().__new__(mcls, name, bases, clsdict)
        except TypeError as ex:
            raise TypeError(
                f'Object metaclass has failed to create class {name}: {ex}')

        for parent in reversed(cls.__mro__):
            if parent is cls:
                fields.update(myfields)
                refdicts.update(mydicts)
            elif isinstance(parent, ObjectMeta):
                fields.update(parent.get_ownfields())
                refdicts.update({k: d.copy()
                                for k, d in parent.get_own_refdicts().items()})

        cls._fields = fields
        cls._hashable_fields = {f for f in fields.values()
                                if f.is_schema_field and f.hashable}
        cls._sorted_fields = collections.OrderedDict(
            sorted(fields.items(), key=lambda e: e[0]))
        # Populated lazily
        cls._object_fields = None

        fa = '{}.{}_fields'.format(cls.__module__, cls.__name__)
        setattr(cls, fa, myfields)

        non_schema_fields = {field.name for field in fields.values()
                             if not field.is_schema_field}
        if non_schema_fields == {'id'} and len(fields) > 1:
            mcls._schema_types.add(cls)

        cls._refdicts_by_refclass = {}

        for dct in refdicts.values():
            if dct.attr not in cls._fields:
                raise RuntimeError(
                    f'object {name} has no refdict field {dct.attr}')
            if dct.local_attr not in cls._fields:
                raise RuntimeError(
                    f'object {name} has no refdict field {dct.local_attr}')

            if cls._fields[dct.attr].inheritable:
                raise RuntimeError(
                    f'{name}.{dct.attr} field must not be inheritable')
            if cls._fields[dct.local_attr].inheritable:
                raise RuntimeError(
                    f'{name}.{dct.local_attr} field must not be inheritable')
            if not cls._fields[dct.attr].ephemeral:
                raise RuntimeError(
                    f'{name}.{dct.attr} field must be ephemeral')
            if not cls._fields[dct.local_attr].ephemeral:
                raise RuntimeError(
                    f'{name}.{dct.local_attr} field must be ephemeral')
            if not cls._fields[dct.attr].coerce:
                raise RuntimeError(
                    f'{name}.{dct.attr} field must be coerced')
            if not cls._fields[dct.local_attr].coerce:
                raise RuntimeError(
                    f'{name}.{dct.local_attr} field must be coerced')

            if isinstance(dct.ref_cls, str):
                ref_cls_getter = getattr(cls, dct.ref_cls)
                try:
                    dct.ref_cls = ref_cls_getter()
                except NotImplementedError:
                    pass

            if not isinstance(dct.ref_cls, str):
                other_dct = cls._refdicts_by_refclass.get(dct.ref_cls)
                if other_dct is not None:
                    raise TypeError(
                        'multiple reference dicts for {!r} in '
                        '{!r}: {!r} and {!r}'.format(dct.ref_cls, cls,
                                                     dct.attr, other_dct.attr))

                cls._refdicts_by_refclass[dct.ref_cls] = dct

        # Refdicts need to be reversed here to respect the __mro__,
        # as we have iterated over it in reverse above.
        cls._refdicts = collections.OrderedDict(reversed(refdicts.items()))

        cls._refdicts_by_field = {rd.attr: rd for rd in cls._refdicts.values()}

        setattr(cls, '{}.{}_refdicts'.format(cls.__module__, cls.__name__),
                     mydicts)

        cls._ref_type = None
        mcls._schema_metaclasses.append(cls)

        return cls

    @property
    def is_schema_object(cls):
        return cls in ObjectMeta._schema_types

    def get_object_fields(cls):
        if cls._object_fields is None:
            cls._object_fields = frozenset(
                f for f in cls._fields.values()
                if issubclass(f.type, (Object, ObjectCollection)))
        return cls._object_fields

    def get_field(cls, name):
        return cls._fields.get(name)

    def get_fields(cls, sorted=False):
        return cls._sorted_fields if sorted else cls._fields

    def get_ownfields(cls):
        return getattr(
            cls, '{}.{}_fields'.format(cls.__module__, cls.__name__))

    def get_own_refdicts(cls):
        return getattr(cls, '{}.{}_refdicts'.format(
            cls.__module__, cls.__name__))

    def get_refdicts(cls):
        return iter(cls._refdicts.values())

    def get_refdict(cls, name):
        return cls._refdicts_by_field.get(name)

    def get_refdict_for_class(cls, refcls):
        for rcls in refcls.__mro__:
            try:
                return cls._refdicts_by_refclass[rcls]
            except KeyError:
                pass
        else:
            raise KeyError(f'{cls} has no refdict for {refcls}')

    @classmethod
    def get_schema_metaclasses(mcls):
        return mcls._schema_metaclasses


class FieldValueNotFoundError(Exception):
    pass


class Object(s_abc.Object, metaclass=ObjectMeta):
    """Base schema item class."""

    # Unique ID for this schema item.
    id = Field(
        uuid.UUID,
        inheritable=False, simpledelta=False)

    # Schema source context for this object
    sourcectx = SchemaField(
        parsing.ParserContext,
        default=None, compcoef=None,
        inheritable=False, introspectable=False, hashable=False,
        ephemeral=True)

    name = SchemaField(
        sn.Name,
        inheritable=False, compcoef=0.670)

    # The path_id_name field is solely for the purposes of the compiler
    # so that this item can act as a transparent proxy for the item
    # it has been derived from, specifically in path ids.
    path_id_name = SchemaField(
        sn.Name,
        inheritable=False, ephemeral=True,
        introspectable=False, default=None)

    def get_shortname(self, schema) -> sn.Name:
        return sn.shortname_from_fullname(self.get_name(schema))

    def get_displayname(self, schema) -> str:
        return str(self.get_shortname(schema))

    def __init__(self, *, _private_init):
        pass

    def __eq__(self, other):
        if type(self) is not type(other):
            return NotImplemented
        return self.id == other.id

    def __hash__(self):
        return hash((self.id, type(self)))

    @classmethod
    def _prepare_id(cls, id: typing.Optional[uuid.UUID],
                    data: dict) -> uuid.UUID:
        if id is None:
            type_id = get_known_type_id(data.get('name'), None)
            if type_id is not None:
                id = type_id
            else:
                id = uuidgen.uuid1mc()
        return id

    @classmethod
    def _create_from_id(cls, id):
        assert id is not None
        obj = cls(_private_init=True)
        obj.__dict__['id'] = id
        return obj

    @classmethod
    def create_in_schema(cls, schema, *, id=None, **data) -> 'Object':
        if not cls.is_schema_object:
            raise TypeError(f'{cls.__name__} type cannot be created in schema')

        if not data.get('name'):
            raise RuntimeError(f'cannot create {cls} without a name')

        obj_data = {}
        for field_name, value in data.items():
            try:
                field = cls._fields[field_name]
            except KeyError:
                raise TypeError(
                    f'type {cls.__name__} has no schema field for '
                    f'keyword argument {field_name!r}') from None

            assert field.is_schema_field

            value = field.coerce_value(schema, value)
            if value is None:
                continue

            obj_data[field_name] = value

        id = cls._prepare_id(id, data)
        scls = cls._create_from_id(id)
        schema = schema._add(id, scls, obj_data)

        return schema, scls

    @classmethod
    def _create(cls, schema, *, id=None, **data) -> 'Object':
        if cls.is_schema_object:
            raise TypeError(
                f'{cls.__name__} type cannot be created outside of a schema')

        obj = cls(_private_init=True)

        id = cls._prepare_id(id, data)
        obj.__dict__['id'] = id

        for field_name, value in data.items():
            try:
                field = cls._fields[field_name]
            except KeyError:
                raise TypeError(
                    f'type {cls.__name__} has no field for '
                    f'keyword argument {field_name!r}') from None

            assert not field.is_schema_field
            obj.__dict__[field_name] = value

        return obj

    def get_fields_values(self, schema):
        for field in self.__class__._fields:
            value = self.get_explicit_field_value(schema, field, None)
            if value is not None:
                yield field, value

    def __setattr__(self, name, value):
        raise RuntimeError(
            f'cannot set value to attribute {self}.{name} directly')

    def _getdefault(self, field_name, field, relaxrequired=False):
        if field.default == field.type:
            if issubclass(field.default, ObjectCollection):
                value = field.default.create_empty()
            else:
                value = field.default()
        elif field.default is NoDefault:
            if relaxrequired:
                value = None
            else:
                raise TypeError(
                    '%s.%s.%s is required' % (
                        self.__class__.__module__, self.__class__.__name__,
                        field_name))
        else:
            value = field.default
        return value

    def _get_schema_field_value(self, schema, field_name, *,
                                allow_default=True):
        val = schema._get_obj_field(self.id, field_name)
        if val is not None:
            return val

        if allow_default:
            field = type(self).get_field(field_name)

            try:
                return self._getdefault(field_name, field)
            except TypeError:
                pass

        raise FieldValueNotFoundError(
            f'{self!r} object has no value for field {field_name!r}')

    def get_field_value(self, schema, field_name, *, allow_default=True):
        field = type(self).get_field(field_name)

        if field.is_schema_field:
            return self._get_schema_field_value(
                schema, field_name, allow_default=allow_default)
        else:
            try:
                return self.__dict__[field_name]
            except KeyError:
                pass

        raise FieldValueNotFoundError(
            f'{self!r} object has no value for field {field_name!r}')

    def get_explicit_field_value(self, schema, field_name, default=NoDefault):
        field = type(self).get_field(field_name)

        if field.is_schema_field:
            val = schema._get_obj_field(self.id, field_name)
            if val is not None:
                return val
            elif default is not NoDefault:
                return default
            else:
                raise FieldValueNotFoundError(
                    f'{self!r} object has no value for field {field_name!r}')

        else:
            try:
                return self.__dict__[field_name]
            except KeyError:
                if default is not NoDefault:
                    return default

            raise FieldValueNotFoundError(
                f'{self!r} object has no value for field {field_name!r}')

    def set_field_value(self, schema, name, value):
        field = type(self)._fields[name]
        assert field.is_schema_field

        if value is None:
            return schema._unset_obj_field(self.id, name)
        else:
            value = field.coerce_value(schema, value)
            return schema._set_obj_field(self.__dict__['id'], name, value)

    def update(self, schema, updates: dict):
        fields = type(self)._fields

        updates = updates.copy()
        for field_name in updates:
            field = fields[field_name]
            assert field.is_schema_field

            new_val = updates[field_name]
            if new_val is not None:
                new_val = field.coerce_value(schema, new_val)
                updates[field_name] = new_val

        return schema._update_obj(self.__dict__['id'], updates)

    def is_type(self):
        return False

    def hash_criteria(self, schema):
        cls = type(self)

        sig = [cls]
        for f in cls._hashable_fields:
            fn = f.name
            val = schema._get_obj_field(self.id, fn)
            if val is None:
                continue
            sig.append((fn, val))

        return frozenset(sig)

    def set_field_value_with_delta(self, schema, name, value, *,
                                   dctx, source, source_context=None):
        """Set the attribute `name` to `value`."""
        from . import delta as sd

        try:
            current = self.get_explicit_field_value(schema, name)
        except FieldValueNotFoundError:
            changed = True
        else:
            changed = current != value

        if changed:
            schema = self.set_field_value(schema, name, value)
            if dctx is not None:
                dctx.current().op.add(sd.AlterObjectProperty(
                    property=name,
                    new_value=value,
                    source=source
                ))

        return schema

    def inheritable_fields(self):
        for fn, f in self.__class__.get_fields().items():
            if f.inheritable:
                yield fn

    def merge(self, *objs, schema, dctx=None):
        """Merge properties of another object into this object.

        Most often use of this method is to implement property
        acquisition through inheritance.
        """
        for obj in objs:
            if (not isinstance(obj, self.__class__) and
                    not isinstance(self, obj.__class__)):
                msg = "cannot merge instances of %s and %s" % \
                    (obj.__class__.__name__, self.__class__.__name__)
                raise errors.SchemaError(msg)

        for field_name in self.inheritable_fields():
            field = self.__class__.get_field(field_name)
            result = field.merge_fn(self, objs, field_name, schema=schema)
            ours = self.get_explicit_field_value(schema, field_name, None)
            if result is not None or ours is not None:
                schema = self.set_field_value_with_delta(
                    schema, field_name, result, dctx=dctx,
                    source='inheritance')

        return schema

    def compare(self, other, *, our_schema, their_schema, context=None):
        if (not isinstance(other, self.__class__) and
                not isinstance(self, other.__class__)):
            return NotImplemented

        context = context or ComparisonContext()

        with context(self, other):
            similarity = 1.0

            fields = self.__class__.get_fields(sorted=True)

            for field_name, field in fields.items():
                if field.compcoef is None:
                    continue

                FieldType = field.type

                ours = self.get_field_value(our_schema, field_name)
                theirs = other.get_field_value(their_schema, field_name)

                comparator = getattr(FieldType, 'compare_values', None)
                if callable(comparator):
                    fcoef = comparator(ours, theirs, context=context,
                                       our_schema=our_schema,
                                       their_schema=their_schema,
                                       compcoef=field.compcoef)
                elif ours != theirs:
                    fcoef = field.compcoef

                else:
                    fcoef = 1.0

                similarity *= fcoef

        return similarity

    @classmethod
    def compare_values(cls, ours, theirs, *,
                       our_schema, their_schema, context, compcoef):
        similarity = 1.0

        if (ours is None) != (theirs is None):
            similarity /= 1.2
        elif ours is not None:
            if type(ours) is not type(theirs):
                similarity /= 1.4
            elif ours.get_name(our_schema) != theirs.get_name(their_schema):
                similarity /= 1.2

        return similarity

    @classmethod
    def delta(cls, old, new, *, context=None, old_schema, new_schema):
        from . import delta as sd

        if context is None:
            context = ComparisonContext()

        with context(old, new):
            command_args = {}

            if old and new:
                try:
                    name = old.get_name(old_schema)
                except AttributeError:
                    pass
                else:
                    command_args['classname'] = name

                alter_class = sd.ObjectCommandMeta.get_command_class_or_die(
                    sd.AlterObject, type(old))
                delta = alter_class(**command_args)
                cls.delta_properties(delta, old, new, context=context,
                                     old_schema=old_schema,
                                     new_schema=new_schema)

            elif not old:
                try:
                    name = new.get_name(new_schema)
                except AttributeError:
                    pass
                else:
                    command_args['classname'] = name

                create_class = sd.ObjectCommandMeta.get_command_class_or_die(
                    sd.CreateObject, type(new))
                delta = create_class(**command_args)
                cls.delta_properties(delta, old, new, context=context,
                                     old_schema=old_schema,
                                     new_schema=new_schema)

            else:
                try:
                    name = old.get_name(old_schema)
                except AttributeError:
                    pass
                else:
                    command_args['classname'] = name

                delete_class = sd.ObjectCommandMeta.get_command_class_or_die(
                    sd.DeleteObject, type(old))
                delta = delete_class(**command_args)

            alter_class = sd.ObjectCommandMeta.get_command_class(
                sd.AlterObject, type(new))

            if isinstance(delta, sd.CreateObject) and alter_class is not None:
                # If this is a CREATE delta, we need to make
                # sure it is returned separately from the creation
                # of references, which will go into a separate ALTER
                # delta.  This is needed to avoid the hassle of
                # sorting the delta order by dependencies or having
                # to maintain ephemeral forward references.
                #
                # Generate an empty delta.
                alter_delta = alter_class(classname=new.get_name(new_schema))
                full_delta = sd.CommandGroup()
                full_delta.add(delta)
            else:
                full_delta = alter_delta = delta

            for refdict in cls.get_refdicts():
                cls._delta_refdict(
                    old, new, delta=alter_delta,
                    refdict=refdict, context=context,
                    old_schema=old_schema, new_schema=new_schema)

            if alter_delta is not full_delta:
                if alter_delta.has_subcommands():
                    full_delta.add(alter_delta)
                else:
                    full_delta = delta

        return full_delta

    @classmethod
    def _delta_refdict(cls, old, new, *, delta, refdict, context,
                       old_schema, new_schema):

        old_idx_key = lambda o: o.get_name(old_schema)
        new_idx_key = lambda o: o.get_name(new_schema)

        def _delta_subdict(attr):
            if old:
                oldcoll = old.get_field_value(old_schema, attr)
                oldcoll_idx = ordered.OrderedIndex(
                    oldcoll.objects(old_schema), key=old_idx_key)
            else:
                oldcoll_idx = {}

            if new:
                newcoll = new.get_field_value(new_schema, attr)
                newcoll_idx = ordered.OrderedIndex(
                    newcoll.objects(new_schema), key=new_idx_key)
            else:
                newcoll_idx = {}

            cls.delta_sets(oldcoll_idx, newcoll_idx, delta, context,
                           old_schema=old_schema, new_schema=new_schema)

        _delta_subdict(refdict.local_attr)
        if refdict.non_inheritable_attr:
            _delta_subdict(refdict.non_inheritable_attr)

    def add_classref(self, schema, collection, obj, replace=False):
        refdict = type(self).get_refdict(collection)
        attr = refdict.attr

        if (refdict.non_inheritable_attr
                and type(obj).get_field('inheritable') is not None
                and not obj.get_inheritable(schema)):
            local_attr = refdict.non_inheritable_attr
        else:
            local_attr = refdict.local_attr

        colltype = type(self).get_field(local_attr).type

        local_coll = self.get_explicit_field_value(schema, local_attr, None)
        all_coll = self.get_explicit_field_value(schema, attr, None)

        if local_coll is not None:
            if not replace:
                schema, local_coll = local_coll.add(schema, obj)
            else:
                schema, local_coll = local_coll.update(schema, [obj])
        else:
            local_coll = colltype.create(schema, [obj])

        schema = self.set_field_value(schema, local_attr, local_coll)

        if all_coll is not None:
            schema, all_coll = all_coll.update(schema, [obj])
        else:
            all_coll = colltype.create(schema, [obj])

        schema = self.set_field_value(schema, attr, all_coll)

        return schema

    def del_classref(self, schema, collection, key):
        refdict = type(self).get_refdict(collection)
        attr = refdict.attr
        local_attr = refdict.local_attr
        non_inh_attr = refdict.non_inheritable_attr

        if non_inh_attr is not None:
            non_inh_coll = self.get_field_value(schema, non_inh_attr)
        else:
            non_inh_coll = None

        local_coll = self.get_field_value(schema, local_attr)
        all_coll = self.get_field_value(schema, attr)

        if local_coll and local_coll.has(schema, key):
            schema, local_coll = local_coll.delete(schema, [key])
            schema = self.set_field_value(schema, local_attr, local_coll)

        if non_inh_coll and non_inh_coll.has(schema, key):
            schema, non_inh_coll = non_inh_coll.delete(schema, [key])
            schema = self.set_field_value(schema, non_inh_attr, non_inh_coll)

        if all_coll and all_coll.has(schema, key):
            schema, all_coll = all_coll.delete(schema, [key])
            schema = self.set_field_value(schema, attr, all_coll)

        return schema

    def _reduce_to_ref(self, schema):
        return ObjectRef(name=self.get_name(schema)), self.get_name(schema)

    def _resolve_ref(self, schema):
        return self

    def _reduce_obj_coll(self, schema, v):
        result = []
        comparison_v = []

        for scls in v.objects(schema):
            ref, comp = scls._reduce_to_ref(schema)
            result.append(ref)
            comparison_v.append(comp)

        return type(v).create(schema, result), tuple(comparison_v)

    _reduce_obj_list = _reduce_obj_coll

    def _reduce_obj_set(self, schema, v):
        result, comparison_v = self._reduce_obj_coll(schema, v)
        return result, frozenset(comparison_v)

    def _reduce_refs(self, schema, value):
        if isinstance(value, ObjectList):
            ref, val = self._reduce_obj_list(schema, value)

        elif isinstance(value, ObjectSet):
            ref, val = self._reduce_obj_set(schema, value)

        elif isinstance(value, Object):
            ref, val = value._reduce_to_ref(schema)

        elif isinstance(value, ObjectCollection):
            raise TypeError(f'reduce_refs: cannot handle {type(value)} type')

        else:
            ref, val = value, value

        return ref, val

    @classmethod
    def delta_properties(cls, delta, old, new, *, context=None,
                         old_schema, new_schema):
        from edb.lang.schema import delta as sd

        ff = type(new).get_fields(sorted=True).items()
        fields = [fn for fn, f in ff
                  if f.simpledelta and not f.ephemeral and f.introspectable]

        if old and new:
            if old.get_name(old_schema) != new.get_name(new_schema):
                delta.add(old.delta_rename(old, new.get_name(new_schema),
                                           old_schema=old_schema,
                                           new_schema=new_schema))

            for f in fields:
                oldattr_v = old.get_explicit_field_value(old_schema, f, None)
                newattr_v = new.get_explicit_field_value(new_schema, f, None)

                oldattr_v, oldattr_v1 = old._reduce_refs(old_schema, oldattr_v)
                newattr_v, newattr_v1 = new._reduce_refs(new_schema, newattr_v)

                if oldattr_v1 != newattr_v1:
                    delta.add(sd.AlterObjectProperty(
                        property=f, old_value=oldattr_v, new_value=newattr_v))
        elif not old:
            # IDs are assigned once when the object is created and
            # never changed.
            id_value = new.get_explicit_field_value(new_schema, 'id')
            delta.add(sd.AlterObjectProperty(
                property='id', old_value=None, new_value=id_value))

            for f in fields:
                value = new.get_explicit_field_value(new_schema, f, None)
                if value is not None:
                    value, _ = new._reduce_refs(new_schema, value)
                    delta.add(sd.AlterObjectProperty(
                        property=f, old_value=None, new_value=value))

    @classmethod
    def delta_rename(cls, obj, new_name, *, old_schema, new_schema):
        from . import delta as sd

        rename_class = sd.ObjectCommandMeta.get_command_class_or_die(
            sd.RenameObject, type(obj))

        return rename_class(classname=obj.get_name(old_schema),
                            new_name=new_name,
                            metaclass=type(obj))

    @classmethod
    def _sort_set(cls, schema, items):
        from . import inheriting as s_inh

        if items:
            probe = next(iter(items))

            if isinstance(probe, s_inh.InheritingObject):
                items_idx = {p.get_name(schema): p for p in items}

                g = {}

                for x in items:
                    deps = {b for b in x._get_deps(schema) if b in items_idx}
                    g[x.get_name(schema)] = {'item': x, 'deps': deps}

                items = topological.sort(g)

        return items

    def _get_deps(self, schema):
        return {b.get_name(schema)
                for b in self.get_bases(schema).objects(schema)}

    @classmethod
    def delta_sets(cls, old, new, result, context=None, *,
                   old_schema, new_schema):
        adds_mods, dels = cls._delta_sets(old, new, context=context,
                                          old_schema=old_schema,
                                          new_schema=new_schema)

        result.update(adds_mods)
        result.update(dels)

    @classmethod
    def _delta_sets(cls, old, new, context=None, *,
                    old_schema, new_schema):
        from edb.lang.schema import delta as sd
        from edb.lang.schema import inheriting as s_inh

        adds_mods = sd.DeltaRoot()
        dels = sd.DeltaRoot()

        if old is None:
            for n in new:
                adds_mods.add(n.delta(None, n, context=context,
                                      old_schema=old_schema,
                                      new_schema=new_schema))
            adds_mods.sort_subcommands_by_type()
            return adds_mods, dels
        elif new is None:
            for o in old:
                dels.add(o.delta(o, None, context=context,
                                 old_schema=old_schema,
                                 new_schema=new_schema))
            return adds_mods, dels

        old = list(old)
        new = list(new)

        oldkeys = {o.id: o.hash_criteria(old_schema) for o in old}
        newkeys = {o.id: o.hash_criteria(new_schema) for o in new}

        unchanged = set(oldkeys.values()) & set(newkeys.values())

        old = ordered.OrderedSet(
            o for o in old
            if oldkeys[o.id] not in unchanged)
        new = ordered.OrderedSet(
            o for o in new
            if newkeys[o.id] not in unchanged)

        comparison = []
        for x, y in itertools.product(new, old):
            comp = x.compare(y, our_schema=new_schema,
                             their_schema=old_schema)
            comparison.append((comp, x, y))

        used_x = set()
        used_y = set()
        altered = ordered.OrderedSet()

        comparison = sorted(comparison, key=lambda item: item[0], reverse=True)

        for s, x, y in comparison:
            if x not in used_x and y not in used_y:
                if s != 1.0:
                    if s > 0.6:
                        altered.add(x.delta(y, x, context=context,
                                            old_schema=old_schema,
                                            new_schema=new_schema))
                        used_x.add(x)
                        used_y.add(y)
                else:
                    used_x.add(x)
                    used_y.add(y)

        deleted = old - used_y
        created = new - used_x

        if created:
            created = cls._sort_set(new_schema, created)
            for x in created:
                adds_mods.add(x.delta(None, x, context=context,
                                      old_schema=old_schema,
                                      new_schema=new_schema))

        if old_schema is not None and new_schema is not None:
            if old:
                probe = next(iter(old))
            elif new:
                probe = next(iter(new))
            else:
                probe = None

            if probe is not None:
                has_bases = isinstance(probe, s_inh.InheritingObject)
            else:
                has_bases = False

            if has_bases:
                g = {}

                altered_idx = {p.classname: p for p in altered}
                for p in altered:
                    for op in p.get_subcommands(
                            type=sd.RenameObject):
                        altered_idx[op.new_name] = p

                for p in altered:
                    old_class = old_schema.get(p.classname)

                    for op in p.get_subcommands(
                            type=sd.RenameObject):
                        new_name = op.new_name
                        break
                    else:
                        new_name = p.classname

                    new_class = new_schema.get(new_name)

                    old_bases = \
                        old_class.get_bases(old_schema).objects(old_schema)
                    new_bases = \
                        new_class.get_bases(new_schema).objects(new_schema)

                    bases = (
                        {b.get_name(old_schema) for b in old_bases} |
                        {b.get_name(new_schema) for b in new_bases}
                    )

                    deps = {b for b in bases if b in altered_idx}

                    g[p.classname] = {'item': p, 'deps': deps}
                    if new_name != p.classname:
                        g[new_name] = {'item': p, 'deps': deps}

                altered = topological.sort(g)

        for p in altered:
            adds_mods.add(p)

        if deleted:
            deleted = cls._sort_set(old_schema, deleted)
            for y in reversed(list(deleted)):
                dels.add(y.delta(y, None, context=context,
                                 old_schema=old_schema,
                                 new_schema=new_schema))

        adds_mods.sort_subcommands_by_type()
        return adds_mods, dels

    def get_classref_origin(self, schema, name, attr, local_attr, classname,
                            farthest=False):
        assert self.get_field_value(schema, attr).has(schema, name)

        result = None

        if self.get_field_value(schema, local_attr).has(schema, name):
            result = self

        if not result or farthest:
            bases = self.compute_mro(schema)[1:]

            for c in bases:
                if c.get_field_value(schema, local_attr).has(schema, name):
                    result = c
                    if not farthest:
                        break

        if result is None:
            raise KeyError(
                'could not find {} "{}" origin'.format(classname, name))

        return result

    def finalize(self, schema, bases=None, *, apply_defaults=True, dctx=None):
        return schema

    def dump(self, schema):
        return (
            f'<{type(self).__name__} name={self.get_name(schema)!r} '
            f'at {id(self):#x}>'
        )

    def __repr__(self):
        return f'<{type(self).__name__} {self.id} at 0x{id(self):#x}>'


class ObjectRef(Object):

    def __init__(self, *, name: str):
        super().__init__(_private_init=True)
        self.__dict__['_name'] = name

    @property
    def name(self):
        return self._name

    def get_name(self, schema):
        return self._name

    def __repr__(self):
        return '<ObjectRef "{}" at 0x{:x}>'.format(self._name, id(self))

    def __eq__(self, other):
        if type(self) is not type(other):
            return NotImplemented
        return self._name == other._name

    def __hash__(self):
        return hash((self._name, type(self)))

    def _reduce_to_ref(self, schema):
        return self, self.get_name(schema)

    def _resolve_ref(self, schema):
        return schema.get(self.get_name(schema))


class ObjectCollectionDuplicateNameError(Exception):
    pass


class ObjectCollection:

    def __init_subclass__(cls, *, type=Object, container=None):
        cls._type = type
        if container is not None:
            cls._container = container

    def __init__(self, ids, *, _private_init):
        self._ids = ids

    def __len__(self):
        return len(self._ids)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._ids == other._ids

    def __hash__(self):
        return hash(self._ids)

    def dump(self, schema):
        return (
            f'<{type(self).__name__} objects='
            f'[{", ".join(o.dump(schema) for o in self.objects(schema))}] '
            f'at {id(self):#x}>'
        )

    @classmethod
    def create(cls, schema, data: typing.Iterable[Object]):
        ids = []

        if isinstance(data, ObjectCollection):
            ids = data._ids
        elif data:
            for v in data:
                ids.append(cls._validate_value(schema, v))

        return cls(cls._container(ids), _private_init=True)

    @classmethod
    def create_empty(cls) -> 'ObjectCollection':
        return cls(cls._container(), _private_init=True)

    @classmethod
    def _validate_value(cls, schema, v):
        if not isinstance(v, cls._type):
            raise TypeError(
                f'invalid input data for ObjectIndexByShortname: '
                f'expected {cls._type} values, got {type(v)}')

        if v.id is not None:
            return v.id
        elif isinstance(v, ObjectRef):
            return v
        else:
            raise TypeError(f'object {v!r} has no ID!')

        return v

    def ids(self, schema):
        result = []

        for item_id in self._ids:
            if isinstance(item_id, ObjectRef):
                result.append(item_id._resolve_ref(schema).id)
            else:
                result.append(item_id)

        return tuple(result)

    def names(self, schema, *, allow_unresolved=False):
        result = []

        for item_id in self._ids:
            if isinstance(item_id, ObjectRef):
                try:
                    obj = item_id._resolve_ref(schema)
                except errors.InvalidReferenceError:
                    if allow_unresolved:
                        result.append(item_id.get_name(schema))
                    else:
                        raise
                else:
                    result.append(obj.get_name(schema))
            else:
                obj = schema.get_by_id(item_id)
                result.append(obj.get_name(schema))

        return type(self)._container(result)

    def objects(self, schema):
        result = []

        for item_id in self._ids:
            if isinstance(item_id, ObjectRef):
                result.append(item_id._resolve_ref(schema))
            else:
                result.append(schema.get_by_id(item_id))

        return tuple(result)

    @classmethod
    def compare_values(cls, ours, theirs, *,
                       our_schema, their_schema, context, compcoef):
        if ours is not None:
            our_names = ours.names(our_schema, allow_unresolved=True)
        else:
            our_names = cls._container()

        if theirs is not None:
            their_names = theirs.names(their_schema, allow_unresolved=True)
        else:
            their_names = cls._container()

        if frozenset(our_names) != frozenset(their_names):
            return compcoef
        else:
            return 1.0


class ObjectIndexBase(ObjectCollection, container=tuple):

    def __init_subclass__(cls, *, key):
        cls._key = key

    @classmethod
    def get_key_for(cls, schema, obj):
        return cls._key(schema, obj)

    @classmethod
    def create(cls, schema, data: typing.Iterable[Object]):
        coll = super().create(schema, data)
        coll._check_duplicates(schema)
        return coll

    def _check_duplicates(self, schema):
        counts = collections.Counter(self.keys(schema))
        duplicates = [v for v, count in counts.items() if count > 1]
        if duplicates:
            raise ObjectCollectionDuplicateNameError(
                'object index contains duplicate key(s): ' +
                ', '.join(repr(duplicates)))

    @classmethod
    def compare_values(cls, ours, theirs, *,
                       our_schema, their_schema, context, compcoef):
        if not ours and not theirs:
            basecoef = 1.0
        elif not ours or not theirs:
            basecoef = 0.2
        else:
            similarity = []

            for k, v in ours.items(our_schema):
                try:
                    theirsv = theirs.get(their_schema, k)
                except KeyError:
                    # key only in ours
                    similarity.append(0.2)
                else:
                    similarity.append(
                        v.compare(theirsv, our_schema=our_schema,
                                  their_schema=their_schema, context=context))

            diff = (
                set(theirs.keys(their_schema)) -
                set(ours.keys(our_schema))
            )
            similarity.extend(0.2 for k in diff)

            basecoef = sum(similarity) / len(similarity)

        return basecoef + (1 - basecoef) * compcoef

    def add(self, schema, item) -> 'ObjectIndexBase':
        """Return a copy of this collection containing the given item.

        If the item is already present in the collection, an
        ``ObjectIndexDuplicateNameError`` is raised.
        """

        key = type(self)._key(schema, item)
        if self.has(schema, key):
            raise ObjectCollectionDuplicateNameError(
                f'object index already contains the {key!r} key')

        return self.update(schema, [item])

    def update(self, schema, reps: typing.Iterable[Object]):
        items = dict(self.items(schema))
        keyfunc = type(self)._key

        for obj in reps:
            items[keyfunc(schema, obj)] = obj

        return schema, type(self).create(schema, items.values())

    def delete(self, schema,
               names: typing.Iterable[str]) -> 'ObjectIndexBase':
        items = dict(self.items(schema))
        for name in names:
            items.pop(name)
        return schema, type(self).create(schema, items.values())

    def items(self, schema):
        result = []
        keyfunc = type(self)._key

        for obj in self.objects(schema):
            result.append((keyfunc(schema, obj), obj))

        return tuple(result)

    def keys(self, schema):
        result = []
        keyfunc = type(self)._key

        for obj in self.objects(schema):
            result.append(keyfunc(schema, obj))

        return tuple(result)

    def has(self, schema, name):
        return name in self.keys(schema)

    def get(self, schema, name, default=...):
        items = dict(self.items(schema))
        if default is ...:
            return items[name]
        else:
            return items.get(name, default)


class ObjectIndexByFullname(
        ObjectIndexBase,
        key=lambda schema, o: o.get_name(schema)):
    pass


class ObjectIndexByShortname(
        ObjectIndexBase,
        key=lambda schema, o: o.get_shortname(schema)):
    pass


class ObjectIndexByUnqualifiedName(
        ObjectIndexBase,
        key=lambda schema, o: o.get_shortname(schema).name):
    pass


class ObjectSet(ObjectCollection, container=frozenset):

    def __repr__(self):
        return f'{{{", ".join(str(id) for id in self._ids)}}}'

    @classmethod
    def merge_values(cls, target, sources, field_name, *, schema):
        result = target.get_explicit_field_value(schema, field_name, None)
        for source in sources:
            theirs = source.get_explicit_field_value(schema, field_name, None)
            if theirs:
                if result is None:
                    result = theirs
                else:
                    result._ids |= theirs._ids

        return result


class ObjectList(ObjectCollection, container=tuple):

    def __repr__(self):
        return f'[{", ".join(str(id) for id in self._ids)}]'

    def first(self, schema, default=NoDefault):
        try:
            return next(iter(self.objects(schema)))
        except StopIteration:
            pass

        if default is NoDefault:
            raise IndexError('ObjectList is empty')
        else:
            return default


@markup.serializer.serializer.register(Object)
@markup.serializer.serializer.register(ObjectCollection)
def _serialize_to_markup(o, *, ctx):
    if 'schema' not in ctx.kwargs:
        orepr = repr(o)
    else:
        orepr = o.dump(ctx.kwargs['schema'])

    return markup.elements.lang.Object(
        id=id(o), class_module=type(o).__module__,
        classname=type(o).__name__,
        repr=orepr)
