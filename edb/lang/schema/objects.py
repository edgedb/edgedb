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
import pathlib
import re
import typing
import uuid
import warnings

from edb.lang.common import parsing
from edb.lang.common import persistent_hash as phash
from edb.lang.common import ordered
from edb.lang.common import struct
from edb.lang.common import topological
from edb.lang.common import typed
from edb.lang.common import uuidgen

from . import error as s_err
from . import name as sn


_TYPE_IDS = None


def load_type_ids():
    import edb.api

    types = pathlib.Path(edb.api.__path__[0]) / 'types.txt'
    typeids = {}

    with open(types, 'rt') as f:
        for line in f:
            if line.startswith('#'):
                continue
            line = line.strip()
            if not line:
                continue
            parts = re.split(r'\s+', line)
            id, name = parts[:2]
            typeids[name] = uuid.UUID(id)

    return typeids


def get_known_type_id(typename):
    global _TYPE_IDS

    if _TYPE_IDS is None:
        _TYPE_IDS = load_type_ids()

    return _TYPE_IDS.get(typename)


def is_named_class(scls):
    if hasattr(scls.__class__, 'get_field'):
        name_field = scls.__class__.get_field('name')
        return name_field is not None

    return False


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

    __slots__ = ('name', 'type', 'default', 'coerce', 'formatters',
                 'frozen',
                 'compcoef', 'inheritable', 'hashable', 'simpledelta',
                 'merge_fn', 'ephemeral', 'introspectable', 'public')

    def __init__(self, type, default=NoDefault, *, coerce=False,
                 str_formatter=str, repr_formatter=repr, frozen=False,
                 compcoef=None, inheritable=True, hashable=True,
                 simpledelta=True, merge_fn=None, ephemeral=False,
                 introspectable=True, public=False, **kwargs):
        """Schema item core attribute definition.

        """
        if not isinstance(type, tuple):
            type = (type, )

        self.type = type
        self.default = default
        self.coerce = coerce
        self.frozen = frozen
        self.public = public

        if coerce and len(type) > 1:
            raise ValueError(
                'unable to coerce values for fields with multiple types')

        self.formatters = {'str': str_formatter, 'repr': repr_formatter}

        self.compcoef = compcoef
        self.inheritable = inheritable
        self.hashable = hashable
        self.simpledelta = simpledelta
        self.introspectable = introspectable

        if merge_fn is not None:
            self.merge_fn = merge_fn
        elif callable(getattr(self.type[0], 'merge_values', None)):
            self.merge_fn = self.type[0].merge_values
        else:
            self.merge_fn = default_field_merge

        self.ephemeral = ephemeral

    @property
    def _attrname(self):
        return self.name

    def copy(self):
        return self.__class__(
            self.type, self.default, coerce=self.coerce,
            str_formatter=self.formatters['str'],
            repr_formatter=self.formatters['repr'])

    def adapt(self, value):
        if not isinstance(value, self.type):
            for t in self.type:
                try:
                    value = t(value)
                except TypeError:
                    pass
                else:
                    break

        return value

    @property
    def required(self):
        return self.default is NoDefault

    def __get__(self, instance, owner):
        if instance is not None:
            return None
        else:
            return self


class SchemaField(Field):

    __slots__ = ('debug_getter',)

    def __init__(self, *args, debug_getter=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.debug_getter = debug_getter

    @property
    def _attrname(self):
        return f'_schema_field_{self.name}'

    def __get__(self, instance, owner):
        if instance is not None:
            if self.debug_getter:
                warnings.warn(
                    f'{type(instance).__name__}.{self.name} direct access',
                    RuntimeWarning, stacklevel=2)
                return instance.get_field_value(None, self.name)
            else:
                raise FieldValueNotFoundError(self.name)
        else:
            return self


class ObjectMeta(type):

    _schema_metaclasses = []

    def __new__(mcls, name, bases, clsdict):
        fields = {}
        myfields = {}

        if '__slots__' in clsdict:
            raise TypeError(
                f'cannot create {name} class: __slots__ are not supported')

        for k, v in tuple(clsdict.items()):
            if not isinstance(v, struct.ProtoField):
                continue
            if not isinstance(v, Field):
                raise TypeError(
                    f'cannot create {name} class: schema.objects.Field '
                    f'expected, got {type(v)}')

            v.name = k
            myfields[k] = v

            if isinstance(v, SchemaField):
                getter_name = f'get_{v.name}'
                if getter_name in clsdict:
                    raise TypeError(
                        f'cannot create {name} class: schema field getter '
                        f'{getter_name}() is already defined')
                clsdict[getter_name] = (
                    lambda self, schema, *, _fn=v.name:
                        self.get_field_value(schema, _fn)
                )

        cls = super().__new__(mcls, name, bases, clsdict)

        for parent in reversed(cls.__mro__):
            if parent is cls:
                fields.update(myfields)
            elif isinstance(parent, ObjectMeta):
                fields.update(parent.get_ownfields())

        cls._fields = fields
        cls._sorted_fields = collections.OrderedDict(
            sorted(fields.items(), key=lambda e: e[0]))
        fa = '{}.{}_fields'.format(cls.__module__, cls.__name__)
        setattr(cls, fa, myfields)

        cls._ref_type = None
        mcls._schema_metaclasses.append(cls)

        return cls

    def get_field(cls, name):
        return cls._fields.get(name)

    def get_fields(cls, sorted=False):
        return cls._sorted_fields if sorted else cls._fields

    def get_ownfields(cls):
        return getattr(
            cls, '{}.{}_fields'.format(cls.__module__, cls.__name__))

    @classmethod
    def get_schema_metaclasses(mcls):
        return mcls._schema_metaclasses


class FieldValueNotFoundError(Exception):
    pass


class Object(metaclass=ObjectMeta):
    """Base schema item class."""

    id = Field(uuid.UUID, inheritable=False, frozen=True, simpledelta=False)
    """Unique ID for this schema item."""

    sourcectx = SchemaField(
        parsing.ParserContext, None, compcoef=None,
        inheritable=False, introspectable=False, hashable=False,
        ephemeral=True, frozen=True)
    """Schema source context for this object"""

    def __init__(self, *, _private_init):
        self._attr_source_contexts = {}

    @classmethod
    def _create(cls, schema, *, _setdefaults_=True, _relaxrequired_=False,
                id=NoDefault, **kwargs) -> 'Object':

        if id is NoDefault:
            type_id = get_known_type_id(kwargs.get('name'))
            if type_id is not None:
                id = type_id
            else:
                id = uuidgen.uuid1mc()

        kwargs['id'] = id

        obj = cls(_private_init=True)

        obj._in_init_ = True
        try:
            obj._init_fields(schema, _setdefaults_, _relaxrequired_, kwargs)
        finally:
            obj._in_init_ = False

        return obj

    def setdefaults(self):
        """Initialize unset fields with default values."""
        fields_set = []
        for field_name, field in self.__class__._fields.items():
            if isinstance(field, SchemaField):
                continue
            value = getattr(self, field_name)
            if value is None and field.default is not None:
                value = self._getdefault(field_name, field)
                setattr(self, field_name, value)
                fields_set.append(field_name)
        return fields_set

    def _copy_and_replace(self, schema, cls, **replacements):
        args = {}
        for field in cls._fields.values():
            try:
                v = self.get_field_value(
                    None, field.name, allow_default=False)  # XXX
            except FieldValueNotFoundError:
                pass
            else:
                args[field.name] = v

        if replacements:
            args.update(replacements)

        return cls.create_in_schema(schema, **args)

    def copy_with_class(self, schema, cls):
        return self._copy_and_replace(schema, cls)

    def copy(self, schema):
        return self.copy_with_class(schema, type(self))

    def get_fields_values(self, schema):
        for field in self.__class__._fields:
            value = self.get_explicit_field_value(schema, field, None)
            if value is not None:
                yield field, value

    def _init_fields(self, schema, setdefaults, relaxrequired, values):
        for field_name, field in self.__class__._fields.items():
            is_schema_field = isinstance(field, SchemaField)
            if field_name not in values and is_schema_field:
                continue

            value = values.get(field_name)
            if is_schema_field:
                # values for SchemaFields will not get validated in
                # __setattr__; do it here.
                value = self._check_field_type(
                    schema, field, field.name, value)
                if value is None:
                    # SchemaFields do not persist their defaults in
                    # the __dict__.
                    continue

            if value is None and field.default is not None and setdefaults:
                value = self._getdefault(field_name, field, relaxrequired)

            setattr(self, field._attrname, value)

    def __setattr__(self, name, value):
        field = self._fields.get(name)
        if field is not None:
            if isinstance(field, SchemaField):
                raise RuntimeError(
                    f'cannot set value to SchemaField {self}.{name} directly')
            value = self._check_field_type(None, field, name, value)
            if field.frozen and not self._in_init_:
                raise ValueError(f'cannot assign to frozen field {name!r}')
        super().__setattr__(name, value)

    def _check_field_type(self, schema, field, name, value):
        if (field.type and value is not None and
                not isinstance(value, field.type)):
            if field.coerce:
                ftype = field.type[0]

                if issubclass(ftype, (typed.AbstractTypedSequence,
                                      typed.AbstractTypedSet)):
                    casted_value = []
                    for v in value:
                        if v is not None and not isinstance(v, ftype.type):
                            v = ftype.type(v)
                        casted_value.append(v)
                    value = casted_value
                elif issubclass(ftype, typed.AbstractTypedMapping):
                    casted_value = {}
                    for k, v in value.items():
                        if k is not None and not isinstance(k, ftype.keytype):
                            k = ftype.keytype(k)
                        if (v is not None and
                                not isinstance(v, ftype.valuetype)):
                            v = ftype.valuetype(v)
                        casted_value[k] = v

                    value = casted_value

                if issubclass(ftype, ObjectCollection):
                    assert schema is not None

                    return ftype.create(schema, value)
                else:
                    try:
                        return ftype(value)
                    except Exception as ex:
                        raise TypeError(
                            'cannot coerce {!r} value {!r} '
                            'to {}'.format(name, value, ftype)) from ex

            raise TypeError(
                '{}.{}.{}: expected {} but got {!r}'.format(
                    self.__class__.__module__, self.__class__.__name__, name,
                    ' or '.join(t.__name__ for t in field.type), value))

        return value

    def _getdefault(self, field_name, field, relaxrequired=False):
        if field.default in field.type:
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

    def get_field_value(self, schema, field_name, *, allow_default=True):
        field = type(self).get_field(field_name)
        try:
            return self.__dict__[field._attrname]
        except KeyError:
            if allow_default:
                try:
                    return self._getdefault(field_name, field)
                except TypeError:
                    pass

        raise FieldValueNotFoundError(
            f'{type(self).__name__} object has no field {field_name!r}')

    def get_explicit_field_value(self, schema, field_name, default=NoDefault):
        try:
            return self.get_field_value(
                schema, field_name, allow_default=False)
        except FieldValueNotFoundError:
            if default is NoDefault:
                raise
            else:
                return default

    def set_field_value(self, schema, name, value):
        return self.update(schema, {name: value})

    def update(self, schema, updates: dict):
        fields = type(self)._fields

        if updates.keys() - fields.keys():
            raise RuntimeError(
                'updates list contains values for non-existant fields: ' +
                ', '.join(updates.keys() - fields.keys()))

        for field_name in updates:
            field = fields[field_name]
            attrname = field._attrname

            new_val = updates[field_name]

            if new_val is None:
                self.__dict__.pop(attrname, None)
            else:
                new_val = self._check_field_type(
                    schema, field, field_name, new_val)
                self.__dict__[attrname] = new_val

        return schema

    def copy_with(self, schema, updates: dict, *, _nameless=False):
        fields = type(self)._fields

        if updates.keys() - fields.keys():
            raise RuntimeError(
                'updates list contains values for non-existant fields: ' +
                ', '.join(updates.keys() - fields.keys()))

        new = {}
        for field_name, field in fields.items():
            if field_name in updates:
                new_val = updates[field_name]
                if new_val is None:
                    continue
                else:
                    new[field_name] = new_val
            else:
                val = self.get_explicit_field_value(schema, field_name, None)
                if val is None:
                    continue
                new[field_name] = val

        return type(self).create_in_schema(schema, **new, _nameless=_nameless)

    def temp_copy(self, schema):
        new_id = uuidgen.uuid1mc()
        return self.copy_with(schema, updates={'id': new_id}, _nameless=True)

    def is_type(self):
        return False

    def hash_criteria_fields(self, schema):
        for fn, f in self.__class__.get_fields(sorted=True).items():
            if f.hashable:
                yield fn

    def hash_criteria(self, schema):
        cls = type(self)
        fields = self.hash_criteria_fields(schema)
        criteria = [('__class__', (cls.__module__, cls.__name__))]
        abc = collections.abc

        for f in fields:
            v = self.get_field_value(schema, f)

            if not isinstance(v, phash.PersistentlyHashable):
                if isinstance(v, abc.Set):
                    v = frozenset(v)
                elif isinstance(v, abc.Mapping):
                    v = frozenset(v.items())
                elif (isinstance(v, abc.Sequence) and
                        not isinstance(v, abc.Hashable)):
                    v = tuple(v)

            if is_named_class(v):
                v = v.name

            criteria.append((f, v))

        return tuple(criteria)

    def set_attribute(self, schema, name, value, *,
                      dctx=None, source=None, source_context=None):
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
            if source_context is not None:
                self._attr_source_contexts[name] = source_context

        return schema

    def get_attribute_source_context(self, schema, name):
        return self._attr_source_contexts.get(name)

    def persistent_hash(self, *, schema):
        """Compute object 'snapshot' hash.

        This is an explicit method since schema Objects are mutable.
        The hash must be externally stable, i.e. stable across the runs
        and thus must not contain default object hashes (addresses),
        including that of None.
        """
        return phash.persistent_hash(self.hash_criteria(schema), schema=schema)

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
                raise s_err.SchemaError(msg)

        for field_name in self.inheritable_fields():
            field = self.__class__.get_field(field_name)
            result = field.merge_fn(self, objs, field_name, schema=schema)
            ours = self.get_explicit_field_value(schema, field_name, None)
            if result is not None or ours is not None:
                schema = self.set_attribute(
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

                FieldType = field.type[0]

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
        if (ours is None) != (theirs is None):
            return compcoef
        elif ours is None:
            return 1.0
        else:
            comp = ours.compare(theirs, our_schema=our_schema,
                                their_schema=their_schema, context=context)
            if comp is NotImplemented:
                return NotImplemented
            else:
                return comp * compcoef

    @classmethod
    def delta(cls, old, new, *, context=None, old_schema, new_schema):
        from . import delta as sd

        command_args = {}

        if old and new:
            try:
                name = old.name
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
                name = new.name
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
                name = old.name
            except AttributeError:
                pass
            else:
                command_args['classname'] = name

            delete_class = sd.ObjectCommandMeta.get_command_class_or_die(
                sd.DeleteObject, type(old))
            delta = delete_class(**command_args)

        return delta

    def _reduce_to_ref(self, schema):
        raise NotImplementedError

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
    def _sort_set(cls, schema, items):
        from . import inheriting as s_inh

        if items:
            probe = next(iter(items))

            if isinstance(probe, s_inh.InheritingObject):
                items_idx = {p.name: p for p in items}

                g = {}

                for x in items:
                    deps = {b for b in x._get_deps(schema) if b in items_idx}
                    g[x.name] = {'item': x, 'deps': deps}

                items = topological.sort(g)

        return items

    def _get_deps(self, schema):
        return {b.name for b in self.get_bases(schema).objects(schema)}

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
        from edb.lang.schema import named as s_named
        from edb.lang.schema import database as s_db
        from edb.lang.schema import inheriting as s_inh

        adds_mods = s_db.AlterDatabase()
        dels = s_db.AlterDatabase()

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

        oldkeys = {o.persistent_hash(schema=old_schema) for o in old}
        newkeys = {o.persistent_hash(schema=new_schema) for o in new}

        unchanged = oldkeys & newkeys

        old = ordered.OrderedSet(
            o for o in old
            if o.persistent_hash(schema=old_schema) not in unchanged)
        new = ordered.OrderedSet(
            o for o in new
            if o.persistent_hash(schema=new_schema) not in unchanged)

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
                            type=s_named.RenameNamedObject):
                        altered_idx[op.new_name] = p

                for p in altered:
                    old_class = old_schema.get(p.classname)

                    for op in p.get_subcommands(
                            type=s_named.RenameNamedObject):
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
                        {b.name for b in old_bases} |
                        {b.name for b in new_bases}
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
        return self

    def finalize(self, schema, bases=None, *, apply_defaults=True, dctx=None):
        if not apply_defaults:
            return schema

        from . import delta as sd

        fields = self.setdefaults()
        if dctx is not None and fields:
            for fieldname in fields:
                field = type(self).get_field(fieldname)
                if field.ephemeral:
                    continue
                val = self.get_explicit_field_value(schema, fieldname, None)
                if val is None:
                    continue
                dctx.current().op.add(sd.AlterObjectProperty(
                    property=fieldname,
                    new_value=val,
                    source='default'
                ))

        return schema


class NamedObject(Object):
    name = Field(sn.Name, inheritable=False, compcoef=0.670)

    # The path_id_name field is solely for the purposes of the compiler
    # so that this item can act as a transparent proxy for the item
    # it has been derived from, specifically in path ids.
    path_id_name = SchemaField(
        sn.Name, inheritable=False, ephemeral=True,
        introspectable=False, default=None)

    title = SchemaField(
        str, default=None, compcoef=0.909, coerce=True,
        public=True)

    description = SchemaField(
        str, default=None, compcoef=0.909, public=True)

    @classmethod
    def create_in_schema(cls, schema, *, _nameless=False, **kwargs):
        name = kwargs.get('name')
        if not name:
            raise RuntimeError(f'cannot create {cls} without a name')
        obj = cls._create(schema, **kwargs)
        schema.add(name, obj, _nameless=_nameless)
        return schema, obj

    @classmethod
    def mangle_name(cls, name) -> str:
        return name.replace('::', '|')

    @classmethod
    def unmangle_name(cls, name) -> str:
        return name.replace('|', '::')

    @classmethod
    def shortname_from_fullname(cls, fullname) -> sn.Name:
        parts = str(fullname.name).split('@@', 1)
        if len(parts) == 2:
            return sn.Name(cls.unmangle_name(parts[0]))
        else:
            return sn.Name(fullname)

    @classmethod
    def get_specialized_name(cls, basename, *qualifiers) -> str:
        return (cls.mangle_name(basename) +
                '@@' +
                '@'.join(cls.mangle_name(qualifier)
                         for qualifier in qualifiers if qualifier))

    def get_shortname(self, schema) -> sn.Name:
        try:
            cached = self._cached_shortname
        except AttributeError:
            pass
        else:
            # `.name` can be overridden at some point, so we
            # want to guard our cache against that.
            if cached[0] == self.name:
                return cached[1]

        shortname = self.shortname_from_fullname(self.name)
        self._cached_shortname = (self.name, shortname)
        return shortname

    def get_displayname(self, schema) -> str:
        return str(self.get_shortname(schema))

    @classmethod
    def delta_properties(cls, delta, old, new, *, context=None,
                         old_schema, new_schema):
        if old and new:
            if old.name != new.name:
                delta.add(old.delta_rename(old, new.name,
                                           old_schema=old_schema,
                                           new_schema=new_schema))

        super().delta_properties(delta, old, new, context=context,
                                 old_schema=old_schema, new_schema=new_schema)

    @classmethod
    def delta_rename(cls, obj, new_name, *, old_schema, new_schema):
        from . import delta as sd
        from . import named

        rename_class = sd.ObjectCommandMeta.get_command_class_or_die(
            named.RenameNamedObject, type(obj))

        return rename_class(classname=obj.name,
                            new_name=new_name,
                            metaclass=type(obj))

    @classmethod
    def compare_values(cls, ours, theirs, *,
                       our_schema, their_schema, context, compcoef):
        similarity = 1.0

        if (ours is None) != (theirs is None):
            similarity /= 1.2
        elif ours is not None:
            if type(ours) is not type(theirs):
                similarity /= 1.4
            elif ours.name != theirs.name:
                similarity /= 1.2

        return similarity

    def __repr__(self):
        cls = self.__class__
        return f'<{cls.__module__}.{cls.__name__} "{self.name}" ' \
               f'at 0x{id(self):x}>'

    __str__ = __repr__

    def _reduce_to_ref(self, schema):
        return ObjectRef(name=self.name), self.name


class ObjectRef(NamedObject):

    def __init__(self, *, id=None, name: str):
        super().__init__(_private_init=True)

        self._id = id
        self._name = name

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    def __repr__(self):
        return '<ObjectRef "{}" at 0x{:x}>'.format(self.name, id(self))

    __str__ = __repr__

    def _reduce_to_ref(self, schema):
        return self, self.name

    def _resolve_ref(self, schema):
        return schema.get(self.name)


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

    @classmethod
    def create(cls, schema, data: typing.Iterable[NamedObject]):
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

    def names(self, schema, *, allow_unresolved=False):
        result = []

        for item_id in self._ids:
            if isinstance(item_id, ObjectRef):
                try:
                    obj = schema.get(item_id.name)
                except s_err.ItemNotFoundError:
                    if allow_unresolved:
                        result.append(item_id.name)
                    else:
                        raise
                else:
                    result.append(obj.name)
            else:
                obj = schema.get_by_id(item_id)
                result.append(obj.name)

        return type(self)._container(result)

    def shortnames(self, schema, *, allow_unresolved=False):
        result = []

        for item_id in self._ids:
            if isinstance(item_id, ObjectRef):
                try:
                    obj = schema.get(item_id.name)
                except s_err.ItemNotFoundError:
                    if allow_unresolved:
                        result.append(
                            NamedObject.shortname_from_fullname(item_id.name))
                    else:
                        raise
                else:
                    result.append(obj.get_shortname(schema))
            else:
                obj = schema.get_by_id(item_id)
                result.append(obj.get_shortname(schema))

        return type(self)._container(result)

    def objects(self, schema):
        result = []

        for item_id in self._ids:
            if isinstance(item_id, ObjectRef):
                result.append(schema.get(item_id.name))
            else:
                result.append(schema.get_by_id(item_id))

        return tuple(result)

    def persistent_hash(self, *, schema):
        return phash.persistent_hash(self.names(schema, allow_unresolved=True))

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
    def create(cls, schema, data: typing.Iterable[NamedObject]):
        coll = super().create(schema, data)
        coll._check_duplicates(schema)
        return coll

    def persistent_hash(self, *, schema):
        vals = []
        for k, v in self.items(schema):
            vals.append((k, v))
        return phash.persistent_hash(frozenset(vals), schema=schema)

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

    def update(self, schema, reps: typing.Iterable[NamedObject]):
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


class ObjectIndexByFullname(ObjectIndexBase,
                            key=lambda schema, o: o.name):
    pass


class ObjectIndexByShortname(ObjectIndexBase,
                             key=lambda schema, o: o.get_shortname(schema)):
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
