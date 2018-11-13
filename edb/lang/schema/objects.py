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


import collections.abc
import itertools
import pathlib
import re
import typing
import uuid

from edb.lang.common import parsing
from edb.lang.common import persistent_hash as phash
from edb.lang.common import topological
from edb.lang.common.ordered import OrderedSet
from edb.lang.common import struct, typed

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
    ours = getattr(target, field_name)
    if ours is None:
        for source in sources:
            theirs = getattr(source, field_name)
            if theirs is not None:
                return theirs
    else:
        return ours


class Field(struct.Field):
    __slots__ = ('compcoef', 'inheritable', 'hashable', 'simpledelta',
                 'merge_fn', 'ephemeral', 'introspectable')

    def __init__(self, *args, compcoef=None, inheritable=True, hashable=True,
                 simpledelta=True, merge_fn=None, ephemeral=False,
                 introspectable=True, **kwargs):
        """Schema item core attribute definition.

        """
        super().__init__(*args, **kwargs)
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

        cls = cls.get_canonical_class()

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


class ObjectMeta(struct.MixedStructMeta):
    _schema_metaclasses = []

    def __new__(mcls, name, bases, dct, **kwargs):
        cls = super().__new__(mcls, name, bases, dct, **kwargs)
        cls._ref_type = None
        mcls._schema_metaclasses.append(cls)
        return cls

    @property
    def ref_type(cls):
        if cls._ref_type is None:
            name = cls.__name__ + '_ref'
            dct = {'__module__': cls.__module__}
            cls._ref_type = cls.__class__(name, (ObjectRef, cls), dct)

            for fn, f in list(cls._ref_type._fields.items()):
                f = f.copy()
                f.default = None
                cls._ref_type._fields[fn] = f

        return cls._ref_type

    @classmethod
    def get_schema_metaclasses(mcls):
        return mcls._schema_metaclasses


class Object(struct.MixedStruct, metaclass=ObjectMeta):
    """Base schema item class."""

    id = Field(uuid.UUID, default=None, compcoef=0.1, inheritable=False,
               frozen=True)
    """Optional known ID for this schema item."""

    sourcectx = Field(parsing.ParserContext, None, compcoef=None,
                      inheritable=False, introspectable=False, hashable=False,
                      ephemeral=True, frozen=True)
    """Schema source context for this object"""

    @classmethod
    def get_canonical_class(cls):
        return cls

    def __init__(self, **kwargs):
        self._attr_sources = {}
        self._attr_source_contexts = {}
        super().__init__(**kwargs)

    def replace(self, schema, **attrs):
        rep = struct.Struct.replace(self, **attrs)
        return schema, rep

    def is_type(self):
        return False

    def hash_criteria_fields(self, schema):
        for fn, f in self.__class__.get_fields(sorted=True).items():
            if f.hashable:
                yield fn

    def hash_criteria(self, schema):
        cls = self.get_canonical_class()
        fields = self.hash_criteria_fields(schema)
        criteria = [('__class__', (cls.__module__, cls.__name__))]
        abc = collections.abc

        for f in fields:
            v = getattr(self, f)

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
            current = getattr(self, name)
        except AttributeError:
            changed = True
        else:
            changed = current != value

        if changed:
            self._attr_sources[name] = source
            setattr(self, name, value)
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

    def set_default_value(self, field_name, value):
        setattr(self, field_name, value)
        self._attr_sources[field_name] = 'default'

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
            ours = getattr(self, field_name)
            if result is not None or ours is not None:
                schema = self.set_attribute(
                    schema, field_name, result, dctx=dctx,
                    source='inheritance')

        return schema

    def compare(self, schema, other, context=None):
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

                ours = getattr(self, field_name)
                theirs = getattr(other, field_name)

                comparator = getattr(FieldType, 'compare_values', None)
                if callable(comparator):
                    fcoef = comparator(schema, ours, theirs, context=context,
                                       compcoef=field.compcoef)
                elif ours != theirs:
                    fcoef = field.compcoef

                else:
                    fcoef = 1.0

                similarity *= fcoef

        return similarity

    @classmethod
    def compare_values(cls, schema, ours, theirs, context, compcoef):
        if (ours is None) != (theirs is None):
            return compcoef
        elif ours is None:
            return 1.0
        else:
            comp = ours.compare(schema, theirs, context=context)
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

    def _reduce_obj_coll(self, schema, v):
        result = []
        comparison_v = []

        for scls in v:
            ref, comp = scls._reduce_to_ref(schema)
            result.append(ref)
            comparison_v.append(comp)

        return result, tuple(comparison_v)

    _reduce_obj_list = _reduce_obj_coll

    def _reduce_obj_set(self, schema, v):
        result, comparison_v = self._reduce_obj_coll(schema, v)
        return result, frozenset(comparison_v)

    def _reduce_refs(self, schema, value):
        if isinstance(value, (ObjectList, TypeList)):
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
                oldattr, oldattr_v = new._reduce_refs(
                    old_schema, getattr(old, f))
                newattr, newattr_v = new._reduce_refs(
                    new_schema, getattr(new, f))

                if oldattr_v != newattr_v:
                    delta.add(sd.AlterObjectProperty(
                        property=f, old_value=oldattr, new_value=newattr))
        elif not old:
            for f in fields:
                value = getattr(new, f)
                if value is not None:
                    value, _ = new._reduce_refs(new_schema, value)
                    delta.add(sd.AlterObjectProperty(
                        property=f, old_value=None, new_value=value))

    @classmethod
    def _sort_set(cls, schema, items):
        if items:
            probe = next(iter(items))
            has_bases = hasattr(probe, 'bases')

            if has_bases:
                items_idx = {p.name: p for p in items}

                g = {}

                for x in items:
                    deps = {b for b in x._get_deps(schema) if b in items_idx}
                    g[x.name] = {'item': x, 'deps': deps}

                items = topological.sort(g)

        return items

    def _get_deps(self, schema):
        return {getattr(b, 'classname', getattr(b, 'name', None))
                for b in self.bases}

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

        old = OrderedSet(
            o for o in old
            if o.persistent_hash(schema=old_schema) not in unchanged)
        new = OrderedSet(
            o for o in new
            if o.persistent_hash(schema=new_schema) not in unchanged)

        comparison = ((x.compare(new_schema, y, context), x, y)
                      for x, y in itertools.product(new, old))

        used_x = set()
        used_y = set()
        altered = OrderedSet()

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
                has_bases = hasattr(probe, 'bases')
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

                    bases = {getattr(b, 'classname', getattr(b, 'name', None))
                             for b in old_class.bases} | \
                            {getattr(b, 'classname', getattr(b, 'name', None))
                             for b in new_class.bases}

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
        assert getattr(self, attr).has(schema, name)
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
                dctx.current().op.add(sd.AlterObjectProperty(
                    property=fieldname,
                    new_value=getattr(self, fieldname),
                    source='default'
                ))

        return schema


class NamedObject(Object):
    name = Field(sn.Name, inheritable=False, compcoef=0.670)
    title = Field(str, default=None, compcoef=0.909, coerce=True)
    description = Field(str, default=None, compcoef=0.909)

    @classmethod
    def mangle_name(cls, name) -> str:
        return name.replace('::', '|')

    @classmethod
    def unmangle_name(cls, name) -> str:
        return name.replace('|', '::')

    @classmethod
    def get_shortname(cls, fullname) -> sn.Name:
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

    def __init__(self, **kwargs):
        type_id = kwargs.pop('id', None)
        type_name = kwargs.pop('name')
        if type_id is None:
            type_id = get_known_type_id(type_name)
        super().__init__(id=type_id, name=type_name, **kwargs)

    @property
    def shortname(self) -> sn.Name:
        try:
            cached = self._cached_shortname
        except AttributeError:
            pass
        else:
            # `.name` can be overridden at some point, so we
            # want to guard our cache against that.
            if cached[0] == self.name:
                return cached[1]

        shortname = self.get_shortname(self.name)
        self._cached_shortname = (self.name, shortname)
        return shortname

    @property
    def displayname(self) -> str:
        return str(self.shortname)

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
                            metaclass=obj.get_canonical_class())

    @classmethod
    def compare_values(cls, schema, ours, theirs, context, compcoef):
        similarity = 1.0

        if (ours is None) != (theirs is None):
            similarity /= 1.2
        elif ours is not None:
            if (ours.__class__.get_canonical_class() !=
                    theirs.__class__.get_canonical_class()):
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
        return ObjectRef(classname=self.name), self.name


class ObjectRef(Object):
    classname = Field(sn.SchemaName, coerce=True)

    @property
    def name(self):
        return self.classname

    def __repr__(self):
        return '<ObjectRef "{}" at 0x{:x}>'.format(self.classname, id(self))

    __str__ = __repr__

    def _reduce_to_ref(self, schema):
        return self, self.classname

    def _resolve_ref(self, schema):
        return schema.get(self.classname)


class ObjectCollection:
    pass


class ObjectMapping(ObjectCollection):

    def __init__(self, data: dict=None):
        self._keys = ()
        self._map = {}

        if data:
            for k, v in data.items():
                if not isinstance(k, str):
                    raise TypeError(
                        f'invalid input data for ObjectMapping: '
                        f'expected str keys, got {type(k)}')
                if not isinstance(v, Object):
                    raise TypeError(
                        f'invalid input data for ObjectMapping: '
                        f'expected Object values, got {type(k)}')

                self._keys += (k,)
                self._map[k] = v

    def persistent_hash(self, *, schema):
        vals = []
        for k, v in self.items(schema):
            vals.append((k, v))
        return phash.persistent_hash(frozenset(vals), schema=schema)

    @classmethod
    def compare_values(cls, schema, ours, theirs, context, compcoef):
        if not ours and not theirs:
            basecoef = 1.0
        elif not ours or not theirs:
            basecoef = 0.2
        else:
            similarity = []

            for k, v in ours.items(schema):
                try:
                    theirsv = theirs.get(schema, k)
                except KeyError:
                    # key only in ours
                    similarity.append(0.2)
                else:
                    similarity.append(
                        v.compare(schema, theirsv, context))

            diff = set(theirs.names(schema)) - set(ours.names(schema))
            similarity.extend(0.2 for k in diff)

            basecoef = sum(similarity) / len(similarity)

        return basecoef + (1 - basecoef) * compcoef

    def replace(self, schema, reps: dict):
        new_map: dict = self._map.copy()

        if isinstance(reps, ObjectMapping):
            keys = reps.items(schema)
        else:
            keys = reps.items()

        for key, obj in keys:
            if obj is None:
                if key not in new_map:
                    raise KeyError(f'{key!r} is not in the mapping')
                new_map.pop(key)
            else:
                new_map[key] = obj

        om = type(self)(new_map)
        return schema, om

    def names(self, schema):
        yield from self._map.keys()

    def items(self, schema):
        yield from self._map.items()

    def objects(self, schema):
        yield from self._map.values()

    def has(self, schema, name):
        return name in self._map

    def get(self, schema, name, default=...):
        if default is not ...:
            return self._map.get(name, default)
        else:
            return self._map[name]

    def __len__(self):
        return len(self._keys)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._map == other._map

    def __hash__(self):
        return hash(frozenset(self._map.items()))


class ObjectSet(typed.TypedSet, ObjectCollection, type=Object):
    @classmethod
    def merge_values(cls, target, sources, field_name, *, schema):
        result = getattr(target, field_name)
        for source in sources:
            theirs = getattr(source, field_name)
            if theirs:
                if result is None:
                    result = theirs.copy()
                else:
                    result.update(theirs)

        return result

    @classmethod
    def compare_values(cls, schema, ours, theirs, context, compcoef):
        if not ours and not theirs:
            basecoef = 1.0
        elif not ours or not theirs:
            basecoef = 0.2
        else:
            comparison = ((x.compare(schema, y, context=context), x, y)
                          for x, y in itertools.product(ours, theirs))
            similarity = []
            used_x = set()
            used_y = set()

            items = sorted(comparison, key=lambda item: item[0], reverse=True)

            for s, x, y in items:
                if x in used_x and y in used_y:
                    continue
                elif x in used_x:
                    similarity.append(0.2)
                    used_y.add(y)
                elif y in used_y:
                    similarity.append(0.2)
                    used_x.add(x)
                else:
                    similarity.append(s)
                    used_x.add(x)
                    used_y.add(y)

            basecoef = sum(similarity) / len(similarity)

        return basecoef + (1 - basecoef) * compcoef


class ObjectList(typed.TypedList, ObjectCollection, type=Object):
    pass


class TypeList(typed.TypedList, ObjectCollection, type=Object):
    pass


class StringList(typed.TypedList, type=str, accept_none=True):
    pass
