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
import uuid

from edb.lang.common import nlang
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


class Field(struct.Field):
    __slots__ = ('compcoef', 'inheritable', 'hashable', 'simpledelta',
                 'merge_fn', 'ephemeral')

    def __init__(self, *args, compcoef=None, inheritable=True, hashable=True,
                 simpledelta=True, merge_fn=None, ephemeral=False,
                 **kwargs):
        """Schema item core attribute definition.

        """
        super().__init__(*args, **kwargs)
        self.compcoef = compcoef
        self.inheritable = inheritable
        self.hashable = hashable
        self.simpledelta = simpledelta
        self.merge_fn = merge_fn
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
        cls = None
        obj = pair[1] if pair[0] is None else pair[0]

        cls = obj.__class__

        if not issubclass(cls, Object):
            raise ValueError('invalid argument type for comparison context')

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

    id = Field(uuid.UUID, default=None, compcoef=0.1, inheritable=False)
    """Optional known ID for this schema item."""

    sourcectx = Field(parsing.ParserContext, None, compcoef=None,
                      inheritable=False, ephemeral=True, hashable=False)
    """Schema source context for this object"""

    @classmethod
    def get_canonical_class(cls):
        return cls

    def __init__(self, **kwargs):
        self._attr_sources = {}
        self._attr_source_contexts = {}
        super().__init__(**kwargs)

    def hash_criteria_fields(self):
        for fn, f in self.__class__.get_fields(sorted=True).items():
            if f.hashable:
                yield fn

    def hash_criteria(self):
        cls = self.get_canonical_class()
        fields = self.hash_criteria_fields()
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

    def set_attribute(self, name, value, *,
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

    def get_attribute_source_context(self, name):
        return self._attr_source_contexts.get(name)

    def set_default_value(self, field_name, value):
        setattr(self, field_name, value)
        self._attr_sources[field_name] = 'default'

    def persistent_hash(self):
        """Compute object 'snapshot' hash.

        This is an explicit method since schema Objects are mutable.
        The hash must be externally stable, i.e. stable across the runs
        and thus must not contain default object hashes (addresses),
        including that of None.
        """
        return phash.persistent_hash(self.hash_criteria())

    def inheritable_fields(self):
        for fn, f in self.__class__.get_fields().items():
            if f.inheritable:
                yield fn

    def merge(self, obj, *, schema, dctx=None):
        """Merge properties of another object into this object.

        Most often use of this method is to implement property
        acquisition through inheritance.
        """
        if (not isinstance(obj, self.__class__) and
                not isinstance(self, obj.__class__)):
            msg = "cannot merge instances of %s and %s" % \
                (obj.__class__.__name__, self.__class__.__name__)
            raise s_err.SchemaError(msg)

        for field_name in self.inheritable_fields():
            field = self.__class__.get_field(field_name)
            FieldType = field.type[0]

            ours = getattr(self, field_name)
            theirs = getattr(obj, field_name)

            merger = field.merge_fn
            if not callable(merger):
                merger = getattr(FieldType, 'merge_values', None)

            if callable(merger):
                result = merger(ours, theirs, schema=schema)
                self.set_attribute(field_name, result, dctx=dctx,
                                   source='inheritance')
            else:
                if ours is None and theirs is not None:
                    self.set_attribute(field_name, theirs, dctx=dctx,
                                       source='inheritance')

    def compare(self, other, context=None):
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
                    fcoef = comparator(ours, theirs, context=context,
                                       compcoef=field.compcoef)
                elif ours != theirs:
                    fcoef = field.compcoef

                else:
                    fcoef = 1.0

                similarity *= fcoef

        return similarity

    @classmethod
    def compare_values(cls, ours, theirs, context, compcoef):
        if (ours is None) != (theirs is None):
            return compcoef
        elif ours is None:
            return 1.0
        else:
            comp = ours.compare(theirs, context=context)
            if comp is NotImplemented:
                return NotImplemented
            else:
                return comp * compcoef

    @classmethod
    def delta_pair(cls, new, old, context=None):
        if new:
            return new.delta(old, context=context)
        elif old:
            return old.delta(new, reverse=True, context=context)
        else:
            return None

    def delta(self, other, reverse=False, context=None):
        from . import delta as sd

        old, new = (other, self) if not reverse else (self, other)

        command_args = {}

        if old and new:
            try:
                name = old.name
            except AttributeError:
                pass
            else:
                command_args['classname'] = name

            alter_class = sd.ObjectCommandMeta.get_command_class_or_die(
                sd.AlterObject, type(self))
            delta = alter_class(**command_args)
            self.delta_properties(delta, other, reverse, context=context)

        elif not old:
            try:
                name = new.name
            except AttributeError:
                pass
            else:
                command_args['classname'] = name

            create_class = sd.ObjectCommandMeta.get_command_class_or_die(
                sd.CreateObject, type(self))
            delta = create_class(**command_args)
            self.delta_properties(delta, other, reverse, context=context)

        else:
            try:
                name = old.name
            except AttributeError:
                pass
            else:
                command_args['classname'] = name

            delete_class = sd.ObjectCommandMeta.get_command_class_or_die(
                sd.DeleteObject, type(self))
            delta = delete_class(**command_args)

        return delta

    def _reduce_to_ref(self):
        raise NotImplementedError

    def _reduce_obj_dict(self, v):
        result = {}
        comparison_v = {}

        for k, scls in v.items():
            result[k], comparison_v[k] = scls._reduce_to_ref()

        return result, frozenset(comparison_v.items())

    def _reduce_obj_coll(self, v):
        result = []
        comparison_v = []

        for scls in v:
            ref, comp = scls._reduce_to_ref()
            result.append(ref)
            comparison_v.append(comp)

        return result, tuple(comparison_v)

    _reduce_obj_list = _reduce_obj_coll

    def _reduce_obj_set(self, v):
        result, comparison_v = self._reduce_obj_coll(v)
        return result, frozenset(comparison_v)

    def _reduce_refs(self, value):
        if isinstance(value, ObjectDict):
            ref, val = self._reduce_obj_dict(value)

        elif isinstance(value, (ObjectList, TypeList)):
            ref, val = self._reduce_obj_list(value)

        elif isinstance(value, ObjectSet):
            ref, val = self._reduce_obj_set(value)

        elif isinstance(value, Object):
            ref, val = value._reduce_to_ref()

        else:
            ref, val = value, value

        return ref, val

    def _restore_refs(self, field_name, ref, resolve):
        ftype = self.__class__.get_field(field_name).type[0]

        if issubclass(ftype, (ObjectSet, ObjectList)):
            val = ftype(r._resolve_ref(resolve) for r in ref)

        elif issubclass(ftype, ObjectDict):
            result = []

            for k, r in ref.items():
                result.append((k, r._resolve_ref(resolve)))

            val = ftype(result)

        elif issubclass(ftype, Object):
            val = ftype._resolve_ref(resolve)

        else:
            val = ref

        return val

    def delta_properties(self, delta, other, reverse=False, context=None):
        from edb.lang.schema import delta as sd

        old, new = (other, self) if not reverse else (self, other)

        ff = self.__class__.get_fields(sorted=True).items()
        fields = [fn for fn, f in ff if f.simpledelta and not f.ephemeral]

        if old and new:
            for f in fields:
                oldattr, oldattr_v = self._reduce_refs(getattr(old, f))
                newattr, newattr_v = self._reduce_refs(getattr(new, f))

                if oldattr_v != newattr_v:
                    delta.add(sd.AlterObjectProperty(
                        property=f, old_value=oldattr, new_value=newattr))
        elif not old:
            for f in fields:
                value = getattr(new, f)
                if value is not None:
                    value, _ = self._reduce_refs(value)
                    delta.add(sd.AlterObjectProperty(
                        property=f, old_value=None, new_value=value))

    @classmethod
    def _sort_set(cls, items):
        if items:
            probe = next(iter(items))
            has_bases = hasattr(probe, 'bases')

            if has_bases:
                items_idx = {p.name: p for p in items}

                g = {}

                for x in items:
                    deps = {b for b in x._get_deps() if b in items_idx}
                    g[x.name] = {'item': x, 'deps': deps}

                items = topological.sort(g)

        return items

    def _get_deps(self):
        return {getattr(b, 'classname', getattr(b, 'name', None))
                for b in self.bases}

    @classmethod
    def delta_sets(cls, old, new, result, context=None, *,
                   old_schema=None, new_schema=None):
        adds_mods, dels = cls._delta_sets(old, new, context=context,
                                          old_schema=old_schema,
                                          new_schema=new_schema)

        result.update(adds_mods)
        result.update(dels)

    @classmethod
    def _delta_sets(cls, old, new, context=None, *,
                    old_schema=None, new_schema=None):
        from edb.lang.schema import named as s_named
        from edb.lang.schema import database as s_db

        adds_mods = s_db.AlterDatabase()
        dels = s_db.AlterDatabase()

        if old is None:
            for n in new:
                adds_mods.add(n.delta(None, context=context))
            adds_mods.sort_subcommands_by_type()
            return adds_mods, dels
        elif new is None:
            for o in old:
                dels.add(o.delta(None, reverse=True, context=context))
            return adds_mods, dels

        old = list(old)
        new = list(new)

        oldkeys = {o.persistent_hash() for o in old}
        newkeys = {o.persistent_hash() for o in new}

        unchanged = oldkeys & newkeys

        old = OrderedSet(o for o in old
                         if o.persistent_hash() not in unchanged)
        new = OrderedSet(o for o in new
                         if o.persistent_hash() not in unchanged)

        comparison = ((x.compare(y, context), x, y)
                      for x, y in itertools.product(new, old))

        used_x = set()
        used_y = set()
        altered = OrderedSet()

        comparison = sorted(comparison, key=lambda item: item[0], reverse=True)

        """LOG [edb.delta.comp] Index comparison
        from edb.lang.common import markup
        markup.dump(comparison)
        """

        for s, x, y in comparison:
            if x not in used_x and y not in used_y:
                if s != 1.0:
                    if s > 0.6:
                        altered.add(x.delta(y, context=context))
                        used_x.add(x)
                        used_y.add(y)
                else:
                    used_x.add(x)
                    used_y.add(y)

        deleted = old - used_y
        created = new - used_x

        if created:
            created = cls._sort_set(created)
            for x in created:
                adds_mods.add(x.delta(None, context=context))

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
            deleted = cls._sort_set(deleted)
            for y in reversed(list(deleted)):
                dels.add(y.delta(None, reverse=True, context=context))

        adds_mods.sort_subcommands_by_type()
        return adds_mods, dels

    def __getstate__(self):
        state = self.__dict__.copy()

        refs = []

        for field_name in self.__class__.get_fields():
            val = state[field_name]
            if val is not None:
                ref, reduced = self._reduce_refs(val)
            else:
                ref = val

            if ref is not val:
                state[field_name] = ref
                refs.append(field_name)

        state['_classrefs'] = refs if refs else None

        return state

    def _finalize_setstate(self, _objects, _resolve):
        classrefs = getattr(self, '_classrefs', None)
        if not classrefs:
            return

        for field_name in classrefs:
            ref = getattr(self, field_name)
            val = self._restore_refs(field_name, ref, _resolve)
            setattr(self, field_name, val)

        delattr(self, '_classrefs')

    def get_classref_origin(self, name, attr, local_attr, classname,
                            farthest=False):
        assert name in getattr(self, attr)
        return self

    def finalize(self, schema, bases=None, *, dctx=None):
        from . import delta as sd

        fields = self.setdefaults()
        if dctx is not None and fields:
            for field in fields:
                dctx.current().op.add(sd.AlterObjectProperty(
                    property=field,
                    new_value=getattr(self, field),
                    source='default'
                ))


class NamedObject(Object):
    name = Field(sn.Name, inheritable=False, compcoef=0.670)
    title = Field(nlang.WordCombination,
                  default=None, compcoef=0.909, coerce=True)
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

    def delta_properties(self, delta, other, reverse=False, context=None):
        old, new = (other, self) if not reverse else (self, other)

        if old and new:
            if old.name != new.name:
                delta.add(old.delta_rename(new.name))

        super().delta_properties(delta, other, reverse, context)

    def delta_rename(self, new_name):
        from . import delta as sd
        from . import named

        rename_class = sd.ObjectCommandMeta.get_command_class_or_die(
            named.RenameNamedObject, type(self))

        return rename_class(classname=self.name,
                            new_name=new_name,
                            metaclass=self.get_canonical_class())

    @classmethod
    def compare_values(cls, ours, theirs, context, compcoef):
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

    def _reduce_to_ref(self):
        return ObjectRef(classname=self.name), self.name


class ObjectRef(Object):
    classname = Field(sn.SchemaName, coerce=True)

    @property
    def name(self):
        return self.classname

    def __repr__(self):
        return '<ObjectRef "{}" at 0x{:x}>'.format(self.classname, id(self))

    __str__ = __repr__

    def _reduce_to_ref(self):
        return self, self.classname

    def _resolve_ref(self, resolve):
        return resolve(self.classname)


class ObjectCollection:
    pass


class ObjectDict(typed.OrderedTypedDict, ObjectCollection,
                 keytype=str, valuetype=Object):

    def persistent_hash(self):
        vals = []
        for k, v in self.items():
            if is_named_class(v):
                v = v.name
            vals.append((k, v))
        return phash.persistent_hash(frozenset(vals))

    @classmethod
    def compare_values(cls, ours, theirs, context, compcoef):
        if not ours and not theirs:
            basecoef = 1.0
        elif not ours or not theirs:
            basecoef = 0.2
        else:
            similarity = []

            for k, v in ours.items():
                try:
                    theirsv = theirs[k]
                except KeyError:
                    # key only in ours
                    similarity.append(0.2)
                else:
                    similarity.append(v.compare(theirsv, context))

            similarity.extend(0.2 for k in set(theirs) - set(ours))
            basecoef = sum(similarity) / len(similarity)

        return basecoef + (1 - basecoef) * compcoef


class ObjectSet(typed.TypedSet, ObjectCollection, type=Object):
    @classmethod
    def merge_values(cls, ours, theirs, schema):
        if ours is None and theirs is not None:
            ours = theirs.copy()
        elif theirs is not None:
            ours.update(theirs)

        return ours

    @classmethod
    def compare_values(cls, ours, theirs, context, compcoef):
        if not ours and not theirs:
            basecoef = 1.0
        elif not ours or not theirs:
            basecoef = 0.2
        else:
            comparison = ((x.compare(y, context=context), x, y)
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

    def copy(self):
        return self.__class__(self)


class ObjectList(typed.TypedList, ObjectCollection, type=Object):
    pass


class TypeList(typed.TypedList, ObjectCollection, type=Object):
    pass


class StringList(typed.TypedList, type=str, accept_none=True):
    pass
