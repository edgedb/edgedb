##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections.abc
import itertools

from edgedb.lang.common import persistent_hash as phash
from edgedb.lang.common import topological
from edgedb.lang.common.ordered import OrderedSet
from edgedb.lang.common import struct, typed

from . import error as s_err
from . import name as sn


def is_named_class(scls):
    if hasattr(scls.__class__, 'get_field'):
        name_field = scls.__class__.get_field('name')
        return name_field is not None

    return False


class Field(struct.Field):
    __name__ = ('compcoef', 'private', 'derived', 'simpledelta',
                'merge_fn', 'introspectable')

    def __init__(self, *args, compcoef=None, private=False, derived=False,
                 simpledelta=True, merge_fn=None, introspectable=True,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.compcoef = compcoef
        self.private = private
        self.derived = derived
        self.simpledelta = simpledelta
        self.merge_fn = merge_fn
        self.introspectable = introspectable


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

    def push(self, pair):
        cls = None
        obj = pair[1] if pair[0] is None else pair[0]

        cls = obj.__class__

        if not issubclass(cls, Class):
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


class MetaClass(struct.MixedStructMeta):
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
            cls._ref_type = cls.__class__(name, (ClassRef, cls), dct)

            for fn, f in list(cls._ref_type._fields.items()):
                f = f.copy()
                f.default = None
                cls._ref_type._fields[fn] = f

        return cls._ref_type

    @classmethod
    def get_schema_metaclasses(mcls):
        return mcls._schema_metaclasses


class Class(struct.MixedStruct, metaclass=MetaClass):
    @classmethod
    def get_canonical_class(cls):
        return cls

    def __init__(self, **kwargs):
        self._attr_sources = {}
        super().__init__(**kwargs)

    def hash_criteria_fields(self):
        for fn, f in self.__class__.get_fields(sorted=True).items():
            if not f.derived:
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

    def set_attribute(self, name, value, *, dctx=None, source=None):
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
                dctx.op.add(sd.AlterClassProperty(
                    property=name,
                    new_value=value,
                    source=source
                ))

    def set_default_value(self, field_name, value):
        setattr(self, field_name, value)
        self._attr_sources[field_name] = 'default'

    def persistent_hash(self):
        """Compute object 'snapshot' hash.

        This is an explicit method since Class objects are mutable.
        The hash must be externally stable, i.e. stable across the runs
        and thus must not contain default object hashes (addresses),
        including that of None.
        """
        return phash.persistent_hash(self.hash_criteria())

    def mergeable_fields(self):
        for fn, f in self.__class__.get_fields().items():
            if not f.private:
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

        for field_name in self.mergeable_fields():
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
        try:
            delta_driver = self.delta_driver
        except AttributeError:
            raise AttributeError(
                'missing required delta driver info for'
                f'{self.__class__.__name__}') from None

        old, new = (other, self) if not reverse else (self, other)

        command_args = {}

        if old and new:
            try:
                name = old.name
            except AttributeError:
                pass
            else:
                command_args['classname'] = name

            delta = delta_driver.alter(metaclass=new.__class__, **command_args)
            self.delta_properties(delta, other, reverse, context=context)

        elif not old:
            try:
                name = new.name
            except AttributeError:
                pass
            else:
                command_args['classname'] = name

            delta = delta_driver.create(metaclass=new.__class__,
                                        **command_args)
            self.delta_properties(delta, other, reverse, context=context)

        else:
            try:
                name = old.name
            except AttributeError:
                pass
            else:
                command_args['classname'] = name

            delta = delta_driver.delete(metaclass=old.__class__,
                                        **command_args)

        return delta

    def _reduce_obj_dict(self, v):
        result = {}
        comparison_v = {}

        for k, p in v.items():
            if is_named_class(p):
                result[k] = ClassRef(classname=p.name)
                comparison_v[k] = p.name

            elif isinstance(p, Collection):
                strefs = []

                for st in p.get_subtypes():
                    strefs.append(ClassRef(classname=st.name))

                result[k] = p.__class__.from_subtypes(strefs)
                comparison_v[k] = \
                    (p.__class__, tuple(r.classname for r in strefs))

            else:
                result[k] = p
                comparison_v[k] = p.classname

        return result, frozenset(comparison_v.items())

    def _reduce_obj_coll(self, v):
        result = []
        comparison_v = []

        for p in v:
            if is_named_class(p):
                result.append(ClassRef(classname=p.name))
                comparison_v.append(p.name)

            else:
                result.append(p)
                comparison_v.append(p.classname)

        return result, tuple(comparison_v)

    _reduce_obj_list = _reduce_obj_coll

    def _reduce_obj_set(self, v):
        result, comparison_v = self._reduce_obj_coll(v)
        return result, frozenset(comparison_v)

    def _reduce_refs(self, value):
        if is_named_class(value):
            val = value.name
            ref = ClassRef(classname=val)

        elif isinstance(value, ClassDict):
            ref, val = self._reduce_obj_dict(value)

        elif isinstance(value, ClassList):
            ref, val = self._reduce_obj_list(value)

        elif isinstance(value, ClassSet):
            ref, val = self._reduce_obj_set(value)

        else:
            ref = value
            val = value

        return ref, val

    def _restore_refs(self, field_name, ref, resolve):
        ftype = self.__class__.get_field(field_name).type[0]

        if is_named_class(ftype):
            val = resolve(ref.classname)

        elif issubclass(ftype, (ClassSet, ClassList)):
            val = ftype(resolve(r.classname) for r in ref)

        elif issubclass(ftype, ClassDict):
            result = []

            for k, r in ref.items():
                if isinstance(r, Collection):
                    subtypes = []
                    for stref in r.get_subtypes():
                        subtypes.append(resolve(stref.classname))

                    r = r.__class__.from_subtypes(subtypes)
                else:
                    r = resolve(r.classname)

                result.append((k, r))

            val = ftype(result)

        else:
            msg = 'unexpected ref type in restore_refs: {!r}'.format(ref)
            raise ValueError(msg)

        return val

    def delta_properties(self, delta, other, reverse=False, context=None):
        from edgedb.lang.schema import delta as sd

        old, new = (other, self) if not reverse else (self, other)

        ff = self.__class__.get_fields(sorted=True).items()
        fields = [fn for fn, f in ff if f.simpledelta]

        if old and new:
            for f in fields:
                oldattr, oldattr_v = self._reduce_refs(getattr(old, f))
                newattr, newattr_v = self._reduce_refs(getattr(new, f))

                if oldattr_v != newattr_v:
                    delta.add(sd.AlterClassProperty(
                        property=f, old_value=oldattr, new_value=newattr))
        elif not old:
            for f in fields:
                value = getattr(new, f)
                if value is not None:
                    value, _ = self._reduce_refs(value)
                    delta.add(sd.AlterClassProperty(
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
        from edgedb.lang.schema import named as s_named
        from edgedb.lang.schema import database as s_db

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

        """LOG [edgedb.delta.comp] Index comparison
        from edgedb.lang.common import markup
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
                    for op in p.get_subcommands(type=s_named.RenameNamedClass):
                        altered_idx[op.new_name] = p

                for p in altered:
                    old_class = old_schema.get(p.classname)

                    for op in p.get_subcommands(type=s_named.RenameNamedClass):
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
                dctx.current().op.add(sd.AlterClassProperty(
                    property=field,
                    new_value=getattr(self, field),
                    source='default'
                ))


class ClassRef(Class):
    classname = Field(sn.SchemaName, coerce=True)

    def __repr__(self):
        return '<ClassRef "{}" at 0x{:x}>'.format(self.classname, id(self))

    __str__ = __repr__


class NodeClass:
    @classmethod
    def compare_values(cls, ours, theirs, context, compcoef):
        if isinstance(ours, Collection) or isinstance(theirs, Collection):
            return ours.__class__.compare_values(
                ours, theirs, context, compcoef)
        elif ours != theirs:
            return compcoef
        else:
            return 1.0


class Collection(Class, NodeClass):
    element_type = Field(Class)

    @property
    def name(self):
        try:
            return self._name_cached
        except AttributeError:
            pass

        subtypes = ",".join(st.name for st in self.get_subtypes())
        self._name_cached = f'{self.schema_name}<{subtypes}>'
        return self._name_cached

    def issubclass(self, parent):
        if not isinstance(parent, Collection) and parent.name == 'std::any':
            return True

        if parent.__class__ is not self.__class__:
            return False

        parent_types = parent.get_subtypes()
        my_types = self.get_subtypes()

        for pt, my in zip(parent_types, my_types):
            if pt.name != 'std::any' and not pt.issubclass(my):
                return False

        return True

    @classmethod
    def compare_values(cls, ours, theirs, context, compcoef):
        if ours.get_canonical_class() != theirs.get_canonical_class():
            basecoef = 0.2
        else:
            my_subtypes = ours.get_subtypes()
            other_subtypes = theirs.get_subtypes()

            similarity = []
            for i, st in enumerate(my_subtypes):
                similarity.append(st.compare(other_subtypes[i], context))

            basecoef = sum(similarity) / len(similarity)

        return basecoef + (1 - basecoef) * compcoef

    def get_container(self):
        raise NotImplementedError

    def get_element_type(self):
        return self.element_type

    def get_subtypes(self):
        return (self.element_type,)

    def get_subtype(self, schema, typeref):
        from . import atoms as s_atoms
        from . import types as s_types

        if isinstance(typeref, ClassRef):
            eltype = schema.get(typeref.classname)
        else:
            eltype = typeref

        if isinstance(eltype, s_atoms.Atom):
            eltype = eltype.get_topmost_base()
            eltype = s_types.BaseTypeMeta.get_implementation(eltype.name)

        return eltype

    def coerce(self, items, schema):
        container = self.get_container()

        elements = []

        eltype = self.get_subtype(schema, self.element_type)

        for item in items:
            if not isinstance(item, eltype):
                item = eltype(item)
            elements.append(item)

        return container(elements)

    @classmethod
    def get_class(cls, schema_name):
        if schema_name == 'array':
            return Array
        elif schema_name == 'set':
            return Set
        elif schema_name == 'map':
            return Map
        else:
            raise ValueError(
                'unknown collection type: {!r}'.format(schema_name))

    @classmethod
    def from_subtypes(cls, subtypes):
        if len(subtypes) != 1:
            raise ValueError(
                f'unexpected number of subtypes, expecting 1: {subtypes!r}')
        return cls(element_type=subtypes[0])


class Set(Collection):
    schema_name = 'set'

    def get_container(self):
        return frozenset


class Array(Collection):
    schema_name = 'array'

    def get_container(self):
        return tuple


class Tuple(Collection):
    schema_name = 'tuple'

    def get_container(self):
        return tuple


class Map(Collection):
    schema_name = 'map'

    key_type = Field(Class)

    def get_container(self):
        return dict

    def get_subtypes(self):
        return (self.key_type, self.element_type,)

    @classmethod
    def from_subtypes(cls, subtypes):
        if len(subtypes) != 2:
            raise ValueError(
                f'unexpected number of subtypes, expecting 2: {subtypes!r}')
        return cls(key_type=subtypes[0], element_type=subtypes[1])


class ClassCollection:
    pass


class ClassDict(typed.OrderedTypedDict, ClassCollection,
                keytype=str, valuetype=Class):

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


class ClassSet(typed.TypedSet, ClassCollection, type=Class):
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


class ClassList(typed.TypedList, ClassCollection, type=Class):
    pass


class TypeList(typed.TypedList, ClassCollection, type=Class):
    pass


class StringList(typed.TypedList, type=str, accept_none=True):
    pass


class ArgDict(typed.TypedDict, keytype=str, valuetype=object):
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
                    similarity.append(1.0 if v == theirsv else 0.4)

            similarity.extend(0.2 for k in set(theirs) - set(ours))
            basecoef = sum(similarity) / len(similarity)

        return basecoef + (1 - basecoef) * compcoef
