##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import builtins
import collections.abc
import itertools
import re
import types

from metamagic.utils.algos import persistent_hash as phash
from metamagic.utils.algos import topological
from metamagic.utils.datastructures import OrderedSet
from metamagic.utils.datastructures import struct
from metamagic.utils.datastructures import typed

from . import error as s_err
from . import name as sn


def is_named_proto(proto):
    if hasattr(proto.__class__, 'get_field'):
        try:
            proto.__class__.get_field('name')
            return True
        except KeyError:
            pass

    return False


class Field(struct.Field):
    def __init__(self, *args, compcoef=None, private=False, derived=False,
                       simpledelta=True, **kwargs):
        super().__init__(*args, **kwargs)
        self.compcoef = compcoef
        self.private = private
        self.derived = derived
        self.simpledelta = simpledelta


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

        if not issubclass(cls, BasePrototype):
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


class PrototypeClass(type):
    pass


class ProtoObject(metaclass=PrototypeClass):
    @classmethod
    def get_canonical_class(cls):
        return cls


class ProtoNode(ProtoObject):
    pass


class PrototypeMeta(PrototypeClass, struct.MixedStructMeta):
    def __new__(mcls, name, bases, dct, **kwargs):
        cls = super().__new__(mcls, name, bases, dct, **kwargs)
        cls._ref_type = None
        return cls

    @property
    def ref_type(cls):
        if cls._ref_type is None:
            name = cls.__name__ + '_ref'
            dct = {'__module__': cls.__module__}
            cls._ref_type = cls.__class__(name, (PrototypeRef, cls), dct)

            for fn, f in list(cls._ref_type._fields.items()):
                f = f.copy()
                f.default = None
                cls._ref_type._fields[fn] = f

        return cls._ref_type


class BasePrototype(struct.MixedStruct, ProtoObject, metaclass=PrototypeMeta):
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

            if is_named_proto(v):
                v = v.name

            criteria.append((f, v))

        return tuple(criteria)

    def persistent_hash(self):
        """Compute object 'snapshot' hash

        This is an explicit method since prototype objects are mutable.
        The hash must be externally stable, i.e. stable across the runs
        and thus must not contain default object hashes (addresses),
        including that of None"""

        return phash.persistent_hash(self.hash_criteria())

    def mergeable_fields(self):
        for fn, f in self.__class__.get_fields().items():
            if not f.private:
                yield fn

    def merge(self, obj, *, schema):
        """Merge properties of another object into this object.

           Most often use of this method is to implement property
           acquisition through inheritance.
        """
        if (not isinstance(obj, self.__class__)
                and not isinstance(self, obj.__class__)):
            msg = "cannot merge instances of %s and %s" % \
                            (obj.__class__.__name__, self.__class__.__name__)
            raise s_err.SchemaError(msg)

        for field_name in self.mergeable_fields():
            field = self.__class__.get_field(field_name)
            FieldType = field.type[0]

            ours = getattr(self, field_name)
            theirs = getattr(obj, field_name)

            merger = getattr(FieldType, 'merge_values', None)
            if callable(merger):
                result = merger(ours, theirs, schema=schema)
                setattr(self, field_name, result)
            else:
                if ours is None and theirs is not None:
                    setattr(self, field_name, theirs)

    def compare(self, other, context=None):
        if (not isinstance(other, self.__class__)
                and not isinstance(self, other.__class__)):
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
            msg = 'missing required delta driver info for {}'.format(
                    self.__class__.__name__)
            raise AttributeError(msg) from None

        old, new = (other, self) if not reverse else (self, other)

        command_args = {}

        if old and new:
            try:
                name = old.name
            except AttributeError:
                pass
            else:
                command_args['prototype_name'] = name

            delta = delta_driver.alter(prototype_class=new.__class__,
                                       **command_args)
            self.delta_properties(delta, other, reverse, context=context)

        elif not old:
            try:
                name = new.name
            except AttributeError:
                pass
            else:
                command_args['prototype_name'] = name

            delta = delta_driver.create(prototype_class=new.__class__,
                                        **command_args)
            self.delta_properties(delta, other, reverse, context=context)

        else:
            try:
                name = old.name
            except AttributeError:
                pass
            else:
                command_args['prototype_name'] = name

            delta = delta_driver.delete(prototype_class=old.__class__,
                                        **command_args)

        return delta

    def _reduce_obj_dict(self, v):
        result = {}
        comparison_v = {}

        for k, p in v.items():
            if is_named_proto(p):
                result[k] = PrototypeRef(prototype_name=p.name)
                comparison_v[k] = p.name

            elif isinstance(p, Collection):
                if is_named_proto(p.element_type):
                    eltype = PrototypeRef(prototype_name=p.element_type.name)
                    result[k] = p.__class__(element_type=eltype)
                    comparison_v[k] = (p.__class__, eltype.prototype_name)

            else:
                result[k] = p
                comparison_v[k] = p.class_name

        return result, frozenset(comparison_v.items())

    def _reduce_obj_coll(self, v):
        result = []
        comparison_v = []

        for p in v:
            if is_named_proto(p):
                result.append(PrototypeRef(prototype_name=p.name))
                comparison_v.append(p.name)

            else:
                result.append(p)
                comparison_v.append(p.class_name)

        return result, tuple(comparison_v)

    _reduce_obj_list = _reduce_obj_coll

    def _reduce_obj_set(self, v):
        result, comparison_v = self._reduce_obj_coll(v)
        return result, frozenset(comparison_v)

    def _reduce_refs(self, value):
        if is_named_proto(value):
            val = value.name
            ref = PrototypeRef(prototype_name=val)

        elif isinstance(value, PrototypeDict):
            ref, val = self._reduce_obj_dict(value)

        elif isinstance(value, PrototypeList):
            ref, val = self._reduce_obj_list(value)

        elif isinstance(value, PrototypeSet):
            ref, val = self._reduce_obj_set(value)

        else:
            ref = value
            val = value

        return ref, val

    def _restore_refs(self, field_name, ref, resolve):
        ftype = self.__class__.get_field(field_name).type[0]

        if is_named_proto(ftype):
            val = resolve(ref.prototype_name)

        elif issubclass(ftype, (PrototypeSet, PrototypeList)):
            val = ftype(resolve(r.prototype_name) for r in ref)

        elif issubclass(ftype, PrototypeDict):
            result = []

            for k, r in ref.items():
                if isinstance(r, Collection):
                    eltype = resolve(r.element_type.prototype_name)
                    r = r.__class__(element_type=eltype)
                else:
                    r = resolve(r.prototype_name)

                result.append((k, r))

            val = ftype(result)

        else:
            msg = 'unexpected ref type in restore_refs: {!r}'.format(ref)
            raise ValueError(msg)

        return val

    def delta_properties(self, delta, other, reverse=False, context=None):
        from metamagic.caos.lang.schema import delta as sd

        old, new = (other, self) if not reverse else (self, other)

        ff = self.__class__.get_fields(sorted=True).items()
        fields = [fn for fn, f in ff if f.simpledelta]

        if old and new:
            for f in fields:
                oldattr, oldattr_v = self._reduce_refs(getattr(old, f))
                newattr, newattr_v = self._reduce_refs(getattr(new, f))

                if oldattr_v != newattr_v:
                    delta.add(sd.AlterPrototypeProperty(
                        property=f, old_value=oldattr, new_value=newattr))
        elif not old:
            for f in fields:
                value = getattr(new, f)
                if value is not None:
                    value, _ = self._reduce_refs(value)
                    delta.add(sd.AlterPrototypeProperty(
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
        return {getattr(b, 'class_name', getattr(b, 'name', None))
                for b in self.bases}

    @classmethod
    # @debug.debug
    def delta_sets(cls, old, new, result, context=None, *,
                                          old_schema=None, new_schema=None):
        adds_mods, dels = cls._delta_sets(old, new, context=context,
                                          old_schema=old_schema,
                                          new_schema=new_schema)

        result.update(adds_mods)
        result.update(dels)

    @classmethod
    # @debug.debug
    def _delta_sets(cls, old, new, context=None, *,
                                   old_schema=None, new_schema=None):
        from metamagic.caos.lang.schema import named as s_named
        from metamagic.caos.lang.schema import realm as s_realm

        adds_mods = s_realm.AlterRealm()
        dels = s_realm.AlterRealm()

        if old is None:
            for n in new:
                adds_mods.add(n.delta(None, context=context))
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

        """LOG [caos.delta.comp] Index comparison
        from metamagic.utils import markup
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

                altered_idx = {p.prototype_name: p for p in altered}
                for p in altered:
                    for op in p(s_named.RenameNamedPrototype):
                        altered_idx[op.new_name] = p

                for p in altered:
                    old_proto = old_schema.get(p.prototype_name)

                    for op in p(s_named.RenameNamedPrototype):
                        new_name = op.new_name
                        break
                    else:
                        new_name = p.prototype_name

                    new_proto = new_schema.get(new_name)

                    bases = {getattr(b, 'class_name', getattr(b, 'name', None))
                             for b in old_proto.bases} | \
                            {getattr(b, 'class_name', getattr(b, 'name', None))
                             for b in new_proto.bases}

                    deps = {b for b in bases if b in altered_idx}

                    g[p.prototype_name] = {'item': p, 'deps': deps}
                    if new_name != p.prototype_name:
                        g[new_name] = {'item': p, 'deps': deps}

                altered = topological.sort(g)

        for p in altered:
            adds_mods.add(p)

        if deleted:
            deleted = cls._sort_set(deleted)
            for y in reversed(list(deleted)):
                dels.add(y.delta(None, reverse=True, context=context))

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

        state['_protorefs'] = refs if refs else None

        return state

    def _finalize_setstate(self, _objects, _resolve):
        protorefs = getattr(self, '_protorefs', None)
        if not protorefs:
            return

        for field_name in protorefs:
            ref = getattr(self, field_name)
            val = self._restore_refs(field_name, ref, _resolve)
            setattr(self, field_name, val)

        delattr(self, '_protorefs')

    def get_protoref_origin(self, name, attr, local_attr, classname,
                                                          farthest=False):
        assert name in getattr(self, attr)
        return self

    def finalize(self, schema, bases=None):
        pass


class PrototypeRef(BasePrototype):
    prototype_name = Field(sn.SchemaName, coerce=True)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __repr__(self):
        cls = self.__class__
        return '<{}.{} "{}" at 0x{:x}>'.format(
                    cls.__module__, cls.__name__,
                    self.prototype_name, id(self))

    __str__ = __repr__


class Collection(BasePrototype, ProtoNode):
    element_type = Field(BasePrototype, None)

    def compare(self, other, context):
        if not isinstance(other, Collection):
            return 0.1

        if self.get_canonical_class() != other.get_canonical_class():
            return 0.2

        return self.element_type.compare(other.element_type, context)

    def get_container(self):
        raise NotImplementedError

    def coerce(self, items, schema):
        from . import atoms as s_atoms
        from . import types as s_types

        container = self.get_container()

        elements = []
        if self.element_type is not None:
            if isinstance(self.element_type, PrototypeRef):
                eltype = schema.get(self.element_type.prototype_name)
            else:
                eltype = self.element_type

            if isinstance(eltype, s_atoms.Atom):
                eltype = eltype.get_topmost_base()
                eltype = s_types.BaseTypeMeta.get_implementation(eltype.name)

            for item in items:
                if not isinstance(item, eltype):
                    item = eltype(item)
                elements.append(item)

        return container(elements)


class Set(Collection):
    def get_container(self):
        return frozenset


class List(Collection):
    def get_container(self):
        return tuple


class PrototypeDict(typed.TypedDict, keytype=str, valuetype=BasePrototype):
    def persistent_hash(self):
        vals = []
        for k, v in self.items():
            if is_named_proto(v):
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


class PrototypeSet(typed.TypedSet, type=BasePrototype):
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


class PrototypeList(typed.TypedList, type=BasePrototype):
    pass


class SchemaTypeConstraint(BasePrototype):
    data = Field(object)


class SchemaTypeConstraintSet(typed.TypedSet, type=SchemaTypeConstraint):
    pass


class SchemaTypeConstraintEnum(SchemaTypeConstraint):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.data = frozenset(self.data)

    def check(self, value):
        if value not in self.data:
            msg = '{{name!r}} must be one of: {}'.format(', '.join(self.data))
            raise ValueError(msg)

    def __eq__(self, other):
        if isinstance(other, SchemaTypeConstraintEnum):
            return self.data == other.data
        else:
            return False

    def __hash__(self):
        return hash(self.hash_criteria())


class SchemaType(BasePrototype):
    _types = {'str', 'int', 'tuple'}
    _containers = {'tuple'}

    main_type = Field(str)
    element_type = Field(str, None)
    constraints = Field(SchemaTypeConstraintSet, None)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        if self.main_type not in self._types:
            raise s_err.SchemaError(
                    'invalid schema type: {!r}'.format(self.main_type))

        if self.element_type is not None:
            if self.main_type not in self._containers:
                raise s_err.SchemaError(
                    'invalid container type: {!r}'.format(self.main_type))

            if self.element_type not in self._types:
                raise s_err.SchemaError(
                    'invalid schema type: {!r}'.format(self.element_type))

        self._init()

    def _init(self):
        if self.element_type:
            name = 'schema_type_{}_{}'.format(self.main_type,
                                              self.element_type)

            if self.main_type == 'tuple':
                self._typ = tuple

            self._elemtyp = getattr(builtins, self.element_type)

            self._validator = types.new_class(name, (typed.TypedList,),
                                              dict(type=self._elemtyp))
        else:
            self._typ = getattr(builtins, self.main_type)
            self._validator = self._typ

    def __eq__(self, other):
        if isinstance(other, SchemaType):
            return (self.main_type == other.main_type and
                    self.element_type == other.element_type and
                    self.constraints == other.constraints)
        else:
            return False

    def __hash__(self):
        return hash(self.hash_criteria())

    def coerce(self, value):
        vv = self._validator(value)
        if self._typ is not self._validator:
            vv = self._typ(vv)

        if self.constraints:
            for constraint in self.constraints:
                constraint.check(vv)

        return vv

    @property
    def is_container(self):
        return self.element_type is not None

    def __getstate__(self):
        return {k: getattr(self, k) for k in self}

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._init()


class TypeRef:
    _typeref_re = re.compile(
        r'''^(?P<type>\w+(?:\.\w+)*)
             (?:\<(?P<eltype>(?:\w+(?:\.\w+)*)|)\>)?$''', re.X)

    @classmethod
    def parse(cls, typeref):
        m = cls._typeref_re.match(typeref)

        if not m:
            msg = 'invalid type: {!r}'.format(typeref)
            raise ValueError(msg)

        type = m.group('type')
        element_type = m.group('eltype')

        if element_type is not None:
            collection_type = type
            type = element_type
            if type == '':
                type = None
        else:
            collection_type = None

        if collection_type and collection_type not in {'set', 'list'}:
            msg = 'invalid collection type: {!r}'.format(collection_type)
            raise ValueError(msg)

        if collection_type == 'set':
            collection_type = Set
        elif collection_type == 'list':
            collection_type = List

        return collection_type, type


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
