##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

from edgedb.lang.common.functional import hybridmethod
from edgedb.lang.common import datastructures as ds

from . import delta as sd
from . import error as schema_error
from . import inheriting
from . import objects as so
from . import named


class RefDict:
    def __init__(self, local_attr=None, *, ordered=False, title=None,
                                           backref='subject', ref_cls,
                                           compcoef=None):
        self.local_attr = local_attr
        self.ordered = ordered
        self.title = title
        self.backref_attr = backref
        self.ref_cls = ref_cls
        self.compcoef = compcoef

    def set_attr_name(self, attr):
        self.attr = attr
        if self.local_attr is None:
            self.local_attr = 'local_{}'.format(attr)

        if self.title is None:
            self.title = attr
            if self.title.endswith('s'):
                self.title = self.title[:-1]

    def get_new(self):
        collection = collections.OrderedDict if self.ordered else dict
        return collection()

    def initialize_in(self, obj):
        setattr(obj, self.attr, self.get_new())
        setattr(obj, self.local_attr, self.get_new())

    def copy(self):
        return self.__class__(local_attr=self.local_attr, ordered=self.ordered,
                              title=self.title, backref=self.backref_attr,
                              ref_cls=self.ref_cls, compcoef=self.compcoef)


class ReferencingPrototypeMeta(type(inheriting.InheritingPrototype)):
    def __new__(mcls, name, bases, clsdict):
        refdicts = {}
        mydicts = {k: v for k, v in clsdict.items() if isinstance(v, RefDict)}
        cls = super().__new__(mcls, name, bases, clsdict)

        for parent in reversed(cls.__mro__):
            if parent is cls:
                refdicts.update(mydicts)
            elif isinstance(parent, ReferencingPrototypeMeta):
                refdicts.update({k: d.copy()
                                for k, d in parent.get_own_refdicts().items()})

        for k, dct in refdicts.items():
            dct.set_attr_name(k)
            if isinstance(dct.ref_cls, str):
                ref_cls_getter = getattr(cls, dct.ref_cls)
                try:
                    dct.ref_cls = ref_cls_getter()
                except NotImplementedError:
                    pass

        cls._refdicts = collections.OrderedDict(
            sorted(refdicts.items(), key=lambda e: e[0])
        )
        setattr(cls, '{}.{}_refdicts'.format(cls.__module__, cls.__name__),
                     mydicts)
        return cls

    def get_own_refdicts(cls):
        return getattr(cls, '{}.{}_refdicts'.format(
                                cls.__module__, cls.__name__))

    def get_refdicts(cls):
        return iter(cls._refdicts.values())

    def get_refdict(cls, name):
        return cls._refdicts.get(name)


class ReferencingPrototype(inheriting.InheritingPrototype,
                           metaclass=ReferencingPrototypeMeta):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        for refdict in self.__class__.get_refdicts():
            refdict.initialize_in(self)

    def __getstate__(self):
        state = super().__getstate__()

        for refdict in self.__class__.get_refdicts():
            attr = refdict.attr
            local_attr = refdict.local_attr
            title = refdict.title
            state[local_attr] = self._get_protoref_dict(local_attr)

            coll = getattr(self, attr)
            state[attr] = [
                (n, self.get_protoref_origin(n, attr, local_attr, title).name)
                for n in coll
            ]

        return state

    def hash_criteria(self):
        criteria = []

        for refdict in self.__class__.get_refdicts():
            attr = refdict.local_attr
            dct = getattr(self, attr)
            criteria.append((attr, frozenset(dct.values())))

        return super().hash_criteria() + tuple(criteria)

    def _finalize_setstate(self, _objects, _resolve):
        super()._finalize_setstate(_objects, _resolve)

        for refdict in self.__class__.get_refdicts():
            attr = refdict.attr
            local_attr = refdict.local_attr
            self._resolve_protoref_dict(
                    _objects, _resolve, local_attr)
            self._resolve_inherited_protoref_dict(
                    _objects, _resolve, attr, local_attr)

    @hybridmethod
    def copy(scope, obj=None):
        if isinstance(scope, type):
            cls = scope
        else:
            obj = scope
            cls = obj.__class__

        result = super(ReferencingPrototype, cls).copy(obj)

        for refdict in obj.__class__.get_refdicts():
            attr = refdict.attr
            local_attr = refdict.local_attr
            all_coll = getattr(obj, attr)
            local_coll = getattr(obj, local_attr)

            coll_copy = {n: p.copy() for n, p in all_coll.items()}
            setattr(result, attr, coll_copy)
            setattr(result, local_attr, {n: coll_copy[n] for n in local_coll})

        return result

    def compare(self, other, context=None):
        context = context or so.ComparisonContext()

        with context(self, other):
            similarity = super().compare(other, context=context)
            if similarity is NotImplemented:
                return NotImplemented

            for refdict in self.__class__.get_refdicts():
                if refdict.compcoef is None:
                    continue

                local_attr = refdict.local_attr
                ours = getattr(self, local_attr).values()
                if other is not None:
                    theirs = getattr(other, local_attr).values()
                else:
                    theirs = set()

                ref_similarity = so.PrototypeSet.compare_values(
                                    ours, theirs, context=context,
                                    compcoef=refdict.compcoef)

                similarity *= ref_similarity

        return similarity

    def merge(self, obj, *, schema):
        super().merge(obj, schema=schema)

        for refdict in self.__class__.get_refdicts():
            # Merge prototype references in each registered collection
            #
            this_coll = getattr(self, refdict.attr)
            other_coll = getattr(obj, refdict.attr)

            this_coll.update({k: v for k, v in other_coll.items()
                                   if k not in this_coll})

    def delta(self, other, reverse=False, context=None):
        old, new = (other, self) if not reverse else (self, other)

        context = context or so.ComparisonContext()

        cls = (old or new).__class__

        with context(old, new):
            delta = super().delta(other, reverse=reverse, context=context)
            if isinstance(delta, sd.CreatePrototype):
                # If this is a CREATE delta, we need to make
                # sure it is returned separately from the creation
                # of references, which will go into a separate ALTER
                # delta.  This is needed to avoid the hassle of
                # sorting the delta order by dependencies or having
                # to maintain ephemeral forward references.
                alter_delta = super().delta(self, context=context)
                full_delta = sd.CommandGroup()
                full_delta.add(delta)
            else:
                full_delta = alter_delta = delta

            idx_key = lambda o: o.persistent_hash()

            for refdict in cls.get_refdicts():
                local_attr = refdict.local_attr

                if old:
                    oldcoll = getattr(old, local_attr).values()
                    oldcoll_idx = ds.OrderedIndex(oldcoll, key=idx_key)
                else:
                    oldcoll_idx = {}

                if new:
                    newcoll = getattr(new, local_attr).values()
                    newcoll_idx = ds.OrderedIndex(newcoll, key=idx_key)
                else:
                    newcoll_idx = {}

                self.delta_sets(oldcoll_idx, newcoll_idx, alter_delta, context)

            if alter_delta is not full_delta:
                if alter_delta.has_subcommands():
                    full_delta.add(alter_delta)
                else:
                    full_delta = delta

        return full_delta

    def get_protoref_origin(self, name, attr, local_attr, classname,
                                                          farthest=False):
        assert name in getattr(self, attr)

        result = None

        if name in getattr(self, local_attr):
            result = self

        if not result or farthest:
            bases = (c for c in self.get_mro()[1:]
                     if isinstance(c, named.NamedPrototype))

            for c in bases:
                if name in getattr(c, local_attr):
                    result = c
                    if not farthest:
                        break

        if result is None:
            raise KeyError(
                    'could not find {} "{}" origin'.format(classname, name))

        return result

    def add_protoref(self, collection, obj, replace=False):
        refdict = self.__class__.get_refdict(collection)
        attr = refdict.attr
        local_attr = refdict.local_attr
        coll_obj = refdict.title

        local_coll = getattr(self, local_attr)
        all_coll = getattr(self, attr)

        key = obj.normalize_name(obj.name)
        existing = local_coll.get(key)
        if existing is not None:
            msg = '{} {!r} is already present in {!r}'.format(
                        coll_obj, key, self.name)
            raise schema_error.SchemaError(msg)

        local_coll[key] = obj
        all_coll[key] = obj

    def del_protoref(self, collection, obj_name, schema):
        refdict = self.__class__.get_refdict(collection)
        attr = refdict.attr
        local_attr = refdict.local_attr
        refcls = refdict.ref_cls

        local_coll = getattr(self, local_attr)
        all_coll = getattr(self, attr)

        key = refcls.normalize_name(obj_name)
        is_local = key in local_coll

        local_coll.pop(key)
        all_coll.pop(key)

        if is_local:
            for descendant in self.descendants(schema):
                descendant_local_coll = getattr(descendant, local_attr)
                if key not in descendant_local_coll:
                    descendant_coll = getattr(descendant, attr)
                    descendant_coll.pop(key, None)

    def _get_protoref_dict(self, attr):
        values = getattr(self, attr)
        result = collections.OrderedDict()

        if values:
            for k, v in values.items():
                if isinstance(v, named.NamedPrototype):
                    v = so.PrototypeRef(prototype_name=v.name)
                result[k] = v

        return result

    def _resolve_protoref_dict(self, _objects, _resolve, local_attr):
        values = getattr(self, local_attr)

        if values:
            for n, v in values.items():
                if isinstance(v, so.PrototypeRef):
                    values[n] = _resolve(v.prototype_name)

    def _resolve_inherited_protoref_dict(self, _objects, _resolve,
                                               attr, local_attr):
        values = getattr(self, attr)

        if values is not None and values.__class__ is list:
            attrs = {}
            _mro = None

            for an, origin in values:
                try:
                    subj = _objects[origin]
                except KeyError:
                    if _mro is None:
                        _mro = {c.name: c for c in self.get_mro()
                                if isinstance(c, named.NamedPrototype)}
                    subj = _objects[origin] = _mro[origin]

                attrs[an] = getattr(subj, local_attr)[an]

            setattr(self, attr, attrs)

    def finalize(self, schema, bases=None):
        super().finalize(schema, bases=bases)

        if bases is None:
            bases = self.bases

        for refdict in self.__class__.get_refdicts():
            attr = refdict.attr
            local_attr = refdict.local_attr
            backref_attr = refdict.backref_attr
            ref_cls = refdict.ref_cls

            ref_keys = self.begin_protoref_dict_merge(
                                     schema, bases=bases, attr=attr)

            self.merge_protoref_dict(schema, bases=bases, attr=attr,
                                     local_attr=local_attr,
                                     backref_attr=backref_attr,
                                     protorefcls=ref_cls,
                                     protoref_keys=ref_keys)

            self.finish_protoref_dict_merge(schema, bases=bases, attr=attr)

    def begin_protoref_dict_merge(self, schema, bases, attr):
        pass

    def finish_protoref_dict_merge(self, schema, bases, attr):
        pass

    def merge_protoref_dict(self, schema, bases, attr, local_attr,
                                  backref_attr, protorefcls,
                                  protoref_keys=None):
        """Perform merging of protoref attributes from base prototypes.

        :param schema:         The prototype schema.

        :param bases:          An iterable containing base prototypes.

        :param str attr:       Name of an attribute containing the full
                               protoref collection.

        :param str local_attr: Name of an attribute containing the collection
                               of protorefs defined locally (not inherited).

        :param str backref_attr: Name of an attribute on a referenced prototype
                                 containing a reference back to this prototype.

        :param protorefcls:    Referenced prototype class.

        :param protorefkeys:   An optional list of protoref keys to consider
                               for merging.  If not specified, protorefs
                               defined on self are used.
        """
        protorefs = getattr(self, attr)
        local_protorefs = getattr(self, local_attr)

        ODict = collections.OrderedDict

        if protoref_keys is None:
            protoref_keys = protorefs

        for protoref_key in protoref_keys:
            local = local_protorefs.get(protoref_key)

            base_refs = [getattr(b, attr, {}).get(protoref_key) for b in bases]
            inherited = filter(lambda i: i is not None, base_refs)

            # Build a list of (source_proto, target_proto) tuples
            # for this key.
            #
            inherited = ODict((getattr(pref, backref_attr), pref)
                              for pref in inherited)

            pure_inheritance = False

            if (1 if local else 0) + len(inherited) > 1:
                # We have multiple protorefs defined, need to perform
                # merging.
                #
                items = []
                if local:
                    items.append(local)
                items.extend(inherited.values())

                merged = protorefcls.merge_many(schema, items, source=self,
                                                               replace=local)

            elif not local and inherited:
                # Pure inheritance
                item = list(inherited.values())[0]
                merged = protorefcls.inherit_pure(schema, item, source=self)
                pure_inheritance = merged is item

            else:
                # Not inherited
                merged = local

            if merged is not local:
                if not pure_inheritance:
                    local_protorefs[protoref_key] = merged

                protorefs[protoref_key] = merged

    def rederive_protorefs(self, schema, add_to_schema=False,
                                         mark_derived=False):
        for refdict in self.__class__.get_refdicts():
            attr = refdict.attr
            local_attr = refdict.local_attr
            all_coll = getattr(self, attr)
            local_coll = getattr(self, local_attr)

            for pn, p in local_coll.items():
                local_coll[pn] = p.derive(schema, self,
                                          add_to_schema=add_to_schema,
                                          mark_derived=mark_derived)

            all_coll.update(local_coll)
