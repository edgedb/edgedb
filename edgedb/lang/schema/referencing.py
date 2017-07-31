##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

from edgedb.lang.common import ordered

from . import delta as sd
from . import derivable
from . import error as schema_error
from . import inheriting
from . import objects as so
from . import name as sn
from . import named


class RefDict:
    def __init__(self, local_attr=None, *, ordered=False, title=None,
                 backref='subject', ref_cls, compcoef=None):
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


class RebaseReferencingClass(inheriting.RebaseNamedClass):
    def apply(self, schema, context):
        this_obj = super().apply(schema, context)

        objects = [this_obj] + list(this_obj.descendants(schema))
        for obj in objects:
            for refdict in this_obj.__class__.get_refdicts():
                attr = refdict.attr
                local_attr = refdict.local_attr
                backref = refdict.backref_attr

                coll = getattr(obj, attr)
                local_coll = getattr(obj, local_attr)

                for ref_name in coll.copy():
                    if ref_name not in local_coll:
                        try:
                            obj.get_classref_origin(
                                ref_name, attr, local_attr, backref)
                        except KeyError:
                            del coll[ref_name]

        return this_obj


class ReferencingClassMeta(type(inheriting.InheritingClass)):
    def __new__(mcls, name, bases, clsdict):
        refdicts = {}
        mydicts = {k: v for k, v in clsdict.items() if isinstance(v, RefDict)}
        cls = super().__new__(mcls, name, bases, clsdict)

        for parent in reversed(cls.__mro__):
            if parent is cls:
                refdicts.update(mydicts)
            elif isinstance(parent, ReferencingClassMeta):
                refdicts.update({k: d.copy()
                                for k, d in parent.get_own_refdicts().items()})

        cls._refdicts_by_refclass = {}

        for k, dct in refdicts.items():
            dct.set_attr_name(k)
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

    def get_refdict_for_class(cls, refcls):
        return cls._refdicts_by_refclass[refcls]


class ReferencedClassCommandMeta(type(derivable.DerivableClassCommand)):
    _transparent_adapter_subclass = True

    def __new__(mcls, name, bases, clsdct, *,
                referrer_context_class=None, **kwargs):
        cls = super().__new__(mcls, name, bases, clsdct, **kwargs)
        if referrer_context_class is not None:
            cls._referrer_context_class = referrer_context_class
        return cls


class ReferencedClassCommand(derivable.DerivableClassCommand,
                             metaclass=ReferencedClassCommandMeta):
    _referrer_context_class = None

    @classmethod
    def get_referrer_context_class(cls):
        if cls._referrer_context_class is None:
            raise TypeError(
                f'referrer_context_class is not defined for {cls}')
        return cls._referrer_context_class

    @classmethod
    def get_referrer_context(cls, context):
        return context.get(cls.get_referrer_context_class())

    @classmethod
    def _classname_from_ast(cls, astnode, context, schema):
        name = super()._classname_from_ast(astnode, context, schema)

        parent_ctx = cls.get_referrer_context(context)
        if parent_ctx is not None:
            referrer_name = parent_ctx.op.classname

            pcls = cls.get_schema_metaclass()
            pnn = pcls.get_specialized_name(
                sn.Name(name), referrer_name
            )

            name = sn.Name(name=pnn, module=referrer_name.module)

        return name

    def _get_ast_node(self, context):
        subject_ctx = self.get_referrer_context(context)
        ref_astnode = getattr(self, 'referenced_astnode', None)
        if subject_ctx is not None and ref_astnode is not None:
            return ref_astnode
        else:
            if isinstance(self.astnode, (list, tuple)):
                return self.astnode[1]
            else:
                return self.astnode

    def _create_begin(self, schema, context):
        referrer_ctx = self.get_referrer_context(context)
        attrs = self.get_struct_properties(schema)
        if referrer_ctx is not None and not attrs.get('is_derived'):
            referrer = referrer_ctx.scls
            basename = self.metaclass.get_shortname(self.classname)
            base = schema.get(basename, type=self.metaclass)
            self.scls = base.derive(schema, referrer, attrs=attrs,
                                    add_to_schema=True, init_props=False)
        else:
            super()._create_begin(schema, context)

    def _create_innards(self, schema, context):
        super()._create_innards(schema, context)

        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is not None:
            referrer = referrer_ctx.scls
            refdict = referrer.__class__.get_refdict_for_class(
                self.scls.__class__)

            if refdict.backref_attr:
                # Set the back-reference on referenced object
                # to the referrer.
                setattr(self.scls, refdict.backref_attr, referrer)
                # Add the newly created referenced object to the
                # appropriate refdict in self and all descendants
                # that don't already have an existing reference.
                #
                referrer.add_classref(refdict.attr, self.scls)
                refname = self.scls.get_shortname(self.scls.name)
                for child in referrer.descendants(schema):
                    child_local_coll = getattr(child, refdict.local_attr)
                    child_coll = getattr(child, refdict.attr)
                    if refname not in child_local_coll:
                        child_coll[refname] = self.scls

    def _rename_innards(self, schema, context, scls):
        super()._rename_innards(schema, context, scls)

        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is not None:
            referrer = referrer_ctx.scls
            old_name = scls.get_shortname(self.old_name)
            new_name = scls.get_shortname(self.new_name)

            refdict = referrer.__class__.get_refdict_for_class(
                scls.__class__)

            attr = refdict.attr
            local_attr = refdict.local_attr

            coll = getattr(referrer, attr)
            local_coll = getattr(referrer, local_attr)

            local = local_coll.pop(old_name, None)
            if local is not None:
                local_coll[new_name] = local

            for child in referrer.children(schema):
                child_coll = getattr(child, attr)
                ref = child_coll.pop(old_name, None)
                if ref is not None:
                    child_coll[new_name] = ref

            ref = coll.pop(old_name, None)
            if ref is not None:
                coll[new_name] = ref

    def _delete_innards(self, schema, context, scls):
        super()._delete_innards(schema, context, scls)

        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is not None:
            referrer = referrer_ctx.scls
            refdict = referrer.__class__.get_refdict_for_class(
                scls.__class__)
            referrer.del_classref(refdict.attr, scls.name, schema)


class CreateReferencedClass(inheriting.CreateInheritingClass):
    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        if isinstance(astnode, cls.referenced_astnode):
            objcls = cls.get_schema_metaclass()
            nname = objcls.get_shortname(cmd.classname)

            cmd.add(
                sd.AlterClassProperty(
                    property='bases',
                    new_value=so.ClassList([
                        so.ClassRef(
                            classname=sn.Name(
                                module=nname.module,
                                name=nname.name
                            )
                        )
                    ])
                )
            )

            referrer_ctx = cls.get_referrer_context(context)
            referrer_class = referrer_ctx.op.metaclass
            referrer_name = referrer_ctx.op.classname
            refdict = referrer_class.get_refdict_for_class(objcls)

            cmd.add(
                sd.AlterClassProperty(
                    property=refdict.backref_attr,
                    new_value=so.ClassRef(
                        classname=referrer_name
                    )
                )
            )

            if getattr(astnode, 'is_abstract', None):
                cmd.add(
                    sd.AlterClassProperty(
                        property='is_abstract',
                        new_value=True
                    )
                )

        return cmd

    @classmethod
    def _classbases_from_ast(cls, astnode, context, schema):
        if isinstance(astnode, cls.referenced_astnode):
            # The bases will be populated by a call to derive()
            # from within _create_begin()
            bases = None
        else:
            bases = super()._classbases_from_ast(astnode, context, schema)

        return bases


class ReferencingClass(inheriting.InheritingClass,
                       metaclass=ReferencingClassMeta):
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
            state[local_attr] = self._get_classref_dict(local_attr)

            coll = getattr(self, attr)
            state[attr] = [
                (n, self.get_classref_origin(n, attr, local_attr, title).name)
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
            self._resolve_classref_dict(
                _objects, _resolve, local_attr)
            self._resolve_inherited_classref_dict(
                _objects, _resolve, attr, local_attr)

    def copy(self):
        result = super(ReferencingClass, self).copy()

        for refdict in self.__class__.get_refdicts():
            attr = refdict.attr
            local_attr = refdict.local_attr
            all_coll = getattr(self, attr)
            local_coll = getattr(self, local_attr)

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

                ref_similarity = so.ClassSet.compare_values(
                    ours, theirs, context=context, compcoef=refdict.compcoef)

                similarity *= ref_similarity

        return similarity

    def merge(self, obj, *, schema, dctx=None):
        super().merge(obj, schema=schema, dctx=None)

        for refdict in self.__class__.get_refdicts():
            # Merge Class references in each registered collection.
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
            if isinstance(delta, sd.CreateClass):
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
                    oldcoll_idx = ordered.OrderedIndex(oldcoll, key=idx_key)
                else:
                    oldcoll_idx = {}

                if new:
                    newcoll = getattr(new, local_attr).values()
                    newcoll_idx = ordered.OrderedIndex(newcoll, key=idx_key)
                else:
                    newcoll_idx = {}

                self.delta_sets(oldcoll_idx, newcoll_idx, alter_delta, context)

            if alter_delta is not full_delta:
                if alter_delta.has_subcommands():
                    full_delta.add(alter_delta)
                else:
                    full_delta = delta

        return full_delta

    def get_classref_origin(self, name, attr, local_attr, classname,
                            farthest=False):
        assert name in getattr(self, attr)

        result = None

        if name in getattr(self, local_attr):
            result = self

        if not result or farthest:
            bases = (c for c in self.get_mro()[1:]
                     if isinstance(c, named.NamedClass))

            for c in bases:
                if name in getattr(c, local_attr):
                    result = c
                    if not farthest:
                        break

        if result is None:
            raise KeyError(
                'could not find {} "{}" origin'.format(classname, name))

        return result

    def add_classref(self, collection, obj, replace=False):
        refdict = self.__class__.get_refdict(collection)
        attr = refdict.attr
        local_attr = refdict.local_attr
        coll_obj = refdict.title

        local_coll = getattr(self, local_attr)
        all_coll = getattr(self, attr)

        key = obj.get_shortname(obj.name)
        existing = local_coll.get(key)
        if existing is not None:
            msg = '{} {!r} is already present in {!r}'.format(
                coll_obj, key, self.name)
            raise schema_error.SchemaError(msg)

        local_coll[key] = obj
        all_coll[key] = obj

    def del_classref(self, collection, obj_name, schema):
        refdict = self.__class__.get_refdict(collection)
        attr = refdict.attr
        local_attr = refdict.local_attr
        refcls = refdict.ref_cls

        local_coll = getattr(self, local_attr)
        all_coll = getattr(self, attr)

        key = refcls.get_shortname(obj_name)
        is_inherited = any(key in getattr(b, attr) for b in self.bases)

        if not is_inherited:
            all_coll.pop(key)

            for descendant in self.descendants(schema):
                descendant_local_coll = getattr(descendant, local_attr)
                if key not in descendant_local_coll:
                    descendant_coll = getattr(descendant, attr)
                    descendant_coll.pop(key, None)

        local_coll.pop(key, None)

    def _get_classref_dict(self, attr):
        values = getattr(self, attr)
        result = collections.OrderedDict()

        if values:
            for k, v in values.items():
                if isinstance(v, named.NamedClass):
                    v = so.ClassRef(classname=v.name)
                result[k] = v

        return result

    def _resolve_classref_dict(self, _objects, _resolve, local_attr):
        values = getattr(self, local_attr)

        if values:
            for n, v in values.items():
                if isinstance(v, so.ClassRef):
                    values[n] = _resolve(v.classname)

    def _resolve_inherited_classref_dict(self, _objects, _resolve,
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
                                if isinstance(c, named.NamedClass)}
                    subj = _objects[origin] = _mro[origin]

                attrs[an] = getattr(subj, local_attr)[an]

            setattr(self, attr, attrs)

    def finalize(self, schema, bases=None, *, dctx=None):
        super().finalize(schema, bases=bases, dctx=dctx)

        if bases is None:
            bases = self.bases

        for refdict in self.__class__.get_refdicts():
            attr = refdict.attr
            local_attr = refdict.local_attr
            backref_attr = refdict.backref_attr
            ref_cls = refdict.ref_cls

            ref_keys = self.begin_classref_dict_merge(
                schema, bases=bases, attr=attr)

            self.merge_classref_dict(schema, bases=bases, attr=attr,
                                     local_attr=local_attr,
                                     backref_attr=backref_attr,
                                     classrefcls=ref_cls,
                                     classref_keys=ref_keys,
                                     dctx=dctx)

            self.finish_classref_dict_merge(schema, bases=bases, attr=attr)

    def begin_classref_dict_merge(self, schema, bases, attr):
        pass

    def finish_classref_dict_merge(self, schema, bases, attr):
        pass

    def merge_classref_dict(self, schema, bases, attr, local_attr,
                            backref_attr, classrefcls,
                            classref_keys=None, *, dctx=None):
        """Merge reference collections from bases.

        :param schema:         The schema.

        :param bases:          An iterable containing base objects.

        :param str attr:       Name of the attribute containing the full
                               reference collection.

        :param str local_attr: Name of the attribute containing the collection
                               of references defined locally (not inherited).

        :param str backref_attr: Name of the attribute on a referenced
                                 object containing the reference back to
                                 this object.

        :param classrefcls:    Referenced object class.

        :param classrefkeys:   An optional list of reference keys to consider
                               for merging.  If not specified, all keys
                               in the collection will be used.
        """
        classrefs = getattr(self, attr)
        local_classrefs = getattr(self, local_attr)

        ODict = collections.OrderedDict

        if classref_keys is None:
            classref_keys = classrefs

        for classref_key in classref_keys:
            local = local_classrefs.get(classref_key)

            base_refs = [getattr(b, attr, {}).get(classref_key) for b in bases]
            inherited = filter(lambda i: i is not None, base_refs)

            # Build a list of (source_class, target_class) tuples
            # for this key.
            #
            inherited = list(ODict((getattr(pref, backref_attr), pref)
                                   for pref in inherited).values())

            pure_inheritance = False

            if local and inherited:
                merged = local.derive_copy(schema, self, merge_bases=inherited,
                                           replace_original=local,
                                           add_to_schema=True, dctx=dctx)

            elif len(inherited) > 1:
                base = inherited[0].bases[0]
                merged = base.derive(schema, self, merge_bases=inherited,
                                     add_to_schema=True, dctx=dctx)

            elif len(inherited) == 1:
                # Pure inheritance
                item = inherited[0]
                # In some cases pure inheritance is not possible, such
                # as when a pointer has abstract constraints that must
                # be materialized on inheritance.  We delegate the
                # decision to the referenced class here.
                merged = classrefcls.inherit_pure(
                    schema, item, source=self, dctx=dctx)
                pure_inheritance = merged is item

            else:
                # Not inherited
                merged = local

            if merged is not local:
                if not pure_inheritance:
                    if dctx is not None:
                        delta = merged.delta(local)
                        if delta.has_subcommands():
                            dctx.current().op.add(delta)

                    local_classrefs[classref_key] = merged

                classrefs[classref_key] = merged

    def init_derived(self, schema, source, *qualifiers, as_copy,
                     merge_bases=None, add_to_schema=False, mark_derived=False,
                     attrs=None, dctx=None, **kwargs):

        derived = super().init_derived(
            schema, source, *qualifiers, as_copy=as_copy,
            mark_derived=mark_derived, add_to_schema=add_to_schema,
            attrs=attrs, dctx=dctx, merge_bases=merge_bases, **kwargs)

        if as_copy:
            derived.rederive_classrefs(schema, add_to_schema=add_to_schema,
                                       mark_derived=mark_derived)

        return derived

    def rederive_classrefs(self, schema, add_to_schema=False,
                           mark_derived=False, dctx=None):
        for refdict in self.__class__.get_refdicts():
            attr = refdict.attr
            local_attr = refdict.local_attr
            all_coll = getattr(self, attr)
            local_coll = getattr(self, local_attr)

            for pn, p in local_coll.items():
                local_coll[pn] = p.derive_copy(schema, self,
                                               add_to_schema=add_to_schema,
                                               mark_derived=mark_derived,
                                               dctx=dctx)

            all_coll.update(local_coll)
