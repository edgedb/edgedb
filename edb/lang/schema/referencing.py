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

from edb.lang.common import ordered, struct
from edb.lang.edgeql import ast as qlast

from . import delta as sd
from . import error as s_err
from . import inheriting
from . import objects as so
from . import name as sn
from . import utils


class RefDict(struct.Struct):

    local_attr = struct.Field(str)
    attr = struct.Field(str)
    backref_attr = struct.Field(str, default='subject')
    requires_explicit_inherit = struct.Field(bool, default=False)
    ref_cls = struct.Field(type)


class RebaseReferencingObject(inheriting.RebaseInheritingObject):
    def apply(self, schema, context):
        this_obj = schema.get(self.classname)
        objects = [this_obj] + list(this_obj.descendants(schema))
        for obj in objects:
            for refdict in this_obj.__class__.get_refdicts():
                attr = refdict.attr
                local_attr = refdict.local_attr
                backref = refdict.backref_attr

                coll = obj.get_field_value(schema, attr)
                local_coll = obj.get_field_value(schema, local_attr)

                for ref_name in tuple(coll.shortnames(schema)):
                    if not local_coll.has(schema, ref_name):
                        try:
                            obj.get_classref_origin(
                                schema, ref_name, attr, local_attr, backref)
                        except KeyError:
                            del coll[ref_name]

        schema, this_obj = super().apply(schema, context)
        return schema, this_obj


class ReferencingObjectMeta(type(inheriting.InheritingObject)):
    def __new__(mcls, name, bases, clsdict):
        refdicts = collections.OrderedDict()
        mydicts = {k: v for k, v in clsdict.items() if isinstance(v, RefDict)}
        cls = super().__new__(mcls, name, bases, clsdict)

        for parent in reversed(cls.__mro__):
            if parent is cls:
                refdicts.update(mydicts)
            elif isinstance(parent, ReferencingObjectMeta):
                refdicts.update({k: d.copy()
                                for k, d in parent.get_own_refdicts().items()})

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

        return cls

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


class ReferencedObjectCommandMeta(type(sd.ObjectCommand)):
    _transparent_adapter_subclass = True

    def __new__(mcls, name, bases, clsdct, *,
                referrer_context_class=None, **kwargs):
        cls = super().__new__(mcls, name, bases, clsdct, **kwargs)
        if referrer_context_class is not None:
            cls._referrer_context_class = referrer_context_class
        return cls


class ReferencedObjectCommand(sd.ObjectCommand,
                              metaclass=ReferencedObjectCommandMeta):
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
    def _classname_from_ast(cls, schema, astnode, context):
        name = super()._classname_from_ast(schema, astnode, context)

        parent_ctx = cls.get_referrer_context(context)
        if parent_ctx is not None:
            referrer_name = parent_ctx.op.classname

            try:
                base_ref = utils.ast_to_typeref(
                    qlast.TypeName(maintype=astnode.name),
                    modaliases=context.modaliases, schema=schema)
            except s_err.ItemNotFoundError:
                base_name = sn.Name(name)
            else:
                base_name = base_ref.get_name(schema)

            pnn = sn.get_specialized_name(base_name, referrer_name)
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

    def _create_innards(self, schema, context):
        schema = super()._create_innards(schema, context)

        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is not None:
            referrer = referrer_ctx.scls
            refdict = referrer.__class__.get_refdict_for_class(
                self.scls.__class__)

            if refdict.backref_attr:
                # Set the back-reference on referenced object
                # to the referrer.
                schema = self.scls.set_field_value(
                    schema, refdict.backref_attr, referrer)
                # Add the newly created referenced object to the
                # appropriate refdict in self and all descendants
                # that don't already have an existing reference.
                schema = referrer.add_classref(schema, refdict.attr, self.scls)
                reftype = type(referrer).get_field(refdict.attr).type
                refname = reftype.get_key_for(schema, self.scls)
                for child in referrer.descendants(schema):
                    child_local_coll = child.get_field_value(
                        schema, refdict.local_attr)
                    child_coll = child.get_field_value(schema, refdict.attr)
                    if not child_local_coll.has(schema, refname):
                        schema, child_coll = child_coll.update(
                            schema, [self.scls])
                        schema = child.set_field_value(
                            schema, refdict.attr, child_coll)

        return schema

    def _delete_innards(self, schema, context, scls):
        schema = super()._delete_innards(schema, context, scls)

        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is not None:
            referrer = referrer_ctx.scls
            referrer_class = type(referrer)
            refdict = referrer_class.get_refdict_for_class(scls.__class__)
            reftype = referrer_class.get_field(refdict.attr).type
            refname = reftype.get_key_for(schema, self.scls)
            schema = referrer.del_classref(schema, refdict.attr, refname)

        return schema


class ReferencedInheritingObjectCommand(
        ReferencedObjectCommand, inheriting.InheritingObjectCommand):

    def _create_begin(self, schema, context):
        referrer_ctx = self.get_referrer_context(context)
        attrs = self.get_struct_properties(schema)

        if referrer_ctx is not None and not attrs.get('is_derived'):
            mcls = self.get_schema_metaclass()
            referrer = referrer_ctx.scls
            basename = sn.shortname_from_fullname(self.classname)
            base = schema.get(basename, type=mcls)
            schema, self.scls = base.derive(schema, referrer, attrs=attrs,
                                            init_props=False)
            return schema
        else:
            return super()._create_begin(schema, context)


class CreateReferencedInheritingObject(inheriting.CreateInheritingObject):
    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if isinstance(astnode, cls.referenced_astnode):
            objcls = cls.get_schema_metaclass()

            try:
                base = utils.ast_to_typeref(
                    qlast.TypeName(maintype=astnode.name),
                    modaliases=context.modaliases, schema=schema)
            except s_err.ItemNotFoundError:
                # Certain concrete items, like pointers create
                # abstract parents implicitly.
                nname = sn.shortname_from_fullname(cmd.classname)
                base = so.ObjectRef(
                    name=sn.Name(
                        module=nname.module,
                        name=nname.name
                    )
                )

            cmd.add(
                sd.AlterObjectProperty(
                    property='bases',
                    new_value=so.ObjectList.create(schema, [base])
                )
            )

            referrer_ctx = cls.get_referrer_context(context)
            referrer_class = referrer_ctx.op.get_schema_metaclass()
            referrer_name = referrer_ctx.op.classname
            refdict = referrer_class.get_refdict_for_class(objcls)

            cmd.add(
                sd.AlterObjectProperty(
                    property=refdict.backref_attr,
                    new_value=so.ObjectRef(
                        name=referrer_name
                    )
                )
            )

            if getattr(astnode, 'is_abstract', None):
                cmd.add(
                    sd.AlterObjectProperty(
                        property='is_abstract',
                        new_value=True
                    )
                )

        return cmd

    @classmethod
    def _classbases_from_ast(cls, schema, astnode, context):
        if isinstance(astnode, cls.referenced_astnode):
            # The bases will be populated by a call to derive()
            # from within _create_begin()
            bases = None
        else:
            bases = super()._classbases_from_ast(schema, astnode, context)

        return bases


class ReferencingObject(inheriting.InheritingObject,
                        metaclass=ReferencingObjectMeta):

    def merge(self, *objs, schema, dctx=None):
        schema = super().merge(*objs, schema=schema, dctx=None)

        for obj in objs:
            for refdict in self.__class__.get_refdicts():
                # Merge Object references in each registered collection.
                #
                this_coll = self.get_explicit_field_value(
                    schema, refdict.attr, None)

                other_coll = obj.get_explicit_field_value(
                    schema, refdict.attr, None)

                if other_coll is None:
                    continue

                if this_coll is None:
                    schema = self.set_field_value(
                        schema, refdict.attr, other_coll)
                else:
                    updates = {v for k, v in other_coll.items(schema)
                               if not this_coll.has(schema, k)}

                    schema, this_coll = this_coll.update(schema, updates)
                    schema = self.set_field_value(
                        schema, refdict.attr, this_coll)

        return schema

    @classmethod
    def delta(cls, old, new, *, context=None, old_schema, new_schema):
        context = context or so.ComparisonContext()

        with context(old, new):
            delta = super().delta(old, new, context=context,
                                  old_schema=old_schema, new_schema=new_schema)
            if isinstance(delta, sd.CreateObject):
                # If this is a CREATE delta, we need to make
                # sure it is returned separately from the creation
                # of references, which will go into a separate ALTER
                # delta.  This is needed to avoid the hassle of
                # sorting the delta order by dependencies or having
                # to maintain ephemeral forward references.
                #
                # Generate an empty delta.
                alter_delta = super().delta(new, new, context=context,
                                            old_schema=new_schema,
                                            new_schema=new_schema)
                full_delta = sd.CommandGroup()
                full_delta.add(delta)
            else:
                full_delta = alter_delta = delta

            old_idx_key = lambda o: o.get_name(old_schema)
            new_idx_key = lambda o: o.get_name(new_schema)

            for refdict in cls.get_refdicts():
                local_attr = refdict.local_attr

                if old:
                    oldcoll = old.get_field_value(old_schema, local_attr)
                    oldcoll_idx = ordered.OrderedIndex(
                        oldcoll.objects(old_schema), key=old_idx_key)
                else:
                    oldcoll_idx = {}

                if new:
                    newcoll = new.get_field_value(new_schema, local_attr)
                    newcoll_idx = ordered.OrderedIndex(
                        newcoll.objects(new_schema), key=new_idx_key)
                else:
                    newcoll_idx = {}

                cls.delta_sets(oldcoll_idx, newcoll_idx, alter_delta, context,
                               old_schema=old_schema, new_schema=new_schema)

            if alter_delta is not full_delta:
                if alter_delta.has_subcommands():
                    full_delta.add(alter_delta)
                else:
                    full_delta = delta

        return full_delta

    def get_classref_origin(self, schema, name, attr, local_attr, classname,
                            farthest=False):
        assert self.get_field_value(schema, attr).has(schema, name)

        result = None

        if self.get_field_value(schema, local_attr).has(schema, name):
            result = self

        if not result or farthest:
            bases = (c for c in self.compute_mro(schema)[1:]
                     if isinstance(c, so.Object))

            for c in bases:
                if c.get_field_value(schema, local_attr).has(schema, name):
                    result = c
                    if not farthest:
                        break

        if result is None:
            raise KeyError(
                'could not find {} "{}" origin'.format(classname, name))

        return result

    def add_classref(self, schema, collection, obj, replace=False):
        refdict = type(self).get_refdict(collection)
        attr = refdict.attr
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

        local_coll = self.get_field_value(schema, local_attr)
        all_coll = self.get_field_value(schema, attr)

        is_inherited = any(b.get_field_value(schema, attr).has(schema, key)
                           for b in self.get_bases(schema).objects(schema))

        if not is_inherited:
            for descendant in self.descendants(schema):
                descendant_local_coll = descendant.get_field_value(
                    schema, local_attr)
                if not descendant_local_coll.has(schema, key):
                    descendant_coll = descendant.get_field_value(schema, attr)
                    schema, descendant_coll = descendant_coll.delete(
                        schema, [key])
                    schema = descendant.set_field_value(
                        schema, attr, descendant_coll)

        if local_coll and local_coll.has(schema, key):
            schema, local_coll = local_coll.delete(schema, [key])
            schema = self.set_field_value(schema, local_attr, local_coll)

        if all_coll and all_coll.has(schema, key):
            schema, all_coll = all_coll.delete(schema, [key])
            schema = self.set_field_value(schema, attr, all_coll)

        return schema

    def finalize(self, schema, bases=None, *, apply_defaults=True, dctx=None):
        schema = super().finalize(
            schema, bases=bases, apply_defaults=apply_defaults,
            dctx=dctx)

        if bases is None:
            bases = self.get_bases(schema).objects(schema)

        for refdict in self.__class__.get_refdicts():
            attr = refdict.attr
            local_attr = refdict.local_attr
            backref_attr = refdict.backref_attr
            ref_cls = refdict.ref_cls
            exp_inh = refdict.requires_explicit_inherit

            schema, ref_keys = self.begin_classref_dict_merge(
                schema, bases=bases, attr=attr)

            schema = self.merge_classref_dict(
                schema, bases=bases, attr=attr,
                local_attr=local_attr,
                backref_attr=backref_attr,
                classrefcls=ref_cls,
                classref_keys=ref_keys,
                requires_explicit_inherit=exp_inh,
                dctx=dctx)

            schema = self.finish_classref_dict_merge(
                schema, bases=bases, attr=attr)

        return schema

    def begin_classref_dict_merge(self, schema, bases, attr):
        return schema, None

    def finish_classref_dict_merge(self, schema, bases, attr):
        return schema

    def merge_classref_dict(self, schema, *,
                            bases, attr, local_attr,
                            backref_attr, classrefcls,
                            classref_keys, requires_explicit_inherit,
                            dctx=None):
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
        classrefs = self.get_explicit_field_value(schema, attr, None)
        colltype = type(self).get_field(local_attr).type
        if classrefs is None:
            classrefs = colltype.create_empty()

        local_classrefs = self.get_explicit_field_value(
            schema, local_attr, None)
        if local_classrefs is None:
            local_classrefs = colltype.create_empty()

        if classref_keys is None:
            classref_keys = classrefs.keys(schema)

        for classref_key in classref_keys:
            local = local_classrefs.get(schema, classref_key, None)
            local_schema = schema

            inherited = []
            for b in bases:
                attrval = b.get_explicit_field_value(schema, attr, None)
                if not attrval:
                    continue
                bref = attrval.get(schema, classref_key, None)
                if bref is not None:
                    inherited.append(bref)

            ancestry = {pref.get_field_value(schema, backref_attr): pref
                        for pref in inherited}

            inherited = list(ancestry.values())

            if not inherited and local is None:
                continue

            pure_inheritance = False

            if local and inherited:
                schema = local.acquire_ancestor_inheritance(schema, inherited)
                schema = local.finalize(schema, bases=inherited)
                merged = local

            elif len(inherited) > 1:
                base = inherited[0].get_bases(schema).first(schema)
                schema, merged = base.derive(
                    schema, self, merge_bases=inherited, dctx=dctx)

            elif len(inherited) == 1:
                # Pure inheritance
                item = inherited[0]
                # In some cases pure inheritance is not possible, such
                # as when a pointer has delegated constraints that must
                # be materialized on inheritance.  We delegate the
                # decision to the referenced class here.
                schema, merged = classrefcls.inherit_pure(
                    schema, item, source=self, dctx=dctx)
                pure_inheritance = schema is local_schema

            else:
                # Not inherited
                merged = local

            if (local is not None and inherited and not pure_inheritance and
                    requires_explicit_inherit and
                    not local.get_declared_inherited(local_schema) and
                    dctx is not None and dctx.declarative):
                # locally defined references *must* use
                # the `inherited` keyword if ancestors have
                # a reference under the same name.
                raise s_err.SchemaDefinitionError(
                    f'{self.get_shortname(schema)}: '
                    f'{local.get_shortname(local_schema)} must be '
                    f'declared using the `inherited` keyword because '
                    f'it is defined in the following ancestor(s): '
                    f'{", ".join(a.get_shortname(schema) for a in ancestry)}',
                    context=local.get_sourcectx(local_schema)
                )

            if not inherited and local.get_declared_inherited(local_schema):
                raise s_err.SchemaDefinitionError(
                    f'{self.get_shortname(schema)}: '
                    f'{local.get_shortname(local_schema)} cannot '
                    f'be declared `inherited` as there are no ancestors '
                    f'defining it.',
                    context=local.get_sourcectx(local_schema)
                )

            if inherited:
                if not pure_inheritance:
                    if dctx is not None:
                        delta = merged.delta(local, merged,
                                             context=None,
                                             old_schema=local_schema,
                                             new_schema=schema)
                        if delta.has_subcommands():
                            dctx.current().op.add(delta)

                    schema, local_classrefs = local_classrefs.update(
                        schema, [merged])

                schema, classrefs = classrefs.update(
                    schema, [merged])

        schema = self.update(schema, {
            attr: classrefs,
            local_attr: local_classrefs
        })

        return schema


class ReferencingObjectCommand(inheriting.InheritingObjectCommand):
    def _apply_fields_ast(self, schema, context, node):
        super()._apply_fields_ast(schema, context, node)

        mcls = self.get_schema_metaclass()

        for refdict in mcls.get_refdicts():
            self._apply_refs_fields_ast(schema, context, node, refdict)

    def _create_innards(self, schema, context):
        schema = super()._create_innards(schema, context)

        mcls = self.get_schema_metaclass()

        for refdict in mcls.get_refdicts():
            schema = self._create_refs(schema, context, self.scls, refdict)

        return schema

    def _alter_innards(self, schema, context, scls):
        schema = super()._alter_innards(schema, context, scls)

        mcls = self.get_schema_metaclass()

        for refdict in mcls.get_refdicts():
            schema = self._alter_refs(schema, context, scls, refdict)

        return schema

    def _delete_innards(self, schema, context, scls):
        schema = super()._delete_innards(schema, context, scls)

        mcls = self.get_schema_metaclass()

        for refdict in mcls.get_refdicts():
            schema = self._delete_refs(schema, context, scls, refdict)

        return schema

    def _apply_refs_fields_ast(self, schema, context, node, refdict):
        for op in self.get_subcommands(metaclass=refdict.ref_cls):
            self._append_subcmd_ast(schema, node, op, context)

    def _create_refs(self, schema, context, scls, refdict):
        for op in self.get_subcommands(metaclass=refdict.ref_cls):
            schema, _ = op.apply(schema, context=context)
        return schema

    def _alter_refs(self, schema, context, scls, refdict):
        for op in self.get_subcommands(metaclass=refdict.ref_cls):
            derived_from = op.get_attribute_value('derived_from')
            if derived_from is not None:
                continue

            schema, _ = op.apply(schema, context=context)
        return schema

    def _delete_refs(self, schema, context, scls, refdict):
        deleted_refs = set()

        all_refs = set(
            scls.get_field_value(schema, refdict.local_attr).objects(schema)
        )

        for op in self.get_subcommands(metaclass=refdict.ref_cls):
            schema, deleted_ref = op.apply(schema, context=context)
            deleted_refs.add(deleted_ref)

        # Add implicit Delete commands for any local refs not
        # deleted explicitly.
        for ref in all_refs - deleted_refs:
            del_cmd = sd.ObjectCommandMeta.get_command_class(
                sd.DeleteObject, type(ref))

            op = del_cmd(classname=ref.get_name(schema))
            schema, _ = op.apply(schema, context=context)
            self.add(op)

        return schema
