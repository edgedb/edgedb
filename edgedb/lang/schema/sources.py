##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.algos.persistent_hash import persistent_hash
from edgedb.lang.common.functional import hybridmethod

from . import error as schema_error
from . import indexes
from . import name as sn
from . import named
from . import primary
from . import referencing
from . import utils


class SourceCommandContext(indexes.IndexSourceCommandContext):
    # context mixin
    pass


class SourceCommand(named.NamedPrototypeCommand, indexes.IndexSourceCommand):
    pass


class Source(primary.Prototype, indexes.IndexableSubject):
    pointers = referencing.RefDict(
                    local_attr='own_pointers', ordered=True,
                    backref='source', ref_cls='get_pointer_class',
                    compcoef=0.857)

    @classmethod
    def get_pointer_class(cls):
        raise NotImplementedError

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ro_pointers = None

    def get_pointer_origin(self, name, farthest=False):
        return self.get_protoref_origin(name, 'pointers', 'own_pointers',
                                        'pointer', farthest=farthest)

    @property
    def readonly_pointers(self):
        if self._ro_pointers is None:
            self._ro_pointers = set(l.normal_name()
                                    for l in self.pointers.values()
                                    if l.readonly)

        return self._ro_pointers

    def get_children_common_pointers(self, schema):
        "Get a set of compatible pointers defined in children but not in self"

        from . import atoms, concepts

        pointer_names = None

        for c in self.children(schema):
            if pointer_names is None:
                pointer_names = set(c.pointers)
            else:
                pointer_names &= set(c.pointers)

        if pointer_names is None:
            pointer_names = set()
        else:
            pointer_names -= set(self.pointers)

        result = set()

        for ptr_name in pointer_names:
            target = None

            for c in self.children(schema):
                ptr = c.pointers.get(ptr_name)

                if target is None:
                    target = ptr.target
                else:
                    if isinstance(ptr.target, atoms.Atom):
                        if not isinstance(target, atoms.Atom):
                            continue
                        elif target.issubclass(ptr.target):
                            target = ptr.target
                        elif not ptr.target.issubclass(target):
                            continue
                    else:
                        if not isinstance(target, concepts.Concept):
                            continue

            ptr = ptr.derive_copy(schema, self, target)
            result.add(ptr)

        return result

    class PointerResolver:
        @classmethod
        def getptr_from_nqname(cls, schema, source, name):
            ptrs = set()

            for ptr_name, ptr in source.pointers.items():
                if ptr_name.name == name:
                    ptrs.add(ptr)

            return ptrs

        @classmethod
        def getptr_from_fqname(cls, schema, source, name):
            ptr = source.pointers.get(name)
            if ptr:
                return {ptr}
            else:
                return set()

        @classmethod
        def getptr(cls, schema, source, name):
            if sn.Name.is_qualified(name):
                return cls.getptr_from_fqname(schema, source, name)
            else:
                return cls.getptr_from_nqname(schema, source, name)

        @classmethod
        def getptr_inherited_from(cls, source, schema, base_ptr_proto,
                                                       skip_atomic):
            result = set()
            for ptr in source.pointers.values():
                if (ptr.issubclass(base_ptr_proto) and
                        (not skip_atomic or not ptr.atomic())):
                    result.add(ptr)
                    break
            return result

    def _check_ptr_name_consistency(self, name, ptrs):
        if not sn.Name.is_qualified(name) and ptrs:
            ambig = set()

            names = {}

            for ptr in ptrs:
                nq_name = ptr.normal_name().name
                fq_name = names.get(nq_name)

                if fq_name is None:
                    names[nq_name] = ptr.normal_name()
                elif fq_name != ptr.normal_name():
                    ambig.add(ptr)

            if ambig:
                raise schema_error.SchemaError(
                        'reference to an ambiguous link: "%s"' % name)

    def _getptr_descending(self, schema, name, resolver, _top=True):
        ptrs = resolver.getptr(schema, self, name)

        if not ptrs:
            for c in self.children(schema):
                ptrs |= c._getptr_descending(schema, name, resolver)

        if _top:
            self._check_ptr_name_consistency(name, ptrs)

        return ptrs

    def getptr_descending(self, schema, name):
        return self._getptr_descending(schema, name,
                                       self.__class__.PointerResolver)

    def _getptr_inherited_from(self, schema, name, resolver):
        ptrs = set()

        ptr_names = []
        if not sn.Name.is_qualified(name):
            ptr_names.append(sn.Name(module=self.name.module, name=name))
            ptr_names.append(sn.Name(module=schema.get_builtins_module(),
                                     name=name))
        else:
            ptr_names.append(name)

        for ptr_name in ptr_names:
            base_ptr_proto = schema.get(ptr_name, default=None)
            if base_ptr_proto:
                root_class = schema.get_root_class(self.get_pointer_class())
                skip_atomic = base_ptr_proto.name == root_class.name
                ptrs = resolver.getptr_inherited_from(
                            self, schema, base_ptr_proto, skip_atomic)
                break

        return ptrs

    def _getptr_ascending(self, schema, name, resolver,
                                              include_inherited=False):
        ptrs = resolver.getptr(schema, self, name)

        if not ptrs:
            if include_inherited:
                ptrs = self._getptr_inherited_from(schema, name, resolver)

        self._check_ptr_name_consistency(name, ptrs)

        return ptrs

    def getptr_ascending(self, schema, name, include_inherited=False):
        ptrs = self._getptr_ascending(
                    schema, name, self.__class__.PointerResolver,
                    include_inherited=include_inherited)

        return ptrs

    def getptr(self, schema, name):
        ptrs = self.getptr_ascending(schema, name)

        if ptrs:
            return next(iter(ptrs))
        else:
            return None

    def getrptr_descending(self, schema, name):
        return []

    def getrptr_ascending(self, schema, name, include_inherited=False):
        return None

    def get_ptr_sources(self, schema, pointer_name,
                              look_in_children=False,
                              include_inherited=False,
                              strict_ancestry=False):

        sources = set()

        ptr = self.getptr_ascending(schema, pointer_name,
                                    include_inherited=include_inherited)

        if ptr:
            sources.add(self)

        elif look_in_children:
            child_ptrs = self.getptr_descending(schema, pointer_name)

            if child_ptrs:
                if strict_ancestry:
                    my_descendants = set(self.descendants(schema))
                    if self.is_virtual:
                        subclass_list = tuple(self.children(schema))
                    else:
                        subclass_list = (self,)

                    for p in child_ptrs:
                        if not p.source.issubclass(subclass_list):
                            for my_descendant in my_descendants:
                                if my_descendant.issubclass(p.source):
                                    sources.add(my_descendant)
                        else:
                            sources.add(p.source)
                else:
                    sources.update(p.source for p in child_ptrs)

        return sources

    def resolve_pointer(self, schema, pointer_name, *,
                              direction='>',
                              far_endpoints=None,
                              look_in_children=False,
                              include_inherited=False,
                              target_most_generic=True):

        # First, lookup the inheritance hierarchy up, and, if requested,
        # down, to select all pointers with the requested name.
        #
        if direction == '>':
            ptrs = self.getptr_ascending(schema, pointer_name,
                                         include_inherited=include_inherited)

            if not ptrs and look_in_children:
                ptrs = self.getptr_descending(schema, pointer_name)
        else:
            ptrs = self.getrptr_ascending(schema, pointer_name,
                                          include_inherited=include_inherited)

            if not ptrs and look_in_children:
                ptrs = self.getrptr_descending(schema, pointer_name)

        # Narrow down the set of pointers by the specified list of
        # endpoints.
        #
        if ptrs and far_endpoints:
            req_endpoints = tuple(far_endpoints)
            targeted_ptrs = []

            req_endpoints = set(far_endpoints)

            for req_endpoint in far_endpoints:
                if getattr(req_endpoint, 'is_virtual', None):
                    req_endpoints.update(req_endpoint.children(schema))

            req_endpoints = tuple(req_endpoints)

            for ptr in ptrs:
                endpoint = ptr.get_far_endpoint(direction)

                if endpoint.issubclass(req_endpoints):
                    targeted_ptrs.append(ptr)
                else:
                    for req_endpoint in req_endpoints:
                        if req_endpoint.issubclass(endpoint):
                            if direction == '>':
                                source = ptr.source
                                target = req_endpoint
                            else:
                                target = ptr.source
                                source = req_endpoint

                            dptr = ptr.get_derived(schema, source, target,
                                                           mark_derived=True,
                                                           add_to_schema=True)

                            targeted_ptrs.append(dptr)

            ptrs = targeted_ptrs

        len_ptrs = len(ptrs)

        if len_ptrs == 1:
            # Found exactly one specialized pointer, just return it
            ptr = next(iter(ptrs))

        elif len_ptrs > 1:
            # More than one specialized pointer, create a
            # virtual subclass of the set.
            #
            targets = frozenset(p.get_far_endpoint(direction) for p in ptrs)

            if target_most_generic:
                minimize_by = 'most_generic'
            else:
                minimize_by = 'least_generic'

            common_parent = \
                utils.get_prototype_nearest_common_ancestor(ptrs)

            target = common_parent.create_common_target(
                        schema, targets, minimize_by)

            if direction == '>':
                ptr_source = self
                ptr_target = target
            else:
                ptr_source = target
                ptr_target = self

            fqname = common_parent.derive_name(ptr_source, ptr_target)
            ptr = schema.get(fqname, default=None)
            if ptr is None:
                common_parent_spec = common_parent.get_derived(
                    schema,
                    source=ptr_source, target=ptr_target,
                    mark_derived=True)

                ptr = common_parent_spec.derive_copy(
                    schema, merge_bases=list(ptrs), add_to_schema=True,
                    source=ptr_source, target=ptr_target, mark_derived=True
                )

                mapping = None
                for base in ptrs:
                    if mapping is None:
                        mapping = base.mapping
                    else:
                        mapping |= base.mapping

                ptr.mapping = mapping

        else:
            # No matching pointers found, caller must deal with this.
            ptr = None

        return ptr

    def resolve_pointers(self, schema, pointer_names, look_in_children=False,
                               include_inherited=False, strict_ancestry=False):
        all_sources = set()

        for pointer_name in pointer_names:
            sources = self.get_ptr_sources(
                                    schema, pointer_name,
                                    look_in_children=look_in_children,
                                    include_inherited=include_inherited,
                                    strict_ancestry=strict_ancestry)

            all_sources.update(sources)

        if len(all_sources) == 0:
            return None
        elif len(all_sources) == 1:
            return next(iter(all_sources))
        else:
            return self.get_nearest_common_descendant(all_sources)

    def add_pointer(self, pointer):
        self.add_protoref('pointers', pointer)
        if pointer.readonly and self._ro_pointers is not None:
            self._ro_pointers.add(pointer)

    def del_pointer(self, pointer, proto_schema):
        self.del_protoref('pointers', pointer.name, proto_schema)
        if self._ro_pointers is not None:
            pointer_name = pointer.normal_name()
            self._ro_pointers.discard(pointer_name)

    @classmethod
    def gen_virt_parent_name(cls, names, module=None):
        name = 'Virtual_%x' % persistent_hash(frozenset(names))
        if module is None:
            module = next(iter(names)).module
        return sn.Name(name=name, module=module)

    @hybridmethod
    def copy(scope, obj=None):
        if isinstance(scope, type):
            cls = scope
        else:
            obj = scope
            cls = obj.__class__

        result = super().copy(obj)

        virt_children = getattr(obj, '_virtual_children', None)
        if virt_children:
            result._virtual_children = virt_children.copy()

        return result
