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


import hashlib

from . import indexes
from . import name as sn
from . import objects as so
from . import pointers
from . import utils


class SourceCommandContext(indexes.IndexSourceCommandContext):
    # context mixin
    pass


class SourceCommand(indexes.IndexSourceCommand):
    pass


class Source(indexes.IndexableSubject):
    pointers_refs = so.RefDict(
        attr='pointers',
        local_attr='own_pointers',
        requires_explicit_inherit=True,
        backref_attr='source',
        ref_cls=pointers.Pointer)

    pointers = so.SchemaField(
        so.ObjectIndexByUnqualifiedName,
        inheritable=False, ephemeral=True, coerce=True,
        default=so.ObjectIndexByUnqualifiedName, hashable=False)

    own_pointers = so.SchemaField(
        so.ObjectIndexByUnqualifiedName, compcoef=0.857,
        inheritable=False, ephemeral=True, coerce=True,
        default=so.ObjectIndexByUnqualifiedName)

    class PointerResolver:
        @classmethod
        def getptr(cls, schema, source, name):
            if sn.Name.is_qualified(name):
                raise ValueError(
                    'references to concrete pointers must not be qualified')
            ptr = source.get_pointers(schema).get(schema, name, None)
            if ptr is None:
                return set()
            else:
                return {ptr}

        @classmethod
        def getptr_inherited_from(cls, source, schema, base_ptr_class,
                                  skip_scalar):
            result = set()
            for ptr in source.get_pointers(schema).objects(schema):
                if (ptr.issubclass(schema, base_ptr_class) and
                        (not skip_scalar or not ptr.scalar())):
                    result.add(ptr)
                    break
            return result

    def _getptr_descending(self, schema, name, resolver):
        ptrs = resolver.getptr(schema, self, name)

        if not ptrs:
            for c in self.children(schema):
                ptrs |= c._getptr_descending(schema, name, resolver)

        return ptrs

    def getptr_descending(self, schema, name):
        return self._getptr_descending(schema, name,
                                       self.__class__.PointerResolver)

    def _getptr_inherited_from(self, schema, name, resolver):
        ptrs = set()

        ptr_names = []
        if not sn.Name.is_qualified(name):
            ptr_names.append(
                sn.Name(module=self.get_name(schema).module, name=name))
            ptr_names.append(
                sn.Name(module='std', name=name))
        else:
            ptr_names.append(name)

        for ptr_name in ptr_names:
            base_ptr_class = schema.get(ptr_name, default=None)
            if base_ptr_class:
                ptrs = resolver.getptr_inherited_from(
                    self, schema, base_ptr_class, False)
                break

        return ptrs

    def _getptr_ascending(self, schema, name, resolver,
                          include_inherited=False):
        ptrs = resolver.getptr(schema, self, name)

        if not ptrs:
            if include_inherited:
                ptrs = self._getptr_inherited_from(schema, name, resolver)

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

    def resolve_pointer(self, schema, pointer_name, *,
                        direction='>',
                        far_endpoint=None,
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

        if not ptrs:
            # No pointer candidates found at all, bail out.
            return schema, None

        targets = set()

        if not far_endpoint:
            targets.update(p.get_far_endpoint(schema, direction)
                           for p in ptrs)
        else:
            # Narrow down the set of pointers by the specified list of
            # endpoints.
            #
            targeted_ptrs = set()

            if far_endpoint.get_is_virtual(schema):
                req_endpoints = tuple(far_endpoint.children(schema))
            else:
                req_endpoints = (far_endpoint,)

            for ptr in ptrs:
                endpoint = ptr.get_far_endpoint(schema, direction)

                if endpoint.get_is_virtual(schema):
                    endpoints = endpoint.children(schema)
                else:
                    endpoints = [endpoint]

                for endpoint in endpoints:
                    if endpoint.issubclass(schema, req_endpoints):
                        targeted_ptrs.add(ptr)
                        targets.add(endpoint)
                    else:
                        for req_endpoint in req_endpoints:
                            if req_endpoint.issubclass(schema, endpoint):
                                if direction == '>':
                                    source = ptr.get_source(schema)
                                    target = req_endpoint
                                else:
                                    target = ptr.get_source(schema)
                                    source = req_endpoint

                                schema, dptr = ptr.get_derived(
                                    schema, source, target, mark_derived=True)

                                targeted_ptrs.add(dptr)
                                targets.add(req_endpoint)

            if not targeted_ptrs:
                # All candidates have been eliminated by the endpoint
                # filter.
                return schema, None

            ptrs = targeted_ptrs

        ptr_targets = frozenset(p.get_far_endpoint(schema, direction)
                                for p in ptrs)

        if len(ptrs) == 1 and targets == ptr_targets:
            # Found exactly one specialized pointer, just return it
            ptr = next(iter(ptrs))

        else:
            # More than one specialized pointer or an endpoint subset,
            # create a virtual subclass of endpoints.
            #
            if target_most_generic:
                minimize_by = 'most_generic'
            else:
                minimize_by = 'least_generic'

            common_parent = utils.get_class_nearest_common_ancestor(
                schema, ptrs)
            schema, target = common_parent.create_common_target(
                schema, targets, minimize_by)

            if direction == '>':
                ptr_source = self
                ptr_target = target
            else:
                ptr_source = target
                ptr_target = self

            fqname = common_parent.derive_name(
                schema, ptr_source, ptr_target.get_name(schema))
            ptr = schema.get(fqname, default=None)
            if ptr is None:
                schema, common_parent_spec = common_parent.get_derived(
                    schema,
                    source=ptr_source, target=ptr_target,
                    mark_derived=True)

                if len(ptrs) == 1:
                    ptr = common_parent_spec
                else:
                    schema, ptr = common_parent_spec.derive(
                        schema, merge_bases=list(ptrs),
                        source=ptr_source, target=ptr_target,
                        mark_derived=True
                    )

        return schema, ptr

    def add_pointer(self, schema, pointer, *, replace=False):
        schema = self.add_classref(
            schema, 'pointers', pointer, replace=replace)
        return schema

    @classmethod
    def gen_virt_parent_name(cls, names, module=None):
        hashed = ';'.join(sorted(set(names)))
        hashed = hashlib.md5(hashed.encode()).hexdigest()
        name = f'Virtual_{hashed}'

        if module is None:
            module = next(iter(names)).module
        return sn.Name(name=name, module=module)
