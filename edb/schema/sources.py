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

    def getptr(self, schema, name):
        if sn.Name.is_qualified(name):
            raise ValueError(
                'references to concrete pointers must not be qualified')
        return self.get_pointers(schema).get(schema, name, None)

    def getrptrs(self, schema, name):
        return set()

    def resolve_pointer(self, schema, pointer_name, *, direction='>'):
        if direction == '>':
            ptr = self.getptr(schema, pointer_name)
            ptrs = set() if ptr is None else {ptr}
        else:
            ptrs = self.getrptrs(schema, pointer_name)

        if not ptrs:
            # No pointer candidates found at all, bail out.
            return schema, None

        targets = set()
        targets = {p.get_far_endpoint(schema, direction) for p in ptrs}

        if len(ptrs) == 1:
            # Found exactly one specialized pointer, just return it
            ptr = next(iter(ptrs))

        else:
            # More than one specialized pointer or an endpoint subset,
            # create a virtual subclass of endpoints.
            common_parent = utils.get_class_nearest_common_ancestor(
                schema, ptrs)
            schema, target = common_parent.create_common_target(
                schema, targets, minimize_by='most_generic')

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
