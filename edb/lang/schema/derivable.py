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


from edb.lang.common import uuidgen

from . import name as sn
from . import objects as so


class DerivableObjectBase:
    # Override name field comparison coefficient on the
    # presumption that the derived names may be different,
    # but base names may be equal.
    #
    def compare(self, other, *, our_schema, their_schema, context=None):
        similarity = super().compare(
            other, our_schema=our_schema,
            their_schema=their_schema, context=context)
        if self.get_shortname(our_schema) != other.get_shortname(their_schema):
            similarity *= 0.625

        return similarity

    def derive_name(self, schema, source, *qualifiers):
        name = sn.get_specialized_name(
            self.get_shortname(schema), source.get_name(schema), *qualifiers)
        return sn.Name(name=name, module=source.get_name(schema).module)

    def generic(self, schema):
        return self.get_shortname(schema) == self.get_name(schema)

    def get_derived_name(self, schema, source,
                         *qualifiers, mark_derived=False):
        return self.derive_name(schema, source, *qualifiers)

    def init_derived(self, schema, source, *qualifiers, as_copy,
                     merge_bases=None, replace_original=None,
                     mark_derived=False, attrs=None, dctx=None,
                     name=None, **kwargs):
        if name is None:
            derived_name = self.get_derived_name(
                schema, source, *qualifiers, mark_derived=mark_derived)
        else:
            derived_name = name

        if attrs is None:
            attrs = {}
        attrs['name'] = derived_name
        if not attrs.get('id'):
            attrs['id'] = uuidgen.uuid1mc()

        existing_derived = schema.get(derived_name, default=None)
        if existing_derived is not None and replace_original is not None:
            schema = schema.mark_as_garbage(existing_derived)

        return self.copy_with(schema, attrs)

    def finalize_derived(self, schema, derived, *, merge_bases=None,
                         replace_original=None, mark_derived=False,
                         apply_defaults=True, attrs=None, dctx=None,
                         **kwargs):

        if merge_bases:
            schema = derived.acquire_ancestor_inheritance(
                schema, bases=merge_bases)

        schema = derived.finalize(schema, bases=merge_bases,
                                  apply_defaults=apply_defaults)

        if mark_derived:
            schema = derived.update(schema, {
                'is_derived': True,
                'derived_from': self,
            })

        return schema, derived

    def derive_copy(self, schema, *qualifiers, merge_bases=None,
                    replace_original=None, mark_derived=False, attrs=None,
                    dctx=None, name=None, **kwargs):
        schema, derived = self.init_derived(
            schema, *qualifiers, name=name,
            as_copy=True, merge_bases=merge_bases,
            attrs=attrs, mark_derived=mark_derived,
            dctx=dctx, replace_original=replace_original,
            **kwargs)

        schema, derived = self.finalize_derived(
            schema, derived, merge_bases=merge_bases,
            mark_derived=mark_derived, dctx=dctx)

        return schema, derived

    def derive(self, schema, source, *qualifiers, merge_bases=None,
               replace_original=None, mark_derived=False, attrs=None,
               dctx=None, name=None, apply_defaults=True, **kwargs):

        schema, derived = self.init_derived(
            schema, source, *qualifiers, name=name,
            as_copy=False, merge_bases=merge_bases,
            attrs=attrs, mark_derived=mark_derived,
            replace_original=replace_original,
            dctx=dctx, **kwargs)

        schema, derived = self.finalize_derived(
            schema, derived, merge_bases=merge_bases,
            mark_derived=mark_derived, apply_defaults=apply_defaults,
            dctx=dctx)

        return schema, derived


class DerivableObject(so.NamedObject, DerivableObjectBase):

    # Indicates that the object has been declared as
    # explicitly inherited.
    declared_inherited = so.SchemaField(
        bool, False, compcoef=None,
        introspectable=False, inheritable=False)

    @classmethod
    def inherit_pure(cls, schema, item, source, *, dctx=None):
        # This method is used by ReferencingObject and must be
        # defined for all Derivables, not just Inheriting ones.
        return schema, item
