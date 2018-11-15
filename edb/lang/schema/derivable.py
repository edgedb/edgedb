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


from . import name as sn
from . import objects as so


class DerivableObjectBase:
    # Override name field comparison coefficient on the
    # presumption that the derived names may be different,
    # but base names may be equal.
    #
    def compare(self, schema, other, context=None):
        similarity = super().compare(schema, other, context=context)
        if self.shortname != other.shortname:
            similarity *= 0.625

        return similarity

    def derive_name(self, source, *qualifiers):
        name = self.get_specialized_name(
            self.shortname, source.name, *qualifiers)
        return sn.Name(name=name, module=source.name.module)

    def generic(self, schema):
        return self.shortname == self.name

    def get_derived_name(self, source, *qualifiers, mark_derived=False):
        return self.derive_name(source, *qualifiers)

    def init_derived(self, schema, source, *qualifiers, as_copy,
                     merge_bases=None, add_to_schema=False, mark_derived=False,
                     attrs=None, dctx=None, name=None, **kwargs):
        if name is None:
            derived_name = self.get_derived_name(
                source, *qualifiers, mark_derived=mark_derived)
        else:
            derived_name = name

        if attrs is None:
            attrs = {}
        attrs['name'] = derived_name
        schema, derived = self.copy_with(schema, attrs)

        return schema, derived

    def finalize_derived(self, schema, derived, *, merge_bases=None,
                         replace_original=None, add_to_schema=False,
                         mark_derived=False, apply_defaults=True,
                         attrs=None, dctx=None, **kwargs):

        if merge_bases:
            schema = derived.acquire_ancestor_inheritance(
                schema, bases=merge_bases)

        schema = derived.finalize(schema, bases=merge_bases,
                                  apply_defaults=apply_defaults)

        if mark_derived:
            derived.is_derived = True
            derived.derived_from = self

        if add_to_schema:
            existing_derived = schema.get(derived.name, default=None)

            if existing_derived is None:
                schema = schema.add(derived)
            elif replace_original is not None:
                schema = schema.discard(existing_derived)
                schema = schema.add(derived)

        return schema, derived

    def derive_copy(self, schema, *qualifiers, merge_bases=None,
                    replace_original=None, add_to_schema=False,
                    mark_derived=False, attrs=None, dctx=None,
                    name=None, **kwargs):
        schema, derived = self.init_derived(
            schema, *qualifiers, name=name,
            as_copy=True, merge_bases=merge_bases,
            attrs=attrs, add_to_schema=add_to_schema,
            mark_derived=mark_derived, dctx=dctx, **kwargs)

        schema, derived = self.finalize_derived(
            schema, derived, merge_bases=merge_bases,
            add_to_schema=add_to_schema, mark_derived=mark_derived,
            dctx=dctx)

        return schema, derived

    def derive(self, schema, source, *qualifiers, merge_bases=None,
               replace_original=None, add_to_schema=False,
               mark_derived=False, attrs=None, dctx=None,
               name=None, apply_defaults=True, **kwargs):
        if not self.generic(schema):
            raise TypeError(
                'cannot derive from specialized {} {!r}'.format(
                    self.__class__.__name__, self.name))

        schema, derived = self.init_derived(
            schema, source, *qualifiers, name=name,
            as_copy=False, merge_bases=merge_bases,
            attrs=attrs, add_to_schema=add_to_schema,
            mark_derived=mark_derived, dctx=dctx, **kwargs)

        schema, derived = self.finalize_derived(
            schema, derived, merge_bases=merge_bases,
            add_to_schema=add_to_schema, mark_derived=mark_derived,
            apply_defaults=apply_defaults, dctx=dctx)

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
