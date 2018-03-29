##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import name as sn
from . import objects as so


class DerivableObjectBase:
    # Override name field comparison coefficient on the
    # presumption that the derived names may be different,
    # but base names may be equal.
    #
    def compare(self, other, context=None):
        similarity = super().compare(other, context=context)
        if self.shortname != other.shortname:
            similarity *= 0.625

        return similarity

    def derive_name(self, source, *qualifiers):
        name = self.get_specialized_name(
            self.shortname, source.name, *qualifiers)
        return sn.Name(name=name, module=source.name.module)

    def generic(self):
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

        derived = self.copy()
        if attrs is not None:
            derived.update(attrs)
        derived.name = derived_name

        return derived

    def finalize_derived(self, schema, derived, *, merge_bases=None,
                         replace_original=None, add_to_schema=False,
                         mark_derived=False, attrs=None, dctx=None, **kwargs):

        if merge_bases:
            derived.acquire_ancestor_inheritance(
                schema, bases=merge_bases)

        derived.finalize(schema, bases=merge_bases)

        if mark_derived:
            derived.is_derived = True

        if add_to_schema:
            existing_derived = schema.get(derived.name, default=None)

            if existing_derived is None:
                schema.add(derived)
            elif replace_original is not None:
                schema.discard(existing_derived)
                schema.add(derived)

        return derived

    def derive_copy(self, schema, *qualifiers, merge_bases=None,
                    replace_original=None, add_to_schema=False,
                    mark_derived=False, attrs=None, dctx=None,
                    name=None, **kwargs):
        derived = self.init_derived(
            schema, *qualifiers, name=name,
            as_copy=True, merge_bases=merge_bases,
            attrs=attrs, add_to_schema=add_to_schema,
            mark_derived=mark_derived, dctx=dctx, **kwargs)

        self.finalize_derived(
            schema, derived, merge_bases=merge_bases,
            add_to_schema=add_to_schema, mark_derived=mark_derived,
            dctx=dctx)

        return derived

    def derive(self, schema, source, *qualifiers, merge_bases=None,
               replace_original=None, add_to_schema=False,
               mark_derived=False, attrs=None, dctx=None,
               name=None, **kwargs):
        if not self.generic():
            raise TypeError(
                'cannot derive from specialized {} {!r}'.format(
                    self.__class__.__name__, self.name))

        derived = self.init_derived(
            schema, source, *qualifiers, name=name,
            as_copy=False, merge_bases=merge_bases,
            attrs=attrs, add_to_schema=add_to_schema,
            mark_derived=mark_derived, dctx=dctx, **kwargs)

        self.finalize_derived(
            schema, derived, merge_bases=merge_bases,
            add_to_schema=add_to_schema, mark_derived=mark_derived,
            dctx=dctx)

        return derived


class DerivableObject(so.NamedObject, DerivableObjectBase):
    @classmethod
    def inherit_pure(cls, schema, item, source, *, dctx=None):
        # This method is used by ReferencingObject and must be
        # defined for all Derivables, not just Inheriting ones.
        return item
