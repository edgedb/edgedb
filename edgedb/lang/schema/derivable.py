##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import inheriting
from . import objects as so
from . import name as sn


class DerivableClassCommand(inheriting.InheritingClassCommand):
    pass


class DerivableClass(inheriting.InheritingClass):
    name = so.Field(sn.Name, private=True, compcoef=0.909)
    is_derived = so.Field(bool, False, compcoef=0.909)

    # Override name field comparison coefficient on the
    # presumption that the derived names may be different,
    # but base names may be equal.
    #
    def compare(self, other, context=None):
        similarity = super().compare(other, context=context)
        if self.shortname != other.shortname:
            similarity *= 0.625

        return similarity

    @classmethod
    def inherit_pure(cls, schema, item, source, *, dctx=None):
        return item

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
                     attrs=None, dctx=None, **kwargs):
        derived_name = self.get_derived_name(
            source, *qualifiers, mark_derived=mark_derived)

        if as_copy:
            derived = self.copy()
            if attrs is not None:
                derived.update(attrs)
            derived.name = derived_name

        else:
            derived_attrs = {}

            if attrs is not None:
                derived_attrs.update(attrs)

            if not derived_attrs.get('bases'):
                derived_attrs['bases'] = [self]

            derived_attrs.pop('name', None)

            cls = type(self)
            derived = cls(name=derived_name, **derived_attrs,
                          _setdefaults_=False, _relaxrequired_=True)

        return derived

    def finalize_derived(self, schema, derived, *, merge_bases=None,
                         replace_original=None, add_to_schema=False,
                         mark_derived=False, attrs=None, dctx=None, **kwargs):

        existing_derived = schema.get(derived.name, default=None)

        if merge_bases:
            derived.acquire_ancestor_inheritance(
                schema, bases=merge_bases)

        derived.finalize(schema, bases=merge_bases)

        if mark_derived:
            derived.is_derived = True

        if add_to_schema:
            if existing_derived is None:
                schema.add(derived)
            elif replace_original is not None:
                schema.discard(existing_derived)
                schema.add(derived)

        return derived

    def derive_copy(self, schema, *qualifiers, merge_bases=None,
                    replace_original=None, add_to_schema=False,
                    mark_derived=False, attrs=None, dctx=None, **kwargs):
        derived = self.init_derived(
            schema, *qualifiers, as_copy=True, merge_bases=merge_bases,
            attrs=attrs, add_to_schema=add_to_schema,
            mark_derived=mark_derived, dctx=dctx, **kwargs)

        self.finalize_derived(
            schema, derived, merge_bases=merge_bases,
            add_to_schema=add_to_schema, mark_derived=mark_derived,
            dctx=dctx)

        return derived

    def derive(self, schema, source, *qualifiers, merge_bases=None,
               replace_original=None, add_to_schema=False,
               mark_derived=False, attrs=None, dctx=None, **kwargs):

        if not self.generic():
            raise TypeError(
                'cannot derive from specialized {} {!r}'.format(
                    self.__class__.__name__, self.name))

        derived = self.init_derived(
            schema, source, *qualifiers,
            as_copy=False, merge_bases=merge_bases,
            attrs=attrs, add_to_schema=add_to_schema,
            mark_derived=mark_derived, dctx=dctx, **kwargs)

        self.finalize_derived(
            schema, derived, merge_bases=merge_bases,
            add_to_schema=add_to_schema, mark_derived=mark_derived,
            dctx=dctx)

        return derived
