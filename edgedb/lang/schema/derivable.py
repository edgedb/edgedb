##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import inheriting
from . import objects as so
from . import name as sn


class DerivablePrototypeCommand(inheriting.InheritingPrototypeCommand):
    pass


class DerivablePrototype(inheriting.InheritingPrototype):
    name = so.Field(sn.Name, private=True, compcoef=0.909)
    is_derived = so.Field(bool, False, compcoef=0.909)

    # Override name field comparison coefficient on the
    # presumption that the derived names may be different,
    # but base names may be equal.
    #
    def compare(self, other, context=None):
        similarity = super().compare(other, context=context)
        if self.normal_name() != other.normal_name():
            similarity *= 0.625

        return similarity

    @classmethod
    def mangle_name(cls, name):
        return name.replace('~', '~~').replace('.', '~') \
                   .replace('|', '||').replace('::', '|')

    @classmethod
    def unmangle_name(cls, name):
        return name.replace('~', '.').replace('|', '::')

    @classmethod
    def generate_specialized_name(cls, source_name, pointer_name, *qualifiers):
        pointer_name = sn.Name(pointer_name)

        parts = [
            pointer_name.name,
            cls.mangle_name(pointer_name),
            cls.mangle_name(source_name)
        ]

        for qualifier in qualifiers:
            if qualifier:
                parts.append(cls.mangle_name(qualifier))

        return '@'.join(parts)

    @classmethod
    def normalize_name(cls, name):
        name = str(name)

        parts = name.split('@')
        fq_index = 1

        if len(parts) < 3:
            return sn.Name(name)
        else:
            return sn.Name(cls.unmangle_name(parts[fq_index]))

    @classmethod
    def inherit_pure(cls, schema, item, source, *, dctx=None):
        return item

    def derive_name(self, source, *qualifiers):
        qualnames = [qualifier.name for qualifier in qualifiers if qualifier]

        name = self.__class__.generate_specialized_name(
            source.name, self.normal_name(), *qualnames)
        fqname = sn.Name(name=name, module=source.name.module)

        return fqname

    def normal_name(self):
        try:
            return self._normal_name
        except AttributeError:
            self._normal_name = self.normalize_name(self.name)
            return self._normal_name

    def generic(self):
        return self.normal_name() == self.name

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

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if name == 'name':
            try:
                delattr(self, '_normal_name')
            except AttributeError:
                pass
