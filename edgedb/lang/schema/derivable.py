##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import objects as so
from . import name as sn
from . import named


class DerivablePrototype(named.NamedPrototype):
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
    def generate_specialized_name(cls, source_name, pointer_name,
                                       *qualifiers):
        pointer_name = sn.Name(pointer_name)
        parts = [pointer_name.name, pointer_name.replace('.', ':'),
                 source_name.replace(':', '::').replace('.', ':')]
        for qualifier in qualifiers:
            parts.append(qualifier.replace(':', '::').replace('.', ':'))
        return '@'.join(parts)

    @classmethod
    def normalize_name(cls, name):
        name = str(name)

        if '@' in name:
            parts = name.split('@')
            fq_index = 1
        else:
            parts = name.split('!')
            fq_index = 2

        if len(parts) < 3:
            return sn.Name(name)
        else:
            return sn.Name(parts[fq_index].replace(':', '.'))

    @classmethod
    def inherit_pure(cls, schema, item, source):
        return item

    @classmethod
    def merge_many(cls, schema, items, *, source, derived=False, replace=None):
        return items[0].derive(schema, source,
                               merge_bases=items[1:],
                               add_to_schema=True,
                               mark_derived=derived,
                               replace_original=replace)

    def derive_name(self, source, *qualifiers):
        qualnames = [qualifier.name for qualifier in qualifiers]

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

    def init_derived(self, schema, source, *qualifiers, **kwargs):
        derived = self.copy()
        derived.name = self.derive_name(source, *qualifiers)
        return derived

    def merge_bases_into_derived(self, schema, derived, merge_bases, **kwargs):
        for base in merge_bases:
            derived.merge(base, schema=schema)

        derived.finalize(schema, bases=merge_bases)

    def derive(self, schema, source, *qualifiers, merge_bases=None,
                     replace_original=None, add_to_schema=False,
                     mark_derived=False, **kwargs):

        derived = self.init_derived(schema, source, *qualifiers,
                                    merge_bases=merge_bases,
                                    replace_original=replace_original,
                                    add_to_schema=add_to_schema,
                                    mark_derived=mark_derived,
                                    **kwargs)

        if merge_bases:
            self.merge_bases_into_derived(
                    schema, derived, merge_bases,
                    replace_original=replace_original,
                    add_to_schema=add_to_schema,
                    mark_derived=mark_derived,
                    **kwargs)

        if mark_derived:
            derived.is_derived = True

        if add_to_schema:
            original = schema.get(derived.name, default=None)

            if original is not None and replace_original is not None:
                schema.discard(original)
                original = None

            if original is None:
                schema.add(derived)

        return derived

    def finalize(self, schema, bases=None):
        super().finalize(schema, bases=bases)

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if name == 'name':
            try:
                delattr(self, '_normal_name')
            except AttributeError:
                pass
