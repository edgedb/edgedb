# DO NOT EDIT. This file was generated with:
#
# $ edb gen-schema-mixins

"""Type definitions for generated methods on schema classes"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edb.schema import schema as s_schema
from edb.schema import getter as s_getter
import uuid
from edb.common import span
from edb.schema import name
from edb.schema import objects
from edb.common import checked


class ObjectMixin:

    def get_internal(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('internal')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )

    def get_sourcectx(
        self, schema: 's_schema.Schema'
    ) -> 'span.Span':
        field = type(self).get_field('sourcectx')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_name(
        self, schema: 's_schema.Schema'
    ) -> 'name.Name':
        field = type(self).get_field('name')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )

    def get_builtin(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('builtin')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )

    def get_computed_fields(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedSet[str]':
        field = type(self).get_field('computed_fields')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )


class InternalObjectMixin:
    pass


class QualifiedObjectMixin:

    def get_name(
        self, schema: 's_schema.Schema'
    ) -> 'name.QualName':
        field = type(self).get_field('name')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )


class ObjectFragmentMixin:
    pass


class GlobalObjectMixin:
    pass


class ExternalObjectMixin:
    pass


class DerivableObjectMixin:
    pass


class SubclassableObjectMixin:

    def get_abstract(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('abstract')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )


class InheritingObjectMixin:

    def get_bases(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[objects.InheritingObject]':
        field = type(self).get_field('bases')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_ancestors(
        self, schema: 's_schema.Schema'
    ) -> 'objects.ObjectList[objects.InheritingObject]':
        field = type(self).get_field('ancestors')
        return s_getter.reducible_getter(
            self,
            schema,
            field,
        )

    def get_inherited_fields(
        self, schema: 's_schema.Schema'
    ) -> 'checked.FrozenCheckedSet[str]':
        field = type(self).get_field('inherited_fields')
        return s_getter.regular_getter(
            self,
            schema,
            field,
        )

    def get_is_derived(
        self, schema: 's_schema.Schema'
    ) -> 'bool':
        field = type(self).get_field('is_derived')
        return s_getter.regular_default_getter(
            self,
            schema,
            field,
        )


class DerivableInheritingObjectMixin:
    pass
