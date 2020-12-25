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

from __future__ import annotations
from typing import *

import collections.abc
import json

from edb import errors

from edb.common import enum

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes
from edb.edgeql import quote as qlquote

from . import abc as s_abc
from . import annos as s_anno
from . import constraints
from . import defines as s_def
from . import delta as sd
from . import expr as s_expr
from . import inheriting
from . import name as sn
from . import objects as so
from . import referencing
from . import schema as s_schema
from . import types as s_types
from . import utils


if TYPE_CHECKING:
    from . import objtypes as s_objtypes
    from . import sources as s_sources


class PointerDirection(enum.StrEnum):
    Outbound = '>'
    Inbound = '<'


def merge_cardinality(
    ptr: Pointer,
    bases: List[Pointer],
    field_name: str,
    *,
    ignore_local: bool,
    schema: s_schema.Schema,
) -> Any:
    current: Optional[qltypes.SchemaCardinality] = None
    current_from = None

    if not ignore_local:
        current = ptr.get_explicit_field_value(schema, field_name, None)
        if current is not None:
            current_from = ptr

    for base in bases:
        # ignore abstract pointers
        if base.generic(schema):
            continue

        nextval: Optional[qltypes.SchemaCardinality] = (
            base.get_field_value(schema, field_name))
        if nextval is None:
            continue

        if current is None:
            current = nextval
            current_from = base
        elif not current.is_known() and nextval is not None:
            current = nextval
            current_from = base
        elif current is not nextval:
            tgt_repr = ptr.get_verbosename(schema, with_parent=True)
            assert current_from is not None
            cf_repr = current_from.get_verbosename(schema, with_parent=True)
            other_repr = base.get_verbosename(schema, with_parent=True)

            if current.is_known():
                current_qual = f'defined as {current.as_ptr_qual()!r}'
            else:
                current_qual = 'unknown'

            if nextval.is_known():
                nextval_qual = f'defined as {nextval.as_ptr_qual()!r}'
            else:
                nextval_qual = 'unknown'

            raise errors.SchemaDefinitionError(
                f'cannot redefine the target cardinality of '
                f'{tgt_repr}: it is {current_qual} in {cf_repr} and '
                f'is {nextval_qual} in {other_repr}.'
            )

    return current


def merge_readonly(
    target: Pointer,
    sources: List[Pointer],
    field_name: str,
    *,
    ignore_local: bool,
    schema: s_schema.Schema,
) -> Any:

    current = None
    current_from = None

    # The target field value is only relevant if it is explicit,
    # otherwise it should be based on the inherited value.
    if not ignore_local:
        current = target.get_explicit_field_value(schema, field_name, None)
        if current is not None:
            current_from = target

    for source in list(sources):
        # ignore abstract pointers
        if source.generic(schema):
            continue

        # We want the field value including the default, not just
        # explicit value.
        nextval = source.get_field_value(schema, field_name)
        if nextval is not None:
            if current is None:
                current = nextval
                current_from = source
            elif current is not nextval:
                assert current_from is not None

                tgt_repr = target.get_verbosename(
                    schema, with_parent=True)
                cf_repr = current_from.get_verbosename(
                    schema, with_parent=True)
                other_repr = source.get_verbosename(
                    schema, with_parent=True)

                raise errors.SchemaDefinitionError(
                    f'cannot redefine the readonly flag of '
                    f'{tgt_repr}: it is defined '
                    f'as {current} in {cf_repr} and '
                    f'as {nextval} in {other_repr}.'
                )

    return current


def merge_required(
    ptr: Pointer,
    bases: List[Pointer],
    field_name: str,
    *,
    ignore_local: bool = False,
    schema: s_schema.Schema,
) -> Optional[bool]:
    """Merge function for the REQUIRED qualifier on links and properties."""

    local_required = ptr.get_explicit_local_field_value(
        schema, field_name, None)

    if ignore_local or local_required is None:
        # No explicit local declaration, so True if any of the bases
        # have it as required, and False otherwise.
        return utils.merge_reduce(
            ptr,
            bases,
            field_name=field_name,
            ignore_local=ignore_local,
            schema=schema,
            f=max,
            type=bool,
        )
    elif local_required:
        # If set locally and True, just use that.
        assert isinstance(local_required, bool)
        return local_required
    else:
        # Explicitly set locally as False, check if any of the bases
        # are REQUIRED, and if so, raise.
        for base in bases:
            base_required = base.get_field_value(schema, field_name)
            if base_required:
                ptr_repr = ptr.get_verbosename(schema, with_parent=True)
                base_repr = base.get_verbosename(schema, with_parent=True)
                raise errors.SchemaDefinitionError(
                    f'cannot make {ptr_repr} optional: its parent {base_repr} '
                    f'is defined as required'
                )

        return False


def merge_target(
    ptr: Pointer,
    bases: List[Pointer],
    field_name: str,
    *,
    ignore_local: bool = False,
    schema: s_schema.Schema,
) -> Optional[s_types.Type]:

    target = None

    for base in bases:
        base_target = base.get_target(schema)
        if base_target is None:
            continue

        if target is None:
            target = base_target
        else:
            schema, target = Pointer.merge_targets(
                schema, ptr, target, base_target, allow_contravariant=True)

    if not ignore_local:
        local_target = ptr.get_target(schema)
        if target is None:
            target = local_target
        elif local_target is not None:
            schema, target = Pointer.merge_targets(
                schema, ptr, target, local_target)

    return target


Pointer_T = TypeVar("Pointer_T", bound="Pointer")


class Pointer(referencing.ReferencedInheritingObject,
              constraints.ConsistencySubject,
              s_anno.AnnotationSubject,
              s_abc.Pointer):

    source = so.SchemaField(
        so.InheritingObject,
        default=None, compcoef=None,
        inheritable=False)

    target = so.SchemaField(
        s_types.Type,
        merge_fn=merge_target,
        default=None, compcoef=0.85)

    required = so.SchemaField(
        bool,
        default=False,
        compcoef=0.909,
        special_ddl_syntax=True,
        describe_visibility=(
            so.DescribeVisibilityPolicy.SHOW_IF_EXPLICIT_OR_DERIVED
        ),
        merge_fn=merge_required,
    )

    readonly = so.SchemaField(
        bool,
        allow_ddl_set=True,
        describe_visibility=(
            so.DescribeVisibilityPolicy.SHOW_IF_EXPLICIT_OR_DERIVED_NOT_DEFAULT
        ),
        default=False,
        compcoef=0.909,
        merge_fn=merge_readonly,
    )

    # For non-derived pointers this is strongly correlated with
    # "expr" below.  Derived pointers might have "computable" set,
    # but expr=None.
    computable = so.SchemaField(
        bool,
        default=False,
        compcoef=0.99,
    )

    # True, if this pointer is defined in an Alias.
    is_from_alias = so.SchemaField(
        bool,
        default=None,
        compcoef=0.99,
        # This value needs to be recorded in the delta commands
        # to signal that we don't want to render this command in DDL.
        aux_cmd_data=True,
    )

    # Computable pointers have this set to an expression
    # defining them.
    expr = so.SchemaField(
        s_expr.Expression,
        default=None, coerce=True, compcoef=0.909)

    default = so.SchemaField(
        s_expr.Expression,
        allow_ddl_set=True,
        describe_visibility=(
            so.DescribeVisibilityPolicy.SHOW_IF_EXPLICIT_OR_DERIVED
        ),
        default=None,
        coerce=True,
        compcoef=0.909,
    )

    cardinality = so.SchemaField(
        qltypes.SchemaCardinality,
        default=qltypes.SchemaCardinality.One,
        compcoef=0.833,
        coerce=True,
        special_ddl_syntax=True,
        describe_visibility=(
            so.DescribeVisibilityPolicy.SHOW_IF_EXPLICIT_OR_DERIVED
        ),
        merge_fn=merge_cardinality,
    )

    union_of = so.SchemaField(
        so.ObjectSet['Pointer'],
        default=None,
        coerce=True,
        type_is_generic_self=True,
    )

    intersection_of = so.SchemaField(
        so.ObjectSet['Pointer'],
        default=None,
        coerce=True,
        type_is_generic_self=True,
    )

    def is_tuple_indirection(self) -> bool:
        return False

    def is_type_intersection(self) -> bool:
        return False

    def is_generated(self, schema: s_schema.Schema) -> bool:
        return bool(self.get_is_from_alias(schema))

    @classmethod
    def get_displayname_static(cls, name: sn.Name) -> str:
        sn = cls.get_shortname_static(name)
        if sn.module == '__':
            return sn.name
        else:
            return str(sn)

    def get_verbosename(
        self,
        schema: s_schema.Schema,
        *,
        with_parent: bool=False,
    ) -> str:
        is_abstract = self.generic(schema)
        vn = super().get_verbosename(schema)
        if is_abstract:
            return f'abstract {vn}'
        else:
            if with_parent:
                source = self.get_source(schema)
                assert source is not None
                pvn = source.get_verbosename(
                    schema, with_parent=True)
                return f'{vn} of {pvn}'
            else:
                return vn

    def is_scalar(self) -> bool:
        return False

    def material_type(
        self,
        schema: s_schema.Schema,
    ) -> Tuple[s_schema.Schema, Pointer]:
        non_derived_parent = self.get_nearest_non_derived_parent(schema)
        source = non_derived_parent.get_source(schema)
        if source is None:
            return schema, self
        else:
            return schema, non_derived_parent

    def get_near_endpoint(
        self,
        schema: s_schema.Schema,
        direction: PointerDirection,
    ) -> Optional[so.Object]:
        if direction == PointerDirection.Outbound:
            return self.get_source(schema)
        else:
            return self.get_target(schema)

    def get_far_endpoint(
        self,
        schema: s_schema.Schema,
        direction: PointerDirection,
    ) -> Optional[so.Object]:
        if direction == PointerDirection.Outbound:
            return self.get_target(schema)
        else:
            return self.get_source(schema)

    def set_target(
        self,
        schema: s_schema.Schema,
        target: s_types.Type,
    ) -> s_schema.Schema:
        return self.set_field_value(schema, 'target', target)

    @classmethod
    def merge_targets(
        cls,
        schema: s_schema.Schema,
        ptr: Pointer,
        t1: s_types.Type,
        t2: s_types.Type,
        *,
        allow_contravariant: bool = False,
    ) -> Tuple[s_schema.Schema, Optional[s_types.Type]]:
        if t1 == t2:
            return schema, t1

        # When two pointers are merged, check target compatibility
        # and return a target that satisfies both specified targets.

        if (isinstance(t1, s_abc.ScalarType) !=
                isinstance(t2, s_abc.ScalarType)):
            # Mixing a property with a link.
            vnp = ptr.get_verbosename(schema, with_parent=True)
            vn = ptr.get_verbosename(schema)
            t1_vn = t1.get_verbosename(schema)
            t2_vn = t2.get_verbosename(schema)
            raise errors.SchemaError(
                f'cannot redefine {vnp} as {t2_vn}',
                details=f'{vn} is defined as a link to {t1_vn} in a '
                        f'parent type'
            )

        elif isinstance(t1, s_abc.ScalarType):
            # Targets are both scalars
            if t1 != t2:
                vnp = ptr.get_verbosename(schema, with_parent=True)
                vn = ptr.get_verbosename(schema)
                t1_vn = t1.get_verbosename(schema)
                t2_vn = t2.get_verbosename(schema)
                raise errors.SchemaError(
                    f'cannot redefine {vnp} as {t2_vn}',
                    details=f'{vn} is defined as {t1_vn} in a parent type, '
                            f'which is incompatible with {t2_vn} ',
                )

            return schema, t1

        else:
            assert isinstance(t1, so.SubclassableObject)
            assert isinstance(t2, so.SubclassableObject)

            if t2.issubclass(schema, t1):
                # The new target is a subclass of the current target, so
                # it is a more specific requirement.
                current_target = t2
            elif allow_contravariant and t1.issubclass(schema, t2):
                current_target = t1
            else:
                # The new target is not a subclass, of the previously seen
                # targets, which creates an unresolvable target requirement
                # conflict.
                vnp = ptr.get_verbosename(schema, with_parent=True)
                vn = ptr.get_verbosename(schema)
                t2_vn = t2.get_verbosename(schema)
                raise errors.SchemaError(
                    f'cannot redefine {vnp} as {t2_vn}',
                    details=(
                        f'{vn} targets {t2_vn} that is not related '
                        f'to a type found in this link in the parent type: '
                        f'{t1.get_displayname(schema)!r}.'))

            return schema, current_target

    def get_derived(
        self: Pointer_T,
        schema: s_schema.Schema,
        source: s_sources.Source,
        target: s_types.Type,
        *,
        derived_name_base: Optional[sn.Name] = None,
        **kwargs: Any
    ) -> Tuple[s_schema.Schema, Pointer_T]:
        fqname = self.derive_name(
            schema, source, derived_name_base=derived_name_base)
        ptr = schema.get(fqname, default=None)

        if ptr is None:
            fqname = self.derive_name(
                schema,
                source,
                str(target.get_name(schema)),
                derived_name_base=derived_name_base,
            )
            ptr = schema.get(fqname, default=None)
            if ptr is None:
                schema, ptr = self.derive_ref(
                    schema, source, target=target,
                    derived_name_base=derived_name_base, **kwargs)
        return schema, ptr  # type: ignore

    def get_derived_name_base(
        self,
        schema: s_schema.Schema,
    ) -> sn.QualName:
        shortname = self.get_shortname(schema)
        return sn.QualName(module='__', name=shortname.name)

    def derive_ref(
        self,
        schema: s_schema.Schema,
        referrer: so.QualifiedObject,
        *qualifiers: str,
        target: Optional[s_types.Type] = None,
        mark_derived: bool = False,
        attrs: Optional[Dict[str, Any]] = None,
        dctx: Optional[sd.CommandContext] = None,
        **kwargs: Any,
    ) -> Tuple[s_schema.Schema, Pointer]:
        if target is None:
            if attrs and 'target' in attrs:
                target = attrs['target']
            else:
                target = self.get_target(schema)

        if attrs is None:
            attrs = {}

        attrs['source'] = referrer
        attrs['target'] = target

        return super().derive_ref(
            schema, referrer, mark_derived=mark_derived,
            dctx=dctx, attrs=attrs, **kwargs)

    def is_pure_computable(self, schema: s_schema.Schema) -> bool:
        return bool(self.get_expr(schema)) or bool(self.get_computable(schema))

    def is_id_pointer(self, schema: s_schema.Schema) -> bool:
        from edb.schema import sources as s_sources
        std_id = schema.get('std::BaseObject',
                            type=s_sources.Source).getptr(schema, 'id')
        std_target = schema.get('std::target', type=so.SubclassableObject)
        assert isinstance(std_id, so.SubclassableObject)
        return self.issubclass(schema, (std_id, std_target))

    def is_endpoint_pointer(self, schema: s_schema.Schema) -> bool:
        std_source = schema.get('std::source', type=so.SubclassableObject)
        std_target = schema.get('std::target', type=so.SubclassableObject)
        return self.issubclass(schema, (std_source, std_target))

    def is_special_pointer(self, schema: s_schema.Schema) -> bool:
        return self.get_shortname(schema).name in {
            'source', 'target', 'id'
        }

    def is_property(self, schema: s_schema.Schema) -> bool:
        raise NotImplementedError

    def is_link_property(self, schema: s_schema.Schema) -> bool:
        raise NotImplementedError

    def is_protected_pointer(self, schema: s_schema.Schema) -> bool:
        return self.get_shortname(schema).name in {'id', '__type__'}

    def is_dumpable(self, schema: s_schema.Schema) -> bool:
        return (
            not self.is_endpoint_pointer(schema)
            and not self.is_pure_computable(schema)
        )

    def generic(self, schema: s_schema.Schema) -> bool:
        return self.get_source(schema) is None

    def get_referrer(self, schema: s_schema.Schema) -> Optional[so.Object]:
        return self.get_source(schema)

    def is_exclusive(self, schema: s_schema.Schema) -> bool:
        if self.generic(schema):
            raise ValueError(f'{self!r} is generic')

        exclusive = schema.get('std::exclusive', type=constraints.Constraint)

        ptr = self.get_nearest_non_derived_parent(schema)

        for constr in ptr.get_constraints(schema).objects(schema):
            if (constr.issubclass(schema, exclusive) and
                    not constr.get_subjectexpr(schema)):

                return True

        return False

    def singular(
        self,
        schema: s_schema.Schema,
        direction: PointerDirection = PointerDirection.Outbound,
    ) -> bool:
        # Determine the cardinality of a given endpoint set.
        if direction == PointerDirection.Outbound:
            cardinality = self.get_cardinality(schema)
            if cardinality is None or not cardinality.is_known():
                vn = self.get_verbosename(schema, with_parent=True)
                raise AssertionError(f'cardinality of {vn} is unknown')
            return cardinality.is_single()
        else:
            return self.is_exclusive(schema)

    def get_implicit_bases(self, schema: s_schema.Schema) -> List[Pointer]:
        bases = super().get_implicit_bases(schema)

        # True implicit bases for pointers will have a different source.
        my_source = self.get_source(schema)
        return [
            b for b in bases
            if b.get_source(schema) != my_source
        ]

    def get_implicit_ancestors(self, schema: s_schema.Schema) -> List[Pointer]:
        ancestors = super().get_implicit_ancestors(schema)

        # True implicit ancestors for pointers will have a different source.
        my_source = self.get_source(schema)
        return [
            b for b in ancestors
            if b.get_source(schema) != my_source
        ]

    def has_user_defined_properties(self, schema: s_schema.Schema) -> bool:
        return False

    def allow_ref_propagation(
        self,
        schema: s_schema.Schema,
        constext: sd.CommandContext,
        refdict: so.RefDict,
    ) -> bool:
        object_type = self.get_source(schema)
        if isinstance(object_type, s_types.Type):
            return not object_type.is_view(schema)
        else:
            return True

    def get_schema_reflection_default(
        self,
        schema: s_schema.Schema,
    ) -> Optional[str]:
        """Return the default expression if this is a reflection of a
           schema class field and the field has a defined default value.
        """
        ptr = self.get_nearest_non_derived_parent(schema)

        src = ptr.get_source(schema)
        if src is None:
            # This is an abstract pointer
            return None

        ptr_name = ptr.get_name(schema)
        if ptr_name.module not in {'schema', 'sys', 'cfg'}:
            # This isn't a reflection type
            return None

        if isinstance(src, Pointer):
            # This is a link property
            tgt = src.get_target(schema)
            assert tgt is not None
            schema_objtype = tgt
        else:
            assert isinstance(src, s_types.Type)
            schema_objtype = src

        assert isinstance(schema_objtype, so.QualifiedObject)
        src_name = schema_objtype.get_name(schema)
        mcls = so.ObjectMeta.maybe_get_schema_class(src_name.name)
        if mcls is None:
            # This schema class is not (publicly) reflected.
            return None
        fname = ptr.get_shortname(schema).name
        if not mcls.has_field(fname):
            # This pointer is not a schema field.
            return None
        field = mcls.get_field(fname)
        if not isinstance(field, so.SchemaField):
            # Not a schema field, no default possible.
            return None
        f_default = field.default
        if (
            f_default is None
            or f_default is so.NoDefault
        ):
            # No explicit default value.
            return None

        tgt = ptr.get_target(schema)

        if f_default is so.DEFAULT_CONSTRUCTOR:
            if (
                issubclass(
                    field.type,
                    (collections.abc.Set, collections.abc.Sequence),
                )
                and not issubclass(field.type, (str, bytes))
            ):
                return f'<{tgt.get_displayname(schema)}>[]'
            else:
                return None

        default = qlquote.quote_literal(json.dumps(f_default))

        if tgt.is_enum(schema):
            return f'<{tgt.get_displayname(schema)}><str>to_json({default})'
        else:
            return f'<{tgt.get_displayname(schema)}>to_json({default})'


class PseudoPointer(s_abc.Pointer):
    # An abstract base class for pointer-like objects, i.e.
    # pseudo-links used by the compiler to represent things like
    # tuple and type intersection.
    def is_tuple_indirection(self) -> bool:
        return False

    def is_type_intersection(self) -> bool:
        return False

    def get_bases(self, schema: s_schema.Schema) -> so.ObjectList[Pointer]:
        return so.ObjectList.create(schema, [])

    def get_ancestors(self, schema: s_schema.Schema) -> so.ObjectList[Pointer]:
        return so.ObjectList.create(schema, [])

    def get_name(self, schema: s_schema.Schema) -> sn.QualName:
        raise NotImplementedError

    def get_shortname(self, schema: s_schema.Schema) -> sn.QualName:
        return self.get_name(schema)

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return str(self.get_name(schema))

    def has_user_defined_properties(self, schema: s_schema.Schema) -> bool:
        return False

    def get_required(self, schema: s_schema.Schema) -> bool:
        return True

    def get_cardinality(
        self,
        schema: s_schema.Schema
    ) -> qltypes.SchemaCardinality:
        raise NotImplementedError

    def get_path_id_name(self, schema: s_schema.Schema) -> sn.QualName:
        return self.get_name(schema)

    def get_is_derived(self, schema: s_schema.Schema) -> bool:
        return False

    def get_is_owned(self, schema: s_schema.Schema) -> bool:
        return True

    def get_union_of(
        self,
        schema: s_schema.Schema,
    ) -> None:
        return None

    def get_intersection_of(
        self,
        schema: s_schema.Schema,
    ) -> None:
        return None

    def get_default(
        self,
        schema: s_schema.Schema,
    ) -> Optional[s_expr.Expression]:
        return None

    def get_expr(self, schema: s_schema.Schema) -> Optional[s_expr.Expression]:
        return None

    def get_source(self, schema: s_schema.Schema) -> so.Object:
        raise NotImplementedError

    def get_target(self, schema: s_schema.Schema) -> s_types.Type:
        raise NotImplementedError

    def get_near_endpoint(
        self,
        schema: s_schema.Schema,
        direction: PointerDirection,
    ) -> so.Object:
        if direction is PointerDirection.Outbound:
            return self.get_source(schema)
        else:
            raise AssertionError(
                f'inbound direction is not valid for {type(self)}'
            )

    def get_far_endpoint(
        self,
        schema: s_schema.Schema,
        direction: PointerDirection,
    ) -> so.Object:
        if direction is PointerDirection.Outbound:
            return self.get_target(schema)
        else:
            raise AssertionError(
                f'inbound direction is not valid for {type(self)}'
            )

    def is_link_property(self, schema: s_schema.Schema) -> bool:
        return False

    def generic(self, schema: s_schema.Schema) -> bool:
        return False

    def singular(
        self,
        schema: s_schema.Schema,
        direction: PointerDirection = PointerDirection.Outbound,
    ) -> bool:
        raise NotImplementedError

    def scalar(self) -> bool:
        raise NotImplementedError

    def material_type(
        self,
        schema: s_schema.Schema,
    ) -> Tuple[s_schema.Schema, PseudoPointer]:
        return schema, self

    def is_pure_computable(self, schema: s_schema.Schema) -> bool:
        return False

    def is_exclusive(self, schema: s_schema.Schema) -> bool:
        return False

    def get_schema_reflection_default(
        self,
        schema: s_schema.Schema,
    ) -> Optional[str]:
        return None


PointerLike = Union[Pointer, PseudoPointer]


class ComputableRef:
    """A shell for a computed target type."""

    expr: qlast.Base

    def __init__(self, expr: qlast.Base) -> None:
        self.expr = expr


class PointerCommandContext(sd.ObjectCommandContext[Pointer_T],
                            s_anno.AnnotationSubjectCommandContext):
    pass


class PointerCommandOrFragment(
    referencing.ReferencedObjectCommandBase[Pointer_T]
):

    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)
        target_ref = self.get_local_attribute_value('target')
        inf_target_ref: Optional[s_types.TypeShell]

        # When cardinality/required is altered, we need to force a
        # reconsideration of expr if it exists in order to check
        # it against the new specifier or compute them on a
        # RESET. This is kind of unfortunate.
        if (
            isinstance(self, sd.AlterObject)
            and (self.has_attribute_value('cardinality')
                 or self.has_attribute_value('required'))
            and not self.has_attribute_value('expr')
            and (expr := self.scls.get_expr(schema)) is not None
        ):
            self.set_attribute_value(
                'expr',
                s_expr.Expression.not_compiled(expr)
            )

        if isinstance(target_ref, ComputableRef):
            schema, inf_target_ref, base = self._parse_computable(
                target_ref.expr, schema, context)
        elif (expr := self.get_local_attribute_value('expr')) is not None:
            schema, inf_target_ref, base = self._parse_computable(
                expr.qlast, schema, context)
        else:
            inf_target_ref = None
            base = None

        if base is not None:
            self.set_attribute_value(
                'bases', so.ObjectList.create(schema, [base]),
            )

            self.set_attribute_value(
                'is_derived', True
            )

            if context.declarative:
                self.set_attribute_value(
                    'declared_overloaded', True
                )

        if inf_target_ref is not None:
            srcctx = self.get_attribute_source_context('target')
            self.set_attribute_value(
                'target',
                inf_target_ref,
                source_context=srcctx,
            )

        schema = s_types.materialize_type_in_attribute(
            schema, context, self, 'target')

        expr = self.get_local_attribute_value('expr')
        if expr is not None:
            # There is an expression, therefore it is a computable.
            self.set_attribute_value('computable', True)

        return schema

    def _parse_computable(
        self,
        expr: qlast.Base,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> Tuple[s_schema.Schema, s_types.TypeShell, Optional[PointerLike]]:
        from edb.ir import ast as irast
        from edb.ir import typeutils as irtyputils
        from edb.ir import utils as irutils

        # "source" attribute is set automatically as a refdict back-attr
        parent_ctx = self.get_referrer_context(context)
        assert parent_ctx is not None
        source_name = context.get_referrer_name(parent_ctx)
        assert isinstance(source_name, sn.QualName)

        source = schema.get(source_name)
        parent_vname = source.get_verbosename(schema)
        ptr_name = self.get_verbosename(parent=parent_vname)
        expression = self.compile_expr_field(
            schema, context,
            field=Pointer.get_field('expr'),
            value=s_expr.Expression.from_ast(expr, schema, context.modaliases),
        )

        assert isinstance(expression.irast, irast.Statement)
        base = None
        target = expression.irast.stype
        target_shell = target.as_shell(expression.irast.schema)
        if (
            isinstance(target_shell, s_types.UnionTypeShell)
            and target_shell.opaque
        ):
            target = schema.get('std::BaseObject', type=s_types.Type)
            target_shell = target.as_shell(schema)

        result_expr = expression.irast.expr
        if isinstance(result_expr, irast.Set):
            result_expr = irutils.unwrap_set(result_expr)
            if result_expr.rptr is not None:
                result_expr, _ = irutils.collapse_type_intersection(
                    result_expr)

        # Process a computable pointer which potentially could be an
        # aliased link that should inherit link properties.
        if isinstance(result_expr, irast.Set) and result_expr.rptr is not None:
            expr_rptr = result_expr.rptr
            if (
                expr_rptr.direction is PointerDirection.Outbound
                and expr_rptr.source.rptr is None
            ):
                new_schema, aliased_ptr = irtyputils.ptrcls_from_ptrref(
                    expr_rptr.ptrref, schema=schema
                )
                # Only pointers coming from the same source as the
                # alias should be "inherited" (in order to preserve
                # link props). Random paths coming from other sources
                # get treated same as any other arbitrary expression
                # in a computable.
                if aliased_ptr.get_source(new_schema) == source:
                    base = aliased_ptr
                    schema = new_schema

        self.set_attribute_value('expr', expression)
        required, card = expression.irast.cardinality.to_schema_value()

        spec_required: Optional[bool] = (
            self.get_specified_attribute_value('required', schema, context))
        spec_card: Optional[qltypes.SchemaCardinality] = (
            self.get_specified_attribute_value('cardinality', schema, context))

        if spec_required and not required:
            srcctx = self.get_attribute_source_context('target')
            raise errors.SchemaDefinitionError(
                f'possibly an empty set returned by an '
                f'expression for the computable '
                f'{ptr_name} '
                f"explicitly declared as 'required'",
                context=srcctx
            )

        if (
            spec_card is qltypes.SchemaCardinality.One
            and card is not qltypes.SchemaCardinality.One
        ):
            srcctx = self.get_attribute_source_context('target')
            raise errors.SchemaDefinitionError(
                f'possibly more than one element returned by an '
                f'expression for the computable '
                f'{ptr_name} '
                f"explicitly declared as 'single'",
                context=srcctx
            )

        if spec_card is None:
            self.set_attribute_value('cardinality', card, computed=True)

        if spec_required is None:
            self.set_attribute_value('required', required, computed=True)

        self.set_attribute_value('computable', True)

        return schema, target_shell, base

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: s_expr.Expression,
        track_schema_ref_exprs: bool=False,
    ) -> s_expr.Expression:
        if field.name in {'default', 'expr'}:
            singletons: List[Union[s_types.Type, Pointer]] = []
            path_prefix_anchor = None
            anchors: Dict[str, Any] = {}
            in_ddl_context_name: Optional[str] = None

            if field.name == 'expr':
                parent_ctx = self.get_referrer_context(context)
                assert parent_ctx is not None
                assert isinstance(parent_ctx.op, sd.ObjectCommand)
                source = parent_ctx.op.get_object(schema, context)

                if (
                    isinstance(source, Pointer)
                    and not source.get_source(schema)
                ):
                    # If the source is an abstract link, we need to
                    # make up an object and graft the link onto it,
                    # because the compiler really does not know what
                    # to make of a link without a source or target.
                    from edb.schema import objtypes as s_objtypes

                    base_obj = schema.get(
                        s_objtypes.ObjectType.get_default_base_name(),
                        type=s_objtypes.ObjectType
                    )
                    schema, view = base_obj.derive_subtype(
                        schema,
                        name=sn.QualName("__derived__",
                                         "FakeAbstractLinkBase"),
                        mark_derived=True, transient=True)
                    schema, source = source.derive_ref(
                        schema, view, target=view,
                        mark_derived=True, transient=True)

                anchors[qlast.Source().name] = source
                assert isinstance(source, (s_types.Type, Pointer))
                singletons = [source]
                path_prefix_anchor = qlast.Source().name

                parent_vname = source.get_verbosename(schema)
                ptr_name = self.get_verbosename(parent=parent_vname)
                in_ddl_context_name = f'computable {ptr_name}'

            return type(value).compiled(
                value,
                schema=schema,
                options=qlcompiler.CompilerOptions(
                    modaliases=context.modaliases,
                    schema_object_context=self.get_schema_metaclass(),
                    anchors=anchors,
                    path_prefix_anchor=path_prefix_anchor,
                    singletons=frozenset(singletons),
                    apply_query_rewrites=not context.stdmode,
                    track_schema_ref_exprs=track_schema_ref_exprs,
                    in_ddl_context_name=in_ddl_context_name,
                ),
            )
        else:
            return super().compile_expr_field(
                schema, context, field, value, track_schema_ref_exprs)

    def _deparse_name(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        name: sn.Name,
    ) -> qlast.ObjectRef:

        ref = super()._deparse_name(schema, context, name)
        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is None:
            return ref
        else:
            ref.module = ''
            return ref


class PointerAlterFragment(
    referencing.ReferencedObjectCommandBase[Pointer_T]
):
    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> referencing.AlterReferencedInheritingObject[Any]:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, PointerCommand)
        if isinstance(astnode, qlast.CreateConcreteLink):
            cmd._process_create_or_alter_ast(schema, astnode, context)
        else:
            expr_cmd = qlast.get_ddl_field_command(astnode, 'expr')
            if expr_cmd is not None:
                expr = expr_cmd.value
                if expr is None:
                    # `RESET EXPRESSION` detected
                    aop = sd.AlterObjectProperty(
                        property='expr',
                        new_value=None,
                        source_context=astnode.context,
                    )
                    cmd.add(aop)

        assert isinstance(cmd, referencing.AlterReferencedInheritingObject)
        return cmd

    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)

        # Handle `RESET EXPRESSION` here
        if (
            self.has_attribute_value('expr')
            and not self.is_attribute_inherited('expr')
            and self.get_attribute_value('expr') is None
        ):
            old_expr = self.get_orig_attribute_value('expr')
            pointer = schema.get(self.classname, type=Pointer)
            if old_expr is None:
                # Get the old value from the schema if the old_expr
                # attribute isn't set.
                old_expr = pointer.get_expr(schema)

            if old_expr is not None:
                # If the expression was explicitly set to None,
                # that means that `RESET EXPRESSION` was executed
                # and this is no longer a computable.
                self.set_attribute_value('computable', False)
                computed_fields = pointer.get_computed_fields(schema)
                if 'required' in computed_fields:
                    self.set_attribute_value('required', None)
                if 'cardinality' in computed_fields:
                    self.set_attribute_value('cardinality', None)

            # Clear the placeholder value for 'expr'.
            self.set_attribute_value('expr', None)

        return schema


class PointerCommand(
    referencing.ReferencedInheritingObjectCommand[Pointer_T],
    constraints.ConsistencySubjectCommand[Pointer_T],
    s_anno.AnnotationSubjectCommand[Pointer_T],
    PointerCommandOrFragment[Pointer_T],
):

    def _set_pointer_type(
        self,
        schema: s_schema.Schema,
        astnode: qlast.CreateConcretePointer,
        context: sd.CommandContext,
        target_ref: Union[so.Object, so.ObjectShell, ComputableRef],
    ) -> None:
        raise NotImplementedError

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._create_begin(schema, context)

        if not context.canonical:
            self._validate_pointer_def(schema, context)
        return schema

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_begin(schema, context)
        if not context.canonical:
            self._validate_pointer_def(schema, context)
        return schema

    def _validate_pointer_def(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        """Check that pointer definition is sound."""
        from edb.ir import ast as irast

        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is None:
            return

        scls = self.scls
        if not scls.get_is_owned(schema):
            return

        default_expr = scls.get_default(schema)

        if default_expr is not None:
            if default_expr.irast is None:
                default_expr = default_expr.compiled(default_expr, schema)

            assert isinstance(default_expr.irast, irast.Statement)

            default_type = default_expr.irast.stype
            assert default_type is not None
            ptr_target = scls.get_target(schema)
            assert ptr_target is not None

            source_context = self.get_attribute_source_context('default')
            if not default_type.assignment_castable_to(ptr_target, schema):
                raise errors.SchemaDefinitionError(
                    f'default expression is of invalid type: '
                    f'{default_type.get_displayname(schema)}, '
                    f'expected {ptr_target.get_displayname(schema)}',
                    context=source_context,
                )
            # "required" status of defaults should not be enforced
            # because it's impossible to actually guarantee that any
            # SELECT involving a path is non-empty
            ptr_cardinality = scls.get_cardinality(schema)
            default_required, default_cardinality = \
                default_expr.irast.cardinality.to_schema_value()

            if (ptr_cardinality is qltypes.SchemaCardinality.One
                    and default_cardinality != ptr_cardinality):
                raise errors.SchemaDefinitionError(
                    f'possibly more than one element returned by '
                    f'the default expression for '
                    f'{scls.get_verbosename(schema)} declared as '
                    f"'single'",
                    context=source_context,
                )

    @classmethod
    def _classname_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        context: sd.CommandContext,
    ) -> sn.QualName:
        referrer_ctx = cls.get_referrer_context(context)
        if referrer_ctx is not None:

            referrer_name = context.get_referrer_name(referrer_ctx)

            shortname = sn.QualName(
                module='__',
                name=astnode.name.name,
            )

            name = sn.QualName(
                module=referrer_name.module,
                name=sn.get_specialized_name(
                    shortname,
                    str(referrer_name),
                ),
            )
        else:
            name = super()._classname_from_ast(schema, astnode, context)

        sname = sn.shortname_from_fullname(name)
        assert isinstance(sname, sn.QualName), "expected qualified name"
        if len(sname.name) > s_def.MAX_NAME_LENGTH:
            raise errors.SchemaDefinitionError(
                f'link or property name length exceeds the maximum of '
                f'{s_def.MAX_NAME_LENGTH} characters',
                context=astnode.context)
        return name

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        referrer_ctx = cls.get_referrer_context(context)
        if referrer_ctx is not None:
            if getattr(astnode, 'declared_overloaded', False):
                cmd.set_attribute_value('declared_overloaded', True)
        return cmd

    def _process_create_or_alter_ast(
        self,
        schema: s_schema.Schema,
        astnode: qlast.CreateConcretePointer,
        context: sd.CommandContext,
    ) -> None:
        """Handle the CREATE {PROPERTY|LINK} AST node.

        This may be called in the context of either Create or Alter.
        """
        if astnode.is_required is not None:
            self.set_attribute_value(
                'required',
                astnode.is_required,
                source_context=astnode.context,
            )

        if astnode.cardinality is not None:
            if isinstance(self, sd.CreateObject):
                self.set_attribute_value(
                    'cardinality',
                    astnode.cardinality,
                    source_context=astnode.context,
                )
            else:
                handler = sd.get_special_field_alter_handler_for_context(
                    'cardinality', context)
                assert handler is not None
                set_field = qlast.SetField(
                    name='cardinality',
                    value=qlast.StringConstant.from_python(
                        str(astnode.cardinality),
                    ),
                    special_syntax=True,
                    context=astnode.context,
                )
                apc = handler._cmd_tree_from_ast(schema, set_field, context)
                self.add(apc)

        parent_ctx = self.get_referrer_context_or_die(context)
        source_name = context.get_referrer_name(parent_ctx)
        self.set_attribute_value('source', so.ObjectShell(name=source_name))

        # FIXME: this is an approximate solution
        targets = qlast.get_targets(astnode.target)
        target_ref: Union[None, s_types.TypeShell, ComputableRef]

        if len(targets) > 1:
            assert isinstance(source_name, sn.QualName)

            new_targets = [
                utils.ast_to_type_shell(
                    t,
                    modaliases=context.modaliases,
                    schema=schema,
                )
                for t in targets
            ]

            target_ref = s_types.UnionTypeShell(
                new_targets,
                module=source_name.module,
            )
        elif targets:
            target_expr = targets[0]
            if isinstance(target_expr, qlast.TypeName):
                target_ref = utils.ast_to_type_shell(
                    target_expr,
                    modaliases=context.modaliases,
                    schema=schema,
                )
            else:
                # computable
                qlcompiler.normalize(
                    target_expr,
                    schema=schema,
                    modaliases=context.modaliases
                )
                target_ref = ComputableRef(target_expr)
        else:
            # Target is inherited.
            target_ref = None

        if isinstance(self, sd.CreateObject):
            assert astnode.target is not None
            self.set_attribute_value(
                'target',
                target_ref,
                source_context=astnode.target.context,
            )

        elif target_ref is not None:
            self._set_pointer_type(schema, astnode, context, target_ref)


class SetPointerType(
    referencing.ReferencedInheritingObjectCommand[Pointer_T],
    inheriting.AlterInheritingObjectFragment[Pointer_T],
    PointerCommandOrFragment[Pointer_T],
):

    def get_friendly_description(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        object: Optional[Pointer_T] = None,
        object_desc: Optional[str] = None,
    ) -> str:
        object_desc = self.get_friendly_object_name_for_description(
            schema,
            context,
            object=object,
            object_desc=object_desc,
        )
        return f'alter the type of {object_desc}'

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super()._alter_begin(schema, context)
        scls = self.scls

        orig_target = scls.get_target(orig_schema)
        new_target = scls.get_target(schema)

        if orig_target == new_target:
            return schema

        vn = scls.get_verbosename(schema, with_parent=True)
        schema = self._propagate_if_expr_refs(
            schema, context, action=f'alter the type of {vn}')

        if not context.canonical:
            if (
                orig_target is not None
                and isinstance(orig_target, s_types.Collection)
            ):
                parent_ctx = context.parent()
                assert parent_ctx
                parent_ctx.op.add(orig_target.as_colltype_delete_delta(
                    schema, expiring_refs={scls}))

            schema = self._propagate_ref_field_alter_in_inheritance(
                schema,
                context,
                field_name='target',
            )

        return schema

    @classmethod
    def _cmd_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.ObjectCommand[Pointer_T]:
        this_op = context.current().op
        assert isinstance(this_op, sd.ObjectCommand)
        return cls(classname=this_op.classname)

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        assert isinstance(astnode, qlast.SetPointerType)
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        targets = qlast.get_targets(astnode.type)
        target_ref: s_types.TypeShell

        if len(targets) > 1:
            new_targets = [
                utils.ast_to_type_shell(
                    t,
                    modaliases=context.modaliases,
                    schema=schema,
                )
                for t in targets
            ]

            target_ref = s_types.UnionTypeShell(
                new_targets,
                module=cls.classname.module,
            )
        else:
            target = targets[0]
            target_ref = utils.ast_to_type_shell(
                target,
                modaliases=context.modaliases,
                schema=schema,
            )

        cmd.set_attribute_value('target', target_ref)

        return cmd


class AlterPointerUpperCardinality(
    referencing.ReferencedInheritingObjectCommand[Pointer_T],
    inheriting.AlterInheritingObjectFragment[Pointer_T],
    sd.AlterSpecialObjectField[Pointer_T],
    PointerCommandOrFragment[Pointer_T],
):
    """Handler for the "cardinality" field changes."""

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super()._alter_begin(schema, context)
        scls = self.scls

        orig_card = scls.get_cardinality(orig_schema)
        new_card = scls.get_cardinality(schema)

        if orig_card == new_card:
            # The actual value hasn't changed, nothing to do here.
            return schema

        vn = scls.get_verbosename(schema, with_parent=True)
        schema = self._propagate_if_expr_refs(
            schema, context, action=f'alter the cardinality of {vn}')

        if not context.canonical:
            schema = self._propagate_ref_field_alter_in_inheritance(
                schema,
                context,
                field_name='cardinality',
            )

        return schema


def get_or_create_union_pointer(
    schema: s_schema.Schema,
    ptrname: str,
    source: s_sources.Source,
    direction: PointerDirection,
    components: Iterable[Pointer],
    *,
    opaque: bool = False,
    modname: Optional[str] = None,
) -> Tuple[s_schema.Schema, Pointer]:
    from . import sources as s_sources

    components = list(components)

    if len(components) == 1 and direction is PointerDirection.Outbound:
        return schema, components[0]

    far_endpoints = [p.get_far_endpoint(schema, direction)
                     for p in components]
    targets: List[s_types.Type] = [p for p in far_endpoints
                                   if isinstance(p, s_types.Type)]

    target: s_types.Type

    schema, target = utils.get_union_type(
        schema, targets, opaque=opaque, module=modname)

    cardinality = qltypes.SchemaCardinality.One
    for component in components:
        if component.get_cardinality(schema) is qltypes.SchemaCardinality.Many:
            cardinality = qltypes.SchemaCardinality.Many
            break

    metacls = type(components[0])
    default_base_name = metacls.get_default_base_name()
    assert default_base_name is not None
    genptr = schema.get(default_base_name, type=Pointer)

    if direction is PointerDirection.Inbound:
        # type ignore below, because the types "Type" and "Source"
        # could only be swapped by their common ancestor so.Object,
        # and here we are considering them both as more specific objects
        source, target = target, source  # type: ignore

    schema, result = genptr.get_derived(
        schema,
        source,
        target,
        derived_name_base=sn.QualName(
            module='__',
            name=ptrname),
        attrs={
            'union_of': so.ObjectSet.create(schema, components),
            'cardinality': cardinality,
        },
    )

    if isinstance(result, s_sources.Source):
        # cast below, because in this case the list of Pointer
        # is also a list of Source (links.Link)
        schema = s_sources.populate_pointer_set_for_source_union(
            schema,
            cast(List[s_sources.Source], components),
            result,
            modname=modname,
        )

    return schema, result


def get_or_create_intersection_pointer(
    schema: s_schema.Schema,
    ptrname: str,
    source: s_objtypes.ObjectType,
    components: Iterable[Pointer], *,
    modname: Optional[str] = None,
) -> Tuple[s_schema.Schema, Pointer]:

    components = list(components)

    if len(components) == 1:
        return schema, components[0]

    targets = list(filter(None, [p.get_target(schema) for p in components]))
    schema, target = utils.get_intersection_type(
        schema, targets, module=modname)

    cardinality = qltypes.SchemaCardinality.One
    for component in components:
        if component.get_cardinality(schema) is qltypes.SchemaCardinality.Many:
            cardinality = qltypes.SchemaCardinality.Many
            break

    metacls = type(components[0])
    default_base_name = metacls.get_default_base_name()
    assert default_base_name is not None
    genptr = schema.get(default_base_name, type=Pointer)

    schema, result = genptr.get_derived(
        schema,
        source,
        target,
        derived_name_base=sn.QualName(
            module='__',
            name=ptrname),
        attrs={
            'intersection_of': so.ObjectSet.create(schema, components),
            'cardinality': cardinality,
        },
    )

    return schema, result
