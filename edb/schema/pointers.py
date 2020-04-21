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

from edb import errors

from edb.common import enum

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes
from edb.schema import defines as s_def

from . import abc as s_abc
from . import annos as s_anno
from . import constraints
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


def merge_cardinality(target: Pointer, sources: List[Pointer],
                      field_name: str, *, schema: s_schema.Schema) -> Any:
    current = None
    current_from = None

    for source in [target] + list(sources):
        nextval = source.get_explicit_field_value(schema, field_name, None)
        if nextval is not None:
            if current is None:
                current = nextval
                current_from = source
            elif current is not nextval:
                tgt_repr = target.get_verbosename(
                    schema, with_parent=True)
                cf_repr = current_from.get_verbosename(
                    schema, with_parent=True)
                other_repr = source.get_verbosename(
                    schema, with_parent=True)

                raise errors.SchemaError(
                    f'cannot redefine the target cardinality of '
                    f'{tgt_repr}: it is defined '
                    f'as {current.as_ptr_qual()!r} in {cf_repr} and '
                    f'as {nextval.as_ptr_qual()!r} in {other_repr}.'
                )

    return current


def merge_readonly(target: Pointer, sources: List[Pointer],
                   field_name: str, *, schema: s_schema.Schema) -> Any:

    current = None
    current_from = None

    # The target field value is only relevant if it is explicit,
    # otherwise it should be based on the inherited value.
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

                raise errors.SchemaError(
                    f'cannot redefine the readonly flag of '
                    f'{tgt_repr}: it is defined '
                    f'as {current} in {cf_repr} and '
                    f'as {nextval} in {other_repr}.'
                )

    return current


def merge_target(
    ptr: Pointer,
    bases: List[Pointer],
    field_name: str,
    *,
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

    local_target = ptr.get_target(schema)
    if target is None:
        target = local_target
    elif local_target is not None:
        schema, target = Pointer.merge_targets(
            schema, ptr, target, local_target)

    return target


class Pointer(referencing.ReferencedInheritingObject,
              constraints.ConsistencySubject,
              s_anno.AnnotationSubject,
              s_abc.Pointer):

    source = so.SchemaField(
        so.Object,
        default=None, compcoef=None,
        inheritable=False)

    target = so.SchemaField(
        s_types.Type,
        merge_fn=merge_target,
        default=None, compcoef=0.85)

    required = so.SchemaField(
        bool,
        default=False, compcoef=0.909,
        merge_fn=utils.merge_sticky_bool)

    readonly = so.SchemaField(
        bool,
        allow_ddl_set=True,
        default=False, compcoef=0.909,
        merge_fn=merge_readonly)

    # For non-derived pointers this is strongly correlated with
    # "expr" below.  Derived pointers might have "computable" set,
    # but expr=None.
    computable = so.SchemaField(
        bool,
        default=None,
        ephemeral=True,
    )

    # Computable pointers have this set to an expression
    # defining them.
    expr = so.SchemaField(
        s_expr.Expression,
        default=None, coerce=True, compcoef=0.909)

    default = so.SchemaField(
        s_expr.Expression,
        allow_ddl_set=True,
        default=None, coerce=True, compcoef=0.909)

    cardinality = so.SchemaField(
        qltypes.SchemaCardinality,
        default=None, compcoef=0.833, coerce=True,
        merge_fn=merge_cardinality)

    union_of = so.SchemaField(
        so.ObjectSet['Pointer'],
        default=None,
        coerce=True)

    intersection_of = so.SchemaField(
        so.ObjectSet['Pointer'],
        default=None,
        coerce=True)

    def is_tuple_indirection(self) -> bool:
        return False

    def is_type_intersection(self) -> bool:
        return False

    def get_displayname(self, schema: s_schema.Schema) -> str:
        sn = self.get_shortname(schema)
        if self.generic(schema):
            return sn
        else:
            return sn.name

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

    def material_type(self, schema: s_schema.Schema) -> Pointer:
        non_derived_parent = self.get_nearest_non_derived_parent(schema)
        if non_derived_parent.generic(schema):
            return self
        else:
            return non_derived_parent

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
        if t1 is t2:
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
        self,
        schema: s_schema.Schema,
        source: s_sources.Source,
        target: s_types.Type,
        *,
        derived_name_base: str = None,
        **kwargs: Any
    ) -> Tuple[s_schema.Schema, Pointer]:
        fqname = self.derive_name(
            schema, source, derived_name_base=derived_name_base)
        ptr = schema.get(fqname, default=None)

        if ptr is None:
            fqname = self.derive_name(
                schema, source, target.get_name(schema),
                derived_name_base=derived_name_base)
            ptr = schema.get(fqname, default=None)
            if ptr is None:
                schema, ptr = self.derive_ref(
                    schema, source, target=target,
                    derived_name_base=derived_name_base, **kwargs)
        assert isinstance(ptr, Pointer)
        return schema, ptr

    def get_derived_name_base(self, schema: s_schema.Schema) -> sn.Name:
        shortname = self.get_shortname(schema)
        return sn.Name(module='__', name=shortname.name)

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
        return bool(self.get_expr(schema))

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
            return (self.get_cardinality(schema) is
                    qltypes.SchemaCardinality.ONE)
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

    def has_user_defined_properties(self, schema: s_schema.Schema) -> bool:
        return False

    def allow_ref_propagation(
        self,
        schema: s_schema.Schema,
        constext: sd.CommandContext,
        refdict: so.RefDict,
    ) -> bool:
        object_type = self.get_source(schema)
        assert isinstance(object_type, s_types.Type)
        return not object_type.is_view(schema)


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

    def get_name(self, schema: s_schema.Schema) -> str:
        raise NotImplementedError

    def get_shortname(self, schema: s_schema.Schema) -> str:
        return self.get_name(schema)

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return self.get_name(schema)

    def has_user_defined_properties(self, schema: s_schema.Schema) -> bool:
        return False

    def get_required(self, schema: s_schema.Schema) -> bool:
        return True

    def get_cardinality(
        self,
        schema: s_schema.Schema
    ) -> qltypes.SchemaCardinality:
        raise NotImplementedError

    def get_path_id_name(self, schema: s_schema.Schema) -> str:
        return self.get_name(schema)

    def get_is_derived(self, schema: s_schema.Schema) -> bool:
        return False

    def get_is_local(self, schema: s_schema.Schema) -> bool:
        return True

    def get_union_of(
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

    def material_type(self, schema: s_schema.Schema) -> PseudoPointer:
        return self

    def is_pure_computable(self, schema: s_schema.Schema) -> bool:
        return False

    def is_exclusive(self, schema: s_schema.Schema) -> bool:
        return False


PointerLike = Union[Pointer, PseudoPointer]


class ComputableRef(so.Object):
    """A shell for a computed target type."""

    expr: qlast.Expr

    def __init__(self, expr: qlast.Base) -> None:
        super().__init__(_private_init=True)
        self.__dict__['expr'] = expr


class PointerCommandContext(sd.ObjectCommandContext[Pointer],
                            s_anno.AnnotationSubjectCommandContext):
    pass


class PointerCommandOrFragment(
    referencing.ReferencedObjectCommandBase[Pointer]
):

    def resolve_refs(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().resolve_refs(schema, context)
        target_ref = self.get_local_attribute_value('target')

        if target_ref is not None:
            srcctx = self.get_attribute_source_context('target')

            if isinstance(target_ref, s_types.TypeExprShell):
                cc_cmd = s_types.ensure_schema_type_expr_type(
                    schema,
                    target_ref,
                    parent_cmd=self,
                    src_context=srcctx,
                    context=context,
                )
                if cc_cmd is not None:
                    schema = cc_cmd.apply(schema, context)

            if isinstance(target_ref, s_types.TypeShell):
                try:
                    target = target_ref.resolve(schema)
                except errors.InvalidReferenceError as e:
                    refname = target_ref.get_refname(schema)
                    if refname is not None:
                        utils.enrich_schema_lookup_error(
                            e,
                            refname,
                            modaliases=context.modaliases,
                            schema=schema,
                            item_type=s_types.Type,
                            context=srcctx,
                        )
                    raise

            elif isinstance(target_ref, ComputableRef):
                schema, target_t, base = self._parse_computable(
                    target_ref.expr, schema, context)

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

                target = target_t

            else:
                target = target_ref

            self.set_attribute_value('target', target, source_context=srcctx)

        return schema

    def _parse_computable(
        self,
        expr: qlast.Base,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> Tuple[s_schema.Schema, s_types.Type, Optional[PointerLike]]:
        from edb.ir import ast as irast
        from edb.ir import typeutils as irtyputils
        from edb.schema import objtypes as s_objtypes

        # "source" attribute is set automatically as a refdict back-attr
        parent_ctx = self.get_referrer_context(context)
        assert parent_ctx is not None
        source_name = parent_ctx.op.classname

        source = schema.get(source_name, type=s_objtypes.ObjectType)
        expression = s_expr.Expression.compiled(
            s_expr.Expression.from_ast(expr, schema, context.modaliases),
            schema=schema,
            options=qlcompiler.CompilerOptions(
                modaliases=context.modaliases,
                anchors={qlast.Source().name: source},
                path_prefix_anchor=qlast.Source().name,
                singletons=frozenset([source]),
            ),
        )

        assert isinstance(expression.irast, irast.Statement)
        base = None
        target = expression.irast.stype

        result_expr = expression.irast.expr.expr

        if (isinstance(result_expr, irast.SelectStmt)
                and result_expr.result.rptr is not None):
            expr_rptr = result_expr.result.rptr
            while isinstance(expr_rptr, irast.TypeIntersectionPointer):
                expr_rptr = expr_rptr.source.rptr

            is_ptr_alias = (
                expr_rptr.direction is PointerDirection.Outbound
            )

            if is_ptr_alias:
                schema, base = irtyputils.ptrcls_from_ptrref(
                    expr_rptr.ptrref, schema=schema
                )

        self.set_attribute_value('expr', expression)
        required, card = expression.irast.cardinality.to_schema_value()
        spec_required = self.get_attribute_value('required')
        spec_card = self.get_attribute_value('cardinality')

        # If cardinality was unspecified and the computable is not
        # required, use the inferred cardinality.
        if spec_card is None and not spec_required:
            self.set_attribute_value('required', required)
            self.set_attribute_value('cardinality', card)
        else:
            # Otherwise honor the spec, so no cardinality change, but check
            # that it's valid.

            if spec_card is None:
                # A computable link is marked explicitly as
                # "required", so we assume that omitted cardinality is
                # "single". Basically, to infer the cardinality both
                # cardinality-related qualifiers need to be omitted.
                spec_card = qltypes.SchemaCardinality.ONE

            if spec_required and not required:
                ptr_name = sn.shortname_from_fullname(
                    self.get_attribute_value('name')).name
                srcctx = self.get_attribute_source_context('target')
                raise errors.QueryError(
                    f'possibly an empty set returned by an '
                    f'expression for a computable '
                    f'{ptr_name!r} '
                    f"declared as 'required'",
                    context=srcctx
                )
            if (spec_card is qltypes.SchemaCardinality.ONE
                    and card != spec_card):
                ptr_name = sn.shortname_from_fullname(
                    self.get_attribute_value('name')).name
                srcctx = self.get_attribute_source_context('target')
                raise errors.QueryError(
                    f'possibly more than one element returned by an '
                    f'expression for a computable '
                    f'{ptr_name!r} '
                    f"declared as 'single'",
                    context=srcctx
                )

        self.set_attribute_value('computable', True)

        return schema, target, base


class PointerCommand(
    referencing.ReferencedInheritingObjectCommand[Pointer],
    constraints.ConsistencySubjectCommand[Pointer],
    s_anno.AnnotationSubjectCommand,
    PointerCommandOrFragment,
):

    def _set_pointer_type(
        self,
        schema: s_schema.Schema,
        astnode: qlast.CreateConcretePointer,
        context: sd.CommandContext,
        target_ref: Union[so.Object, so.ObjectShell],
    ) -> None:
        return None

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
        if not scls.get_is_local(schema):
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

            if (ptr_cardinality is qltypes.SchemaCardinality.ONE
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
    ) -> sn.Name:
        referrer_ctx = cls.get_referrer_context(context)
        if referrer_ctx is not None:

            referrer_name = referrer_ctx.op.classname
            assert isinstance(referrer_name, sn.Name)

            shortname = sn.Name(
                module='__',
                name=astnode.name.name,
            )

            name = sn.Name(
                module=referrer_name.module,
                name=sn.get_specialized_name(
                    shortname,
                    referrer_name,
                ),
            )
        else:
            name = super()._classname_from_ast(schema, astnode, context)

        shortname = sn.shortname_from_fullname(name)
        if len(shortname.name) > s_def.MAX_NAME_LENGTH:
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
            self.set_attribute_value('required', astnode.is_required)

        if astnode.cardinality is not None:
            self.set_attribute_value('cardinality', astnode.cardinality)

        parent_ctx = self.get_referrer_context_or_die(context)
        source_name = parent_ctx.op.classname
        self.set_attribute_value('source', so.ObjectShell(name=source_name))

        # FIXME: this is an approximate solution
        targets = qlast.get_targets(astnode.target)
        target_ref: Union[None, s_types.TypeShell, ComputableRef]

        if len(targets) > 1:
            assert isinstance(source_name, sn.Name)

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
                target_ref = ComputableRef(
                    s_expr.imprint_expr_context(
                        target_expr,
                        context.modaliases,
                    )
                )
        else:
            # Target is inherited.
            target_ref = None

        if isinstance(target_ref, s_types.CollectionTypeShell):
            assert astnode.target is not None
            s_types.ensure_schema_collection(
                schema,
                target_ref,
                parent_cmd=self,
                src_context=astnode.target.context,
                context=context,
            )

        if isinstance(self, sd.CreateObject):
            assert astnode.target is not None
            self.set_attribute_value(
                'target',
                target_ref,
                source_context=astnode.target.context,
            )

            # If target is a computable ref defer cardinality
            # enforcement until the expression is parsed.
            if not isinstance(target_ref, ComputableRef):
                if self.get_attribute_value('cardinality') is None:
                    self.set_attribute_value(
                        'cardinality', qltypes.SchemaCardinality.ONE)

                if self.get_attribute_value('required') is None:
                    self.set_attribute_value(
                        'required', False)

        elif target_ref is not None:
            self._set_pointer_type(schema, astnode, context, target_ref)

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: s_expr.Expression,
    ) -> s_expr.Expression:
        from . import sources as s_sources

        if field.name in {'default', 'expr'}:
            singletons: List[s_types.Type] = []
            path_prefix_anchor = None
            anchors: Dict[str, Any] = {}

            if field.name == 'expr':
                # type ignore below, because the class is used as mixin
                parent_ctx = context.get_ancestor(
                    s_sources.SourceCommandContext,  # type: ignore
                    self
                )
                assert parent_ctx is not None
                source_name = parent_ctx.op.classname
                source = schema.get(source_name, default=None)
                anchors[qlast.Source().name] = source
                if not isinstance(source, Pointer):
                    assert source is not None
                    singletons = [source]
                    path_prefix_anchor = qlast.Source().name

            return type(value).compiled(
                value,
                schema=schema,
                options=qlcompiler.CompilerOptions(
                    modaliases=context.modaliases,
                    schema_object_context=self.get_schema_metaclass(),
                    anchors=anchors,
                    path_prefix_anchor=path_prefix_anchor,
                    singletons=frozenset(singletons),
                ),
            )
        else:
            return super().compile_expr_field(schema, context, field, value)

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        if context.descriptive_mode:
            # When generating AST for DESCRIBE AS TEXT, we want to
            # omit 'readonly' flag if it's inherited and it actually
            # has the default value.
            if op.property == 'readonly':
                pointer_obj = self.get_object(schema, context)
                field = type(pointer_obj).get_field('readonly')
                assert isinstance(field, so.SchemaField)
                dval = field.default

                if op.source == 'inheritance' and op.new_value is dval:
                    return

        super()._apply_field_ast(schema, context, node, op)


class SetPointerType(
        referencing.ReferencedInheritingObjectCommand[Pointer],
        inheriting.AlterInheritingObjectFragment[Pointer],
        PointerCommandOrFragment):

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_begin(schema, context)
        scls = self.scls

        context.altered_targets.add(scls)

        # Type alters of pointers used in expressions is prohibited.
        # Eventually we may be able to relax this by allowing to
        # alter to the type that is compatible (i.e. does not change)
        # with all expressions it is used in.
        vn = scls.get_verbosename(schema)
        self._prohibit_if_expr_refs(
            schema, context, action=f'alter the type of {vn}')

        if not context.canonical:
            implicit_bases = scls.get_implicit_bases(schema)
            non_altered_bases = []

            tgt = scls.get_target(schema)
            for base in set(implicit_bases) - context.altered_targets:
                assert tgt is not None
                base_tgt = base.get_target(schema)
                assert isinstance(base_tgt, so.SubclassableObject)
                if not tgt.issubclass(schema, base_tgt):
                    non_altered_bases.append(base)

            # This pointer is inherited from one or more ancestors that
            # are not altered in the same op, and this is an error.
            if non_altered_bases:
                bases_str = ', '.join(
                    b.get_verbosename(schema, with_parent=True)
                    for b in non_altered_bases
                )

                vn = scls.get_verbosename(schema)

                raise errors.SchemaDefinitionError(
                    f'cannot change the target type of inherited {vn}',
                    details=(
                        f'{vn} is inherited from '
                        f'{bases_str}'
                    ),
                    context=self.source_context,
                )

            if context.enable_recursion:
                tgt = self.get_attribute_value('target')

                def _set_type(
                    alter_cmd: sd.Command,
                    refname: Any
                ) -> None:
                    s_t = type(self)(
                        classname=alter_cmd.classname,
                    )
                    s_t.set_attribute_value('target', tgt)
                    alter_cmd.add(s_t)

                schema = self._propagate_ref_op(
                    schema, context, self.scls, cb=_set_type)

        else:
            for op in self.get_subcommands(type=sd.ObjectCommand):
                schema = op.apply(schema, context)

        return schema

    @classmethod
    def _cmd_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.ObjectCommand[Pointer]:
        return cls(classname=context.current().op.classname)

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

    cardinality = qltypes.SchemaCardinality.ONE
    for component in components:
        if component.get_cardinality(schema) is qltypes.SchemaCardinality.MANY:
            cardinality = qltypes.SchemaCardinality.MANY
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
        derived_name_base=sn.Name(
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

    cardinality = qltypes.SchemaCardinality.ONE
    for component in components:
        if component.get_cardinality(schema) is qltypes.SchemaCardinality.MANY:
            cardinality = qltypes.SchemaCardinality.MANY
            break

    metacls = type(components[0])
    default_base_name = metacls.get_default_base_name()
    assert default_base_name is not None
    genptr = schema.get(default_base_name, type=Pointer)

    schema, result = genptr.get_derived(
        schema,
        source,
        target,
        derived_name_base=sn.Name(
            module='__',
            name=ptrname),
        attrs={
            'intersection_of': so.ObjectSet.create(schema, components),
            'cardinality': cardinality,
        },
    )

    return schema, result
