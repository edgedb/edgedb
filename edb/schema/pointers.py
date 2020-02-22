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

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.common import enum

from edb import errors

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
    from . import sources as s_sources


class PointerDirection(enum.StrEnum):
    Outbound = '>'
    Inbound = '<'


MAX_NAME_LENGTH = 63


def merge_cardinality(target: Pointer, sources: List[so.Object],
                      field_name: str, *, schema: s_schema.Schema) -> object:
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


def merge_readonly(target: Pointer, sources: List[so.Object],
                   field_name: str, *, schema: s_schema.Schema) -> object:

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


def merge_target(ptr: Pointer, bases: List[so.Pointer],
                 field_name: str, *, schema) -> Pointer:

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
    # definining them.
    expr = so.SchemaField(
        s_expr.Expression,
        default=None, coerce=True, compcoef=0.909)

    default = so.SchemaField(
        s_expr.Expression,
        allow_ddl_set=True,
        default=None, coerce=True, compcoef=0.909)

    cardinality = so.SchemaField(
        qltypes.Cardinality,
        default=None, compcoef=0.833, coerce=True,
        merge_fn=merge_cardinality)

    union_of = so.SchemaField(
        so.ObjectSet,
        default=None,
        coerce=True)

    intersection_of = so.SchemaField(
        so.ObjectSet,
        default=None,
        coerce=True)

    def is_tuple_indirection(self):
        return False

    def is_type_intersection(self):
        return False

    def get_displayname(self, schema) -> str:
        sn = self.get_shortname(schema)
        if self.generic(schema):
            return sn
        else:
            return sn.name

    def get_verbosename(self, schema, *, with_parent: bool=False) -> str:
        is_abstract = self.generic(schema)
        vn = super().get_verbosename(schema)
        if is_abstract:
            return f'abstract {vn}'
        else:
            if with_parent:
                pvn = self.get_source(schema).get_verbosename(
                    schema, with_parent=True)
                return f'{vn} of {pvn}'
            else:
                return vn

    def is_scalar(self) -> bool:
        return False

    def material_type(self, schema):
        non_derived_parent = self.get_nearest_non_derived_parent(schema)
        if non_derived_parent.generic(schema):
            return self
        else:
            return non_derived_parent

    def get_near_endpoint(self, schema, direction):
        if direction == PointerDirection.Outbound:
            return self.get_source(schema)
        else:
            return self.get_target(schema)

    def get_far_endpoint(self, schema, direction):
        if direction == PointerDirection.Outbound:
            return self.get_target(schema)
        else:
            return self.get_source(schema)

    def set_target(self, schema, target):
        return self.set_field_value(schema, 'target', target)

    @classmethod
    def merge_targets(cls, schema, ptr, t1, t2, *, allow_contravariant=False):
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

    def get_derived(self, schema, source, target, *,
                    derived_name_base=None, **kwargs):
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
                    schema, source, target,
                    derived_name_base=derived_name_base, **kwargs)

        return schema, ptr

    def get_derived_name_base(self, schema):
        shortname = self.get_shortname(schema)
        return sn.Name(module='__', name=shortname.name)

    def derive_ref(
        self: referencing.ReferencedT,
        schema: s_schema.Schema,
        source: s_sources.Source,
        target: Optional[s_types.Type] = None,
        *qualifiers: str,
        mark_derived: bool = False,
        attrs: Optional[Mapping[str, Any]] = None,
        dctx: Optional[sd.CommandContext] = None,
        **kwargs: Any,
    ) -> Tuple[s_schema.Schema, referencing.ReferencedT]:

        if target is None:
            if attrs and 'target' in attrs:
                target = attrs['target']
            else:
                target = self.get_target(schema)

        if attrs is None:
            attrs = {}

        attrs['source'] = source
        attrs['target'] = target

        return super().derive_ref(
            schema, source, mark_derived=mark_derived,
            dctx=dctx, attrs=attrs, **kwargs)

    def is_pure_computable(self, schema):
        return bool(self.get_expr(schema))

    def is_id_pointer(self, schema):
        std_id = schema.get('std::Object').getptr(schema, 'id')
        std_target = schema.get('std::target')
        return self.issubclass(schema, (std_id, std_target))

    def is_endpoint_pointer(self, schema):
        std_source = schema.get('std::source')
        std_target = schema.get('std::target')
        return self.issubclass(schema, (std_source, std_target))

    def is_special_pointer(self, schema):
        return self.get_shortname(schema).name in {
            'source', 'target', 'id'
        }

    def is_property(self, schema):
        raise NotImplementedError

    def is_link_property(self, schema):
        raise NotImplementedError

    def is_protected_pointer(self, schema):
        return self.get_shortname(schema).name in {'id', '__type__'}

    def generic(self, schema):
        return self.get_source(schema) is None

    def get_referrer(self, schema):
        return self.get_source(schema)

    def is_exclusive(self, schema) -> bool:
        if self.generic(schema):
            raise ValueError(f'{self!r} is generic')

        exclusive = schema.get('std::exclusive')

        for constr in self.get_constraints(schema).objects(schema):
            if (constr.issubclass(schema, exclusive) and
                    not constr.get_subjectexpr(schema)):
                return True

        return False

    def singular(self, schema, direction=PointerDirection.Outbound):
        # Determine the cardinality of a given endpoint set.
        if direction == PointerDirection.Outbound:
            return self.get_cardinality(schema) is qltypes.Cardinality.ONE
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

    def has_user_defined_properties(self, schema):
        return False

    def allow_ref_propagation(
        self,
        schema: s_schema.Schema,
        constext: sd.CommandContext,
        refdict: so.RefDict,
    ) -> bool:
        return not self.get_source(schema).is_view(schema)


class PseudoPointer(s_abc.Pointer):
    # An abstract base class for pointer-like objects, i.e.
    # pseudo-links used by the compiler to represent things like
    # tuple and type intersection.
    def is_tuple_indirection(self):
        return False

    def is_type_intersection(self):
        return False

    def get_bases(self, schema):
        return so.ObjectList.create(schema, [])

    def get_ancestors(self, schema):
        return so.ObjectList.create(schema, [])

    def get_name(self, schema):
        raise NotImplementedError

    def get_shortname(self, schema):
        return self.get_name(schema)

    def get_displayname(self, schema):
        return self.get_name(schema)

    def has_user_defined_properties(self, schema):
        return False

    def get_required(self, schema):
        return True

    def get_cardinality(self, schema):
        raise NotImplementedError

    def get_path_id_name(self, schema):
        return self.get_name(schema)

    def get_is_derived(self, schema):
        return False

    def get_is_local(self, schema):
        return True

    def get_union_of(self, schema):
        return None

    def get_default(self, schema):
        return None

    def get_expr(self, schema):
        return None

    def get_source(self, schema) -> so.Object:
        raise NotImplementedError

    def get_target(self, schema) -> s_types.Type:
        raise NotImplementedError

    def get_near_endpoint(self, schema, direction):
        if direction is PointerDirection.Outbound:
            return self.get_source(schema)
        else:
            raise AssertionError(
                f'inbound direction is not valid for {type(self)}'
            )

    def get_far_endpoint(self, schema, direction):
        if direction is PointerDirection.Outbound:
            return self.get_target(schema)
        else:
            raise AssertionError(
                f'inbound direction is not valid for {type(self)}'
            )

    def is_link_property(self, schema):
        return False

    def generic(self, schema):
        return False

    def singular(self, schema, direction=PointerDirection.Outbound) -> bool:
        raise NotImplementedError

    def scalar(self):
        raise NotImplementedError

    def material_type(self, schema):
        return self

    def is_pure_computable(self, schema):
        return False


PointerLike = Union[Pointer, PseudoPointer]


class ComputableRef(so.Object):
    """A shell for a computed target type."""

    def __init__(self, expr: str) -> None:
        super().__init__(_private_init=True)
        self.__dict__['expr'] = expr


class PointerCommandContext(sd.ObjectCommandContext,
                            s_anno.AnnotationSubjectCommandContext):
    pass


class PointerCommandOrFragment:

    def _resolve_refs_in_pointer_def(self, schema, context):
        target_ref = self.get_local_attribute_value('target')

        if target_ref is not None:
            srcctx = self.get_attribute_source_context('target')

            if isinstance(target_ref, s_types.TypeExprRef):
                target = s_types.ensure_schema_type_expr_type(
                    schema, target_ref, parent_cmd=self,
                    src_context=srcctx, context=context,
                )

            elif isinstance(target_ref, so.ObjectRef):
                try:
                    target = target_ref._resolve_ref(schema)
                except errors.InvalidReferenceError as e:
                    utils.enrich_schema_lookup_error(
                        e, target_ref.get_refname(schema),
                        modaliases=context.modaliases,
                        schema=schema,
                        item_type=s_types.Type,
                        context=srcctx,
                    )
                    raise

            elif isinstance(target_ref, ComputableRef):
                target_t, base = self._parse_computable(
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

                target = utils.reduce_to_typeref(schema, target_t)

            elif isinstance(target_ref, s_types.Collection):
                srcctx = self.get_attribute_source_context('target')
                target = utils.resolve_typeref(target_ref, schema)
                s_types.ensure_schema_collection(
                    schema,
                    target,
                    parent_cmd=self,
                    src_context=srcctx,
                    context=context,
                )
            else:
                target = target_ref

            self.set_attribute_value('target', target, source_context=srcctx)

        return schema

    def _parse_computable(self, expr, schema, context) -> so.ObjectRef:
        from edb.ir import ast as irast
        from edb.ir import typeutils as irtyputils

        # "source" attribute is set automatically as a refdict back-attr
        parent_ctx = self.get_referrer_context(context)
        source_name = parent_ctx.op.classname

        source = schema.get(source_name)
        expr = s_expr.Expression.compiled(
            s_expr.Expression.from_ast(expr, schema, context.modaliases),
            schema=schema,
            modaliases=context.modaliases,
            anchors={qlast.Source: source},
            path_prefix_anchor=qlast.Source,
            singletons=[source],
        )

        base = None
        target = expr.irast.stype

        result_expr = expr.irast.expr.expr

        if (isinstance(result_expr, irast.SelectStmt)
                and result_expr.result.rptr is not None):
            expr_rptr = result_expr.result.rptr
            while isinstance(expr_rptr, irast.TypeIntersectionPointer):
                expr_rptr = expr_rptr.source.rptr

            is_ptr_alias = (
                expr_rptr.direction is PointerDirection.Outbound
            )

            if is_ptr_alias:
                base = irtyputils.ptrcls_from_ptrref(
                    expr_rptr.ptrref, schema=schema
                )

        self.set_attribute_value('expr', expr)
        self.set_attribute_value('cardinality', expr.irast.cardinality)
        self.set_attribute_value('computable', True)

        return target, base


class PointerCommand(
    referencing.ReferencedInheritingObjectCommand,
    constraints.ConsistencySubjectCommand,
    s_anno.AnnotationSubjectCommand,
    PointerCommandOrFragment,
):

    def _create_begin(self, schema, context):
        if not context.canonical:
            schema = self._resolve_refs_in_pointer_def(schema, context)

        schema = super()._create_begin(schema, context)

        if not context.canonical:
            self._validate_pointer_def(schema, context)
        return schema

    def _alter_begin(self, schema, context):
        if not context.canonical:
            schema = self._resolve_refs_in_pointer_def(schema, context)

        schema = super()._alter_begin(schema, context)
        if not context.canonical:
            self._validate_pointer_def(schema, context)
        return schema

    def _validate_pointer_def(self, schema, context):
        """Check that pointer definition is sound."""

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
            default_type = default_expr.irast.stype
            ptr_target = scls.get_target(schema)
            source_context = self.get_attribute_source_context('default')
            if not default_type.assignment_castable_to(ptr_target, schema):
                raise errors.SchemaDefinitionError(
                    f'default expression is of invalid type: '
                    f'{default_type.get_displayname(schema)}, '
                    f'expected {ptr_target.get_displayname(schema)}',
                    context=source_context,
                )

            ptr_cardinality = scls.get_cardinality(schema)
            default_cardinality = default_expr.irast.cardinality
            if (ptr_cardinality is qltypes.Cardinality.ONE
                    and default_cardinality is qltypes.Cardinality.MANY):
                raise errors.SchemaDefinitionError(
                    f'possibly more than one element returned by '
                    f'the default expression for '
                    f'{scls.get_verbosename(schema)} declared as '
                    f'\'single\'',
                    context=source_context,
                )

    @classmethod
    def _classname_from_ast(cls, schema, astnode, context):
        referrer_ctx = cls.get_referrer_context(context)
        if referrer_ctx is not None:

            referrer_name = referrer_ctx.op.classname

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
        if len(shortname.name) > MAX_NAME_LENGTH:
            raise errors.SchemaDefinitionError(
                f'link or property name length exceeds the maximum of '
                f'{MAX_NAME_LENGTH} characters',
                context=astnode.context)
        return name

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        referrer_ctx = cls.get_referrer_context(context)
        if referrer_ctx is not None:
            if getattr(astnode, 'declared_overloaded', False):
                cmd.set_attribute_value('declared_overloaded', True)
        return cmd

    def _process_create_or_alter_ast(self, schema, astnode, context):
        """Handle the CREATE {PROPERTY|LINK} AST node.

        This may be called in the context of either Create or Alter.
        """
        if astnode.is_required is not None:
            self.set_attribute_value('required', astnode.is_required)

        if astnode.cardinality is not None:
            self.set_attribute_value('cardinality', astnode.cardinality)

        parent_ctx = self.get_referrer_context(context)
        source_name = parent_ctx.op.classname
        self.set_attribute_value('source', so.ObjectRef(name=source_name))

        # FIXME: this is an approximate solution
        targets = qlast.get_targets(astnode.target)

        if len(targets) > 1:
            new_targets = [
                utils.ast_to_typeref(
                    t, modaliases=context.modaliases,
                    schema=schema, metaclass=s_types.Type)
                for t in targets
            ]

            target_ref = s_types.UnionTypeRef(
                new_targets, module=source_name.module)
        elif targets:
            target_expr = targets[0]
            if isinstance(target_expr, qlast.TypeName):
                target_ref = utils.ast_to_typeref(
                    target_expr, modaliases=context.modaliases, schema=schema,
                    metaclass=s_types.Type)
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

        if isinstance(self, sd.CreateObject):
            self.set_attribute_value(
                'target', target_ref, source_context=astnode.target.context)

            if self.get_attribute_value('cardinality') is None:
                self.set_attribute_value(
                    'cardinality', qltypes.Cardinality.ONE)

            if self.get_attribute_value('required') is None:
                self.set_attribute_value(
                    'required', False)
        elif target_ref is not None:
            self._set_pointer_type(schema, astnode, context, target_ref)

    @classmethod
    def _extract_union_operands(cls, expr, operands):
        if expr.op == 'UNION':
            cls._extract_union_operands(expr.op_larg, operands)
            cls._extract_union_operands(expr.op_rarg, operands)
        else:
            operands.append(expr)

    def compile_expr_field(self, schema, context, field, value):
        from . import sources as s_sources

        if field.name in {'default', 'expr'}:
            singletons = []
            path_prefix_anchor = None
            anchors = {}

            if field.name == 'expr':
                parent_ctx = context.get_ancestor(
                    s_sources.SourceCommandContext, self)
                source_name = parent_ctx.op.classname
                source = schema.get(source_name, default=None)
                anchors[qlast.Source] = source
                if not isinstance(source, Pointer):
                    singletons = [source]
                    path_prefix_anchor = qlast.Source

            return type(value).compiled(
                value,
                schema=schema,
                modaliases=context.modaliases,
                parent_object_type=self.get_schema_metaclass(),
                anchors=anchors,
                path_prefix_anchor=path_prefix_anchor,
                singletons=singletons,
            )
        else:
            return super().compile_expr_field(schema, context, field, value)

    def _apply_field_ast(self, schema, context, node, op):
        if context.descriptive_mode:
            # When generating AST for DESCRIBE AS TEXT, we want to
            # omit 'readonly' flag if it's inherited and it actually
            # has the default value.
            if op.property == 'readonly':
                pointer_obj = self.get_object(schema, context)
                field = type(pointer_obj).get_field('readonly')
                dval = field.default

                if op.source == 'inheritance' and op.new_value is dval:
                    return

        super()._apply_field_ast(schema, context, node, op)


class SetPointerType(
        referencing.ReferencedInheritingObjectCommand,
        inheriting.AlterInheritingObjectFragment,
        PointerCommandOrFragment):

    def _alter_begin(self, schema, context):
        if not context.canonical:
            schema = self._resolve_refs_in_pointer_def(schema, context)

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

            tgt = self.get_attribute_value('target')
            if tgt.is_collection():
                srcctx = self.get_attribute_source_context('target')
                s_types.ensure_schema_collection(
                    schema, tgt, self,
                    src_context=srcctx,
                    context=context,
                )

            for base in set(implicit_bases) - context.altered_targets:
                base_tgt = base.get_target(schema)
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

                def _set_type(alter_cmd, refname):
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
    def _cmd_from_ast(cls, schema, astnode, context):
        return cls(classname=context.current().op.classname)

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        targets = qlast.get_targets(astnode.type)

        if len(targets) > 1:
            new_targets = [
                utils.ast_to_typeref(
                    t, modaliases=context.modaliases,
                    schema=schema)
                for t in targets
            ]

            target_ref = s_types.UnionTypeRef(
                new_targets, module=cls.classname.module)
        else:
            target = targets[0]
            target_ref = utils.ast_to_typeref(
                target, modaliases=context.modaliases, schema=schema)

        cmd.set_attribute_value('target', target_ref)

        return cmd


def get_or_create_union_pointer(
    schema,
    ptrname: str,
    source,
    direction: PointerDirection,
    components: Iterable[Pointer], *,
    opaque: bool = False,
    modname: Optional[str] = None,
) -> Tuple[s_schema.Schema, Pointer]:
    from . import sources as s_sources

    components = list(components)

    if len(components) == 1 and direction is PointerDirection.Outbound:
        return schema, components[0]

    targets = [p.get_far_endpoint(schema, direction) for p in components]
    schema, target = utils.get_union_type(
        schema, targets, opaque=opaque, module=modname)

    cardinality = qltypes.Cardinality.ONE
    for component in components:
        if component.get_cardinality(schema) is qltypes.Cardinality.MANY:
            cardinality = qltypes.Cardinality.MANY
            break

    metacls = type(components[0])
    genptr = schema.get(metacls.get_default_base_name())

    if direction is PointerDirection.Inbound:
        source, target = target, source

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
        schema = s_sources.populate_pointer_set_for_source_union(
            schema,
            components,
            result,
            modname=modname,
        )

    return schema, result


def get_or_create_intersection_pointer(
    schema,
    ptrname: str,
    source,
    components: Iterable[Pointer], *,
    modname: Optional[str] = None,
) -> Tuple[s_schema.Schema, Pointer]:

    components = list(components)

    if len(components) == 1:
        return components[0]

    targets = [p.get_target(schema) for p in components]
    schema, target = utils.get_intersection_type(
        schema, targets, module=modname)

    cardinality = qltypes.Cardinality.ONE
    for component in components:
        if component.get_cardinality(schema) is qltypes.Cardinality.MANY:
            cardinality = qltypes.Cardinality.MANY
            break

    metacls = type(components[0])
    genptr = schema.get(metacls.get_default_base_name())

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
