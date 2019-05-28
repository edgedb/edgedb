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

import typing

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.common import enum

from edb import errors

from . import abc as s_abc
from . import annotations
from . import constraints
from . import delta as sd
from . import expr as s_expr
from . import name as sn
from . import objects as so
from . import referencing
from . import types as s_types
from . import utils


class PointerDirection(enum.StrEnum):
    Outbound = '>'
    Inbound = '<'


MAX_NAME_LENGTH = 63


def merge_cardinality(target: so.Object, sources: typing.List[so.Object],
                      field_name: str, *, schema) -> object:
    current = None
    current_from = None

    target_source = target.get_source(schema)

    for source in [target] + list(sources):
        nextval = source.get_explicit_field_value(schema, field_name, None)
        if nextval is not None:
            if current is None:
                current = nextval
                current_from = source
            elif current is not nextval:
                current_from_source = current_from.get_source(schema)
                source_source = source.get_source(schema)

                tgt_repr = (
                    f'{target_source.get_displayname(schema)}.'
                    f'{target.get_displayname(schema)}'
                )
                cf_repr = (
                    f'{current_from_source.get_displayname(schema)}.'
                    f'{current_from.get_displayname(schema)}'
                )
                other_repr = (
                    f'{source_source.get_displayname(schema)}.'
                    f'{source.get_displayname(schema)}'
                )

                raise errors.SchemaError(
                    f'cannot redefine the target cardinality of '
                    f'{tgt_repr!r}: it is defined '
                    f'as {current.as_ptr_qual()!r} in {cf_repr!r} and '
                    f'as {nextval.as_ptr_qual()!r} in {other_repr!r}.'
                )

    return current


def merge_target(ptr: Pointer, bases: typing.List[so.Pointer],
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
                schema, ptr, target, base_target)

    local_target = ptr.get_target(schema)
    if target is None:
        target = local_target
    elif local_target is not None:
        schema, target = Pointer.merge_targets(
            schema, ptr, target, local_target)

    return target


class PointerLike:
    # An abstract base class for pointer-like objects, which
    # include actual schema properties and links, as well as
    # pseudo-links used by the compiler to represent things like
    # tuple and type indirection.
    def is_tuple_indirection(self):
        return False

    def is_type_indirection(self):
        return False


class Pointer(referencing.ReferencedInheritingObject,
              constraints.ConsistencySubject,
              annotations.AnnotationSubject,
              PointerLike, s_abc.Pointer):

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
        merge_fn=utils.merge_sticky_bool)

    computable = so.SchemaField(
        bool,
        default=None, compcoef=0.909,
        merge_fn=utils.merge_weak_bool)

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
        if self.generic(schema):
            return self
        elif self.get_source(schema).is_scalar():
            return self
        else:
            source = self.get_source(schema)
            mptr = source.material_type(schema).getptr(
                schema, self.get_shortname(schema).name)
            if mptr is not None:
                return mptr
            else:
                return self

    def as_locally_defined(self, schema):
        if self.get_is_local(schema) or self.generic(schema):
            return [self]

        ancestors = []

        for a in self.get_ancestors(schema).objects(schema):
            if not a.generic(schema) and a.get_is_local(schema):
                ancestors.append(a)

        return utils.minimize_class_set_by_least_generic(schema, ancestors)

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
    def merge_targets(cls, schema, ptr, t1, t2):
        from . import objtypes as s_objtypes

        if t1 is t2:
            return schema, t1

        # When two pointers are merged, check target compatibility
        # and return a target that satisfies both specified targets.
        #

        source = ptr.get_source(schema)

        if (isinstance(t1, s_abc.ScalarType) !=
                isinstance(t2, s_abc.ScalarType)):
            # Targets are not of the same node type

            pn = ptr.get_shortname(schema)
            ccn1 = type(t1).__name__
            ccn2 = type(t2).__name__

            detail = (
                f'[{source.get_name(schema)}].[{pn}] '
                f'targets {ccn1} "{t1.get_name(schema)}"'
                f'while it also targets {ccn2} "{t2.get_name(schema)}"'
                'in other parent.'
            )

            raise errors.SchemaError(
                f'could not merge "{pn}" pointer: invalid ' +
                'target type mix', details=detail)

        elif isinstance(t1, s_abc.ScalarType):
            # Targets are both scalars
            if t1 != t2:
                vn = ptr.get_verbosename(schema, with_parent=True)
                raise errors.SchemaError(
                    f'could not merge {vn!r}: targets conflict',
                    details=f'{vn} targets scalar type '
                            f'{t1.get_displayname(schema)!r} while it also '
                            f'targets incompatible scalar type '
                            f'{t2.get_displayname(schema)!r} in a supertype.')

            return schema, t1

        else:
            # Targets are both objects
            t1_union_of = t1.get_union_of(schema)
            if t1_union_of:
                tt1 = tuple(t1_union_of.objects(schema))
            else:
                tt1 = (t1,)

            t2_union_of = t2.get_union_of(schema)
            if t2_union_of:
                tt2 = tuple(t2_union_of.objects(schema))
            else:
                tt2 = (t2,)

            new_targets = []

            if all(tgt2.issubclass(schema, tt1) for tgt2 in tt2):
                # The new target is a subclass of the current target, so
                # it is a more specific requirement.
                new_targets = tt2
            else:
                # The link is neither a subclass, nor a superclass
                # of the previously seen targets, which creates an
                # unresolvable target requirement conflict.
                vn = ptr.get_verbosename(schema, with_parent=True)
                raise errors.SchemaError(
                    f'could not merge {vn} pointer: targets conflict',
                    details=(
                        f'{vn} targets '
                        f'object {t2.get_verbosename(schema)!r} which '
                        f'is not related to any of targets found in '
                        f'other sources being merged: '
                        f'{t1.get_displayname(schema)!r}.'))

            if len(new_targets) > 1:
                schema, current_target = s_objtypes.get_or_create_union_type(
                    schema, new_targets, module=source.get_name(schema).module)
            else:
                current_target = new_targets[0]

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
                schema, ptr = self.derive(
                    schema, source, target,
                    derived_name_base=derived_name_base, **kwargs)

        return schema, ptr

    def get_derived_name_base(self, schema):
        shortname = self.get_shortname(schema)
        return sn.Name(module='__', name=shortname.name)

    def derive(self, schema, source,
               target=None,
               *qualifiers,
               mark_derived=False,
               attrs=None,
               dctx=None, **kwargs):

        if target is None:
            if attrs and 'target' in attrs:
                target = attrs['target']
            else:
                target = self.get_target(schema)

        if attrs is None:
            attrs = {}

        attrs['source'] = source
        attrs['target'] = target

        return super().derive(
            schema, source, mark_derived=mark_derived,
            dctx=dctx, attrs=attrs, **kwargs)

    def is_pure_computable(self, schema):
        return self.get_computable(schema) and bool(self.get_default(schema))

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


class PointerCommandContext(sd.ObjectCommandContext,
                            annotations.AnnotationSubjectCommandContext):
    pass


class PointerCommand(constraints.ConsistencySubjectCommand,
                     annotations.AnnotationSubjectCommand,
                     referencing.ReferencedInheritingObjectCommand):

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
            if getattr(astnode, 'declared_inherited', False):
                cmd.set_attribute_value('declared_inherited', True)
        return cmd

    @classmethod
    def _extract_union_operands(cls, expr, operands):
        if expr.op == 'UNION':
            cls._extract_union_operands(expr.op_larg, operands)
            cls._extract_union_operands(expr.op_rarg, operands)
        else:
            operands.append(expr)

    @classmethod
    def _parse_default(cls, cmd):
        return

    def nullify_expr_field(self, schema, context, field, op):
        from edb.edgeql.compiler import astutils as qlastutils

        if field.name == 'default':
            metaclass = self.get_schema_metaclass()
            target_ref = self.get_attribute_value('target')
            target = self._resolve_attr_value(
                target_ref, 'target',
                metaclass.get_field('target'), schema)

            target_ql = qlastutils.type_to_ql_typeref(target, schema=schema)

            empty = qlast.TypeCast(
                expr=qlast.Set(),
                type=target_ql,
            )

            op.new_value = s_expr.Expression.from_ast(
                empty, schema=schema, modaliases=context.modaliases,
            )

            return True
        else:
            super().nullify_expr_field(schema, context, field, op)

    def compile_expr_field(self, schema, context, field, value):
        from . import sources as s_sources

        if field.name == 'default':
            parent_ctx = context.get_ancestor(
                s_sources.SourceCommandContext, self)
            source_name = parent_ctx.op.classname
            source = schema.get(source_name, default=None)
            if not isinstance(source, Pointer):
                singletons = [source]
                path_prefix_anchor = qlast.Source
            else:
                singletons = []
                path_prefix_anchor = None

            return type(value).compiled(
                value,
                schema=schema,
                modaliases=context.modaliases,
                parent_object_type=self.get_schema_metaclass(),
                anchors={qlast.Source: source},
                path_prefix_anchor=path_prefix_anchor,
                singletons=singletons,
            )
        else:
            return super().compile_expr_field(schema, context, field, value)

    def _encode_default(self, schema, context, node, op):
        if op.new_value:
            expr = op.new_value
            if not isinstance(expr, s_expr.Expression):
                expr_t = qlast.SelectQuery(
                    result=qlast.BaseConstant.from_python(expr)
                )
                op.new_value = s_expr.Expression.from_ast(
                    expr_t, schema, context.modaliases,
                )
            super()._apply_field_ast(schema, context, node, op)

    def _parse_computable(self, expr, schema, context) -> so.ObjectRef:
        from edb.ir import ast as irast
        from edb.ir import typeutils as irtyputils
        from . import sources as s_sources

        # "source" attribute is set automatically as a refdict back-attr
        parent_ctx = context.get_ancestor(s_sources.SourceCommandContext, self)
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
        target = utils.reduce_to_typeref(schema, expr.irast.stype)

        result_expr = expr.irast.expr.expr

        if (isinstance(result_expr, irast.SelectStmt)
                and result_expr.result.rptr is not None):
            expr_rptr = result_expr.result.rptr
            while isinstance(expr_rptr, irast.TypeIndirectionPointer):
                expr_rptr = expr_rptr.source.rptr

            is_ptr_alias = (
                expr_rptr.direction is PointerDirection.Outbound
            )

            if is_ptr_alias:
                base = irtyputils.ptrcls_from_ptrref(
                    expr_rptr.ptrref, schema=schema
                )

        self.add(
            sd.AlterObjectProperty(
                property='default',
                new_value=expr,
            )
        )

        self.add(
            sd.AlterObjectProperty(
                property='computable',
                new_value=True
            )
        )

        self.add(
            sd.AlterObjectProperty(
                property='cardinality',
                new_value=expr.irast.cardinality
            )
        )

        return target, base


def get_or_create_union_pointer(
        schema,
        ptrname: str,
        source,
        direction: PointerDirection,
        components: typing.Iterable[Pointer], *,
        modname: typing.Optional[str]=None) -> Pointer:

    targets = [p.get_far_endpoint(schema, direction) for p in components]
    schema, target = utils.get_union_type(
        schema, targets, opaque=False, module=modname)

    cardinality = qltypes.Cardinality.ONE
    for component in components:
        if component.get_cardinality(schema) is qltypes.Cardinality.MANY:
            cardinality = qltypes.Cardinality.MANY
            break

    components = list(components)
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

    return schema, result
