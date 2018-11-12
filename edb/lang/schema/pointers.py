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


import typing

from edb.lang import edgeql
from edb.lang.edgeql import ast as qlast
from edb.lang.common import enum

from . import constraints
from . import delta as sd
from . import error as schema_error
from . import expr as sexpr
from . import inheriting
from . import name as sn
from . import objects as so
from . import referencing
from . import types as s_types
from . import utils


class PointerDirection(enum.StrEnum):
    Outbound = '>'
    Inbound = '<'


Cardinality = qlast.Cardinality


def merge_cardinality(target: so.Object, sources: typing.List[so.Object],
                      field_name: str, *, schema) -> object:
    current = None
    current_from = None

    for source in [target] + list(sources):
        nextval = getattr(source, field_name)
        if nextval is not None:
            if current is None:
                current = nextval
                current_from = source
            else:
                if current is not nextval:
                    tgt_repr = (f'{target.source.displayname}.'
                                f'{target.displayname}')
                    cf_repr = (f'{current_from.source.displayname}.'
                               f'{current_from.displayname}')
                    other_repr = (f'{source.source.displayname}.'
                                  f'{source.displayname}')

                    raise schema_error.SchemaError(
                        f'cannot redefine the target cardinality of '
                        f'{tgt_repr!r}: it is defined '
                        f'as {current.as_ptr_qual()!r} in {cf_repr!r} and '
                        f'as {nextval.as_ptr_qual()!r} in {other_repr!r}.'
                    )

        return current


class Pointer(constraints.ConsistencySubject):

    source = so.Field(so.Object, None, compcoef=None)
    target = so.Field(s_types.Type, None, compcoef=0.833)

    required = so.Field(bool, default=False, compcoef=0.909,
                        merge_fn=utils.merge_sticky_bool)
    readonly = so.Field(bool, default=False, compcoef=0.909,
                        merge_fn=utils.merge_sticky_bool)
    computable = so.Field(bool, default=None, compcoef=0.909,
                          merge_fn=utils.merge_weak_bool)
    default = so.Field(sexpr.ExpressionText, default=None,
                       coerce=True, compcoef=0.909)
    cardinality = so.Field(qlast.Cardinality, default=None,
                           compcoef=0.833, coerce=True,
                           merge_fn=merge_cardinality)

    @property
    def displayname(self) -> str:
        return self.shortname.name

    def material_type(self, schema):
        if self.generic():
            raise ValueError(f'{self!r} is generic')

        return self.source.material_type(schema).getptr(schema, self.shortname)

    def get_near_endpoint(self, direction):
        return (self.source if direction == PointerDirection.Outbound
                else self.target)

    def get_far_endpoint(self, direction):
        return (self.target if direction == PointerDirection.Outbound
                else self.source)

    def get_common_target(self, schema, targets, minimize_by=None):
        return inheriting.create_virtual_parent(
            schema, targets, module_name=self.name.module,
            minimize_by=minimize_by)

    def create_common_target(self, schema, targets, minimize_by=False):
        target = self.get_common_target(schema, targets,
                                        minimize_by=minimize_by)
        if not schema.get(target.name, default=None):
            target.is_derived = True
            schema = schema.add(target)
        return schema, target

    def finalize(self, schema, bases=None, *, apply_defaults=True, dctx=None):
        schema = super().finalize(
            schema, bases=bases, apply_defaults=apply_defaults,
            dctx=dctx)

        if not self.generic() and apply_defaults:
            if self.cardinality is None:
                self.cardinality = qlast.Cardinality.ONE

                if dctx is not None:
                    from . import delta as sd

                    dctx.current().op.add(sd.AlterObjectProperty(
                        property='cardinality',
                        new_value=self.cardinality,
                        source='default'
                    ))

        return schema

    @classmethod
    def merge_targets(cls, schema, ptr, t1, t2):
        from . import scalars as s_scalars, objtypes as s_objtypes

        # When two pointers are merged, check target compatibility
        # and return a target that satisfies both specified targets.
        #

        if (isinstance(t1, s_scalars.ScalarType) !=
                isinstance(t2, s_scalars.ScalarType)):
            # Targets are not of the same node type

            pn = ptr.shortname
            ccn1 = t1.get_canonical_class().__name__
            ccn2 = t2.get_canonical_class().__name__

            detail = (f'[{ptr.source.name}].[{pn}] targets {ccn1} "{t1.name}"'
                      f'while it also targets {ccn2} "{t2.name}"'
                      'in other parent.')

            raise schema_error.SchemaError(
                f'could not merge "{pn}" pointer: invalid ' +
                'target type mix', details=detail)

        elif isinstance(t1, s_scalars.ScalarType):
            # Targets are both scalars
            if t1 != t2:
                pn = ptr.shortname
                raise schema_error.SchemaError(
                    f'could not merge {pn!r} pointer: targets conflict',
                    details=f'({ptr.source.name}).({pn}) targets scalar type'
                            f'{t1.name!r} while it also targets incompatible'
                            f'scalar type {t2.name!r} in other parent.')

            return schema, t1

        else:
            # Targets are both objects
            if t1.is_virtual:
                tt1 = tuple(t1.children(schema))
            else:
                tt1 = (t1,)

            if t2.is_virtual:
                tt2 = tuple(t2.children(schema))
            else:
                tt2 = (t2,)

            new_targets = []

            for tgt2 in tt2:
                if all(tgt2.issubclass(tgt1) for tgt1 in tt1):
                    # This target is a subclass of the current target, so
                    # it is a more specific requirement.
                    new_targets.append(tgt2)
                elif all(tgt1.issubclass(tgt2) for tgt1 in tt1):
                    # Current target is a subclass of this target, no need to
                    # do anything here.
                    pass
                else:
                    # The link is neither a subclass, nor a superclass
                    # of the previously seen targets, which creates an
                    # unresolvable target requirement conflict.
                    pn = ptr.shortname
                    raise schema_error.SchemaError(
                        f'could not merge {pn!r} pointer: targets conflict',
                        details=f'({ptr.source.name}).({pn}) targets object'
                                f' {t2.name!r} which is not related to any of'
                                f' targets found in other sources being'
                                f' merged: {t1.name!r}.')

            for tgt1 in tt1:
                if not any(tgt2.issubclass(tgt1) for tgt2 in tt2):
                    new_targets.append(tgt1)

            if len(new_targets) > 1:
                tnames = (t.name for t in new_targets)
                module = ptr.source.name.module
                parent_name = s_objtypes.ObjectType.gen_virt_parent_name(
                    tnames, module)
                current_target = s_objtypes.ObjectType(
                    name=parent_name, is_abstract=True, is_virtual=True)
                schema = schema.update_virtual_inheritance(
                    current_target, new_targets)
            else:
                current_target = new_targets[0]

            return schema, current_target

    def get_derived(self, schema, source, target, **kwargs):
        fqname = self.derive_name(source)
        ptr = schema.get(fqname, default=None)
        if ptr is not None:
            if ptr.target != target:
                ptr = None

        if ptr is None:
            fqname = self.derive_name(source, target.name)
            ptr = schema.get(fqname, default=None)
            if ptr is None:
                if self.generic():
                    schema, ptr = self.derive(schema, source, target, **kwargs)
                else:
                    schema, ptr = self.derive_copy(
                        schema, source, target, **kwargs)

        return schema, ptr

    def get_derived_name(self, source, target, *qualifiers,
                         mark_derived=False):
        if mark_derived:
            fqname = self.derive_name(source, target.name)
        else:
            fqname = self.derive_name(source)

        return fqname

    def init_derived(self, schema, source, *qualifiers,
                     as_copy, mark_derived=False, add_to_schema=False,
                     merge_bases=None, attrs=None,
                     dctx=None, **kwargs):

        if qualifiers:
            target = qualifiers[0]
        else:
            target = None

        if target is None:
            if attrs and 'target' in attrs:
                target = attrs['target']
            else:
                target = self.target

            if merge_bases:
                for base in merge_bases:
                    if target is None:
                        target = base.target
                    else:
                        schema, target = self.merge_targets(
                            schema, self, target, base.target)

        if attrs is None:
            attrs = {}

        attrs['source'] = source
        attrs['target'] = target

        return super().init_derived(
            schema, source, target, as_copy=as_copy, mark_derived=mark_derived,
            add_to_schema=add_to_schema, dctx=dctx, merge_bases=merge_bases,
            attrs=attrs, **kwargs)

    def is_pure_computable(self):
        return self.computable and bool(self.default)

    def is_id_pointer(self):
        return self.shortname in {'std::target', 'std::id'}

    def is_endpoint_pointer(self):
        return self.shortname in {'std::source', 'std::target'}

    def is_special_pointer(self):
        return self.shortname in {'std::source', 'std::target', 'std::id'}

    def is_protected_pointer(self):
        return self.is_special_pointer() or self.shortname in {'std::__type__'}

    def generic(self):
        return self.source is None

    def is_exclusive(self, schema):
        if self.generic():
            raise ValueError(f'{self!r} is generic')

        return 'std::exclusive' in self.get_constraints(schema)

    def singular(self, schema, direction=PointerDirection.Outbound):
        # Determine the cardinality of a given endpoint set.
        if direction == PointerDirection.Outbound:
            return self.cardinality is qlast.Cardinality.ONE
        else:
            return self.is_exclusive(schema)


class PointerVector(sn.Name):
    __slots__ = ('module', 'name', 'direction', 'target', 'is_linkprop')

    def __new__(cls, name, module=None, direction=PointerDirection.Outbound,
                target=None, is_linkprop=False):
        result = super().__new__(cls, name, module=module)
        result.direction = direction
        result.target = target
        result.is_linkprop = is_linkprop
        return result

    def __repr__(self):
        return '<edb.schema.PointerVector {}>'.format(self)

    def __hash__(self):
        if self.direction == PointerDirection.Outbound:
            return super().__hash__()
        else:
            return hash((str(self), self.direction))

    def __eq__(self, other):
        if isinstance(other, PointerVector):
            return (str(self) == str(other) and
                    self.direction == other.direction)
        elif isinstance(other, str):
            return (str(self) == other and
                    self.direction == PointerDirection.Outbound)
        else:
            return False


class PointerCommandContext(sd.ObjectCommandContext):
    pass


class PointerCommand(constraints.ConsistencySubjectCommand,
                     referencing.ReferencedInheritingObjectCommand):

    @classmethod
    def _extract_union_operands(cls, expr, operands):
        if expr.op == qlast.UNION:
            cls._extract_union_operands(expr.op_larg, operands)
            cls._extract_union_operands(expr.op_rarg, operands)
        else:
            operands.append(expr)

    @classmethod
    def _parse_default(cls, cmd):
        return

    def _encode_default(self, context, node, op):
        if op.new_value:
            expr = op.new_value
            if not isinstance(expr, sexpr.ExpressionText):
                expr_t = qlast.SelectQuery(
                    result=qlast.BaseConstant.from_python(expr)
                )
                expr = edgeql.generate_source(expr_t, pretty=False)

                op.new_value = sexpr.ExpressionText(expr)
            super()._apply_field_ast(context, node, op)

    def _create_begin(self, schema, context):
        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is not None:
            # This is a specialized pointer, check that appropriate
            # generic parent exists, and if not, create it.
            base_ref = self.get_attribute_value('bases')[0]
            base_name = base_ref.classname
            base = schema.get(base_name, default=None)
            if base is None:
                cls = self.get_schema_metaclass()
                std_link = schema.get(cls.get_default_base_name())
                base = cls(name=base_name, bases=[std_link])
                delta = base.delta(None, base,
                                   old_schema=None,
                                   new_schema=schema)
                schema, _ = delta.apply(schema, context=context.at_top())
                top_ctx = referrer_ctx
                refref_cls = getattr(
                    top_ctx.op, 'referrer_context_class', None)
                if refref_cls is not None:
                    refref_ctx = context.get(refref_cls)
                    if refref_ctx is not None:
                        top_ctx = refref_ctx

                top_ctx.op.after(delta)

        return super()._create_begin(schema, context)

    def _parse_computable(self, expr, schema, context) -> so.ObjectRef:
        from edb.lang.edgeql import utils as ql_utils
        from edb.lang.ir import ast as irast
        from edb.lang.ir import inference as ir_inference
        from edb.lang.ir import utils as ir_utils
        from . import sources as s_sources

        # "source" attribute is set automatically as a refdict back-attr
        parent_ctx = context.get(s_sources.SourceCommandContext)
        source_name = parent_ctx.op.classname
        target_type = None

        source = schema.get(source_name, default=None)
        if source is None:
            raise schema_error.SchemaDefinitionError(
                f'cannot define link/property computables in CREATE TYPE',
                hint='Perform a CREATE TYPE without the link '
                     'followed by ALTER TYPE defining the computable',
                context=expr.context
            )

        ir, _, target_expr = ql_utils.normalize_tree(
            expr, schema, anchors={qlast.Source: source})

        try:
            target_type = ir_utils.infer_type(ir, schema)
        except edgeql.EdgeQLError as e:
            raise schema_error.SchemaDefinitionError(
                'could not determine the result type of '
                'computable expression',
                context=target_expr.context) from e

        target = utils.reduce_to_typeref(target_type)

        self.add(
            sd.AlterObjectProperty(
                property='default',
                new_value=target_expr
            )
        )

        self.add(
            sd.AlterObjectProperty(
                property='computable',
                new_value=True
            )
        )

        scope_tree = irast.new_scope_tree()
        scope_tree.attach_path(irast.PathId(source))
        cardinality = ir_inference.infer_cardinality(
            ir, scope_tree.attach_fence(), schema)

        self.add(
            sd.AlterObjectProperty(
                property='cardinality',
                new_value=cardinality
            )
        )

        return target
