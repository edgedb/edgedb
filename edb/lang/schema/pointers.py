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


import functools

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
from . import policy
from . import referencing
from . import types as s_types
from . import utils


class PointerDirection(enum.StrEnum):
    Outbound = '>'
    Inbound = '<'


class PointerCardinality(enum.StrEnum):
    OneToOne = '11'
    OneToMany = '1*'
    ManyToOne = '*1'
    ManyToMany = '**'

    def __and__(self, other):
        if not isinstance(other, PointerCardinality):
            return NotImplemented

        if self == PointerCardinality.OneToOne:
            return self
        elif other == PointerCardinality.OneToOne:
            return other
        elif self == PointerCardinality.OneToMany:
            if other == PointerCardinality.ManyToOne:
                err = 'mappings %r and %r are mutually incompatible'
                raise ValueError(err % (self, other))
            return self
        elif self == PointerCardinality.ManyToOne:
            if other == PointerCardinality.OneToMany:
                err = 'mappings %r and %r are mutually incompatible'
                raise ValueError(err % (self, other))
            return self
        else:
            return other

    def __or__(self, other):
        if not isinstance(other, PointerCardinality):
            return NotImplemented
        # We use the fact that '*' is less than '1'
        return self.__class__(min(self[0], other[0]) + min(self[1], other[1]))

    @classmethod
    def merge_values(cls, target, sources, field_name, *, schema):
        def f(values):
            return functools.reduce(lambda a, b: a & b, values)
        return utils.merge_reduce(target, sources, field_name,
                                  schema=schema, f=f)


class Pointer(constraints.ConsistencySubject,
              policy.PolicySubject, policy.InternalPolicySubject):

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
    cardinality = so.Field(PointerCardinality, default=None,
                           compcoef=0.833, coerce=True)

    @property
    def displayname(self) -> str:
        return self.shortname.name

    def material_type(self):
        if self.generic():
            raise ValueError(f'{self!r} is generic')

        return self.source.material_type().pointers.get(self.shortname)

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
            schema.add(target)
        return target

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

            return t1

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
                schema.update_virtual_inheritance(current_target, new_targets)
            else:
                current_target = new_targets[0]

            return current_target

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
                    ptr = self.derive(schema, source, target, **kwargs)
                else:
                    ptr = self.derive_copy(schema, source, target, **kwargs)

        return ptr

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
                        target = self.merge_targets(schema, self, target,
                                                    base.target)

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

    def singular(self, direction=PointerDirection.Outbound):
        if direction == PointerDirection.Outbound:
            return self.cardinality in \
                (PointerCardinality.OneToOne, PointerCardinality.ManyToOne)
        else:
            return self.cardinality in \
                (PointerCardinality.OneToOne, PointerCardinality.OneToMany)

    def merge_defaults(self, other):
        if not self.default:
            if other.default:
                self.default = other.default

    def normalize_defaults(self):
        pass


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

    def __mm_serialize__(self):
        return dict(
            name=str(self),
            direction=self.direction,
            target=self.target,
            is_linkprop=self.is_linkprop,
        )

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
                delta = base.delta(None)
                delta.apply(schema, context=context.at_top())
                top_ctx = referrer_ctx
                refref_cls = getattr(
                    top_ctx.op, 'referrer_context_class', None)
                if refref_cls is not None:
                    refref_ctx = context.get(refref_cls)
                    if refref_ctx is not None:
                        top_ctx = refref_ctx

                top_ctx.op.after(delta)

        super()._create_begin(schema, context)
