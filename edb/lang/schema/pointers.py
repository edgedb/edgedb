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

from edb import errors

from . import abc as s_abc
from . import attributes
from . import constraints
from . import delta as sd
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
            else:
                if current is not nextval:
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


class PointerLike:
    # An abstract base class for pointer-like objects, which
    # include actual schema properties and links, as well as
    # pseudo-links used by the compiler to represent things like
    # tuple and type indirection.
    pass


class Pointer(constraints.ConsistencySubject, attributes.AttributeSubject,
              PointerLike):

    source = so.SchemaField(
        so.Object,
        default=None, compcoef=None)

    target = so.SchemaField(
        s_types.Type,
        default=None, compcoef=0.833)

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
        sexpr.ExpressionText,
        allow_ddl_set=True,
        default=None, coerce=True, compcoef=0.909)

    cardinality = so.SchemaField(
        qlast.Cardinality,
        default=None, compcoef=0.833, coerce=True,
        merge_fn=merge_cardinality)

    def get_displayname(self, schema) -> str:
        return self.get_shortname(schema).name

    def material_type(self, schema):
        if self.generic(schema):
            raise ValueError(f'{self!r} is generic')

        source = self.get_source(schema)
        return source.material_type(schema).getptr(
            schema, self.get_shortname(schema).name)

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

    def create_common_target(self, schema, targets, minimize_by=False):
        schema, target = inheriting.create_virtual_parent(
            schema, targets, module_name=self.get_name(schema).module,
            minimize_by=minimize_by)

        schema = target.set_field_value(schema, 'is_derived', True)

        return schema, target

    def finalize(self, schema, bases=None, *, apply_defaults=True, dctx=None):
        schema = super().finalize(
            schema, bases=bases, apply_defaults=apply_defaults,
            dctx=dctx)

        if not self.generic(schema) and apply_defaults:
            if self.get_cardinality(schema) is None:
                schema = self.set_field_value(
                    schema, 'cardinality', qlast.Cardinality.ONE)

                if dctx is not None:
                    from . import delta as sd

                    dctx.current().op.add(sd.AlterObjectProperty(
                        property='cardinality',
                        new_value=self.get_cardinality(schema),
                        source='default'
                    ))

        return schema

    @classmethod
    def merge_targets(cls, schema, ptr, t1, t2):
        from . import objtypes as s_objtypes

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
                pn = ptr.get_shortname(schema)
                raise errors.SchemaError(
                    f'could not merge {pn!r} pointer: targets conflict',
                    details=f'({source.get_name(schema)}).({pn}) '
                            f'targets scalar type {t1.get_name(schema)!r} '
                            f'while it also targets incompatible scalar type '
                            f'{t2.get_name(schema)!r} in other parent.')

            return schema, t1

        else:
            # Targets are both objects
            if t1.get_is_virtual(schema):
                tt1 = tuple(t1.children(schema))
            else:
                tt1 = (t1,)

            if t2.get_is_virtual(schema):
                tt2 = tuple(t2.children(schema))
            else:
                tt2 = (t2,)

            new_targets = []

            for tgt2 in tt2:
                if all(tgt2.issubclass(schema, tgt1) for tgt1 in tt1):
                    # This target is a subclass of the current target, so
                    # it is a more specific requirement.
                    new_targets.append(tgt2)
                elif all(tgt1.issubclass(schema, tgt2) for tgt1 in tt1):
                    # Current target is a subclass of this target, no need to
                    # do anything here.
                    pass
                else:
                    # The link is neither a subclass, nor a superclass
                    # of the previously seen targets, which creates an
                    # unresolvable target requirement conflict.
                    pn = ptr.get_displayname(schema)
                    raise errors.SchemaError(
                        f'could not merge {pn!r} pointer: targets conflict',
                        details=f'{source.get_name(schema)}.{pn} targets '
                                f'object {t2.get_name(schema)!r} which '
                                f'is not related to any of targets found in '
                                f'other sources being merged: '
                                f'{t1.get_name(schema)!r}.')

            for tgt1 in tt1:
                if not any(tgt2.issubclass(schema, tgt1) for tgt2 in tt2):
                    new_targets.append(tgt1)

            if len(new_targets) > 1:
                tnames = (t.get_name(schema) for t in new_targets)
                module = source.get_name(schema).module
                parent_name = s_objtypes.ObjectType.gen_virt_parent_name(
                    tnames, module)
                schema, current_target = \
                    s_objtypes.ObjectType.create_in_schema(
                        schema,
                        name=parent_name, is_abstract=True, is_virtual=True)
                schema = schema.update_virtual_inheritance(
                    current_target, new_targets)
            else:
                current_target = new_targets[0]

            return schema, current_target

    def get_derived(self, schema, source, target, **kwargs):
        fqname = self.derive_name(schema, source)
        ptr = schema.get(fqname, default=None)
        if ptr is not None:
            if ptr.get_target(schema) != target:
                ptr = None

        if ptr is None:
            fqname = self.derive_name(schema, source, target.get_name(schema))
            ptr = schema.get(fqname, default=None)
            if ptr is None:
                if self.generic(schema):
                    schema, ptr = self.derive(schema, source, target, **kwargs)
                else:
                    schema, ptr = self.derive_copy(
                        schema, source, target, **kwargs)

        return schema, ptr

    def get_derived_name(self, schema, source, target, *qualifiers,
                         mark_derived=False):
        if mark_derived:
            fqname = self.derive_name(schema, source, target.get_name(schema))
        else:
            fqname = self.derive_name(schema, source)

        return fqname

    def init_derived(self, schema, source, *qualifiers,
                     as_copy, mark_derived=False,
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
                target = self.get_target(schema)

            if merge_bases:
                for base in merge_bases:
                    if target is None:
                        target = base.get_target(schema)
                    else:
                        schema, target = self.merge_targets(
                            schema, self, target, base.get_target(schema))

        if attrs is None:
            attrs = {}

        attrs['source'] = source
        attrs['target'] = target

        return super().init_derived(
            schema, source, target, as_copy=as_copy, mark_derived=mark_derived,
            dctx=dctx, merge_bases=merge_bases, attrs=attrs, **kwargs)

    def is_pure_computable(self, schema):
        return self.get_computable(schema) and bool(self.get_default(schema))

    def is_id_pointer(self, schema):
        return self.get_shortname(schema) in {'std::target', 'std::id'}

    def is_endpoint_pointer(self, schema):
        return self.get_shortname(schema) in {'std::source', 'std::target'}

    def is_special_pointer(self, schema):
        return self.get_shortname(schema) in {
            'std::source', 'std::target', 'std::id'
        }

    def is_property(self, schema):
        raise NotImplementedError

    def is_protected_pointer(self, schema):
        return (self.is_special_pointer(schema) or
                self.get_shortname(schema) in {'std::__type__'})

    def generic(self, schema):
        return self.get_source(schema) is None

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
            return self.get_cardinality(schema) is qlast.Cardinality.ONE
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
                     attributes.AttributeSubjectCommand,
                     referencing.ReferencedInheritingObjectCommand):

    @classmethod
    def _classname_from_ast(cls, schema, astnode, context):
        name = super()._classname_from_ast(schema, astnode, context)
        shortname = sn.shortname_from_fullname(name)
        if len(shortname.name) > MAX_NAME_LENGTH:
            raise errors.SchemaDefinitionError(
                f'link or property name length exceeds the maximum of '
                f'{MAX_NAME_LENGTH} characters',
                context=astnode.context)
        return name

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

    def _encode_default(self, schema, context, node, op):
        if op.new_value:
            expr = op.new_value
            if not isinstance(expr, sexpr.ExpressionText):
                expr_t = qlast.SelectQuery(
                    result=qlast.BaseConstant.from_python(expr)
                )
                expr = edgeql.generate_source(expr_t, pretty=False)

                op.new_value = sexpr.ExpressionText(expr)
            super()._apply_field_ast(schema, context, node, op)

    def _create_begin(self, schema, context):
        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is not None:
            # This is a specialized pointer, check that appropriate
            # generic parent exists, and if not, create it.
            bases = self.get_attribute_value('bases')
            if not isinstance(bases, so.ObjectList):
                bases = so.ObjectList.create(schema, bases)

            try:
                base = next(iter(bases.objects(schema)))
            except errors.InvalidReferenceError:
                base = None
                base_name = sn.shortname_from_fullname(self.classname)

            if base is None:
                cls = self.get_schema_metaclass()
                std_ptr = schema.get(cls.get_default_base_name())
                schema, base = cls.create_in_schema_with_inheritance(
                    schema, name=base_name, bases=[std_ptr])
                delta = base.delta(None, base,
                                   old_schema=None,
                                   new_schema=schema)
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
        from . import sources as s_sources

        # "source" attribute is set automatically as a refdict back-attr
        parent_ctx = context.get(s_sources.SourceCommandContext)
        source_name = parent_ctx.op.classname

        source = schema.get(source_name, default=None)
        if source is None:
            raise errors.SchemaDefinitionError(
                f'cannot define link/property computables in CREATE TYPE',
                hint='Perform a CREATE TYPE without the link '
                     'followed by ALTER TYPE defining the computable',
                context=expr.context
            )

        ir, _, target_expr = ql_utils.normalize_tree(
            expr, schema, anchors={qlast.Source: source}, singletons=[source])

        target = utils.reduce_to_typeref(schema, ir.stype)

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

        self.add(
            sd.AlterObjectProperty(
                property='cardinality',
                new_value=ir.cardinality
            )
        )

        return target
