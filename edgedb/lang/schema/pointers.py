##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import datastructures as ds

from edgedb.lang import caosql
from edgedb.lang.caosql import ast as qlast

from . import constraints
from . import delta as sd
from . import derivable
from edgedb.lang.common import enum
from . import error as schema_error
from . import expr as sexpr
from . import name as sn
from . import objects as so
from . import policy
from . import primary
from . import sources
from . import utils


class PointerDirection(enum.StrEnum):
    Outbound = '>'
    Inbound = '<'


class PointerLoading(enum.StrEnum):
    Eager = 'eager'
    Lazy = 'lazy'


class PointerExposedBehaviour(enum.StrEnum):
    FirstItem = 'first-item'
    Set = 'set'


class PointerVector(sn.Name):
    __slots__ = ('module', 'name', 'direction', 'target')

    def __new__(cls, name, module=None, direction=PointerDirection.Outbound,
                target=None):
        result = super().__new__(cls, name, module=module)
        result.direction = direction
        result.target = target
        return result

    def __repr__(self):
        return '<edgedb.schema.PointerVector {}>'.format(self)

    def __mm_serialize__(self):
        return dict(
            name=str(self),
            direction=self.direction,
            target=self.target
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


class PointerCommandContext(sd.PrototypeCommandContext):
    pass


class PointerCommand(sd.PrototypeCommand):
    @classmethod
    def _protoname_from_ast(cls, astnode, context):
        name = super()._protoname_from_ast(astnode, context)

        parent_ctx = context.get(sources.SourceCommandContext)
        if parent_ctx:
            subject_name = parent_ctx.op.prototype_name

            pcls = cls._get_prototype_class()
            pnn = pcls.generate_specialized_name(
                subject_name, sn.Name(name)
            )

            name = sn.Name(name=pnn, module=subject_name.module)

        return name

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
        for sub in cmd(sd.AlterPrototypeProperty):
            if sub.property == 'default':
                if isinstance(sub.new_value, sexpr.ExpressionText):
                    expr = caosql.parse(sub.new_value)

                    if expr.op == qlast.UNION:
                        candidates = []
                        cls._extract_union_operands(expr, candidates)
                        deflt = []

                        for candidate in candidates:
                            cexpr = candidate.targets[0].expr
                            if isinstance(cexpr, qlast.ConstantNode):
                                deflt.append(cexpr.value)
                            else:
                                text = caosql.generate_source(candidate,
                                                              pretty=False)
                                deflt.append(sexpr.ExpressionText(text))
                    else:
                        deflt = [sub.new_value]

                else:
                    deflt = [sub.new_value]

                sub.new_value = deflt

    def _encode_default(self, context, node, op):
        if op.new_value:
            expr = op.new_value
            if not isinstance(expr, sexpr.ExpressionText):
                expr_t = qlast.SelectQueryNode(
                    targets=[qlast.SelectExprNode(
                        expr=qlast.ConstantNode(value=expr)
                    )]
                )
                expr = caosql.generate_source(expr_t, pretty=False)

                op.new_value = sexpr.ExpressionText(expr)
            super()._apply_field_ast(context, node, op)


class BasePointer(primary.Prototype, derivable.DerivablePrototype):
    source = so.Field(primary.Prototype, None, compcoef=0.933)
    target = so.Field(primary.Prototype, None, compcoef=0.833)

    def __iter__(self):
        # XXX: temporary measure for compatibility with linkset-dependent code
        yield self

    def get_near_endpoint(self, direction):
        return self.source if direction == PointerDirection.Outbound \
                           else self.target

    def get_far_endpoint(self, direction):
        return self.target if direction == PointerDirection.Outbound \
                           else self.source

    def get_common_target(self, schema, targets, minimize_by=None):
        from . import atoms, concepts

        if len(targets) == 1:
            return next(iter(targets))

        if minimize_by == 'most_generic':
            targets = utils.minimize_prototype_set_by_most_generic(
                            targets)
        elif minimize_by == 'least_generic':
            targets = utils.minimize_prototype_set_by_least_generic(
                            targets)

        if len(targets) == 1:
            return next(iter(targets))

        _targets = set()
        for t in targets:
            if getattr(t, 'is_virtual', False):
                _targets.update(t.children(schema))
            else:
                _targets.add(t)

        targets = _targets

        name = sources.Source.gen_virt_parent_name((t.name for t in targets),
                                                   module=self.name.module)

        target = schema.get(name, default=None)

        if target:
            schema.update_virtual_inheritance(target, targets)
            return target

        seen_atoms = False
        seen_concepts = False

        for target in targets:
            if isinstance(target, atoms.Atom):
                if seen_concepts:
                    raise schema_error.SchemaError(
                        'cannot mix atoms and concepts in link target list')
                seen_atoms = True
            else:
                if seen_atoms:
                    raise schema_error.SchemaError(
                        'cannot mix atoms and concepts in link target list')
                seen_concepts = True

        if seen_atoms and len(targets) > 1:
            target = utils.get_prototype_nearest_common_ancestor(targets)
            if target is None:
                raise schema_error.SchemaError(
                        'cannot set multiple atom targets for a link')
        else:
            target = concepts.Concept(name=name, is_abstract=True,
                                      is_virtual=True)
            schema.update_virtual_inheritance(target, targets)

        return target

    def create_common_target(self, schema, targets, minimize_by=False):
        target = self.get_common_target(schema, targets,
                                        minimize_by=minimize_by)
        if not schema.get(target.name, default=None):
            target.is_derived = True
            schema.add(target)
        return target

    @classmethod
    def merge_targets(cls, schema, ptr, t1, t2):
        from . import atoms, concepts

        # When two pointers are merged, check target compatibility
        # and return a target that satisfies both specified targets.
        #

        if isinstance(t1, atoms.Atom) != isinstance(t2, atoms.Atom):
            # Targets are not of the same node type

            pn = ptr.normal_name()
            ccn1 = t1.get_canonical_class().__name__
            ccn2 = t2.get_canonical_class().__name__

            msg = ('could not merge "{}" pointer: invalid atom/concept ' +
                   'target mix').format(pn)
            detail = ('[{}].[{}] targets {} "{}" while it also targets ' +
                      '{} "{}" in other parent.') \
                      .format(ptr.source.name, pn,
                              ccn1, t1.name, ccn2, t2.name)

            raise schema_error.SchemaError(msg, details=detail)

        elif isinstance(t1, atoms.Atom):
            # Targets are both atoms

            if t1 != t2:
                pn = ptr.normal_name()
                msg = 'could not merge "{}" pointer: targets conflict' \
                                                            .format(pn)
                detail = ('[{}].[{}] targets atom "{}" while it also '
                          'targets incompatible atom "{}" in other parent.') \
                          .format(ptr.source.name, pn, t1.name, t2.name)
                raise schema_error.SchemaError(msg, details=detail)

            return t1

        else:
            # Targets are both concepts
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
                    pn = ptr.normal_name()
                    msg = 'could not merge "{}" pointer: targets conflict' \
                                                            .format(pn)
                    detail = ('[{}].[{}] targets concept "{}" which '
                              ' is not related to any of targets found in'
                              ' other sources being merged: {}') \
                              .format(ptr.source.name, pn, t2.name, t1.name)
                    raise schema_error.SchemaError(msg, details=detail)

            for tgt1 in tt1:
                if not any(tgt2.issubclass(tgt1) for tgt2 in tt2):
                    new_targets.append(tgt1)

            if len(new_targets) > 1:
                tnames = (t.name for t in new_targets)
                module = ptr.source.name.module
                parent_name = concepts.Concept.gen_virt_parent_name(
                    tnames, module)
                current_target = concepts.Concept(
                    name=parent_name, is_abstract=True, is_virtual=True)
                schema.update_virtual_inheritance(current_target, new_targets)
            else:
                current_target = new_targets[0]

            return current_target

    @classmethod
    def merge_many(cls, schema, items, *, source, target=None, relaxed=False,
                                          derived=False, replace=None):
        ptr = items[0]

        if target is None:
            for parent in items:
                if target is None:
                    target = parent.target
                else:
                    target = cls.merge_targets(schema, ptr, target,
                                               parent.target)

        return ptr.derive(schema, source, target,
                          merge_bases=items[1:],
                          add_to_schema=True,
                          mark_derived=derived,
                          relaxed=relaxed,
                          replace_original=replace)

    def get_derived(self, schema, source, target, **kwargs):
        fqname = self.derive_name(source)
        ptr = schema.get(fqname, default=None)
        if ptr is not None:
            if ptr.target != target:
                ptr = None

        if ptr is None:
            fqname = self.derive_name(source, target)
            ptr = schema.get(fqname, default=None)
            if ptr is None:
                ptr = self.derive(schema, source, target, **kwargs)

        return ptr

    def init_derived(self, schema, source, target, *,
                           mark_derived=False, add_to_schema=False,
                           **kwargs):
        ptr = super().init_derived(
                    schema, source, target,
                    mark_derived=mark_derived,
                    add_to_schema=add_to_schema,
                    **kwargs)

        if mark_derived:
            fqname = self.derive_name(source, target)
        else:
            fqname = self.derive_name(source)

        ptr.name = fqname
        ptr.bases = [self.bases[0] if not self.generic() else self]
        ptr.source = source
        ptr.target = target

        if fqname != self.name:
            ptr.rederive_protorefs(schema, add_to_schema=add_to_schema,
                                           mark_derived=mark_derived)

        return ptr

    def merge_bases_into_derived(self, schema, derived, merge_bases, *,
                                       relaxed, **kwargs):
        for base in merge_bases:
            derived.merge_specialized(schema, base, relaxed=relaxed)

        derived.finalize(schema, bases=merge_bases)

    def is_pure_computable(self):
        return self.readonly and bool(self.default) and \
                not self.is_id_pointer() and \
                not self.normal_name() in {
                    'std.ctime',
                    'std.mtime',
                }

    def is_id_pointer(self):
        return self.normal_name() in {'std.linkid',
                                      'std.id'}

    def is_endpoint_pointer(self):
        return self.normal_name() in {'std.source',
                                      'std.target'}

    def is_special_pointer(self):
        return self.normal_name() in {'std.source',
                                      'std.target',
                                      'std.linkid',
                                      'std.id'}


class Pointer(BasePointer, constraints.ConsistencySubject,
              policy.PolicySubject, policy.InternalPolicySubject):

    required = so.Field(bool, default=False, compcoef=0.909)
    readonly = so.Field(bool, default=False, compcoef=0.909)
    loading = so.Field(PointerLoading, default=None, compcoef=0.909)
    default = so.Field(sexpr.ExpressionText, default=None,
                       coerce=True, compcoef=0.909)

    def generic(self):
        return self.source is None

    def get_loading_behaviour(self):
        if self.loading is not None:
            return self.loading
        else:
            if not self.generic():
                if self.atomic() and self.singular():
                    return PointerLoading.Eager
                else:
                    return PointerLoading.Lazy
            else:
                return None

    def merge_defaults(self, other):
        if not self.default:
            if other.default:
                self.default = other.default

    def normalize_defaults(self):
        pass
