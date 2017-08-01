##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang import edgeql
from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.common import enum

from . import constraints
from . import delta as sd
from . import derivable
from . import error as schema_error
from . import expr as sexpr
from . import inheriting
from . import name as sn
from . import objects as so
from . import policy
from . import primary
from . import referencing


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
    __slots__ = ('module', 'name', 'direction', 'target', 'is_linkprop')

    def __new__(cls, name, module=None, direction=PointerDirection.Outbound,
                target=None, is_linkprop=False):
        result = super().__new__(cls, name, module=module)
        result.direction = direction
        result.target = target
        result.is_linkprop = is_linkprop
        return result

    def __repr__(self):
        return '<edgedb.schema.PointerVector {}>'.format(self)

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


class PointerCommandContext(sd.ClassCommandContext):
    pass


class PointerCommand(constraints.ConsistencySubjectCommand,
                     referencing.ReferencedClassCommand):

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
        for sub in cmd(sd.AlterClassProperty):
            if sub.property == 'default':
                if isinstance(sub.new_value, sexpr.ExpressionText):
                    expr = edgeql.parse(sub.new_value)

                    if expr.op == qlast.UNION:
                        candidates = []
                        cls._extract_union_operands(expr, candidates)
                        deflt = []

                        for candidate in candidates:
                            cexpr = candidate.result
                            if isinstance(cexpr, qlast.Constant):
                                deflt.append(cexpr.value)
                            else:
                                text = edgeql.generate_source(candidate,
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
                expr_t = qlast.SelectQuery(
                    result=qlast.Constant(value=expr)
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


class BasePointer(primary.PrimaryClass, derivable.DerivableClass):
    source = so.Field(primary.PrimaryClass, None, compcoef=0.933)
    target = so.Field(so.NodeClass, None, compcoef=0.833)

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
        from . import atoms, concepts

        # When two pointers are merged, check target compatibility
        # and return a target that satisfies both specified targets.
        #

        if isinstance(t1, atoms.Atom) != isinstance(t2, atoms.Atom):
            # Targets are not of the same node type

            pn = ptr.shortname
            ccn1 = t1.get_canonical_class().__name__
            ccn2 = t2.get_canonical_class().__name__

            detail = (f'[{ptr.source.name}].[{pn}] targets {ccn1} "{t1.name}"'
                      f'while it also targets {ccn2} "{t2.name}"'
                      'in other parent.')

            raise schema_error.SchemaError(
                f'could not merge "{pn}" pointer: invalid atom/concept ' +
                'target mix', details=detail)

        elif isinstance(t1, atoms.Atom):
            # Targets are both atoms
            if t1 != t2:
                pn = ptr.shortname
                raise schema_error.SchemaError(
                    f'could not merge {pn!r} pointer: targets conflict',
                    details=f'({ptr.source.name}).({pn}) targets atom'
                            f'{t1.name!r} while it also targets incompatible'
                            f'atom {t2.name!r} in other parent.')

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
                    pn = ptr.shortname
                    raise schema_error.SchemaError(
                        f'could not merge {pn!r} pointer: targets conflict',
                        details=f'({ptr.source.name}).({pn}) targets concept'
                                f' {t2.name!r} which is not related to any of'
                                f' targets found in other sources being'
                                f' merged: {t1.name!r}.')

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
        return (self.readonly and bool(self.default) and
                not self.is_id_pointer() and
                self.shortname not in {'std::ctime', 'std::mtime'})

    def is_id_pointer(self):
        return self.shortname in {'std::linkid', 'std::id'}

    def is_endpoint_pointer(self):
        return self.shortname in {'std::source', 'std::target'}

    def is_special_pointer(self):
        return self.shortname in {'std::source', 'std::target',
                                  'std::linkid', 'std::id'}


def _merge_sticky_bool(ours, theirs, schema):
    if ours is not None and theirs is not None:
        result = max(ours, theirs)
    else:
        result = theirs if theirs is not None else ours

    return result


class Pointer(BasePointer, constraints.ConsistencySubject,
              policy.PolicySubject, policy.InternalPolicySubject):

    required = so.Field(bool, default=False, compcoef=0.909,
                        merge_fn=_merge_sticky_bool)
    readonly = so.Field(bool, default=False, compcoef=0.909,
                        merge_fn=_merge_sticky_bool)
    loading = so.Field(PointerLoading, default=None, compcoef=0.909,
                       introspectable=False)
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
