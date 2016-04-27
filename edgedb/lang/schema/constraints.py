##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import itertools

from metamagic.caos.ir import utils as ir_utils
from metamagic.caos import caosql
from metamagic.caos.caosql import ast as qlast

from . import delta as sd
from . import derivable
from . import expr as s_expr
from . import name as sn
from . import named
from . import objects as so
from . import primary
from . import referencing
from . import utils


class ConsistencySubjectCommandContext:
    # context mixin
    pass


class ConstraintCommandContext(sd.PrototypeCommandContext):
    pass


class ConstraintCommand(sd.PrototypeCommand):
    context_class = ConstraintCommandContext

    @classmethod
    def _get_prototype_class(cls):
        return Constraint

    def add_constraint(self, constraint, parent, schema):
        parent.add_constraint(constraint)

    def delete_constraint(self, constraint_name, parent, schema):
        parent.del_constraint(constraint_name, schema)

    def _process_type_mapping(self, context, node, op):
        if not op.new_value:
            return

        items = []

        for key, value in op.new_value.items():
            if isinstance(value, so.PrototypeRef):
                v = qlast.ConstantNode(value=value.prototype_name)
                v = qlast.FunctionCallNode(func='typeref', args=[v])

            elif isinstance(value, so.Collection):
                if isinstance(value, so.Set):
                    maintype = 'set'
                else:
                    maintype = 'list'

                maintype = qlast.ConstantNode(value=maintype)
                subtype = qlast.ConstantNode(
                            value=value.element_type.prototype_name)

                v = qlast.FunctionCallNode(func='typeref',
                                           args=[maintype, subtype])

            elif utils.is_nontrivial_container(value):
                v = qlast.SequenceNode(elements=[
                    qlast.ConstantNode(value=el) for el in value
                ])

            else:
                v = qlast.ConstantNode(value=value)

            items.append((qlast.ConstantNode(value=key), v))

        self._set_attribute_ast(context, node, op.property,
                                qlast.MappingNode(items=items))

    @classmethod
    def _protoname_from_ast(cls, astnode, context):
        name = super()._protoname_from_ast(astnode, context)

        parent_ctx = context.get(ConsistencySubjectCommandContext)
        if parent_ctx:
            subject_name = parent_ctx.op.prototype_name

            pcls = cls._get_prototype_class()
            pnn = pcls.generate_specialized_name(
                subject_name, sn.Name(name)
            )

            name = sn.Name(name=pnn, module=subject_name.module)

        return name

    def _fix_args(self, constraint, protoschema, context):
        if not constraint.args:
            return

        types = {}
        if constraint.inferredparamtypes:
            types.update(constraint.inferredparamtypes)

        if constraint.paramtypes:
            types.update(constraint.paramtypes)

        for k, v in constraint.args.items():
            arg_type = types[k]
            constraint.args[k] = arg_type.coerce(v, protoschema)

        for op in self(sd.AlterPrototypeProperty):
            if op.property == 'args':
                if op.new_value:
                    for k, v in op.new_value.items():
                        arg_type = types[k]
                        op.new_value[k] = arg_type.coerce(v, protoschema)


class CreateConstraint(ConstraintCommand, named.CreateNamedPrototype):
    astnode = [qlast.CreateConcreteConstraintNode, qlast.CreateConstraintNode]

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        cmd = super()._cmd_tree_from_ast(astnode, context)

        if isinstance(astnode, qlast.CreateConcreteConstraintNode):
            nname = Constraint.normalize_name(cmd.prototype_name)

            cmd.add(
                sd.AlterPrototypeProperty(
                    property='bases',
                    new_value=so.PrototypeList([
                        so.PrototypeRef(
                            prototype_name=sn.Name(
                                module=nname.module,
                                name=nname.name
                            )
                        )
                    ])
                )
            )

            parent_ctx = context.get(ConsistencySubjectCommandContext)
            subject_name = parent_ctx.op.prototype_name

            cmd.add(
                sd.AlterPrototypeProperty(
                    property='subject',
                    new_value=so.PrototypeRef(
                        prototype_name=subject_name
                    )
                )
            )

            if astnode.is_abstract:
                cmd.add(
                    sd.AlterPrototypeProperty(
                        property='is_abstract',
                        new_value=True
                    )
                )

        return cmd

    def _get_ast_node(self, context):
        subject_ctx = context.get(ConsistencySubjectCommandContext)
        if subject_ctx:
            return qlast.CreateConcreteConstraintNode
        else:
            return qlast.CreateConstraintNode

    def _apply_field_ast(self, context, node, op):
        if op.property == 'is_derived':
            pass
        elif op.property == 'is_abstract':
            node.is_abstract = op.new_value
        elif op.property == 'subject':
            pass
        elif op.property in {'paramtypes', 'inferredparamtypes', 'args'}:
            self._process_type_mapping(context, node, op)
        else:
            super()._apply_field_ast(context, node, op)

    def apply(self, protoschema, context):
        context = context or so.CommandContext()

        constraint = named.CreateNamedPrototype.apply(
                        self, protoschema, context)
        # Argument values might have been mangled by the delta, fix them up
        self._fix_args(constraint, protoschema, context)

        subject_ctx = context.get(ConsistencySubjectCommandContext)
        if subject_ctx:
            constraint.subject = subject_ctx.proto
            subject_ctx.proto.add_constraint(constraint)

        constraint.acquire_ancestor_inheritance(protoschema)

        return constraint


class RenameConstraint(ConstraintCommand, named.RenameNamedPrototype):
    def apply(self, schema, context):
        constraint = super().apply(schema, context)

        if not constraint.generic():
            subject_ctx = context.get(ConsistencySubjectCommandContext)
            msg = "Specialized constraint commands must be run in " + \
                  "ConsistencySubject context"
            assert subject_ctx, msg

            subject = subject_ctx.proto

            norm = Constraint.normalize_name
            cur_name = norm(self.prototype_name)
            new_name = norm(self.new_name)

            local = subject.local_constraints.pop(cur_name, None)
            if local:
                subject.local_constraints[new_name] = local

            inherited = subject.constraints.pop(cur_name, None)
            if inherited is not None:
                subject.constraints[new_name] = inherited

        return constraint


class AlterConstraint(ConstraintCommand, named.AlterNamedPrototype):
    astnode = [qlast.AlterConcreteConstraintNode, qlast.AlterConstraintNode]

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        cmd = super()._cmd_tree_from_ast(astnode, context)

        if isinstance(astnode, qlast.AlterConcreteConstraintNode):
            subject_ctx = context.get(ConsistencySubjectCommandContext)
            new_subject_name = None
            for op in subject_ctx.op(named.RenameNamedPrototype):
                new_subject_name = op.new_name

            if new_subject_name is not None:
                cmd.add(
                    sd.AlterPrototypeProperty(
                        property='subject',
                        new_value=so.PrototypeRef(
                            prototype_name=new_subject_name
                        )
                    )
                )

            new_name = None
            for op in cmd(RenameConstraint):
                new_name = op.new_name

            if new_name is not None:
                cmd.add(
                    sd.AlterPrototypeProperty(
                        property='name',
                        new_value=new_name
                    )
                )

        return cmd

    def _get_ast_node(self, context):
        subject_ctx = context.get(ConsistencySubjectCommandContext)
        if subject_ctx:
            return qlast.AlterConcreteConstraintNode
        else:
            return qlast.AlterConstraintNode

    def _apply_field_ast(self, context, node, op):
        if op.property in {'paramtypes', 'inferredparamtypes', 'args'}:
            self._process_type_mapping(context, node, op)
        elif op.property == 'subject':
            pass
        else:
            super()._apply_field_ast(context, node, op)

    def apply(self, schema, context):

        attrsubj = context.get(ConsistencySubjectCommandContext)
        assert attrsubj, "Constraint commands must be run in " + \
                         "ConstraintSubject context"

        with context(ConstraintCommandContext(self, None)):
            constraint = super().apply(schema, context)
            # Argument values might have been mangled by the delta, fix them up
            self._fix_args(constraint, schema, context)
            return constraint


class DeleteConstraint(ConstraintCommand, named.DeleteNamedPrototype):
    astnode = [qlast.DropConcreteConstraintNode, qlast.DropConstraintNode]

    def _get_ast_node(self, context):
        subject_ctx = context.get(ConsistencySubjectCommandContext)
        if subject_ctx:
            return qlast.DropConcreteConstraintNode
        else:
            return qlast.DropConstraintNode

    def apply(self, protoschema, context):
        subject_ctx = context.get(ConsistencySubjectCommandContext)
        if subject_ctx is not None:
            subject = subject_ctx.proto
            self.delete_constraint(self.prototype_name, subject, protoschema)

        return super().apply(protoschema, context)


class CumulativeBoolExpr(s_expr.ExpressionText):
    @classmethod
    def merge_values(cls, ours, theirs, schema):
        if ours and theirs and ours != theirs:
            result = '({}) and ({})'.format(ours, theirs)
        elif not ours and theirs:
            result = theirs
        else:
            result = ours

        return result


class Constraint(primary.Prototype, derivable.DerivablePrototype):
    _type = 'constraint'

    expr = so.Field(s_expr.ExpressionText, default=None, compcoef=0.909,
                    coerce=True)

    subjectexpr = so.Field(s_expr.ExpressionText,
                           default=None, compcoef=0.833, coerce=True)

    localfinalexpr = so.Field(CumulativeBoolExpr, default=None,
                              coerce=True, derived=True, private=True)

    finalexpr = so.Field(CumulativeBoolExpr, default=None,
                         coerce=True, derived=True, compcoef=0.909)

    subject = so.Field(so.BasePrototype, default=None, private=True)

    paramtypes = so.Field(so.PrototypeDict, default=None, coerce=True,
                          compcoef=0.857)

    inferredparamtypes = so.Field(so.PrototypeDict, default=None,
                                  coerce=True, derived=True)

    args = so.Field(so.ArgDict, default=None, coerce=True, private=True,
                    compcoef=0.875)

    errmessage = so.Field(str, default=None, compcoef=0.971)

    delta_driver = sd.DeltaDriver(
        create=CreateConstraint,
        alter=AlterConstraint,
        rename=RenameConstraint,
        delete=DeleteConstraint
    )

    def generic(self):
        return self.subject is None

    def merge_localexprs(self, obj, schema):
        self.localfinalexpr = CumulativeBoolExpr.merge_values(
                                self.localfinalexpr, obj.localfinalexpr,
                                schema=schema)

    def init_derived(self, schema, source, *, replace_original=None,
                                              **kwargs):
        constraint = super().init_derived(
                        schema, source, replace_original=replace_original,
                        **kwargs)

        constraint.subject = source
        constraint.bases = [self.bases[0] if not self.generic() else self]

        if not replace_original:
            constraint.is_abstract = False

        return constraint

    def get_metaclass(self, proto_schema):
        from metamagic.caos.constraint import ConstraintMeta
        return ConstraintMeta

    @classmethod
    def _dummy_subject(cls):
        from . import atoms

        # Point subject placeholder to a dummy pointer to make CaosQL
        # pipeline happy.
        return atoms.Atom(name=sn.Name('metamagic.caos.builtins._subject_tgt'))

    @classmethod
    def _parse_constraint_expr(cls, schema, module_aliases, expr, subject,
                                    inline_anchors=False):
        from metamagic.caos.caosql import utils as caosql_utils

        ir, caosql_tree, _ = caosql_utils.normalize_tree(
                                    expr, schema,
                                    module_aliases=module_aliases,
                                    anchors={'subject': subject},
                                    inline_anchors=inline_anchors)

        arg_types = ir_utils.infer_arg_types(ir, schema)

        sel = ir.selector
        if len(sel) != 1:
            msg = 'invalid constraint expression: must be a simple expression'
            raise ValueError(msg)

        caos_tree = sel[0].expr

        return caosql_tree.targets[0].expr, caos_tree, arg_types

    @classmethod
    def normalize_constraint_expr(cls, schema, module_aliases, expr):
        subject = cls._dummy_subject()
        caosql_tree, tree, arg_types = cls._parse_constraint_expr(
            schema, module_aliases, expr, subject)

        expr = caosql.generate_source(caosql_tree, pretty=False)
        # XXX: check that expr has boolean result
        return expr

    @classmethod
    def normalize_constraint_subject_expr(cls, schema, module_aliases, expr):
        subject = cls._dummy_subject()
        caosql_tree, _, _ = cls._parse_constraint_expr(
            schema, module_aliases, expr, subject)
        expr = caosql.generate_source(caosql_tree, pretty=False)
        return expr

    @classmethod
    def process_specialized_constraint(cls, schema, constraint):
        from metamagic.caos.caosql import utils as caosql_utils

        assert constraint.subject is not None

        subject = constraint.subject
        subjectexpr = constraint.get_field_value('subjectexpr')

        if not subjectexpr:
            # Special case for zero-argument exprs, where the subject is an
            # argument, e.g. unique constraints.
            #
            *_, arg_types = cls._parse_constraint_expr(
                schema, {}, constraint.expr, subject)

            if not arg_types and constraint._params:
                subjectexpr = constraint._params.pop('param')
                constraint.subjectexpr = subjectexpr

        if subjectexpr:
            _, subject, _ = cls._parse_constraint_expr(
                schema, {}, subjectexpr, subject)

        expr = constraint.get_field_value('expr')
        if not expr:
            err = 'missing constraint expression in {!r}'.format(
                        constraint.name)
            raise ValueError(err)

        caosql_tree, tree, arg_types = cls._parse_constraint_expr(
                                          schema, {}, constraint.expr, subject)

        constraint.expr = cls.normalize_constraint_expr(schema, {}, expr)

        if constraint.paramtypes:
            all_arg_types = arg_types.copy()
            all_arg_types.update(constraint.paramtypes)
        else:
            all_arg_types = arg_types

        params = {}

        if constraint._params:
            fmtparams = {}
            exprparams = {}

            for pn, pv in constraint._params.items():
                try:
                    arg_type = all_arg_types[pn]
                except KeyError:
                    # XXX: warn
                    pass
                else:
                    param = arg_type.coerce(pv, schema)
                    params[pn] = param

                    if isinstance(param, (frozenset, tuple)):
                        # This assumes that the datatype in this collection
                        # is orderable.  If this ever breaks, use OrderedSet.
                        param = list(sorted(param))
                        fmtparams[pn] = ', '.join(param)
                    else:
                        fmtparams[pn] = str(param)

                    exprparams[pn] = param

            caosql_utils.inline_constants(caosql_tree, exprparams,
                                          all_arg_types)

            constraint.errmessage = constraint.errmessage.format(
                subject='{subject}', **fmtparams)

        text = caosql.generate_source(caosql_tree, pretty=False)

        constraint.localfinalexpr = text
        constraint.finalexpr = text
        constraint.inferredparamtypes = arg_types
        constraint.args = params or None


class ConsistencySubject(referencing.ReferencingPrototype):
    constraints = referencing.RefDict(ref_cls=Constraint, compcoef=0.887)

    @classmethod
    def inherit_pure(cls, schema, item, source):
        item = super().inherit_pure(schema, item, source)

        ac = [c for c in item.constraints.values() if c.is_abstract]

        if ac:
            # Have abstract constraints, cannot go pure inheritance,
            # must create a derived prototype with materialized
            # constraints.
            item = cls.merge_many(schema, [item], source=source)

        return item

    def begin_protoref_dict_merge(self, schema, bases, attr):
        if attr == 'constraints':
            # Make sure abstract constraints from parents are mixed in
            # properly.
            constraints = set(self.constraints)
            inherited = itertools.chain.from_iterable(
                            getattr(b, 'constraints', {}).values()
                            for b in bases)
            constraints.update(c.normal_name()
                               for c in inherited if c.is_abstract)
            return constraints
        else:
            return super().begin_protoref_dict_merge(schema, bases, attr)

    def finish_protoref_dict_merge(self, schema, bases, attr):
        super().finish_protoref_dict_merge(schema, bases, attr)

        if attr == 'constraints':
            # Materialize unmerged abstract constraints
            for cn, constraint in self.constraints.items():
                if constraint.is_abstract and cn not in self.local_constraints:
                    constraint = constraint.derive(schema, self,
                                                   add_to_schema=True)
                    self.constraints[cn] = constraint
                    self.local_constraints[cn] = constraint

    def add_constraint(self, constraint, replace=False):
        self.add_protoref('constraints', constraint, replace=replace)

    def del_constraint(self, constraint_name, proto_schema):
        self.del_protoref('constraints', constraint_name, proto_schema)

    @classmethod
    def delta_constraints(cls, set1, set2, delta, context=None):
        oldconstraints = set(set1)
        newconstraints = set(set2)

        for constraint in oldconstraints - newconstraints:
            d = set1[constraint].delta(None, reverse=True, context=context)
            delta.add(d)

        for constraint in newconstraints - oldconstraints:
            d = set2[constraint].delta(None, context=context)
            delta.add(d)

        for constraint in newconstraints & oldconstraints:
            oldconstr = set1[constraint]
            newconstr = set2[constraint]

            if newconstr.compare(oldconstr, context=context) != 1.0:
                d = newconstr.delta(oldconstr, context=context)
                delta.add(d)

    def delta_all_constraints(self, old, new, delta, context):
        oldconstraints = old.local_constraints if old else {}
        newconstraints = new.local_constraints if new else {}

        self.delta_constraints(oldconstraints, newconstraints, delta, context)
