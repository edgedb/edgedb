##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import itertools

from edgedb.lang.ir import utils as ir_utils
from edgedb.lang import edgeql
from edgedb.lang.edgeql import ast as qlast

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


class ConsistencySubjectCommand(sd.PrototypeCommand):
    def _create_innards(self, schema, context):
        super()._create_innards(schema, context)

        for op in self(ConstraintCommand):
            op.apply(schema, context=context)

    def _alter_innards(self, schema, context, prototype):
        super()._alter_innards(schema, context, prototype)

        for op in self(ConstraintCommand):
            op.apply(schema, context=context)

    def _delete_innards(self, schema, context, prototype):
        super()._delete_innards(schema, context, prototype)

        for op in self(ConstraintCommand):
            op.apply(schema, context=context)

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        for op in self(ConstraintCommand):
            self._append_subcmd_ast(node, op, context)


class ConstraintCommandContext(sd.PrototypeCommandContext):
    pass


class ConstraintCommand(referencing.ReferencedPrototypeCommand):
    context_class = ConstraintCommandContext
    referrer_context_class = ConsistencySubjectCommandContext

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
                args = [qlast.ConstantNode(value=value.schema_name)]

                for subtype in value.get_subtypes():
                    args.append(qlast.ConstantNode(
                        value=subtype.prototype_name))

                v = qlast.FunctionCallNode(func='typeref', args=args)

            elif utils.is_nontrivial_container(value):
                v = qlast.SequenceNode(elements=[
                    qlast.ConstantNode(value=el) for el in value
                ])

            else:
                v = qlast.ConstantNode(value=value)

            items.append((qlast.ConstantNode(value=key), v))

        self._set_attribute_ast(context, node, op.property,
                                qlast.MappingNode(items=items))

    def _create_begin(self, schema, context):
        super()._create_begin(schema, context)

        referrer_ctx = context.get(self.referrer_context_class)
        if referrer_ctx is not None and self.prototype.finalexpr is None:
            Constraint.process_specialized_constraint(
                schema, self.prototype, self.prototype.args)

    def _alter_begin(self, schema, context, prototype):
        super()._alter_begin(schema, context, prototype)


class CreateConstraint(ConstraintCommand,
                       referencing.CreateReferencedPrototype):
    astnode = [qlast.CreateConcreteConstraintNode, qlast.CreateConstraintNode]
    referenced_astnode = qlast.CreateConcreteConstraintNode

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


class RenameConstraint(ConstraintCommand, named.RenameNamedPrototype):
    pass


class AlterConstraint(ConstraintCommand, named.AlterNamedPrototype):
    astnode = [qlast.AlterConcreteConstraintNode, qlast.AlterConstraintNode]
    referenced_astnode = qlast.AlterConcreteConstraintNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

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

    def _apply_field_ast(self, context, node, op):
        if op.property in {'paramtypes', 'inferredparamtypes', 'args'}:
            self._process_type_mapping(context, node, op)
        elif op.property == 'subject':
            pass
        else:
            super()._apply_field_ast(context, node, op)


class DeleteConstraint(ConstraintCommand, named.DeleteNamedPrototype):
    astnode = [qlast.DropConcreteConstraintNode, qlast.DropConstraintNode]
    referenced_astnode = qlast.DropConcreteConstraintNode


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

    def init_derived(self, schema, source, *qualifiers,
                     as_copy, mark_derived=False, add_to_schema=False,
                     merge_bases=None, attrs=None,
                     dctx=None, **kwargs):

        if attrs is None:
            attrs = {}

        attrs['subject'] = source

        return super().init_derived(
            schema, source, *qualifiers, as_copy=as_copy,
            mark_derived=mark_derived, add_to_schema=add_to_schema,
            merge_bases=merge_bases, attrs=attrs, dctx=dctx, **kwargs)

    @classmethod
    def _dummy_subject(cls):
        from . import atoms

        # Point subject placeholder to a dummy pointer to make EdgeQL
        # pipeline happy.
        return atoms.Atom(name=sn.Name('std::_subject_tgt'))

    @classmethod
    def _normalize_constraint_expr(cls, schema, module_aliases, expr, subject,
                                   inline_anchors=False):
        from edgedb.lang.edgeql import utils as edgeql_utils

        if isinstance(expr, str):
            tree = edgeql.parse(expr, module_aliases)
        else:
            tree = expr

        ir, edgeql_tree, _ = edgeql_utils.normalize_tree(
            tree, schema, module_aliases=module_aliases,
            anchors={'subject': subject}, inline_anchors=inline_anchors)

        arg_types = ir_utils.infer_arg_types(ir, schema)

        sel = ir.selector
        if len(sel) != 1:
            msg = 'invalid constraint expression: must be a simple expression'
            raise ValueError(msg)

        edgedb_tree = sel[0].expr

        return edgeql_tree.targets[0].expr, edgedb_tree, arg_types

    @classmethod
    def normalize_constraint_expr(cls, schema, module_aliases, expr):
        subject = cls._dummy_subject()
        edgeql_tree, tree, arg_types = cls._normalize_constraint_expr(
            schema, module_aliases, expr, subject)

        expr = edgeql.generate_source(edgeql_tree, pretty=False)
        # XXX: check that expr has boolean result
        return expr

    @classmethod
    def normalize_constraint_subject_expr(cls, schema, module_aliases, expr):
        subject = cls._dummy_subject()
        edgeql_tree, _, _ = cls._normalize_constraint_expr(
            schema, module_aliases, expr, subject)
        expr = edgeql.generate_source(edgeql_tree, pretty=False)
        return expr

    @classmethod
    def process_specialized_constraint(cls, schema, constraint, params):
        from edgedb.lang.edgeql import utils as edgeql_utils

        assert constraint.subject is not None

        subject = constraint.subject
        subjectexpr = constraint.get_field_value('subjectexpr')

        if not subjectexpr:
            # Special case for zero-argument exprs, where the subject is an
            # argument, e.g. unique constraints.
            #
            *_, arg_types = cls._normalize_constraint_expr(
                schema, {}, constraint.expr, subject)

            if not arg_types and params:
                subjectexpr = params.pop('param')

        if subjectexpr:
            edgeql_tree, subject, _ = cls._normalize_constraint_expr(
                schema, {}, subjectexpr, subject)

            if constraint.subjectexpr is None:
                constraint.subjectexpr = edgeql.generate_source(
                    edgeql_tree, pretty=False)

        expr = constraint.get_field_value('expr')
        if not expr:
            err = 'missing constraint expression in ' \
                  '{!r}'.format(constraint.name)
            raise ValueError(err)

        edgeql_tree, tree, arg_types = cls._normalize_constraint_expr(
            schema, {}, constraint.expr, subject)

        constraint.expr = cls.normalize_constraint_expr(schema, {}, expr)

        if constraint.paramtypes:
            all_arg_types = arg_types.copy()
            all_arg_types.update(constraint.paramtypes)
        else:
            all_arg_types = arg_types

        args = {}

        if params:
            fmtparams = {}
            exprparams = {}

            for pn, pv in params.items():
                try:
                    arg_type = all_arg_types[pn]
                except KeyError:
                    # XXX: warn
                    pass
                else:
                    arg = arg_type.coerce(pv, schema)
                    args[pn] = arg

                    if isinstance(arg, (frozenset, tuple)):
                        # This assumes that the datatype in this collection
                        # is orderable.  If this ever breaks, use OrderedSet.
                        fmtparams[pn] = ', '.join(sorted(arg))
                    else:
                        fmtparams[pn] = str(arg)

                    exprparams[pn] = arg

            edgeql_utils.inline_constants(edgeql_tree, exprparams,
                                          all_arg_types)

            constraint.errmessage = constraint.errmessage.format(
                subject='{subject}', **fmtparams)

        text = edgeql.generate_source(edgeql_tree, pretty=False)

        constraint.localfinalexpr = text
        constraint.finalexpr = text
        constraint.inferredparamtypes = arg_types
        constraint.args = args or None

    def format_error_message(self):
        errmsg = self.errmessage
        subjtitle = self.subject.title

        if not subjtitle:
            try:
                subjname = self.subject.normal_name()
            except AttributeError:
                subjname = self.subject.name

            subjtitle = subjname.name

        formatted = errmsg.format(subject=subjtitle)

        return formatted


class ConsistencySubject(referencing.ReferencingPrototype):
    constraints = referencing.RefDict(ref_cls=Constraint, compcoef=0.887)

    @classmethod
    def inherit_pure(cls, schema, item, source, *, dctx=None):
        item = super().inherit_pure(schema, item, source, dctx=dctx)

        if any(c.is_abstract for c in item.constraints.values()):
            # Have abstract constraints, cannot go pure inheritance,
            # must create a derived prototype with materialized
            # constraints.
            generic = item.bases[0]
            item = generic.derive(schema, source=source, add_to_schema=True,
                                  merge_bases=[item], dctx=dctx)

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
                    constraint = constraint.derive_copy(
                        schema, self, add_to_schema=True,
                        attrs=dict(is_abstract=False))

                    self.add_constraint(constraint)

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
