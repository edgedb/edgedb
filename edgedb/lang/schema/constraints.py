##
# Copyright (c) 2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic import caos
from metamagic.caos import proto
from metamagic.caos.ir import ast as irast
from metamagic.caos.ir import utils as ir_utils
from metamagic.caos import caosql
from metamagic.caos.caosql import utils as caosql_utils

from metamagic.utils import ast

from metamagic.utils.datastructures import interval


class ConstraintsSchema:
    @classmethod
    def _dummy_subject(cls):
        # Point subject placeholder to a dummy pointer to make CaosQL pipeline happy.
        return proto.Atom(name=caos.Name('metamagic.caos.builtins._subject_tgt'))

    @classmethod
    def _parse_constraint_expr(cls, schema, module_aliases, expr, subject,
                                    inline_anchors=False):
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
        caosql_tree, tree, arg_types = cls._parse_constraint_expr(schema, module_aliases, expr,
                                                                  subject)
        expr = caosql.generate_source(caosql_tree, pretty=False)
        ### XXX: check that expr has boolean result
        return expr

    @classmethod
    def normalize_constraint_subject_expr(cls, schema, module_aliases, expr):
        subject = cls._dummy_subject()
        caosql_tree, _, _ = cls._parse_constraint_expr(schema, module_aliases, expr, subject)
        expr = caosql.generate_source(caosql_tree, pretty=False)
        return expr

    @classmethod
    def process_specialized_constraint(cls, schema, constraint):
        assert constraint.subject is not None

        subject = constraint.subject
        subjectexpr = constraint.get_field_value('subjectexpr')

        if not subjectexpr:
            # Special case for zero-argument exprs, where the subject is an argument,
            # e.g. unique constraints.
            #
            *_, arg_types = cls._parse_constraint_expr(schema, {}, constraint.expr, subject)
            if not arg_types and constraint._params:
                subjectexpr = constraint._params.pop('param')
                constraint.subjectexpr = subjectexpr

        if subjectexpr:
            _, subject, _ = cls._parse_constraint_expr(schema, {}, subjectexpr, subject)

        expr = constraint.get_field_value('expr')
        if not expr:
            raise ValueError('missing constraint expression in {!r}'.format(constraint.name))

        caosql_tree, tree, arg_types = cls._parse_constraint_expr(schema, {}, constraint.expr,
                                                                  subject)

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

            constraint.errmessage = constraint.errmessage.format(subject='{subject}', **fmtparams)

        text = caosql.generate_source(caosql_tree, pretty=False)

        constraint.localfinalexpr = text
        constraint.finalexpr = text
        constraint.inferredparamtypes = arg_types
        constraint.args = params or None
