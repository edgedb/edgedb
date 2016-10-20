##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re

from edgedb.lang import edgeql
from edgedb.lang.common import ast
from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.graphql import ast as gqlast, parser as gqlparser
from edgedb.lang.schema import types as s_types, error as s_error

from .errors import GraphQLValidationError


GQL_OPS_MAP = {
    '__eq': ast.ops.EQ, '__ne': ast.ops.NE,
    '__in': ast.ops.IN, '__ni': ast.ops.NOT_IN,
}

PY_COERCION_MAP = {
    str: (s_types.string.Str, s_types.uuid.UUID),
    int: (s_types.int.Int, s_types.numeric.Float, s_types.numeric.Decimal,
          s_types.uuid.UUID),
    float: (s_types.numeric.Float, s_types.numeric.Decimal),
    bool: s_types.boolean.Bool,
}

GQL_TYPE_NAMES_MAP = {
    'String': s_types.string.Str,
    'Int': s_types.int.Int,
    'Float': s_types.numeric.Float,
    'Boolean': s_types.boolean.Bool,
    'ID': s_types.uuid.UUID,
}


class GraphQLTranslatorContext:
    def __init__(self, *, schema, variables):
        self.schema = schema
        self.variables = variables
        self.fragments = {}
        self.validated_fragments = {}
        self.vars = {}
        self.fields = []
        self.path = []
        self.optype = None


class GraphQLTranslator(ast.NodeVisitor):
    # def visit(self, node):
    #     return self._translate(node)

    def visit_Document(self, node):
        # we need to index all of the fragments before we process operations
        #
        self._context.fragments = {
            f.name: f for f in node.definitions
            if isinstance(f, gqlast.FragmentDefinition)
        }

        return dict(d for d in self.visit(node.definitions) if d is not None)

    def visit_FragmentDefinition(self, node):
        # fragments are already processed, no need to do anything here
        return None

    def visit_OperationDefinition(self, node):
        # create a dict of variables that will be marked as
        # critical or not
        #
        self._context.vars = {name: [val, False]
                      for name, val in self._context.variables.items()}
        opname = None
        self._context.optype = None

        if node.type is None or node.type == 'query':
            stmt = self._visit_query(node)
            if node.name:
                opname = 'query {}'.format(node.name)
        elif node.type == 'mutation':
            stmt = self._visit_mutation(node)
            if node.name:
                opname = 'mutation {}'.format(node.name)
        else:
            raise ValueError('unsupported definition type: {!r}'.format(
                node.type))

        # produce the list of variables critical to the shape
        # of the query
        #
        critvars = [(name, val) for name, (val, crit)
                    in self._context.vars.items() if crit]
        critvars.sort()

        return (opname, (stmt, critvars))

    def _visit_query(self, node):
        self._context.optype = 'query'
        query = None
        # populate input variables with defaults, where applicable
        #
        if node.variables:
            self.visit(node.variables)

        module = self._get_module(node.directives)

        # special treatment of the selection_set, different from inner
        # recursion
        #
        for selection in node.selection_set.selections:
            self._context.path = [[[module, None]]]
            selquery = qlast.SelectQueryNode(
                targets=[
                    self._visit_query_selset(selection)
                ],
                where=self._visit_select_where(selection.arguments)
            )

            if query is None:
                query = selquery
            else:
                query = qlast.SelectQueryNode(
                    op=qlast.UNION,
                    op_larg=query,
                    op_rarg=selquery
                )

        return query

    def _visit_mutation(self, node):
        query = None
        # populate input variables with defaults, where applicable
        #
        if node.variables:
            self.visit(node.variables)

        module = self._get_module(node.directives)

        # special treatment of the selection_set, different from inner
        # recursion
        #
        for selection in node.selection_set.selections:
            self._context.path = [[[module, None]]]

            # in addition to figuring out the returned structure, this
            # will determine the type of mutation
            #
            targets = [
                self._visit_query_selset(selection)
            ]
            subject = self._visit_operation_subject()

            if self._context.optype == 'delete':
                mutation = qlast.DeleteQueryNode(
                    targets=targets,
                    subject=subject,
                    where=self._visit_select_where(selection.arguments),
                )
            elif self._context.optype == 'insert':
                mutation = qlast.InsertQueryNode(
                    targets=targets,
                    subject=subject,
                    pathspec=self._visit_insert_data(selection.arguments),
                )

            if query is None:
                query = mutation
            else:
                raise GraphQLValidationError(
                    "unexpected field {!r}".format(
                        selection.name), context=selection.context)

        return query

    def get_concept_name(self, raw_name, context):
        if self._context.optype is None:
            # operation type has not been determined, which means
            # it's one of the following mutations: 'insert', 'delete', 'update'
            #
            match = re.match(r'(delete|insert|update)_*(.+)', raw_name)
            if match:
                self._context.optype, name = match.groups()
                return name
            else:
                raise GraphQLValidationError(
                    "unexpected field {!r}".format(raw_name), context=context)

        else:
            return raw_name

    def _visit_query_selset(self, selection):
        concept = self.get_concept_name(selection.name, selection.context)
        base = self._context.path[0][0] = (self._context.path[0][0][0],
                                           concept)
        self._context.fields.append({})

        try:
            self._context.schema.get(base)
        except s_error.SchemaError:
            raise GraphQLValidationError(
                "{!r} does not exist in the schema module {!r}".format(
                    base[1], base[0]), context=selection.context)

        expr = qlast.SelectExprNode(
            expr=qlast.PathNode(
                steps=[qlast.PathStepNode(namespace=base[0], expr=base[1])],
                pathspec=self.visit(selection.selection_set)
            )
        )

        self._context.fields.pop()

        return expr

    def _visit_operation_subject(self):
        base = self._context.path[0][0]
        return qlast.PathNode(
            steps=[qlast.PathStepNode(namespace=base[0], expr=base[1])],
        )

    def _visit_insert_data(self, arguments):
        base = self._context.path[0][0]

        if not arguments:
            return None

        for arg in arguments:
            if arg.name == '__data':
                return self._visit_mutation_data(arg.value)
            else:
                raise GraphQLValidationError(
                    "unknown argument {!r}".format(arg.name),
                    context=arg.context)

    def _visit_mutation_data(self, node):
        result = []

        for field in node.value:
            result.append(qlast.SelectPathSpecNode(
                expr=qlast.PathNode(
                    steps=[qlast.LinkExprNode(
                        expr=qlast.LinkNode(name=field.name)
                    )]
                ),
                compexpr=self.visit(field.value)
            ))

        return result

    def _visit_select_where(self, arguments):
        if not arguments:
            return None

        def get_path_prefix():
            base = self._context.path[0][0]
            return [qlast.PathStepNode(namespace=base[0], expr=base[1])]

        return self._join_expressions(self._visit_arguments(
                arguments, get_path_prefix=get_path_prefix))

    def _get_module(self, directives):
        module = None
        for directive in directives:
            if directive.name == 'edgedb':
                args = {a.name: a.value.value for a in directive.arguments}
                module = args['module']

        return module

    def _should_include(self, directives):
        for directive in directives:
            if directive.name in ('include', 'skip'):
                cond = [a.value for a in directive.arguments
                        if a.name == 'if'][0]
                if isinstance(cond, gqlast.Variable):
                    var = self._context.vars[cond.value]
                    cond = var[0]
                    var[1] = True  # mark the variable as critical
                else:
                    cond = cond.value

                if not isinstance(cond, bool):
                    raise GraphQLValidationError(
                        "'if' argument of {} directive must be a Boolean"
                        .format(directive.name), context=directive.context)

                if directive.name == 'include' and cond is False:
                    return False
                elif directive.name == 'skip' and cond is True:
                    return False

        return True

    def visit_VariableDefinition(self, node):
        # it is invalid to declare a non-nullable variable with a default
        #
        if node.value is not None and not node.type.nullable:
            raise GraphQLValidationError(
                "variable {!r} cannot be non-nullable and have a default"
                .format(node.name), context=node.context)

        variables = self._context.vars
        if not variables.get(node.name):
            if node.value is None:
                variables[node.name] = [None, False]
            else:
                variables[node.name] = [node.value.topython(), False]

        val = variables[node.name][0]
        # also need to type-check here w.r.t. built-in and
        # possibly custom types
        #
        if val is None:
            if not node.type.nullable:
                raise GraphQLValidationError(
                    "non-nullable variable {!r} is missing a value"
                    .format(node.name), context=node.context)
        else:
            if node.type.list:
                if not isinstance(val, list):
                    raise GraphQLValidationError(
                        "variable {!r} should be a List".format(node.name),
                        context=node.context)
                self._validate_value(
                    node.name, val,
                    GQL_TYPE_NAMES_MAP[node.type.name.name],
                    context=node.context,
                    as_sequence=True)
            else:
                self._validate_value(
                    node.name, val,
                    GQL_TYPE_NAMES_MAP[node.type.name],
                    context=node.context,
                    as_sequence=False)

    def visit_SelectionSet(self, node):
        pathspec = []

        for sel in node.selections:
            if not self._should_include(sel.directives):
                continue

            spec = self.visit(sel)
            if spec is not None:
                pathspec.append(spec)
            pathspec = self.combine_field_results(pathspec)

        return pathspec

    def visit_Field(self, node):
        base = self._context.path[-1]
        base.append(node.name)

        # if this field is a duplicate, that is not identical to the
        # original, throw an exception
        #
        dup = self._context.fields[-1].get(node.name)
        if dup:
            if not ast.nodes_equal(dup, node):
                raise GraphQLValidationError(
                    "field {!r} has ambiguous definition".format(node.name),
                    context=node.context)
            else:

                return
        else:
            self._context.fields[-1][node.name] = node

        # validate the field
        #
        target = baseType = self._context.schema.get(base[0])
        for step in base[1:]:
            target = target.resolve_pointer(self._context.schema, step)
            if target is None:
                raise GraphQLValidationError(
                    "field {!r} is invalid for {}".format(
                        step, baseType.name.name), context=node.context)
            target = target.target

        spec = qlast.SelectPathSpecNode(
            expr=qlast.PathNode(
                steps=[qlast.LinkExprNode(
                    expr=qlast.LinkNode(
                        name=node.name
                    )
                )]
            ),
            where=self._visit_path_where(node.arguments)
        )

        if node.selection_set is not None:
            self._context.fields.append({})
            spec.pathspec = self.visit(node.selection_set)
            self._context.fields.pop()
        base.pop()

        return spec

    def visit_InlineFragment(self, node):
        self._validate_fragment_type(node, node)
        result = self.visit(node.selection_set)
        if node.on is not None:
            self._context.path.pop()
        return result

    def visit_FragmentSpread(self, node):
        frag = self._context.fragments[node.name]
        self._validate_fragment_type(frag, node)
        result = self.visit(frag.selection_set)
        self._context.path.pop()
        return result

    def _validate_fragment_type(self, frag, spread):
        # validate the fragment type w.r.t. the base
        #
        if frag.on is None:
            return

        fragmodule = self._get_module(frag.directives)
        if fragmodule is None:
            fragmodule = self._context.path[0][0][0]
        self._context.path.append([(fragmodule, frag.on)])

    def _visit_path_where(self, arguments):
        if not arguments:
            return None

        def get_path_prefix():
            path = [step
                    for steps in self._context.path
                    for step in steps]
            path = path[0:1] + [step for step in path if type(step) is str]
            prefix = [qlast.PathStepNode(namespace=path[0][0],
                                         expr=path[0][1])]
            prefix.extend([qlast.LinkExprNode(expr=qlast.LinkNode(name=name))
                           for name in path[1:]])
            return prefix

        return self._join_expressions(self._visit_arguments(
                arguments, get_path_prefix=get_path_prefix))

    def _visit_arguments(self, args, *, get_path_prefix):
        result = []
        for arg in args:
            result.append(self.visit_Argument(
                arg, get_path_prefix=get_path_prefix))

        return result

    def visit_Argument(self, node, *, get_path_prefix):
        if node.name[-4:] in GQL_OPS_MAP:
            op = GQL_OPS_MAP[node.name[-4:]]
            name_parts = node.name[:-4]
        else:
            op = ast.ops.EQ
            name_parts = node.name

        name = get_path_prefix()
        name.extend([
            qlast.LinkExprNode(expr=qlast.LinkNode(name=part))
            for part in name_parts.split('__')])
        name = qlast.PathNode(steps=name)

        value = self.visit(node.value)
        if getattr(value, 'index', None):
            # check the variable value
            #
            check_value = self._context.vars[node.value.value][0]
        elif isinstance(value, qlast.SequenceNode):
            check_value = [el.value for el in value.elements]
        else:
            check_value = value.value

        # depending on the operation used, we have a single value
        # or a sequence to validate
        #
        if op in (ast.ops.IN, ast.ops.NOT_IN):
            self._validate_arg(name, check_value,
                               context=node.context, as_sequence=True)
        else:
            self._validate_arg(name, check_value, context=node.context)

        return qlast.BinOpNode(left=name, op=op, right=value)

    def _validate_arg(self, path, value, *, context, as_sequence=False):
        # None is always valid argument for our case, simply means
        # that no filtering is necessary
        #
        if value is None:
            return

        target = self._context.schema.get(
            (path.steps[0].namespace, path.steps[0].expr))
        for step in path.steps[1:]:
            target = target.resolve_pointer(self._context.schema,
                                            step.expr.name).target
        base_t = target.get_implementation_type()

        self._validate_value(step.expr.name, value, base_t,
                             context=context, as_sequence=as_sequence)

    def _validate_value(self, name, value, base_t, *, context,
                        as_sequence=False):
        if as_sequence:
            if not isinstance(value, list):
                raise GraphQLValidationError(
                    "argument {!r} should be a List".format(name),
                    context=context)
        else:
            value = [value]

        for val in value:
            if not issubclass(base_t, PY_COERCION_MAP[type(val)]):
                raise GraphQLValidationError(
                    "value {!r} is not of type {} accepted by {!r}".format(
                        val, base_t, name), context=context)

    def visit_ListLiteral(self, node):
        return qlast.SequenceNode(elements=self.visit(node.value))

    def visit_ObjectLiteral(self, node):
        raise GraphQLValidationError(
            "don't know how to translate an Object literal to EdgeQL",
            context=node.context)

    def visit_Variable(self, node):
        return qlast.ConstantNode(index=node.value[1:])

    def visit_LiteralNode(self, node):
        return qlast.ConstantNode(value=node.value)

    def _join_expressions(self, exprs, op=ast.ops.AND):
        if len(exprs) == 1:
            return exprs[0]

        result = qlast.BinOpNode(
            left=exprs[0],
            op=op,
            right=exprs[1]
        )
        for expr in exprs[2:]:
            result = qlast.BinOpNode(
                left=result,
                op=op,
                right=expr
            )

        return result

    def combine_field_results(self, results, *, flatten=True):
        if flatten:
            flattened = []
            for res in results:
                if ast.is_container(res):
                    flattened.extend(res)
                else:
                    flattened.append(res)
            return flattened
        else:
            return results


def translate(schema, graphql, variables=None):
    if variables is None:
        variables = {}
    parser = gqlparser.GraphQLParser()
    gqltree = parser.parse(graphql)
    context = GraphQLTranslatorContext(schema=schema, variables=variables)
    edge_forest_map = GraphQLTranslator(context=context).visit(gqltree)

    code = []
    for name, (tree, critvars) in sorted(edge_forest_map.items()):
        if name:
            code.append('# {}'.format(name))
        if critvars:
            crit = ['{}={!r}'.format(vname, val) for vname, val in critvars]
            code.append('# critical variables: {}'.format(', '.join(crit)))
        code += [edgeql.generate_source(tree), ';']

    return '\n'.join(code)
