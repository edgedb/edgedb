##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang import edgeql
from edgedb.lang.common import ast
from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.graphql import ast as gqlast, parser as gqlparser
from edgedb.lang.schema import types as s_types

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


class GraphQLTranslator:
    def __init__(self, schema):
        self.schema = schema

    def translate(self, gqltree, variables):
        self._fragments = {
            f.name: f for f in gqltree.definitions
            if isinstance(f, gqlast.FragmentDefinition)
        }

        result = {}

        for definition in gqltree.definitions:
            if isinstance(definition, gqlast.OperationDefinition):

                # create a dict of variables that will be marked as
                # critical or not
                #
                inputvars = {name: [val, False]
                             for name, val in variables.items()}
                query = self._process_definition(definition, inputvars)

                # produce the list of variables critical to the shape
                # of the query
                #
                critvars = [(name, val)
                            for name, (val, crit) in inputvars.items() if crit]
                critvars.sort()

                result[definition.name] = query, critvars

        return result

    def _should_include(self, directives, variables):
        for directive in directives:
            if directive.name in ('include', 'skip'):
                cond = [a.value for a in directive.arguments
                        if a.name == 'if'][0]
                if isinstance(cond, gqlast.Variable):
                    var = variables[cond.value]
                    cond = var[0]
                    var[1] = True  # mark the variable as critical
                else:
                    cond = cond.value

                if not isinstance(cond, bool):
                    raise GraphQLValidationError(
                        "'if' argument of {} directive must be a Boolean"
                        .format(directive.name))

                if directive.name == 'include' and cond is False:
                    return False
                elif directive.name == 'skip' and cond is True:
                    return False

        return True

    def _populate_variable_defaults(self, declarations, variables):
        if not declarations:
            return

        for decl in declarations:
            # it is invalid to declare a non-nullable variable with a default
            #
            if decl.value is not None and not decl.type.nullable:
                raise GraphQLValidationError(
                    "variable {!r} cannot be non-nullable and have a default"
                    .format(decl.name))

            if not variables.get(decl.name):
                if decl.value is None:
                    variables[decl.name] = [None, False]
                else:
                    variables[decl.name] = [decl.value.topython(), False]

            val = variables[decl.name][0]
            # also need to type-check here w.r.t. built-in and
            # possibly custom types
            #
            if val is None:
                if not decl.type.nullable:
                    raise GraphQLValidationError(
                        "non-nullable variable {!r} is missing a value"
                        .format(decl.name))
            else:
                if decl.type.list:
                    if not isinstance(val, list):
                        raise GraphQLValidationError(
                            "variable {!r} should be a List".format(decl.name))
                    self._validate_value(
                        decl.name, val,
                        GQL_TYPE_NAMES_MAP[decl.type.name.name],
                        as_sequence=True)
                else:
                    self._validate_value(
                        decl.name, val,
                        GQL_TYPE_NAMES_MAP[decl.type.name],
                        as_sequence=False)

    def _process_definition(self, definition, variables):
        query = None

        if definition.type is None or definition.type == 'query':
            # populate input variables with defaults, where applicable
            #
            self._populate_variable_defaults(definition.variables, variables)

            module = None
            for directive in definition.directives:
                args = {a.name: a.value.value for a in directive.arguments}
                if directive.name == 'edgedb':
                    module = args['module']

            for selset in definition.selection_set.selections:
                selquery = qlast.SelectQueryNode(
                    namespaces=[
                        qlast.NamespaceAliasDeclNode(
                            namespace=module
                        )
                    ],
                    targets=[
                        self._process_selset(selset, variables)
                    ],
                    where=self._process_select_where(selset, variables)
                )

                if query is None:
                    query = selquery
                else:
                    query = qlast.SelectQueryNode(
                        op=qlast.UNION,
                        op_larg=query,
                        op_rarg=selquery
                    )

        else:
            raise ValueError('unsupported definition type: {!r}'.format(
                definition.type))

        return query

    def _process_selset(self, selset, variables):
        concept = selset.name

        expr = qlast.SelectExprNode(
            expr=qlast.PathNode(
                steps=[qlast.PathStepNode(expr=concept)],
                pathspec=self._process_pathspec(
                    [selset.name],
                    selset.selection_set.selections,
                    variables)
            )
        )

        return expr

    def _process_pathspec(self, base, selections, variables):
        pathspec = []

        for sel in selections:
            if not self._should_include(sel.directives, variables):
                continue

            if isinstance(sel, gqlast.Field):
                pathspec.append(self._process_field(base, sel, variables))
            elif isinstance(sel, gqlast.InlineFragment):
                pathspec.extend(self._process_inline_fragment(
                    base, sel, variables))
            elif isinstance(sel, gqlast.FragmentSpread):
                pathspec.extend(self._process_spread(base, sel, variables))

        return pathspec

    def _process_field(self, base, field, variables):
        base = base + [field.name]
        spec = qlast.SelectPathSpecNode(
            expr=qlast.LinkExprNode(
                expr=qlast.LinkNode(
                    name=field.name
                )
            ),
            where=self._process_path_where(base, field.arguments, variables)
        )

        if field.selection_set is not None:
            spec.pathspec = self._process_pathspec(
                base,
                field.selection_set.selections,
                variables)

        return spec

    def _process_inline_fragment(self, base, inline_frag, variables):
        return self._process_pathspec(base,
                                      inline_frag.selection_set.selections,
                                      variables)

    def _process_spread(self, base, spread, variables):
        return self._process_pathspec(
            base,
            self._fragments[spread.name].selection_set.selections,
            variables)

    def _process_select_where(self, selset, variables):
        if not selset.arguments:
            return None

        def get_path_prefix():
            return [qlast.PathStepNode(expr=selset.name)]

        args = [
            qlast.BinOpNode(left=left, op=op, right=right)
            for left, op, right in self._process_arguments(get_path_prefix,
                                                           selset.arguments,
                                                           variables)]

        return self._join_expressions(args)

    def _process_path_where(self, base, arguments, variables):
        if not arguments:
            return None

        def get_path_prefix():
            prefix = [qlast.PathStepNode(expr=base[0])]
            prefix.extend([qlast.LinkExprNode(expr=qlast.LinkNode(name=name))
                           for name in base[1:]])
            return prefix

        args = [
            qlast.BinOpNode(left=left, op=op, right=right)
            for left, op, right in self._process_arguments(
                get_path_prefix, arguments, variables)]

        return self._join_expressions(args)

    def _process_arguments(self, get_path_prefix, args, variables):
        result = []
        for arg in args:
            if arg.name[-4:] in GQL_OPS_MAP:
                op = GQL_OPS_MAP[arg.name[-4:]]
                name_parts = arg.name[:-4]
            else:
                op = ast.ops.EQ
                name_parts = arg.name

            name = get_path_prefix()
            name.extend([
                qlast.LinkExprNode(expr=qlast.LinkNode(name=part))
                for part in name_parts.split('__')])
            name = qlast.PathNode(steps=name)

            value = self._process_literal(arg.value)
            if getattr(value, 'index', None):
                # check the variable value
                #
                check_value = variables[arg.value.value][0]
            elif isinstance(value, qlast.SequenceNode):
                check_value = [el.value for el in value.elements]
            else:
                check_value = value.value

            # depending on the operation used, we have a single value
            # or a sequence to validate
            #
            if op in (ast.ops.IN, ast.ops.NOT_IN):
                self._validate_arg(name, check_value, as_sequence=True)
            else:
                self._validate_arg(name, check_value)

            result.append((name, op, value))

        return result

    def _validate_arg(self, path, value, *, as_sequence=False):
        # None is always valid argument for our case, simply means
        # that no filtering is necessary
        #
        if value is None:
            return

        target = self.schema.get(path.steps[0].expr)
        for step in path.steps[1:]:
            target = target.resolve_pointer(self.schema,
                                            step.expr.name).target
        base_t = target.get_implementation_type()

        self._validate_value(step.expr.name, value, base_t,
                             as_sequence=as_sequence)

    def _validate_value(self, name, value, base_t, *, as_sequence=False):
        if as_sequence:
            if not isinstance(value, list):
                raise GraphQLValidationError(
                    "argument {!r} should be a List".format(name))
        else:
            value = [value]

        for val in value:
            if not issubclass(base_t, PY_COERCION_MAP[type(val)]):
                raise GraphQLValidationError(
                    "value {!r} is not of type {} accepted by {!r}".format(
                        val, base_t, name))

    def _process_literal(self, literal):
        if isinstance(literal, gqlast.ListLiteral):
            return qlast.SequenceNode(elements=[
                self._process_literal(el) for el in literal.value
            ])
        elif isinstance(literal, gqlast.ObjectLiteral):
            raise GraphQLValidationError(
                "don't know how to translate an Object literal to EdgeQL")
        elif isinstance(literal, gqlast.Variable):
            return qlast.ConstantNode(index=literal.value[1:])
        else:
            return qlast.ConstantNode(value=literal.value)

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


def translate(schema, graphql, variables=None):
    if variables is None:
        variables = {}
    parser = gqlparser.GraphQLParser()
    gqltree = parser.parse(graphql)
    edge_forest_map = GraphQLTranslator(schema).translate(gqltree, variables)

    code = []
    for name, (tree, critvars) in sorted(edge_forest_map.items()):
        if name:
            code.append('# query {}'.format(name))
        if critvars:
            crit = ['{}={!r}'.format(name, val) for name, val in critvars]
            code.append('# critical variables: {}'.format(', '.join(crit)))
        code.append(edgeql.generate_source(tree))

    return '\n'.join(code)
