##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang import edgeql
from edgedb.lang.common import ast
from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.graphql import ast as gqlast, parser as gqlparser
from edgedb.lang.schema import error as s_error

from .errors import GraphQLValidationError
from .types import PY_COERCION_MAP, GQL_TYPE_NAMES_MAP, GQLType, GQL_TYPE_MAP


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
        self.include_base = [False]


class GraphQLTranslator(ast.NodeVisitor):
    def get_type(self, name, *, context=None):
        # the type may be from the EdgeDB schema or some special
        # GraphQL type/adapter
        assert not isinstance(name, str)

        try:
            return (GQL_TYPE_MAP.get(name[1]) or
                    self._context.schema.get(name))

        except s_error.SchemaError:
            if context:
                raise GraphQLValidationError(
                    f"{name[1]!r} does not exist in the schema " +
                    f"module {name[0]!r}",
                    context=context)
            raise

    def get_field_type(self, base, name, *, context=None):
        target = base.resolve_pointer(self._context.schema, name)
        if target is None and context:
            raise GraphQLValidationError(
                f"field {name!r} is invalid for {base.name.name}",
                context=context)
        return target.target

    def visit_Document(self, node):
        # we need to index all of the fragments before we process operations
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
        self._context.vars = {
            name: [val, False]
            for name, val in self._context.variables.items()}
        opname = None
        self._context.optype = None

        if node.type is None or node.type == 'query':
            stmt = self._visit_query(node)
            if node.name:
                opname = f'query {node.name}'
        elif node.type == 'mutation':
            stmt = self._visit_mutation(node)
            if node.name:
                opname = f'mutation {node.name}'
        else:
            raise ValueError(f'unsupported definition type: {node.type!r}')

        # produce the list of variables critical to the shape
        # of the query
        critvars = [(name, val) for name, (val, crit)
                    in self._context.vars.items() if crit]
        critvars.sort()

        return (opname, (stmt, critvars))

    def _visit_query(self, node):
        self._context.optype = 'query'
        # populate input variables with defaults, where applicable
        if node.variables:
            self.visit(node.variables)

        module = self._get_module(node.directives)

        # special treatment of the selection_set, different from inner
        # recursion
        query = qlast.SelectQuery(
            result=qlast.Shape(
                expr=qlast.Path(
                    steps=[qlast.ClassRef(name='Query', module='graphql')]
                ),
                elements=[]
            ),
        )

        self._context.fields.append({})
        self._context.path = [[[module, None]]]
        query.result.elements = self.visit(node.selection_set)
        self._context.fields.pop()

        return query

    def _visit_mutation(self, node):
        raise NotImplementedError

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
                        f"'if' argument of {directive.name} " +
                        "directive must be a Boolean",
                        context=directive.context)

                if directive.name == 'include' and cond is False:
                    return False
                elif directive.name == 'skip' and cond is True:
                    return False

        return True

    def visit_VariableDefinition(self, node):
        # it is invalid to declare a non-nullable variable with a default
        if node.value is not None and not node.type.nullable:
            raise GraphQLValidationError(
                f"variable {node.name!r} cannot be non-nullable and have a " +
                "default",
                context=node.context)

        variables = self._context.vars
        if not variables.get(node.name):
            if node.value is None:
                variables[node.name] = [None, False]
            else:
                variables[node.name] = [node.value.topython(), False]

        val = variables[node.name][0]
        # also need to type-check here w.r.t. built-in and
        # possibly custom types
        if val is None:
            if not node.type.nullable:
                raise GraphQLValidationError(
                    f"non-nullable variable {node.name!r} is missing a value",
                    context=node.context)
        else:
            if node.type.list:
                if not isinstance(val, list):
                    raise GraphQLValidationError(
                        f"variable {node.name!r} should be a List",
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
        elements = []

        for sel in node.selections:
            if not self._should_include(sel.directives):
                continue

            spec = self.visit(sel)
            if spec is not None:
                elements.append(spec)

        elements = self.combine_field_results(elements)

        return elements

    def _is_duplicate_field(self, node):
        # if this field is a duplicate, that is not identical to the
        # original, throw an exception
        dup = self._context.fields[-1].get(node.name)
        if dup:
            if not ast.nodes_equal(dup, node):
                raise GraphQLValidationError(
                    f"field {node.name!r} has ambiguous definition",
                    context=node.context)
            else:
                return True
        else:
            self._context.fields[-1][node.name] = node

        return False

    def _is_top_level_field(self, node, fail=None):
        top = False

        base = self._context.path[-1]
        # there is different handling of top-level, built-in and inner
        # fields
        top = (len(self._context.path) == 1 and
               len(base) == 1 and
               base[0][1] is None)

        if top:
            # handle regular fields
            base = self._context.path[0][0] = (self._context.path[0][0][0],
                                               node.name)
        elif fail:
            raise GraphQLValidationError(
                f"field {node.name!r} can only appear at the top-level Query",
                context=node.context)

        return top

    def visit_Field(self, node):
        base = self._context.path[-1]
        # there is different handling of top-level, built-in and inner
        # fields
        if self._is_top_level_field(node):
            result = self._visit_top_field(node)
            # reset the base
            self._context.path[0][0] = (base[0][0], None)
            return result
        else:
            return self._visit_inner_field(node)

    def _visit_inner_field(self, node):
        base = self._context.path[-1]
        base.append(node.name)
        include_base = self._context.include_base[-1]

        if self._is_duplicate_field(node):
            return

        target = self.get_type(base[0])

        for step in base[1:]:
            target = self.get_field_type(target, step, context=node.context)

        # insert normal or specialized link
        steps = []

        if include_base:
            steps.append(qlast.ClassRef(
                module=base[0][0], name=base[0][1]))

        steps.append(qlast.Ptr(
            ptr=qlast.ClassRef(
                name=node.name
            )
        ))

        spec = qlast.ShapeElement(
            expr=qlast.Path(steps=steps),
            where=self._visit_path_where(node.arguments)
        )

        if node.selection_set is not None:
            self._context.fields.append({})
            spec.elements = self.visit(node.selection_set)
            self._context.fields.pop()
        base.pop()

        return spec

    def visit_TypenameField(self, node):
        base = self._context.path[-1]
        include_base = self._context.include_base[-1]

        if self._is_duplicate_field(node):
            return

        # insert normal or specialized link
        steps = []

        if include_base:
            steps.append(qlast.ClassRef(
                module=base[0][0], name=base[0][1]))

        steps.append(qlast.Ptr(
            ptr=qlast.ClassRef(
                name=node.name
            )
        ))

        if self._is_top_level_field(node):
            # this field may appear as the top-level Query field
            spec = qlast.ShapeElement(
                expr=qlast.Path(steps=steps),
                compexpr=qlast.Constant(value='Query'),
            )
            # reset the base
            self._context.path[0][0] = (base[0], None)
        elif base[0][1] == '__schema':
            spec = qlast.ShapeElement(
                expr=qlast.Path(steps=steps),
                compexpr=qlast.Constant(value='__Schema'),
            )
        elif base[0][1] == '__type':
            spec = qlast.ShapeElement(
                expr=qlast.Path(steps=steps),
                compexpr=qlast.Constant(value='__Type'),
            )
        else:
            # create the computable path
            path = [step
                    for psteps in self._context.path
                    for step in psteps]
            path = path[0:1] + [step for step in path if type(step) is str]
            path += ['__class__', 'name']
            typename = [
                qlast.ClassRef(module=path[0][0], name=path[0][1])
            ]
            typename.extend(
                qlast.Ptr(ptr=qlast.ClassRef(name=name))
                for name in path[1:]
            )

            spec = qlast.ShapeElement(
                expr=qlast.Path(steps=steps),
                compexpr=qlast.Path(steps=typename),
            )

        if node.selection_set:
            raise GraphQLValidationError(
                f"field {node.name!r} must not have a sub selection",
                context=node.selection_set.context)
        if node.arguments:
            raise GraphQLValidationError(
                f"field {node.name!r} must not have any arguments",
                context=node.selection_set.context)

        return spec

    def visit_SchemaField(self, node):
        base = self._context.path[-1]

        # this field cannot be anywhere other than in the top-level Query
        self._is_top_level_field(node, fail=True)

        if self._is_duplicate_field(node):
            return

        spec = qlast.ShapeElement(
            expr=qlast.Path(
                steps=[qlast.Ptr(
                    ptr=qlast.ClassRef(
                        name=node.name
                    )
                )]
            ),
            compexpr=qlast.SelectQuery(
                result=qlast.Shape(
                    expr=qlast.Path(
                        steps=[qlast.ClassRef(name='Query', module='graphql')]
                    )
                )
            )
        )

        if node.arguments:
            raise GraphQLValidationError(
                f"field {node.name!r} must not have any arguments",
                context=node.arguments[0].context)

        if node.selection_set is not None:
            self._context.fields.append({})
            spec.compexpr.result.elements = self.visit(node.selection_set)
            self._context.fields.pop()
        else:
            raise GraphQLValidationError(
                f"field {node.name!r} must have a sub selection",
                context=node.context)

        # reset the base
        self._context.path[0][0] = (base[0], None)
        return spec

    def visit_TypeField(self, node):
        base = self._context.path[-1]

        # this field cannot be anywhere other than in the top-level Query
        self._is_top_level_field(node, fail=True)

        if self._is_duplicate_field(node):
            return

        spec = qlast.ShapeElement(
            expr=qlast.Path(
                steps=[qlast.Ptr(
                    ptr=qlast.ClassRef(
                        name=node.name
                    )
                )]
            ),
            compexpr=qlast.SelectQuery(
                result=qlast.Shape(
                    expr=qlast.Path(
                        steps=[qlast.ClassRef(name='Query', module='graphql')]
                    )
                )
            )
        )

        if node.selection_set is not None:
            self._context.fields.append({})
            spec.compexpr.result.elements = self.visit(node.selection_set)
            self._context.fields.pop()
        else:
            raise GraphQLValidationError(
                f"field {node.name!r} must have a sub selection",
                context=node.context)

        # reset the base
        self._context.path[0][0] = (base[0], None)
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
        is_specialized = False

        # validate the fragment type w.r.t. the base
        if frag.on is None:
            return

        fragmodule = self._get_module(frag.directives)
        if fragmodule is None:
            fragmodule = self._context.path[0][0][0]

        frag_path = (fragmodule, frag.on)
        # validate the base if it's nested
        if len(self._context.path) > 0:
            base = self._context.path[-1]
            base_type = self.get_type(base[0])
            for step in base[1:]:
                base_type = self.get_field_type(base_type, step)

            frag_type = self.get_type(frag_path)

            if base_type.issubclass(frag_type):
                # legal hierarchy, no change
                pass
            elif frag_type.issubclass(base_type):
                # specialized link, but still legal
                is_specialized = True
            else:
                raise GraphQLValidationError(
                    f"{base_type.name.name} and {frag_type.name.name} " +
                    "are not related", context=spread.context)

        self._context.path.append([frag_path])
        self._context.include_base.append(is_specialized)

    def _visit_top_field(self, selection):
        concept = selection.name
        where = None

        if concept == '__typename':
            # handle special meta-fields
            expr = self.visit(selection)
        else:
            # handle regular fields
            base = self._context.path[0][0]
            base_type = self.get_type(base, context=selection.context)

            if isinstance(base_type, GQLType):
                # handle special meta-fields
                expr = self.visit(selection)
            else:
                self._context.fields.append({})
                expr = qlast.Shape(
                    expr=qlast.Path(
                        steps=[qlast.ClassRef(module=base[0], name=base[1])]
                    ),
                    elements=self.visit(selection.selection_set)
                )
                self._context.fields.pop()
                if expr:
                    where = self._visit_where(selection.arguments)

        if expr:
            return qlast.ShapeElement(
                expr=qlast.Path(
                    steps=[qlast.ClassRef(name=selection.name)]
                ),
                compexpr=qlast.SelectQuery(
                    result=expr,
                    where=where,
                ),
            )

    def _visit_where(self, arguments):
        if not arguments:
            return None

        def get_path_prefix():
            base = self._context.path[0][0]
            return [qlast.ClassRef(module=base[0], name=base[1])]

        return self._join_expressions(self._visit_arguments(
            arguments, get_path_prefix=get_path_prefix))

    def _visit_path_where(self, arguments):
        if not arguments:
            return None

        def get_path_prefix():
            path = [step
                    for steps in self._context.path
                    for step in steps]
            path = path[0:1] + [step for step in path if type(step) is str]
            prefix = [
                qlast.ClassRef(module=path[0][0], name=path[0][1])
            ]
            prefix.extend(
                qlast.Ptr(ptr=qlast.ClassRef(name=name))
                for name in path[1:]
            )
            return prefix

        return self._join_expressions(
            self._visit_arguments(arguments, get_path_prefix=get_path_prefix))

    def _visit_arguments(self, args, *, get_path_prefix):
        result = []
        for arg in args:
            result.append(self.visit_Argument(
                arg, get_path_prefix=get_path_prefix))

        return result

    def visit_Argument(self, node, *, get_path_prefix):
        op = ast.ops.EQ
        name_parts = node.name

        name = get_path_prefix()
        name.append(qlast.Ptr(ptr=qlast.ClassRef(name=name_parts)))
        name = qlast.Path(steps=name)

        value = self.visit(node.value)
        if isinstance(value, qlast.Parameter):
            # check the variable value
            check_value = self._context.vars[node.value.value][0]
        elif isinstance(value, qlast.Array):
            check_value = [el.value for el in value.elements]
        else:
            check_value = value.value

        # depending on the operation used, we have a single value
        # or a sequence to validate
        if op in (ast.ops.IN, ast.ops.NOT_IN):
            self._validate_arg(name, check_value,
                               context=node.context, as_sequence=True)
            # In EdgeQL `IN` expects a set as a right operand, not an array,
            # so compile this into array_cointains(right, left)
            ql_result = qlast.FunctionCall(
                func=('std', 'array_contains'),
                args=[qlast.FuncArg(arg=value), qlast.FuncArg(arg=name)]
            )

            if op == ast.ops.NOT_IN:
                ql_result = qlast.UnaryOp(
                    op=ast.ops.NOT,
                    operand=ql_result
                )
        else:
            self._validate_arg(name, check_value, context=node.context)

            ql_result = qlast.BinOp(left=name, op=op, right=value)

        return ql_result

    def _validate_arg(self, path, value, *, context, as_sequence=False):
        # None is always valid argument for our case, simply means
        # that no filtering is necessary
        if value is None:
            return

        target = self.get_type(
            (path.steps[0].module, path.steps[0].name))
        for step in path.steps[1:]:
            target = self.get_field_type(target, step.ptr.name)
        base_t = target.get_implementation_type()

        self._validate_value(step.ptr.name, value, base_t,
                             context=context, as_sequence=as_sequence)

    def _validate_value(self, name, value, base_t, *, context,
                        as_sequence=False):
        if as_sequence:
            if not isinstance(value, list):
                raise GraphQLValidationError(
                    f"argument {name!r} should be a List",
                    context=context)
        else:
            value = [value]

        for val in value:
            if not issubclass(base_t, PY_COERCION_MAP[type(val)]):
                raise GraphQLValidationError(
                    f"value {val!r} is not of type {base_t} " +
                    f"accepted by {name!r}",
                    context=context)

    def visit_ListLiteral(self, node):
        return qlast.Array(elements=self.visit(node.value))

    def visit_ObjectLiteral(self, node):
        raise GraphQLValidationError(
            "don't know how to translate an Object literal to EdgeQL",
            context=node.context)

    def visit_Variable(self, node):
        return qlast.Parameter(name=node.value[1:])

    def visit_Literal(self, node):
        return qlast.Constant(value=node.value)

    def _join_expressions(self, exprs, op=ast.ops.AND):
        if not exprs:
            return None
        elif len(exprs) == 1:
            return exprs[0]

        result = qlast.BinOp(
            left=exprs[0],
            op=op,
            right=exprs[1]
        )
        for expr in exprs[2:]:
            result = qlast.BinOp(
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
            code.append(f'# {name}')
        if critvars:
            crit = [f'{vname}={val!r}' for vname, val in critvars]
            code.append(f'# critical variables: {", ".join(crit)}')
        code += [edgeql.generate_source(tree), ';']

    return '\n'.join(code)
