##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from collections import namedtuple

from edgedb.lang import edgeql
from edgedb.lang.common import ast
from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.graphql import ast as gqlast, parser as gqlparser
from edgedb.lang.schema import error as s_error

from . import types as gt
from .errors import GraphQLValidationError


class GraphQLTranslatorContext:
    def __init__(self, *, schema, variables):
        self.schema = schema
        self.variables = variables
        self.fragments = {}
        self.validated_fragments = {}
        self.vars = {}
        self.fields = []
        self.path = []
        self.include_base = [False]
        self.gql_type_map = {}


Step = namedtuple('Step', ['name', 'type'])


def normalize_name(name, *, simple=False):
    if isinstance(name, str):
        return name
    else:
        if simple:
            return name[1]
        else:
            if not name[0] or name[1] in {'__schema', '__type', '__typename'}:
                return name[1]
            else:
                return f'{name[0]}::{name[1]}'


class GraphQLTranslator(ast.NodeVisitor):
    def get_type(self, name, *, context=None):
        # the type may be from the EdgeDB schema or some special
        # GraphQL type/adapter
        assert not isinstance(name, str)
        norm = normalize_name(name)

        result = self._context.gql_type_map.get(norm)

        if result:
            return result
        else:
            try:
                result = self._context.schema.get(name)
                result = gt._GQLType(
                    edb_base=result,
                    schema=self._context.schema,
                    shadow=True,
                )
                self._context.gql_type_map[norm] = result
                return result

            except s_error.SchemaError:
                if context:
                    raise GraphQLValidationError(
                        f"{name[1]!r} does not exist in the schema " +
                        f"module {name[0]!r}",
                        context=context)
                raise

    def get_field_type(self, base, name, *, context=None):
        if not isinstance(name, str):
            name = (name[1], name[0])

        try:
            target = base.get_field_type(name)
        except s_error.SchemaError:
            if not context:
                raise
            target = None

        if target is None:
            if context:
                raise GraphQLValidationError(
                    f"field {normalize_name(name, simple=True)!r} is " +
                    f"invalid for {base.short_name}",
                    context=context)
        else:
            self._context.gql_type_map[target.name] = target

        return target

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
        # populate input variables with defaults, where applicable
        if node.variables:
            self.visit(node.variables)

        module = self._get_module(node.directives)
        # base Query needs to be configured specially
        base = self._context.gql_type_map.get(f'{module}--Query')
        if not base:
            base = gt.GQLQuery(self._context.schema, module)
            self._context.gql_type_map[base.name] = base

        # special treatment of the selection_set, different from inner
        # recursion
        query = qlast.SelectQuery(
            result=qlast.Shape(
                expr=qlast.Path(
                    steps=[qlast.ObjectRef(name='Query', module='graphql')]
                ),
                elements=[]
            ),
        )

        self._context.fields.append({})
        self._context.path.append([Step(None, base)])
        query.result.elements = self.visit(node.selection_set)
        self._context.fields.pop()
        self._context.path.pop()

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
                    gt.GQL_TYPE_NAMES_MAP[node.type.name.name],
                    context=node.context,
                    as_sequence=True)
            else:
                self._validate_value(
                    node.name, val,
                    gt.GQL_TYPE_NAMES_MAP[node.type.name],
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

        path = self._context.path[-1]
        # there is different handling of top-level, built-in and inner
        # fields
        top = (len(self._context.path) == 1 and
               len(path) == 1 and
               path[0].name is None)

        prevt = path[-1].type
        target = self.get_field_type(prevt, node.name, context=node.context)
        path.append(Step(name=node.name, type=target))

        if not top and fail:
            raise GraphQLValidationError(
                f"field {node.name!r} can only appear at the top-level Query",
                context=node.context)

        return top

    def _get_parent_and_current_type(self):
        path = self._context.path[-1]
        cur = path[-1].type
        if len(path) > 1:
            par = path[-2].type
        else:
            par = self._context.path[-2][-1].type

        return par, cur

    def visit_Field(self, node):
        path = self._context.path[-1]
        # there is different handling of top-level, built-in and inner
        # fields
        self._is_top_level_field(node)
        result = self._visit_inner_field(node)
        # reset the base
        path.pop()
        return result

    def _visit_inner_field(self, node):
        path = self._context.path[-1]
        include_base = self._context.include_base[-1]

        if self._is_duplicate_field(node):
            return

        prevt, target = self._get_parent_and_current_type()

        # insert normal or specialized link
        steps = []
        if include_base:
            base = path[0].type
            steps.append(qlast.ObjectRef(
                module=base.module, name=base.short_name))
        steps.append(qlast.Ptr(
            ptr=qlast.ObjectRef(
                name=node.name
            )
        ))

        # determine if there needs to be extra subqueries
        if prevt.shadow:
            filterable = shape = spec = qlast.ShapeElement(
                expr=qlast.Path(steps=steps),
            )
        else:
            # if the parent is NOT a shadowed type, we need an explicit SELECT
            spec = qlast.ShapeElement(
                expr=qlast.Path(
                    steps=[qlast.Ptr(
                        ptr=qlast.ObjectRef(
                            name=node.name
                        )
                    )]
                ),
                compexpr=qlast.SelectQuery(
                    result=qlast.Shape(
                        expr=qlast.Path(
                            steps=[qlast.ObjectRef(
                                name=target.edb_base.name.name,
                                module=target.edb_base.name.module,
                            )]
                        )
                    )
                )
            )
            filterable = spec.compexpr
            shape = filterable.result

        if node.selection_set is not None:
            self._context.fields.append({})
            shape.elements = self.visit(node.selection_set)
            self._context.fields.pop()
            filterable.where = self._visit_path_where(node.arguments)

        return spec

    def visit_TypenameField(self, node):
        path = self._context.path[-1]
        include_base = self._context.include_base[-1]

        if self._is_duplicate_field(node):
            return

        # insert normal or specialized link
        steps = []

        if include_base:
            base = path[0]
            steps.append(qlast.ObjectRef(
                module=base.module, name=base.short_name))

        steps.append(qlast.Ptr(
            ptr=qlast.ObjectRef(
                name=node.name
            )
        ))

        if self._is_top_level_field(node):
            # reset the base, as there's no further recursion
            path.pop()
            # this field may appear as the top-level Query field
            spec = qlast.ShapeElement(
                expr=qlast.Path(steps=steps),
                compexpr=qlast.Constant(value='Query'),
            )

        else:
            # reset the base, as there's no further recursion
            path.pop()

            # get the prefix field type
            prevt = path[-1].type

            if not prevt.shadow:
                # the parent is a special graphql type of some sort
                spec = qlast.ShapeElement(
                    expr=qlast.Path(steps=steps),
                    compexpr=qlast.Constant(value=prevt.short_name),
                )
            else:
                # shadowed EdgeDB types are pretty straight-forward

                # flatten the path
                path = [step
                        for psteps in self._context.path
                        for step in psteps]

                # find the first shadowed root
                for i, step in enumerate(path):
                    base = step.type
                    if base.shadow:
                        break

                # trim the rest of the path
                path = path[i + 1:]
                typename = [
                    qlast.ObjectRef(module=base.module, name=base.short_name)
                ]
                # convert the path to list of str and add a couple more steps
                path = [step.name for step in path] + ['__type__', 'name']
                typename.extend(
                    qlast.Ptr(ptr=qlast.ObjectRef(name=name))
                    for name in path
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
        path = self._context.path[-1]

        # this field cannot be anywhere other than in the top-level Query
        self._is_top_level_field(node, fail=True)

        if self._is_duplicate_field(node):
            return

        spec = qlast.ShapeElement(
            expr=qlast.Path(
                steps=[qlast.Ptr(
                    ptr=qlast.ObjectRef(
                        name=node.name
                    )
                )]
            ),
            compexpr=qlast.SelectQuery(
                result=qlast.Shape(
                    expr=qlast.Path(
                        steps=[qlast.ObjectRef(name='Query', module='graphql')]
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
        path.pop()
        return spec

    def visit_TypeField(self, node):
        path = self._context.path[-1]

        # this field cannot be anywhere other than in the top-level Query
        self._is_top_level_field(node, fail=True)

        if self._is_duplicate_field(node):
            return

        spec = qlast.ShapeElement(
            expr=qlast.Path(
                steps=[qlast.Ptr(
                    ptr=qlast.ObjectRef(
                        name=node.name
                    )
                )]
            ),
            compexpr=qlast.SelectQuery(
                result=qlast.Shape(
                    expr=qlast.Path(
                        steps=[qlast.ObjectRef(name='Query', module='graphql')]
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
        path.pop()
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
            fragmodule = self._context.path[0][0].type.module

        # validate the base if it's nested
        if len(self._context.path) > 0:
            path = self._context.path[-1]
            base_type = path[-1].type
            frag_type = self.get_type((fragmodule, frag.on))

            if base_type.issubclass(frag_type):
                # legal hierarchy, no change
                pass
            elif frag_type.issubclass(base_type):
                # specialized link, but still legal
                is_specialized = True
            else:
                raise GraphQLValidationError(
                    f"{base_type.short_name} and {frag_type.short_name} " +
                    "are not related", context=spread.context)

        self._context.path.append([Step(frag.on, frag_type)])
        self._context.include_base.append(is_specialized)

    def _visit_where(self, arguments):
        if not arguments:
            return None

        def get_path_prefix():
            path = self._context.path[0]
            return [qlast.ObjectRef(module=path[1].module,
                                    name=path[1].short_name)]

        return self._join_expressions(self._visit_arguments(
            arguments, get_path_prefix=get_path_prefix))

    def _visit_path_where(self, arguments):
        if not arguments:
            return None

        def get_path_prefix():
            # flatten the path
            path = [step
                    for psteps in self._context.path
                    for step in psteps]

            # find the first shadowed root
            for i, step in enumerate(path):
                base = step.type
                if base.shadow:
                    break

            # trim the rest of the path
            path = path[i + 1:]
            prefix = [
                qlast.ObjectRef(module=base.module, name=base.short_name)
            ]
            prefix.extend(
                qlast.Ptr(ptr=qlast.ObjectRef(name=step.name))
                for step in path
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
        name.append(qlast.Ptr(ptr=qlast.ObjectRef(name=name_parts)))
        name = qlast.Path(steps=name)

        value = self.visit(node.value)
        if isinstance(value, qlast.Parameter):
            # check the variable value
            check_value = self._context.vars[node.value.value][0]
        elif isinstance(value, qlast.Array):
            check_value = [el.value for el in value.elements]
        else:
            check_value = value.value

        self._validate_arg(name, check_value, context=node.context)
        ql_result = qlast.BinOp(left=name, op=op, right=value)

        return ql_result

    def _validate_arg(self, qlname, value, *, context, as_sequence=False):
        # None is always valid argument for our case, simply means
        # that no filtering is necessary
        if value is None:
            return

        qlbase = qlname.steps[0]
        target = self.get_type((qlbase.module, qlbase.name))
        for step in qlname.steps[1:]:
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
            if not issubclass(base_t, gt.PY_COERCION_MAP[type(val)]):
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
