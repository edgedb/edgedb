#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


from collections import namedtuple
from graphql import graphql as gql_proc, GraphQLString, GraphQLID
import json
import re

from edb.lang import edgeql
from edb.lang.common import ast
from edb.lang.edgeql import ast as qlast
from edb.lang.graphql import ast as gqlast, parser as gqlparser
from edb.lang.schema import error as s_error

from . import types as gt
from .errors import GraphQLValidationError, GraphQLCoreError


class GraphQLTranslatorContext:
    def __init__(self, *, schema, gqlcore, variables, operation_name, query):
        self.schema = schema
        self.variables = variables
        self.operation_name = operation_name
        self.fragments = {}
        self.validated_fragments = {}
        self.vars = {}
        self.fields = []
        self.path = []
        self.include_base = [False]
        self.gql_schema = gt.Schema(gqlcore)
        self.gqlcore_schema = gqlcore._gql_schema
        self.query = query


Step = namedtuple('Step', ['name', 'type'])
Field = namedtuple('Field', ['name', 'value'])


class GraphQLTranslator(ast.NodeVisitor):
    def get_type(self, name, *, context=None):
        # the type may be from the EdgeDB schema or some special
        # GraphQL type/adapter
        assert isinstance(name, str)

        try:
            return self._context.gql_schema.get(name)

        except s_error.SchemaError:
            if context:
                raise GraphQLValidationError(
                    f"{name!r} does not exist in the schema",
                    context=context)
            raise

    def get_field_type(self, base, name, *, args=None, context=None):
        try:
            target = base.get_field_type(name, args)
        except s_error.SchemaError:
            if not context:
                raise
            target = None

        if target is None:
            if context:
                raise GraphQLValidationError(
                    f"field {name!r} is " +
                    f"invalid for {base.short_name}",
                    context=context)

        return target

    def visit_Document(self, node):
        # we need to index all of the fragments before we process operations
        self._context.fragments = {
            f.name: f for f in node.definitions
            if isinstance(f, gqlast.FragmentDefinition)
        }

        gqlresult = gql_proc(
            self._context.gqlcore_schema,
            self._context.query,
            variable_values={
                name[1:]: val for name, val in self._context.variables.items()
            },
            operation_name=self._context.operation_name,
        )

        if gqlresult.errors:
            for err in gqlresult.errors:
                raise GraphQLCoreError(
                    err.message,
                    line=err.locations[0].line,
                    col=err.locations[0].column,
                )

        translated = dict(
            d for d in self.visit(node.definitions) if d is not None)
        eql = next(v for v in translated.values())

        for el in eql[0].result.elements:
            # swap in the json bits
            if (isinstance(el.compexpr, qlast.TypeCast) and
                    el.compexpr.type.maintype.name == 'json'):
                name = el.expr.steps[0].ptr.name
                el.compexpr.expr.value = json.dumps(
                    gqlresult.data[name], indent=4)

        return translated

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

        if (self._context.operation_name and
                node.name != self._context.operation_name):
            return None

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

        # base Query needs to be configured specially
        base = self._context.gql_schema.get('Query')

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
        variables = self._context.vars
        if not variables.get(node.name):
            if node.value is None:
                variables[node.name] = [None, False]
            else:
                variables[node.name] = [node.value.topython(), False]

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
        name = node.alias or node.name
        dup = self._context.fields[-1].get(name)
        if dup:
            return True
        else:
            self._context.fields[-1][name] = node

        return False

    # XXX: this might need to be trimmed
    def _is_top_level_field(self, node, fail=None):
        top = False

        path = self._context.path[-1]
        # there is different handling of top-level, built-in and inner
        # fields
        top = (len(self._context.path) == 1 and
               len(path) == 1 and
               path[0].name is None)

        prevt = path[-1].type
        target = self.get_field_type(
            prevt, node.name,
            args={
                arg.name:
                    (self._context.vars[arg.value.value]
                     if isinstance(arg.value, gqlast.Variable) else
                     arg.value.topython())
                for arg in node.arguments
            },
            context=node.context)
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

    def _prepare_field(self, node):
        path = self._context.path[-1]
        include_base = self._context.include_base[-1]

        is_top = self._is_top_level_field(node)

        spath = self._context.path[-1]
        prevt, target = self._get_parent_and_current_type()

        # insert normal or specialized link
        steps = []
        if include_base:
            base = spath[0].type
            steps.append(qlast.ObjectRef(
                module=base.module, name=base.short_name))
        steps.append(qlast.Ptr(
            ptr=qlast.ObjectRef(
                name=node.name
            )
        ))

        return is_top, path, prevt, target, steps

    def visit_Field(self, node):
        if self._is_duplicate_field(node):
            return

        is_top, path, prevt, target, steps = \
            self._prepare_field(node)

        json_mode = False

        # determine if there needs to be extra subqueries
        if not prevt.dummy and target.dummy:
            json_mode = True

            # this is a special introspection type
            eql, shape, filterable = target.get_template()

            spec = qlast.ShapeElement(
                expr=qlast.Path(
                    steps=[qlast.Ptr(
                        ptr=qlast.ObjectRef(
                            name=node.alias or node.name
                        )
                    )]
                ),
                compexpr=eql,
            )

        elif prevt.is_field_shadowed(node.name):
            if prevt.has_native_field(node.name) and not node.alias:
                spec = filterable = shape = qlast.ShapeElement(
                    expr=qlast.Path(steps=steps),
                )
            else:
                prefix = qlast.Path(steps=self.get_path_prefix(-1))
                eql, shape, filterable = prevt.get_field_template(
                    node.name,
                    parent=prefix,
                    has_shape=bool(node.selection_set)
                )
                spec = qlast.ShapeElement(
                    expr=qlast.Path(
                        steps=[qlast.Ptr(
                            ptr=qlast.ObjectRef(
                                # this is already a sub-query
                                name=node.alias or node.name
                            )
                        )]
                    ),
                    compexpr=eql
                )

        else:
            # if the parent is NOT a shadowed type, we need an explicit SELECT
            eql, shape, filterable = target.get_template()
            spec = qlast.ShapeElement(
                expr=qlast.Path(
                    steps=[qlast.Ptr(
                        ptr=qlast.ObjectRef(
                            # this is already a sub-query
                            name=node.alias or node.name
                        )
                    )]
                ),
                compexpr=eql
            )

        if node.selection_set is not None:
            if json_mode:
                pass

            else:
                # a single recursion target, so we can process
                # selection set now
                self._context.fields.append({})
                vals = self.visit(node.selection_set)
                self._context.fields.pop()

                if shape:
                    shape.elements = vals
                if filterable:
                    filterable.where = self._visit_path_where(
                        node.arguments)

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
        # in case of secondary type, recurse into a copy to avoid
        # memoized results
        selection_set = frag.selection_set

        result = self.visit(selection_set)
        self._context.path.pop()
        return result

    def _validate_fragment_type(self, frag, spread):
        is_specialized = False
        base_type = None

        # validate the fragment type w.r.t. the base
        if frag.on is None:
            return

        # validate the base if it's nested
        if len(self._context.path) > 0:
            path = self._context.path[-1]
            base_type = path[-1].type
            frag_type = self.get_type(frag.on)

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

        return self._join_expressions(
            self._visit_arguments(arguments,
                                  get_path_prefix=self.get_path_prefix))

    def get_path_prefix(self, end_trim=None):
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
        path = path[i + 1:end_trim]
        prefix = [
            qlast.ObjectRef(module=base.module, name=base.short_name)
        ]
        prefix.extend(
            qlast.Ptr(ptr=qlast.ObjectRef(name=step.name))
            for step in path
        )
        return prefix

    def _visit_arguments(self, args, *, get_path_prefix):
        result = []
        for arg in args:
            result.append(self.visit_Argument(
                arg, get_path_prefix=get_path_prefix))

        return result

    def visit_Argument(self, node, *, get_path_prefix):
        op = ast.ops.EQ
        name_parts = node.name

        _, target = self._get_parent_and_current_type()

        name = get_path_prefix()
        name.append(qlast.Ptr(ptr=qlast.ObjectRef(name=name_parts)))
        name = qlast.Path(steps=name)

        value = self.visit(node.value)

        # potentially need to cast the 'name' side into a <str>, so as
        # to be compatible with the 'value'
        typename = target.get_field_type(name_parts).short_name
        if (typename != 'str' and
            gt.EDB_TO_GQL_SCALARS_MAP[typename] in {GraphQLString,
                                                    GraphQLID}):
            name = qlast.TypeCast(
                expr=name,
                type=qlast.TypeName(maintype=qlast.ObjectRef(name='str')),
            )

        return qlast.BinOp(left=name, op=op, right=value)

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
                if isinstance(res, Field):
                    flattened.append(res)
                elif ast.is_container(res):
                    flattened.extend(res)
                else:
                    flattened.append(res)
            return flattened
        else:
            return results


def translate(schema, graphql, *, variables=None, operation_name=None):
    if variables is None:
        variables = {}

    # HACK
    query = re.sub(r'@edgedb\(.*?\)', '', graphql)
    schema2 = gt.GQLCoreSchema(schema)

    parser = gqlparser.GraphQLParser()
    gqltree = parser.parse(graphql)
    context = GraphQLTranslatorContext(
        schema=schema, gqlcore=schema2, query=query,
        variables=variables, operation_name=operation_name)
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
