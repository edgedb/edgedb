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


import json
import typing

import graphql
from graphql.language import ast as gql_ast

from edb.common import debug
from edb.common import typeutils

from edb.edgeql import ast as qlast
from edb.edgeql import codegen as ql_codegen
from edb.edgeql import qltypes
from edb.edgeql import quote as eql_quote

from . import types as gt
from . import errors as g_errors
from . import codegen as gqlcodegen


class GraphQLTranslatorContext:
    def __init__(self, *, gqlcore: gt.GQLCoreSchema,
                 variables, query, document_ast):
        self.variables = variables
        self.fragments = {}
        self.validated_fragments = {}
        self.vars = {}
        self.fields = []
        self.path = []
        self.filter = None
        self.include_base = [False]
        self.gqlcore = gqlcore
        self.query = query
        self.document_ast = document_ast


class Step(typing.NamedTuple):
    name: object
    type: object


class Field(typing.NamedTuple):
    name: object
    value: object


class Var(typing.NamedTuple):
    val: object
    defn: gql_ast.VariableDefinition
    critical: bool


class Operation(typing.NamedTuple):
    name: object
    stmt: object
    critvars: object
    vars: object


class GraphQLTranslator:

    def __init__(self, *, context=None):
        self._context = context

    def node_visit(self, node):
        for cls in node.__class__.__mro__:
            method = 'visit_' + cls.__name__
            visitor = getattr(self, method, None)
            if visitor is not None:
                break
        result = visitor(node)
        return result

    def visit(self, node):
        if typeutils.is_container(node):
            return [self.node_visit(n) for n in node]
        else:
            return self.node_visit(node)

    def get_loc(self, node):
        if node.loc:
            position = node.loc.start
            lines = self._context.query[:position].splitlines()
            if lines:
                line = len(lines)
                column = len(lines[-1]) + 1
            else:
                line = 1
                column = 1
            return (line, column)
        else:
            return None

    def get_type(self, name):
        # the type may be from the EdgeDB schema or some special
        # GraphQL type/adapter
        assert isinstance(name, str)
        return self._context.gqlcore.get(name)

    def is_list_type(self, node):
        return (
            isinstance(node, gql_ast.ListType) or
            (isinstance(node, gql_ast.NonNullType) and
                self.is_list_type(node.type))
        )

    def get_field_type(self, base, name, *, args=None):
        return base.get_field_type(name)

    def get_optname(self, node):
        if node.name:
            return node.name.value
        else:
            return None

    def visit_Document(self, node):
        # we need to index all of the fragments before we process operations
        if node.definitions:
            self._context.fragments = {
                f.name.value: f for f in node.definitions
                if isinstance(f, gql_ast.FragmentDefinition)
            }
        else:
            self._context.fragments = {}

        if node.definitions:
            translated = {d.name: d
                          for d in self.visit(node.definitions)
                          if d is not None}
        else:
            translated = {}

        for opname in translated:
            stmt = translated[opname].stmt

            for el in stmt.result.expr.elements:
                # swap in the json bits
                if (isinstance(el.compexpr, qlast.FunctionCall) and
                        el.compexpr.func == 'to_json'):

                    # An introspection query; let graphql evaluate it for us.
                    result = graphql.execute(
                        self._context.gqlcore.graphql_schema,
                        self._context.document_ast,
                        operation_name=opname,
                        variables=self._context.variables)

                    if result.errors:
                        err = result.errors[0]
                        if isinstance(err, graphql.GraphQLError):
                            err_loc = (err.locations[0].line,
                                       err.locations[0].column)
                            raise g_errors.GraphQLCoreError(
                                err.message,
                                loc=err_loc)
                        else:
                            raise err

                    name = el.expr.steps[0].ptr.name
                    el.compexpr.args[0].arg = qlast.StringConstant.from_python(
                        json.dumps(result.data[name]))

        return translated

    def visit_FragmentDefinition(self, node):
        # fragments are already processed, no need to do anything here
        return None

    def visit_OperationDefinition(self, node):
        # create a dict of variables that will be marked as
        # critical or not
        self._context.vars = {
            name: Var(val=val, defn=None, critical=False)
            for name, val in self._context.variables.items()}
        opname = None
        if node.name:
            opname = node.name.value

        if node.operation is None or node.operation == 'query':
            stmt = self._visit_query(node)
        else:
            raise ValueError(f'unsupported operation: {node.operation!r}')

        # produce the list of variables critical to the shape
        # of the query
        critvars = {name: var.val for name, var
                    in self._context.vars.items() if var.critical}
        # variables that were defined in this operation
        defvars = {name: var.val for name, var in self._context.vars.items()
                   if var.defn is not None}

        return Operation(
            name=opname,
            stmt=stmt,
            critvars=critvars,
            vars=defvars,
        )

    def _visit_query(self, node):
        # populate input variables with defaults, where applicable
        if node.variable_definitions:
            self.visit(node.variable_definitions)

        # base Query needs to be configured specially
        base = self._context.gqlcore.get('Query')

        # special treatment of the selection_set, different from inner
        # recursion
        query = qlast.SelectQuery(
            result=qlast.Shape(
                expr=qlast.Path(
                    steps=[qlast.ObjectRef(name='Query',
                                           module='stdgraphql')]
                ),
                elements=[]
            ),
            limit=qlast.IntegerConstant(value='1')
        )

        self._context.fields.append({})
        self._context.path.append([Step(None, base)])
        query.result.elements = self.visit(node.selection_set)
        self._context.fields.pop()
        self._context.path.pop()

        query.result = qlast.TypeCast(
            expr=query.result,
            type=qlast.TypeName(
                maintype=qlast.ObjectRef(module='std', name='json')))

        return query

    def _should_include(self, directives):
        for directive in directives:
            if directive.name.value in ('include', 'skip'):
                cond = [a.value for a in directive.arguments
                        if a.name.value == 'if'][0]

                if isinstance(cond, gql_ast.Variable):
                    varname = cond.name.value
                    var = self._context.vars[varname]
                    # mark the variable as critical
                    self._context.vars[varname] = var._replace(critical=True)
                    cond = var.val

                    if cond is None:
                        raise g_errors.GraphQLValidationError(
                            f"no value for the {varname!r} variable",
                            loc=self.get_loc(directive.name))

                if not isinstance(cond, gql_ast.BooleanValue):
                    raise g_errors.GraphQLValidationError(
                        f"'if' argument of {directive.name.value} " +
                        "directive must be a Boolean",
                        loc=self.get_loc(directive.name))

                if directive.name.value == 'include' and cond.value == 'false':
                    return False
                elif directive.name.value == 'skip' and cond.value == 'true':
                    return False

        return True

    def visit_VariableDefinition(self, node):
        varname = node.variable.name.value
        variables = self._context.vars
        var = variables.get(varname)
        if not var:
            if node.default_value is None:
                variables[varname] = Var(
                    val=None, defn=node, critical=False)
            else:
                variables[varname] = Var(
                    val=node.default_value, defn=node, critical=False)
        else:
            # we have the variable, but we still need to update the defn field
            variables[varname] = Var(
                val=var.val, defn=node, critical=var.critical)

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
        name = (node.alias or node.name).value
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
            prevt, node.name.value)
        path.append(Step(name=node.name.value, type=target))

        if not top and fail:
            raise g_errors.GraphQLValidationError(
                f"field {node.name.value!r} can only appear "
                f"at the top-level Query",
                loc=self.get_loc(node))

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
            steps.append(qlast.TypeIndirection(
                type=qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        module=base.module,
                        name=base.short_name
                    ),
                ),
            ))
        steps.append(qlast.Ptr(
            ptr=qlast.ObjectRef(
                name=node.name.value
            )
        ))

        return is_top, path, prevt, target, steps

    def visit_Field(self, node):
        if self._is_duplicate_field(node):
            return

        is_top, path, prevt, target, steps = \
            self._prepare_field(node)

        json_mode = False
        is_shadowed = prevt.is_field_shadowed(node.name.value)

        # determine if there needs to be extra subqueries
        if not prevt.dummy and target.dummy:
            json_mode = True

            # this is a special introspection type
            eql, shape, filterable = target.get_template()

            spec = qlast.ShapeElement(
                expr=qlast.Path(
                    steps=[qlast.Ptr(
                        ptr=qlast.ObjectRef(
                            name=(node.alias or node.name).value
                        )
                    )]
                ),
                compexpr=eql,
            )

        elif is_shadowed and not node.alias:
            # shadowed field that doesn't need an alias
            spec = filterable = shape = qlast.ShapeElement(
                expr=qlast.Path(steps=steps),
            )

        elif not node.selection_set or is_shadowed and node.alias:
            # this is either an unshadowed terminal field or an aliased
            # shadowed field
            prefix = qlast.Path(steps=self.get_path_prefix(-1))
            eql, shape, filterable = prevt.get_field_template(
                node.name.value,
                parent=prefix,
                has_shape=bool(node.selection_set)
            )
            spec = qlast.ShapeElement(
                expr=qlast.Path(
                    steps=[qlast.Ptr(
                        ptr=qlast.ObjectRef(
                            # this is already a sub-query
                            name=(node.alias or node.name).value
                        )
                    )]
                ),
                compexpr=eql,
                # preserve the original cardinality of the computable
                # aliased fields
                cardinality=prevt.get_field_cardinality(node.name.value),
            )

        else:
            # if the parent is NOT a shadowed type, we need an explicit SELECT
            eql, shape, filterable = target.get_template()
            spec = qlast.ShapeElement(
                expr=qlast.Path(
                    steps=[qlast.Ptr(
                        ptr=qlast.ObjectRef(
                            # this is already a sub-query
                            name=(node.alias or node.name).value
                        )
                    )]
                ),
                compexpr=eql,
                # preserve the original cardinality of the computable,
                # which is basically one of the top-level query
                # fields, all of which are returning lists
                cardinality=qltypes.Cardinality.MANY,
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
                    where, orderby, offset, limit = \
                        self._visit_arguments(node.arguments)
                    filterable.where = where
                    filterable.orderby = orderby
                    filterable.offset = offset
                    filterable.limit = limit

        path.pop()
        return spec

    def visit_InlineFragment(self, node):
        self._validate_fragment_type(node, node)
        result = self.visit(node.selection_set)
        if node.type_condition is not None:
            self._context.path.pop()
        return result

    def visit_FragmentSpread(self, node):
        frag = self._context.fragments[node.name.value]
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
        if frag.type_condition is None:
            return

        # validate the base if it's nested
        if len(self._context.path) > 0:
            path = self._context.path[-1]
            base_type = path[-1].type
            frag_type = self.get_type(frag.type_condition.name.value)

            if base_type.issubclass(frag_type):
                # legal hierarchy, no change
                pass
            elif frag_type.issubclass(base_type):
                # specialized link, but still legal
                is_specialized = True
            else:
                raise g_errors.GraphQLValidationError(
                    f"{base_type.short_name} and {frag_type.short_name} " +
                    "are not related",
                    loc=self.get_loc(frag))

        self._context.path.append([Step(frag.type_condition, frag_type)])
        self._context.include_base.append(is_specialized)

    def _visit_arguments(self, arguments):
        where = offset = limit = None
        orderby = []
        first = last = before = after = None

        def validate_positive_int(arg):
            if not isinstance(arg.value, gql_ast.IntValue):
                raise g_errors.GraphQLValidationError(
                    f"invalid value for {arg.name.value!r}: "
                    f"expected an int",
                    loc=self.get_loc(arg.value)) from None
            try:
                val = int(arg.value.value)
            except (TypeError, ValueError):
                raise g_errors.GraphQLValidationError(
                    f"invalid value for {arg.name.value!r}: "
                    f"expected an int, got {arg.value.value!r}",
                    loc=self.get_loc(arg.value)) from None
            if val < 0:
                raise g_errors.GraphQLValidationError(
                    f"invalid value for {arg.name.value!r}: "
                    f"expected a non-negative int",
                    loc=self.get_loc(arg.value))
            return val

        def validate_positive_str_int(arg):
            if not isinstance(arg.value, gql_ast.StringValue):
                raise g_errors.GraphQLValidationError(
                    f"invalid value for {arg.name.value!r}: "
                    f"expected a string castable to an int",
                    loc=self.get_loc(arg.value)) from None
            try:
                val = int(arg.value.value)
            except (TypeError, ValueError):
                raise g_errors.GraphQLValidationError(
                    f"invalid value for {arg.name.value!r}: "
                    f"expected a string castable to a non-negative int, "
                    f"got {arg.value.value!r}",
                    loc=self.get_loc(arg.value)) from None
            if val < 0:
                raise g_errors.GraphQLValidationError(
                    f"invalid value for {arg.name.value!r}: "
                    f"expected a string castable to a non-negative int, "
                    f"got {val}",
                    loc=self.get_loc(arg.value))
            return val

        for arg in arguments:
            if arg.name.value == 'filter':
                where = self.visit(arg.value)
            elif arg.name.value == 'order':
                orderby = self.visit_order(arg.value)
            elif arg.name.value == 'first':
                first = validate_positive_int(arg)
            elif arg.name.value == 'last':
                last = validate_positive_int(arg)
            elif arg.name.value == 'before':
                before = validate_positive_str_int(arg)
            elif arg.name.value == 'after':
                after = validate_positive_str_int(arg)
                # The +1 is to make 'after' into an appropriate index.
                #
                # 0--a--1--b--2--c--3-- ... we call element at
                # index 0 (or "element 0" for short), the element
                # immediately after the mark 0. So after "element
                # 0" really means after "index 1".
                after += 1

        # convert before, after, first and last into offset and limit
        if after is not None:
            offset = after
        if before is not None:
            limit = before - (after or 0)
        if first is not None:
            if limit is None:
                limit = first
            else:
                limit = min(first, limit)
        if last is not None:
            if limit is not None:
                if last < limit:
                    offset = (offset or 0) + limit - last
                    limit = last
            else:
                # FIXME: there wasn't any limit, so we can define last
                # in terms of offset alone without negative OFFSET
                # implementation
                raise g_errors.GraphQLTranslationError(
                    f'last={last} translates to a negative OFFSET in '
                    f'EdgeQL which is currently unsupported')

        # convert integers into qlast literals
        if offset is not None and not isinstance(offset, qlast.Base):
            offset = qlast.BaseConstant.from_python(max(0, offset))
        if limit is not None:
            limit = qlast.BaseConstant.from_python(max(0, limit))

        return where, orderby, offset, limit

    def get_path_prefix(self, end_trim=None):
        # flatten the path
        path = [step
                for psteps in self._context.path
                for step in psteps]

        # find the first shadowed root
        prev_base = None
        for i, step in enumerate(path):
            base = step.type

            # if the field is specifically shadowed, then this is
            # appropriate shadow base
            if (prev_base is not None and
                    prev_base.is_field_shadowed(step.name)):
                base = prev_base
                break

            # otherwise the base must be shadowing an entire type
            elif isinstance(base, gt.GQLShadowType):
                break

            prev_base = base

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

    def visit_ListValue(self, node):
        return qlast.Array(elements=self.visit(node.values))

    def visit_ObjectValue(self, node):
        # this represents some expression to be used in filter
        result = []
        for field in node.fields:
            result.append(self.visit(field))

        return self._join_expressions(result)

    def visit_ObjectField(self, node):
        fname = node.name.value

        # handle boolean ops
        if fname == 'and':
            return self._visit_list_of_inputs(node.value, 'AND')
        elif fname == 'or':
            return self._visit_list_of_inputs(node.value, 'OR')
        elif fname == 'not':
            return qlast.UnaryOp(op='NOT', operand=self.visit(node.value))

        # handle various scalar ops
        op = gt.GQL_TO_OPS_MAP.get(fname)

        if op:
            value = self.visit(node.value)
            return qlast.BinOp(left=self._context.filter, op=op, right=value)

        # we're at the beginning of a scalar op
        _, target = self._get_parent_and_current_type()

        name = self.get_path_prefix()
        name.append(qlast.Ptr(ptr=qlast.ObjectRef(name=fname)))
        name = qlast.Path(steps=name)

        typename = target.get_field_type(fname).short_name
        if typename not in {'str', 'uuid'}:
            if gt.EDB_TO_GQL_SCALARS_MAP[typename] == graphql.GraphQLString:
                # potentially need to cast the 'name' side into a
                # <str>, so as to be compatible with the 'value'
                name = qlast.TypeCast(
                    expr=name,
                    type=qlast.TypeName(maintype=qlast.ObjectRef(name='str')),
                )

        self._context.filter = name

        value = self.visit(node.value)
        # we need to cast a target string into <uuid>
        if typename == 'uuid' and not isinstance(value.right, qlast.TypeCast):
            value.right = qlast.TypeCast(
                expr=value.right,
                type=qlast.TypeName(maintype=qlast.ObjectRef(name='uuid')),
            )

        return value

    def visit_order(self, node):
        if not isinstance(node, gql_ast.ObjectValue):
            raise g_errors.GraphQLTranslationError(
                f'an object is expected for "order"')

        # if there is no specific ordering, then order by id
        if not node.fields:
            return [qlast.SortExpr(
                path=qlast.Path(
                    steps=[qlast.Ptr(ptr=qlast.ObjectRef(name='id'))],
                    partial=True,
                ),
                direction=qlast.SortAsc,
            )]

        # Ordering is handled by specifying a list of special Ordering objects.
        # Validation is already handled by this point.
        orderby = []
        for enum in node.fields:
            name, direction, nulls = self._visit_order_item(enum)
            orderby.append(qlast.SortExpr(
                path=qlast.Path(
                    steps=[qlast.Ptr(ptr=qlast.ObjectRef(name=name))],
                    partial=True,
                ),
                direction=direction,
                nones_order=nulls,
            ))

        return orderby

    def _visit_order_item(self, node):
        if not isinstance(node, gql_ast.ObjectField):
            raise g_errors.GraphQLTranslationError(
                f'an object is expected for "order"')

        if not isinstance(node.value, gql_ast.ObjectValue):
            raise g_errors.GraphQLTranslationError(
                f'an object is expected for "order"')

        name = node.name.value
        direction = nulls = None

        for part in node.value.fields:
            if part.name.value == 'dir':
                direction = part.value.value
            if part.name.value == 'nulls':
                nulls = part.value.value

        # direction is a required field, so we can rely on it having
        # one of two values
        if direction == 'ASC':
            direction = qlast.SortAsc
            # nulls are optional, but are 'SMALLEST' by default
            if nulls == 'BIGGEST':
                nulls = qlast.NonesLast
            else:
                nulls = qlast.NonesFirst

        else:  # DESC
            direction = qlast.SortDesc
            # nulls are optional, but are 'SMALLEST' by default
            if nulls == 'BIGGEST':
                nulls = qlast.NonesFirst
            else:
                nulls = qlast.NonesLast

        return name, direction, nulls

    def visit_Variable(self, node):
        varname = node.name.value
        var = self._context.vars[varname]

        vartype = var.defn.type
        if isinstance(vartype, gql_ast.NonNullType):
            # TODO: Add non-null validation to the produced EdgeQL?
            vartype = vartype.type

        casttype = qlast.TypeName(
            maintype=qlast.ObjectRef(
                name=gt.GQL_TO_EDB_SCALARS_MAP[vartype.name.value])
        )
        # potentially this is an array
        if self.is_list_type(vartype):
            casttype = qlast.TypeName(
                maintype=qlast.ObjectRef(name='array'),
                subtypes=[casttype]
            )

        return qlast.TypeCast(
            type=casttype,
            expr=qlast.Parameter(name=varname)
        )

    def visit_StringValue(self, node):
        return qlast.StringConstant(value=node.value, quote='"')

    def visit_IntValue(self, node):
        return qlast.IntegerConstant(value=node.value)

    def visit_FloatValue(self, node):
        return qlast.FloatConstant(value=node.value)

    def visit_BooleanValue(self, node):
        value = 'true' if node.value else 'false'
        return qlast.BooleanConstant(value=value)

    def _visit_list_of_inputs(self, inputlist, op):
        if not isinstance(inputlist, gql_ast.ListValue):
            raise g_errors.GraphQLTranslationError(
                f'a list was expected')

        result = [self.visit(node) for node in inputlist.values]
        return self._join_expressions(result, op)

    def _join_expressions(self, exprs, op='AND'):
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
                elif typeutils.is_container(res):
                    flattened.extend(res)
                else:
                    flattened.append(res)
            return flattened
        else:
            return results


def value_node_from_pyvalue(val: object):
    if isinstance(val, str):
        val = val.replace('\\', '\\\\')
        value = eql_quote.quote_literal(val)
        return gql_ast.StringValue(value=value[1:-1])
    elif isinstance(val, bool):
        return gql_ast.BooleanValue(value=bool(val))
    elif isinstance(val, int):
        return gql_ast.IntValue(value=str(val))
    elif isinstance(val, float):
        return gql_ast.FloatValue(value=str(val))
    elif isinstance(val, list):
        return gql_ast.ListValue(
            values=[value_node_from_pyvalue(v) for v in val])
    elif isinstance(val, dict):
        return gql_ast.ObjectValue(
            fields=[
                gql_ast.ObjectField(name=n, value=value_node_from_pyvalue(v))
                for n, v in val.items()
            ])
    else:
        raise ValueError(f'unexpected constant type: {type(val)!r}')


def translate(gqlcore: gt.GQLCoreSchema, query, *, variables=None):
    try:
        document_ast = graphql.parse(query)
    except graphql.GraphQLError as err:
        err_loc = (err.locations[0].line,
                   err.locations[0].column)
        raise g_errors.GraphQLCoreError(err.message, loc=err_loc) from None

    if variables is None:
        variables = {}

    if debug.flags.graphql_compile:
        debug.header('GraphQL compiler')
        print(query)
        print(f'variables: {variables}')

    gql_vars = {}
    for n, v in variables.items():
        gql_vars[n] = value_node_from_pyvalue(v)

    validation_errors = graphql.validate(gqlcore.graphql_schema, document_ast)
    if validation_errors:
        err = validation_errors[0]
        if isinstance(err, graphql.GraphQLError):
            err_loc = (err.locations[0].line, err.locations[0].column)
            raise g_errors.GraphQLCoreError(err.message, loc=err_loc)
        else:
            raise err

    context = GraphQLTranslatorContext(
        gqlcore=gqlcore, query=query,
        variables=gql_vars, document_ast=document_ast)

    results = {}
    edge_forest_map = GraphQLTranslator(context=context).visit(document_ast)

    if debug.flags.graphql_compile:
        for opname, op in sorted(edge_forest_map.items()):
            print(f'== operationName: {opname!r} =============')
            print(ql_codegen.generate_source(op.stmt))

    for opname, op in sorted(edge_forest_map.items()):
        # convert critvars and vars to JSON-like format
        critvars = {}
        for name, val in op.critvars.items():
            if val is not None:
                critvars[name] = json.loads(gqlcodegen.generate_source(val))

        defvars = {}
        for name, val in op.vars.items():
            if val is not None:
                defvars[name] = json.loads(gqlcodegen.generate_source(val))

        # generate the specific result
        results[opname] = {
            'operation_name': opname,
            'edgeql': op.stmt,
            'cacheable': True,
            'cache_deps_vars': dict(critvars) if critvars else None,
            'variables_desc': defvars,
        }

    return results
