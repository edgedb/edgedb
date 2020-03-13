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


from __future__ import annotations

import contextlib
import decimal
import json
import re
from typing import *

import graphql
from graphql.language import ast as gql_ast
from graphql.language import lexer as gql_lexer
from graphql import error as gql_error
from graphql import language as gql_lang

from edb import errors

from edb.common import debug
from edb.common import typeutils

from edb.edgeql import ast as qlast
from edb.edgeql import codegen as ql_codegen
from edb.edgeql import qltypes
from edb.edgeql import quote as eql_quote
from edb.schema import utils as s_utils

from . import types as gt
from . import errors as g_errors


ARG_TYPES = {
    'Int': gql_ast.IntValueNode,
    'String': gql_ast.StringValueNode,
}

REWRITE_TYPE_ERROR = re.compile(
    r"Variable '\$(?P<var_name>_edb_arg__\d+)' of type 'String!'"
    r" used in position expecting type '(?P<type>[^']+)'"
)
INT_FLOAT_ERROR = re.compile(
    r"Variable '\$[^']+' of type 'Int!?'"
    r" used in position expecting type 'Float!?'"
)


class GraphQLTranslatorContext:
    def __init__(self, *, gqlcore: gt.GQLCoreSchema,
                 variables, query, document_ast, operation_name):
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
        self.operation_name = operation_name

        # only used inside ObjectFieldNode
        self.base_expr = None
        self.right_cast = None

        # auto-incrementing counter
        self._counter = 0

    @property
    def counter(self):
        val = self._counter
        self._counter += 1
        return val


class Step(NamedTuple):
    name: Any
    type: Any
    eql_alias: str


class Field(NamedTuple):
    name: Any
    value: Any


class Var(NamedTuple):
    val: Any
    defn: gql_ast.VariableDefinitionNode
    critical: bool


class Operation(NamedTuple):
    name: Any
    stmt: Any
    critvars: Dict[str, Any]
    vars: Dict[str, Any]


class TranspiledOperation(NamedTuple):

    edgeql_ast: qlast.Base
    cacheable: bool
    cache_deps_vars: dict
    variables_desc: dict


class BookkeepDict(dict):

    def __init__(self, values):
        self.update(values)
        self.touched = set()

    def __getitem__(self, key):
        self.touched.add(key)
        return super().__getitem__(key)

    def values(self):
        raise NotImplementedError()

    def items(self):
        raise NotImplementedError()


class GraphQLTranslator:

    def __init__(self, *, context=None):
        self._context = context

    def node_visit(self, node):
        for cls in node.__class__.__mro__:
            method = 'visit_' + cls.__name__
            visitor = getattr(self, method, None)
            if visitor is not None:
                break
        if visitor is None:
            raise AssertionError(f"Unexpected node {node.__class__}")
        result = visitor(node)
        return result

    def visit(self, node):
        if typeutils.is_container(node):
            return [self.node_visit(n) for n in node]
        else:
            return self.node_visit(node)

    def get_loc(self, node):
        if node.loc:
            token = node.loc.start_token
            return token.line, token.column
        else:
            return None

    def get_type(self, name):
        # the type may be from the EdgeDB schema or some special
        # GraphQL type/adapter
        assert isinstance(name, str)
        return self._context.gqlcore.get(name)

    def is_list_type(self, node):
        return (
            isinstance(node, gql_ast.ListTypeNode) or
            (isinstance(node, gql_ast.NonNullTypeNode) and
                self.is_list_type(node.type))
        )

    def get_field_type(self, base, name, *, args=None):
        return base.get_field_type(name)

    def get_optname(self, node):
        if node.name:
            return node.name.value
        else:
            return None

    def visit_DocumentNode(self, node):
        # we need to index all of the fragments before we process operations
        if node.definitions:
            self._context.fragments = {
                f.name.value: f for f in node.definitions
                if isinstance(f, gql_ast.FragmentDefinitionNode)
            }
        else:
            self._context.fragments = {}

        operation_name = self._context.operation_name
        if operation_name is None:
            opnames = []
            for opnode in node.definitions:
                if not isinstance(opnode, gql_ast.OperationDefinitionNode):
                    continue
                opname = None
                if opnode.name:
                    opname = opnode.name.value
                opnames.append(opname)
            if len(opnames) > 1:
                raise errors.QueryError(
                    'must provide operation name if query contains '
                    'multiple operations')
            operation_name = self._context.operation_name = opnames[0]

        if node.definitions:
            translated = {d.name: d
                          for d in self.visit(node.definitions)
                          if d is not None}
        else:
            translated = {}

        if operation_name not in translated:
            if operation_name:
                raise errors.QueryError(
                    f'unknown operation named "{operation_name}"')

        operation = translated[operation_name]
        for el in operation.stmt.result.elements:
            # swap in the json bits
            if (isinstance(el.compexpr, qlast.FunctionCall) and
                    el.compexpr.func == 'to_json'):

                # An introspection query; let graphql evaluate it for us.

                vars = BookkeepDict(self._context.variables)
                result = graphql.execute(
                    self._context.gqlcore.graphql_schema,
                    self._context.document_ast,
                    operation_name=operation_name,
                    variable_values=vars)
                for var_name in vars.touched:
                    var = self._context.vars.get(var_name)
                    self._context.vars[var_name] = var._replace(critical=True)

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
                el.compexpr.args[0] = qlast.StringConstant.from_python(
                    json.dumps(result.data[name]))
                for var in vars.touched:
                    operation.critvars[var] = self._context.vars[var].val

        return translated

    def visit_FragmentDefinitionNode(self, node):
        # fragments are already processed, no need to do anything here
        return None

    def visit_OperationDefinitionNode(self, node):
        # create a dict of variables that will be marked as
        # critical or not
        self._context.vars = {
            name: Var(val=val, defn=None, critical=False)
            for name, val in self._context.variables.items()}
        opname = None
        if node.name:
            opname = node.name.value

        if opname != self._context.operation_name:
            return None

        if (node.operation is None or
                node.operation == graphql.OperationType.QUERY):
            stmt = self._visit_query(node)
        elif (node.operation is None or
                node.operation == graphql.OperationType.MUTATION):
            stmt = self._visit_mutation(node)
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
        self._context.path.append([Step(None, base, None)])
        query.result.elements = self.visit(node.selection_set)
        self._context.fields.pop()
        self._context.path.pop()

        return query

    def _visit_mutation(self, node):
        # populate input variables with defaults, where applicable
        if node.variable_definitions:
            self.visit(node.variable_definitions)

        # base Mutation needs to be configured specially
        base = self._context.gqlcore.get('Mutation')

        # special treatment of the selection_set, different from inner
        # recursion
        query = qlast.SelectQuery(
            result=qlast.Shape(
                expr=qlast.Path(
                    steps=[qlast.ObjectRef(name='Mutation',
                                           module='stdgraphql')]
                ),
                elements=[]
            ),
            limit=qlast.IntegerConstant(value='1')
        )

        self._context.fields.append({})
        self._context.path.append([Step(None, base, None)])
        query.result.elements = self.visit(node.selection_set)
        self._context.fields.pop()
        self._context.path.pop()

        return query

    def _should_include(self, directives):
        for directive in directives:
            if directive.name.value in ('include', 'skip'):
                cond = [a.value for a in directive.arguments
                        if a.name.value == 'if'][0]

                if isinstance(cond, gql_ast.VariableNode):
                    varname = cond.name.value
                    var = self._context.vars[varname]
                    # mark the variable as critical
                    self._context.vars[varname] = var._replace(critical=True)
                    value = var.val

                    if value is None:
                        raise g_errors.GraphQLValidationError(
                            f"no value for the {varname!r} variable",
                            loc=self.get_loc(directive.name))
                elif isinstance(cond, gql_ast.BooleanValueNode):
                    value = cond.value

                if not isinstance(value, bool):
                    raise g_errors.GraphQLValidationError(
                        f"'if' argument of {directive.name.value} " +
                        "directive must be a Boolean",
                        loc=self.get_loc(directive.name))

                if directive.name.value == 'include' and not value:
                    return False
                elif directive.name.value == 'skip' and value:
                    return False

        return True

    def visit_VariableDefinitionNode(self, node):
        varname = node.variable.name.value
        variables = self._context.vars
        var = variables.get(varname)
        if not var:
            if node.default_value is None:
                variables[varname] = Var(
                    val=None, defn=node, critical=False)
            else:
                val = convert_default(node.default_value, varname)
                variables[varname] = Var(val=val, defn=node, critical=False)
        else:
            # we have the variable, but we still need to update the defn field
            variables[varname] = Var(
                val=var.val, defn=node, critical=var.critical)

    def visit_SelectionSetNode(self, node):
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
        path.append(Step(name=node.name.value, type=target, eql_alias=None))

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
            steps.append(qlast.TypeIntersection(
                type=qlast.TypeName(
                    maintype=base.edb_base_name_ast
                )
            ))
        steps.append(qlast.Ptr(
            ptr=qlast.ObjectRef(
                name=node.name.value
            )
        ))

        return is_top, path, prevt, target, steps

    def visit_FieldNode(self, node):
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

        # INSERT mutations have different arguments from queries
        if not is_shadowed and node.name.value.startswith('insert_'):
            # a single recursion target, so we can process
            # selection set now
            with self._update_path_for_eql_alias():
                alias = self._context.path[-1][-1].eql_alias
                self._context.fields.append({})
                shape.elements = self.visit(node.selection_set)
                insert_shapes = self._visit_insert_arguments(node.arguments)
                self._context.fields.pop()

            filterable.aliases = [
                qlast.AliasedExpr(
                    alias=alias,
                    expr=qlast.Set(elements=[
                        qlast.InsertQuery(
                            subject=shape.expr,
                            shape=sh,
                        ) for sh in insert_shapes
                    ])
                )
            ]
            filterable.result.expr = qlast.Path(
                steps=[qlast.ObjectRef(name=alias)])

        elif node.selection_set is not None:
            delete_mode = (not is_shadowed and
                           node.name.value.startswith('delete_'))
            update_mode = (not is_shadowed and
                           node.name.value.startswith('update_'))

            if not json_mode:
                # a single recursion target, so we can process
                # selection set now
                with self._update_path_for_eql_alias(
                        delete_mode or update_mode):
                    # set up a unique alias for the deleted object
                    alias = self._context.path[-1][-1].eql_alias
                    self._context.fields.append({})
                    vals = self.visit(node.selection_set)
                    self._context.fields.pop()

                if shape:
                    shape.elements = vals
                if filterable:
                    where, orderby, offset, limit = \
                        self._visit_query_arguments(node.arguments)

                    filterable.where = where
                    filterable.orderby = orderby
                    filterable.offset = offset
                    filterable.limit = limit

            if delete_mode:
                # this should be a DELETE operation, so we'll rearrange the
                # components of the SelectQuery
                filterable.aliases = [
                    qlast.AliasedExpr(
                        alias=alias,
                        expr=qlast.DeleteQuery(
                            subject=filterable.result.expr,
                            where=filterable.where,
                        )
                    )
                ]
                filterable.where = None
                filterable.result.expr = qlast.Path(
                    steps=[qlast.ObjectRef(name=alias)])
            elif update_mode:
                update_shape = self._visit_update_arguments(node.arguments)
                # this should be an UPDATE operation, so we'll rearrange the
                # components of the SelectQuery and add data operations
                filterable.aliases = [
                    qlast.AliasedExpr(
                        alias=alias,
                        expr=qlast.UpdateQuery(
                            subject=filterable.result.expr,
                            where=filterable.where,
                            shape=update_shape,
                        )
                    )
                ]
                filterable.where = None
                filterable.result.expr = qlast.Path(
                    steps=[qlast.ObjectRef(name=alias)])

        path.pop()
        return spec

    def visit_InlineFragmentNode(self, node):
        self._validate_fragment_type(node, node)
        result = self.visit(node.selection_set)
        if node.type_condition is not None:
            self._context.path.pop()
        return result

    def visit_FragmentSpreadNode(self, node):
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

        self._context.path.append([
            Step(name=frag.type_condition, type=frag_type, eql_alias=None)])
        self._context.include_base.append(is_specialized)

    def _visit_query_arguments(self, arguments):
        where = None
        orderby = []
        first = last = before = after = None

        for arg in arguments:
            if arg.name.value == 'filter':
                where = self.visit(arg.value)
            elif arg.name.value == 'order':
                orderby = self.visit_order(arg.value)
            elif arg.name.value == 'first':
                first = self._visit_pagination_arg(
                    arg, 'Int',
                    expected='an int')
            elif arg.name.value == 'last':
                last = self._visit_pagination_arg(
                    arg, 'Int',
                    expected='an int')
            elif arg.name.value == 'before':
                before = self._visit_pagination_arg(
                    arg, 'String',
                    expected='a string castable to an int')
            elif arg.name.value == 'after':
                after = self._visit_pagination_arg(
                    arg, 'String',
                    expected='a string castable to an int')

        # convert before, after, first and last into offset and limit
        offset, limit = self.get_offset_limit(after, before, first, last)
        # FIXME: it may be a good idea to create special scalar
        # (positive integer) so that the values used for offset and
        # limit can be cast into it and appropriate errors will be
        # produced.

        return where, orderby, offset, limit

    def _visit_pagination_arg(self, node, argtype, expected):
        if isinstance(node.value, gql_ast.VariableNode):
            # variables will be type-checked by this point, so assume
            # the type is valid
            return self.visit(node.value)

        elif not isinstance(node.value, ARG_TYPES[argtype]):
            raise g_errors.GraphQLValidationError(
                f"invalid value for {node.name.value!r}: "
                f"expected {expected}",
                loc=self.get_loc(node.value)) from None

        try:
            return int(node.value.value)
        except (TypeError, ValueError):
            raise g_errors.GraphQLValidationError(
                f"invalid value for {node.name.value!r}: "
                f"expected {expected}, "
                f"got {node.value.value!r}",
                loc=self.get_loc(node.value)) from None

    def get_offset_limit(self, after, before, first, last):
        # if all the parameters here are constants we can compute and
        # compile shorter and simpler OFFSET/LIMIT values
        if any(isinstance(x, qlast.Base)
               for x in [after, before, first, last] if x is not None):
            return self._get_general_offset_limit(after, before, first, last)
        else:
            return self._get_static_offset_limit(after, before, first, last)

    def _get_static_offset_limit(self, after, before, first, last):
        if after is not None:
            # The +1 is to make 'after' into an appropriate index.
            #
            # 0--a--1--b--2--c--3-- ... we call element at
            # index 0 (or "element 0" for short), the element
            # immediately after the mark 0. So after "element
            # 0" really means after "index 1".
            after += 1

        offset = limit = None
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
            offset = qlast.IntegerConstant(value=str(max(0, offset)))
        if limit is not None:
            limit = qlast.IntegerConstant(value=str(max(0, limit)))

        return offset, limit

    def _get_int64_slice_value(self, value):
        if value is None:
            return None
        if isinstance(value, qlast.Base):
            return qlast.TypeCast(
                type=qlast.TypeName(
                    maintype=qlast.ObjectRef(name='int64')),
                expr=value
            )
        else:
            return qlast.IntegerConstant(value=str(value))

    def _get_general_offset_limit(self, after, before, first, last):
        # Convert any static values to corresponding qlast and
        # normalize them as int64.
        after = self._get_int64_slice_value(after)
        before = self._get_int64_slice_value(before)
        first = self._get_int64_slice_value(first)
        last = self._get_int64_slice_value(last)

        offset = limit = None
        # convert before, after, first and last into offset and limit
        if after is not None:
            # The +1 is to make 'after' into an appropriate index.
            #
            # 0--a--1--b--2--c--3-- ... we call element at
            # index 0 (or "element 0" for short), the element
            # immediately after the mark 0. So after "element
            # 0" really means after "index 1".
            offset = qlast.BinOp(
                left=after,
                op='+',
                right=qlast.IntegerConstant(value='1')
            )

        if before is not None:
            # limit = before - (after or 0)
            if after:
                limit = qlast.BinOp(
                    left=before,
                    op='-',
                    right=offset,
                )
            else:
                limit = before

        if first is not None:
            if limit is None:
                limit = first
            else:
                limit = qlast.IfElse(
                    if_expr=first,
                    condition=qlast.BinOp(
                        left=first,
                        op='<',
                        right=limit
                    ),
                    else_expr=limit
                )

        if last is not None:
            if limit is not None:
                if offset:
                    offset = qlast.BinOp(
                        left=offset,
                        op='+',
                        right=qlast.BinOp(
                            left=limit,
                            op='-',
                            right=last
                        )
                    )
                else:
                    offset = qlast.BinOp(
                        left=limit,
                        op='-',
                        right=last
                    )

                limit = qlast.IfElse(
                    if_expr=last,
                    condition=qlast.BinOp(
                        left=last,
                        op='<',
                        right=limit
                    ),
                    else_expr=limit
                )

            else:
                # FIXME: there wasn't any limit, so we can define last
                # in terms of offset alone without negative OFFSET
                # implementation
                raise g_errors.GraphQLTranslationError(
                    f'last translates to a negative OFFSET in '
                    f'EdgeQL which is currently unsupported')

        return offset, limit

    @contextlib.contextmanager
    def _update_path_for_eql_alias(self, alias_needed=True):
        if alias_needed:
            # we need to update the path of the delete field to keep track
            # of the delete alias
            alias = f'x{self._context.counter}'

            # just replace the last path element with the same
            # element, but aliased
            step = self._context.path[-1].pop()
            self._context.path.append([
                Step(name=step.name, type=step.type, eql_alias=alias)])
        yield
        # replace it back
        if alias_needed:
            self._context.path[-1].pop()
            self._context.path[-1].append(step)

    def _visit_update_arguments(self, arguments):
        result = []

        for arg in arguments:
            if arg.name.value == 'data':
                # the node is an ObjectNode with the update spec
                for field in arg.value.fields:
                    fname = field.name.value
                    # capture the full path to the field being updated
                    eqlpath = self.get_path_prefix()
                    eqlpath.append(
                        qlast.Ptr(ptr=qlast.ObjectRef(name=fname)))
                    eqlpath = qlast.Path(steps=eqlpath)

                    # set-up the current path to point to the thing
                    # being updated (so that SELECT can be applied if needed)
                    with self._update_path_for_insert_field(field):
                        _, target = self._get_parent_and_current_type()

                        # normalize potential link lists
                        if isinstance(field.value, gql_ast.ListValueNode):
                            input_data = field.value.values
                        else:
                            input_data = [field.value]
                        vals = [
                            self._visit_update_op(fval, eqlpath, target)
                            for fval in input_data
                        ]

                        if len(vals) == 1:
                            vals = vals[0]
                        else:
                            vals = qlast.Set(elements=vals)

                        result.append(
                            qlast.ShapeElement(
                                expr=qlast.Path(
                                    steps=[
                                        qlast.Ptr(
                                            ptr=qlast.ObjectRef(
                                                name=field.name.value
                                            )
                                        )
                                    ]
                                ),
                                compexpr=vals,
                            )
                        )

        return result

    def _visit_update_op(self, node, eqlpath, ftype):
        # the node is an ObjectNode with the update spec

        # we're at the beginning of a scalar op
        for field in node.fields:
            fname = field.name.value

            # NOTE: there will be more operations in the future
            if fname == 'set':
                return self._visit_insert_value(field.value)
            elif fname == 'clear':
                # just check if the value is True (we need the visit
                # to resolve potential variables)
                if self.visit(field.value).value == 'true':
                    # empty set to clear the value
                    return qlast.Set()
                else:
                    raise g_errors.GraphQLTranslationError(
                        '"clear" parameter can only take the value of "true"')
            elif fname == 'increment':
                return qlast.BinOp(
                    left=eqlpath,
                    op='+',
                    right=self._visit_insert_value(field.value)
                )
            elif fname == 'decrement':
                return qlast.BinOp(
                    left=eqlpath,
                    op='-',
                    right=self._visit_insert_value(field.value)
                )
            elif fname == 'prepend':
                return qlast.BinOp(
                    left=self._visit_insert_value(field.value),
                    op='++',
                    right=eqlpath
                )
            elif fname == 'append':
                return qlast.BinOp(
                    left=eqlpath,
                    op='++',
                    right=self._visit_insert_value(field.value)
                )
            elif fname == 'slice':
                args = field.value.values
                num_args = len(args)
                if num_args == 1:
                    start = self.visit(args[0])
                    stop = None
                elif num_args == 2:
                    start = self.visit(args[0])
                    stop = self.visit(args[1])
                else:
                    raise g_errors.GraphQLTranslationError(
                        f'"slice" must be a list of 1 or 2 integers')

                return qlast.Indirection(
                    arg=eqlpath,
                    indirection=[qlast.Slice(
                        start=start,
                        stop=stop
                    )]
                )
            elif fname == 'add':
                # this is a set
                newset = self._visit_insert_value(field.value)
                newset.elements.append(eqlpath)
                return qlast.UnaryOp(
                    op='DISTINCT',
                    operand=newset,
                )
            elif fname == 'remove':
                alias = f'x{self._context.counter}'
                return qlast.SelectQuery(
                    result_alias=alias,
                    result=eqlpath,
                    where=qlast.BinOp(
                        left=qlast.Path(steps=[qlast.ObjectRef(name=alias)]),
                        op='NOT IN',
                        right=self._visit_insert_value(field.value)
                    )
                )

    def _visit_insert_arguments(self, arguments):
        input_data = []

        for arg in arguments:
            if arg.name.value == 'data':
                # normalize the value to a list
                if isinstance(arg.value, gql_ast.ListValueNode):
                    input_data = arg.value.values
                else:
                    input_data = [arg.value]

        return [self._get_shape_from_input_data(node) for node in input_data]

    def _get_shape_from_input_data(self, node):
        # the node is an ObjectNode with the input spec
        result = []
        for field in node.fields:
            # set-up the current path to point to the thing being inserted
            with self._update_path_for_insert_field(field):
                result.append(
                    qlast.ShapeElement(
                        expr=qlast.Path(
                            steps=[
                                qlast.Ptr(
                                    ptr=qlast.ObjectRef(name=field.name.value)
                                )
                            ]
                        ),
                        compexpr=self._visit_insert_value(field.value),
                    )
                )

        return result

    @contextlib.contextmanager
    def _update_path_for_insert_field(self, node):
        # we need to update the path of the insert field to keep track
        # of the insert types
        path = self._context.path[-1]

        prevt = path[-1].type
        target = self.get_field_type(
            prevt, node.name.value)

        self._context.path.append([
            Step(name=None, type=target, eql_alias=None)])

        yield
        self._context.path.pop()

    def _visit_insert_value(self, node):
        # get the type of the value being inserted
        _, target = self._get_parent_and_current_type()
        if isinstance(node, gql_ast.ObjectValueNode):
            # get a template AST
            eql, shape, filterable = target.get_template()

            if node.fields[0].name.value == 'data':
                # this may be a new object spec
                data_node = node.fields[0].value

                return qlast.InsertQuery(
                    subject=shape.expr,
                    shape=self._get_shape_from_input_data(data_node),
                )
            else:
                eql.result = shape.expr
                # this is a filter spec
                where, orderby, offset, limit = \
                    self._visit_query_arguments(node.fields)
                filterable.where = where
                filterable.orderby = orderby
                filterable.offset = offset
                filterable.limit = limit

                return eql

        elif isinstance(node, gql_ast.ListValueNode) and not target.is_array:
            # not an actual array, but a set represented as a list
            return qlast.Set(elements=[
                self._visit_insert_value(el) for el in node.values])

        else:
            # some scalar value
            val = self.visit(node)

            if target.is_json:
                # JSON must use a converter function and not a cast

                return qlast.FunctionCall(
                    func='to_json',
                    args=[val]
                )
            elif target.edb_base_name != 'std::str':

                # bigint data would require a bigint input, so
                # check if the expression is using a parameter
                if (target.edb_base_name == 'std::bigint'
                        and isinstance(node, gql_ast.VariableNode)
                        and val.type.maintype.name == 'int64'):

                    res = val
                    res.type.maintype.name = target.edb_base_name

                else:
                    res = qlast.TypeCast(
                        expr=val,
                        type=qlast.TypeName(
                            maintype=target.edb_base_name_ast
                        )
                    )

                if target.is_array:
                    res = qlast.TypeCast(
                        expr=val,
                        type=qlast.TypeName(
                            maintype=qlast.ObjectRef(name='array'),
                            subtypes=[res.type],
                        )
                    )

                return res
            else:
                return val

    def get_path_prefix(self, end_trim=None):
        # flatten the path
        path = [step
                for psteps in self._context.path
                for step in psteps]

        # find the first shadowed root
        prev_step = None
        base_step = None
        partial = False
        base_i = 0
        for i, step in enumerate(path):
            cur = step.type

            # if the field is specifically shadowed, then this is
            # appropriate shadow base
            if base_step is None and not partial:
                if (prev_step is not None and
                        prev_step.type.is_field_shadowed(step.name)):
                    base_step = prev_step
                    base_i = i
                    break

                # otherwise the base must be shadowing an entire type
                elif isinstance(cur, gt.GQLShadowType):
                    base_step = step
                    base_i = i

            # we have a base, but we might find out that we need to
            # override it with a partial path
            elif step.name is None and isinstance(cur, gt.GQLShadowType):
                partial = True
                base_step = None
                base_i = i

            # this is where the actual partial path steps start
            elif partial and step.name is not None:
                break

            prev_step = step
        else:
            # we got to the end of the list without hitting other
            # conditions, so that's the base
            if base_step is None:
                base_step = step
                base_i = i

        # trim the rest of the path
        path = path[base_i + 1:end_trim]
        if base_step is None:
            # if the base_step is of the form (None, GQLShadowType), then
            # we don't want any prefix, because we'll use partial paths
            prefix = []
        elif base_step.eql_alias:
            # the root may be aliased
            prefix = [qlast.ObjectRef(name=base_step.eql_alias)]
        else:
            prefix = [base_step.type.edb_base_name_ast]
        prefix.extend(
            qlast.Ptr(ptr=qlast.ObjectRef(name=step.name))
            for step in path
        )

        return prefix

    def visit_ListValueNode(self, node):
        return qlast.Array(elements=self.visit(node.values))

    def visit_ObjectValueNode(self, node):
        # this represents some expression to be used in filter
        result = []
        for field in node.fields:
            result.append(self.visit(field))

        return self._join_expressions(result)

    def visit_ObjectFieldNode(self, node):
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
            if self._context.right_cast is not None:
                value = qlast.TypeCast(
                    expr=value,
                    type=self._context.right_cast,
                )

            return qlast.BinOp(
                left=self._context.base_expr, op=op, right=value)

        # we're at the beginning of a scalar op
        _, target = self._get_parent_and_current_type()

        name = self.get_path_prefix()
        name.append(qlast.Ptr(ptr=qlast.ObjectRef(name=fname)))
        name = qlast.Path(
            steps=name,
            # paths that start with a Ptr are partial
            partial=isinstance(name[0], qlast.Ptr),
        )

        ftype = target.get_field_type(fname)
        typename = ftype.edb_base_name
        if typename not in {'std::str', 'std::uuid'}:
            gql_type = gt.EDB_TO_GQL_SCALARS_MAP.get(typename)
            if gql_type == graphql.GraphQLString:
                # potentially need to cast the 'name' side into a
                # <str>, so as to be compatible with the 'value'
                name = qlast.TypeCast(
                    expr=name,
                    type=qlast.TypeName(maintype=qlast.ObjectRef(name='str')),
                )

        # ### Set up context for the nested visitor ###
        self._context.base_expr = name
        # potentially the right-hand-side needs to be cast into a float
        if ftype.is_float:
            self._context.right_cast = qlast.TypeName(
                maintype=ftype.edb_base_name_ast)
        elif typename == 'std::uuid':
            self._context.right_cast = qlast.TypeName(
                maintype=qlast.ObjectRef(name='uuid'))

        path = self._context.path[-1]
        path.append(Step(name=fname, type=ftype, eql_alias=None))
        try:
            value = self.visit(node.value)
        finally:
            path.pop()
            self._context.right_cast = None
            self._context.base_expr = None

        # we need to cast a target string into <uuid> or enum
        if typename == 'std::uuid' and not isinstance(
                value.right, qlast.TypeCast):
            value.right = qlast.TypeCast(
                expr=value.right,
                type=qlast.TypeName(maintype=ftype.edb_base_name_ast),
            )
        elif ftype.is_enum:
            value.right = qlast.TypeCast(
                expr=value.right,
                type=qlast.TypeName(maintype=ftype.edb_base_name_ast),
            )

        return value

    def visit_order(self, node):
        if not isinstance(node, gql_ast.ObjectValueNode):
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
        if not isinstance(node, gql_ast.ObjectFieldNode):
            raise g_errors.GraphQLTranslationError(
                f'an object is expected for "order"')

        if not isinstance(node.value, gql_ast.ObjectValueNode):
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

    def visit_VariableNode(self, node):
        varname = node.name.value
        var = self._context.vars[varname]

        vartype = var.defn.type
        if isinstance(vartype, gql_ast.NonNullTypeNode):
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

    def visit_StringValueNode(self, node):
        return qlast.StringConstant.from_python(node.value)

    def visit_IntValueNode(self, node):
        # produces an int64 or bigint
        val = int(node.value)
        if s_utils.MIN_INT64 <= val <= s_utils.MAX_INT64:
            return qlast.IntegerConstant(value=str(val))
        else:
            return qlast.BigintConstant(value=f'{val}n')

    def visit_FloatValueNode(self, node):
        # Treat all Float as Decimal by default and downcast as necessary
        return qlast.DecimalConstant(value=f'{node.value}n')

    def visit_BooleanValueNode(self, node):
        value = 'true' if node.value else 'false'
        return qlast.BooleanConstant(value=value)

    def visit_EnumValueNode(self, node):
        return qlast.StringConstant.from_python(node.value)

    def _visit_list_of_inputs(self, inputlist, op):
        if not isinstance(inputlist, gql_ast.ListValueNode):
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


def value_node_from_pyvalue(val: Any):
    if val is None:
        return None
    elif isinstance(val, str):
        val = val.replace('\\', '\\\\')
        value = eql_quote.quote_literal(val)
        return gql_ast.StringValueNode(value=value[1:-1])
    elif isinstance(val, bool):
        return gql_ast.BooleanValueNode(value=bool(val))
    elif isinstance(val, int):
        return gql_ast.IntValueNode(value=str(val))
    elif isinstance(val, (float, decimal.Decimal)):
        return gql_ast.FloatValueNode(value=str(val))
    elif isinstance(val, list):
        return gql_ast.ListValueNode(
            values=[value_node_from_pyvalue(v) for v in val])
    elif isinstance(val, dict):
        return gql_ast.ObjectValueNode(
            fields=[
                gql_ast.ObjectFieldNode(
                    name=n,
                    value=value_node_from_pyvalue(v)
                )
                for n, v in val.items()
            ])
    else:
        raise ValueError(f'unexpected constant type: {type(val)!r}')


def parse_text(query: str) -> graphql.Document:
    try:
        return graphql.parse(query)
    except graphql.GraphQLError as err:
        err_loc = (err.locations[0].line,
                   err.locations[0].column)
        raise g_errors.GraphQLCoreError(err.message, loc=err_loc) from None


class TokenLexer(graphql.language.lexer.Lexer):

    def __init__(self, source, tokens, eof_pos):
        self.__tokens = tokens
        self.__index = 0
        self.__eof_pos = eof_pos
        self.source = source
        kind, start, end, line, col, body = self.__tokens[0]
        self.token = gql_lexer.Token(kind, start, end, line, col, None, body)

    def advance(self) -> gql_lexer.Token:
        self.last_token = self.token
        token = self.token = self.lookahead()
        self.__index += 1
        return token

    def lookahead(self) -> gql_lexer.Token:
        token = self.token
        if token.kind != gql_lexer.TokenKind.EOF:
            if token.next:
                return self.token.next
            kind, start, end, line, col, body = self.__tokens[self.__index + 1]
            token.next = gql_lexer.Token(
                kind, start, end, line, col, token, body)
            return token.next
        else:
            return token


def parse_tokens(
    text: str,
    tokens: List[Tuple[gql_lexer.TokenKind, int, int, int, int, str]]
) -> graphql.Document:
    try:
        src = graphql.Source(text)
        parser = graphql.language.parser.Parser(src)
        parser._lexer = TokenLexer(src, tokens, len(text))
        return parser.parse_document()
    except graphql.GraphQLError as err:
        err_loc = (err.locations[0].line,
                   err.locations[0].column)
        raise g_errors.GraphQLCoreError(err.message, loc=err_loc) from None


def convert_errors(
    errs: List[gql_error.GraphQLError], *,
    substitutions: Optional[Dict[str, Tuple[str, int, int]]],
) -> List[gql_error.GraphQLErrors]:
    result = []
    for err in errs:
        m = REWRITE_TYPE_ERROR.match(err.message)
        if not m:
            # we allow convefsion from Int to Float, and that is allowed by
            # graphql spec. It's unclear why graphql-core chokes on this
            if INT_FLOAT_ERROR.match(err.message):
                continue

            result.append(err)
            continue
        if m.group("type") in ("ID", "ID!"):
            # skip the error, we avoid it in the execution code
            continue
        value, line, col = substitutions[m.group("var_name")]
        err = gql_error.GraphQLError(
            f"Expected type {m.group('type')}, found {value}.")
        err.locations = [gql_lang.SourceLocation(line, col)]
        result.append(err)
    return result


def translate_ast(
    gqlcore: gt.GQLCoreSchema,
    document_ast: graphql.Document,
    *,
    operation_name: Optional[str]=None,
    variables: Dict[str, Any]=None,
    substitutions: Optional[Dict[str, Tuple[str, int, int]]],
) -> TranspiledOperation:

    if variables is None:
        variables = {}

    validation_errors = convert_errors(
        graphql.validate(gqlcore.graphql_schema, document_ast),
        substitutions=substitutions)
    if validation_errors:
        err = validation_errors[0]
        if isinstance(err, graphql.GraphQLError):

            # possibly add additional information and/or hints to the
            # error message
            msg = augment_error_message(gqlcore, err.message)

            err_loc = (err.locations[0].line, err.locations[0].column)
            raise g_errors.GraphQLCoreError(msg, loc=err_loc)
        else:
            raise err

    context = GraphQLTranslatorContext(
        gqlcore=gqlcore, query=None,
        variables=variables, document_ast=document_ast,
        operation_name=operation_name)

    edge_forest_map = GraphQLTranslator(context=context).visit(document_ast)

    if debug.flags.graphql_compile:
        for opname, op in sorted(edge_forest_map.items()):
            print(f'== operationName: {opname!r} =============')
            print(ql_codegen.generate_source(op.stmt))

    op = next(iter(edge_forest_map.values()))

    # generate the specific result
    return TranspiledOperation(
        edgeql_ast=op.stmt,
        cacheable=True,
        cache_deps_vars=dict(op.critvars) if op.critvars else None,
        variables_desc=op.vars,
    )


def augment_error_message(gqlcore: gt.GQLCoreSchema, message: str):
    # If the error is about wrong Query field, we can add more details
    # about what seems to have gone wrong. The type is missing,
    # possibly because this connection is to the wrong DB. However,
    # this is only relevant if the message doesn't contain a hint already.
    if (re.match(r"^Cannot query field '(.+?)' on type 'Query'\.$", message)):
        field = message.split("'", 2)[1]
        name = gqlcore.gql_to_edb_name(field)

        message += (
            f' There\'s no corresponding type or alias "{name}" exposed in '
            'EdgeDB. Please check the configuration settings for this port '
            'to make sure that you\'re connecting to the right database.'
        )

    return message


def convert_default(
    node: gql_ast.ValueNode,
    varname: str
) -> Union[str, float, int, bool]:
    if isinstance(node, (gql_ast.StringValueNode, gql_ast.BooleanValueNode)):
        return node.value
    elif isinstance(node, gql_ast.IntValueNode):
        return int(node.value)
    elif isinstance(node, gql_ast.FloatValueNode):
        return float(node.value)
    else:
        raise errors.QueryError(
            f"Only scalar defaults are allowed. "
            f"Variable {varname!r} has non-scalar default value.")
