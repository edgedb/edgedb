##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang import edgeql
from edgedb.lang.common import ast
from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.graphql import ast as gqlast
from edgedb.lang.graphql import parser as gqlparser


class GraphQLTranslator:
    def __init__(self, schema):
        self.schema = schema

    def translate(self, gqltree):
        self._fragments = {
            f.name: f for f in gqltree.definitions
            if isinstance(f, gqlast.FragmentDefinition)
        }

        for definition in gqltree.definitions:
            if isinstance(definition, gqlast.OperationDefinition):
                query = self._process_definition(definition)

        return query

    def _should_include(self, directives):
        for directive in directives:
            if (directive.name == 'include' and
                    not [a.value.value for a in directive.arguments
                         if a.name == 'if'][0]):
                return False
            elif (directive.name == 'skip' and
                    [a.value.value for a in directive.arguments
                     if a.name == 'if'][0]):
                return False
        return True

    def _process_definition(self, definition):
        query = None

        if definition.type is None or definition.type == 'query':
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
                        self._process_selset(selset)
                    ],
                    where=self._process_select_where(selset)
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

    def _process_selset(self, selset):
        concept = selset.name

        expr = qlast.SelectExprNode(
            expr=qlast.PathNode(
                steps=[qlast.PathStepNode(expr=concept)],
                pathspec=self._process_pathspec(
                    selset.name,
                    selset.selection_set.selections)
            )
        )

        return expr

    def _process_pathspec(self, root, selections):
        pathspec = []

        for sel in selections:
            if not self._should_include(sel.directives):
                continue

            if isinstance(sel, gqlast.Field):
                pathspec.append(self._process_field(root, sel))
            elif isinstance(sel, gqlast.InlineFragment):
                pathspec.extend(self._process_inline_fragment(root, sel))
            elif isinstance(sel, gqlast.FragmentSpread):
                pathspec.extend(self._process_spread(root, sel))

        return pathspec

    def _process_field(self, root, field):
        spec = qlast.SelectPathSpecNode(
            expr=qlast.LinkExprNode(
                expr=qlast.LinkNode(
                    name=field.name
                )
            ),
            where=self._process_path_where(root, field)
        )

        if field.selection_set is not None:
            spec.pathspec = self._process_pathspec(
                root,
                field.selection_set.selections)

        return spec

    def _process_inline_fragment(self, root, inline_frag):
        return self._process_pathspec(root,
                                      inline_frag.selection_set.selections)

    def _process_spread(self, root, spread):
        return self._process_pathspec(
            root,
            self._fragments[spread.name].selection_set.selections)

    def _process_select_where(self, selset):
        if not selset.arguments:
            return None

        args = [
            qlast.BinOpNode(
                left=qlast.PathNode(
                    steps=[qlast.PathStepNode(expr=selset.name), left],
                ),
                op=ast.ops.EQ,
                right=right
            )
            for left, right in self._process_arguments(selset.arguments)]

        return self._join_expressions(args)

    def _process_path_where(self, root, field):
        if not field.arguments:
            return None

        args = [
            qlast.BinOpNode(
                left=qlast.PathNode(
                    steps=[qlast.PathStepNode(expr=root),
                           qlast.LinkExprNode(
                               expr=qlast.LinkNode(
                                   name=field.name
                               )
                           ),
                           left],
                ),
                op=ast.ops.EQ,
                right=right
            )
            for left, right in self._process_arguments(field.arguments)]

        return self._join_expressions(args)

    def _process_arguments(self, args):
        return [
            (
                qlast.LinkExprNode(
                    expr=qlast.LinkNode(
                        name=arg.name
                    )
                ),
                qlast.ConstantNode(value=arg.value.value)
            ) for arg in args]

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


def translate(schema, graphql):
    parser = gqlparser.GraphQLParser()
    gqltree = parser.parse(graphql)
    edgeql_tree = GraphQLTranslator(schema).translate(gqltree)
    return edgeql.generate_source(edgeql_tree)
