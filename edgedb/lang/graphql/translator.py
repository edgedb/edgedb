##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang import caosql
from edgedb.lang.caosql import ast as qlast
from edgedb.lang.graphql import ast as gqlast
from edgedb.lang.graphql import parser as gqlparser


class GraphQLTranslator:
    def __init__(self, schema):
        self.schema = schema

    def translate(self, gqltree):
        for definition in gqltree.definitions:
            query = self._process_definition(definition)

        return query

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
                    ]
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
                    selset.selection_set.selections)
            )
        )

        return expr

    def _process_pathspec(self, selections):
        pathspec = []

        for sel in selections:
            spec = qlast.SelectPathSpecNode(
                expr=qlast.LinkExprNode(
                    expr=qlast.LinkNode(
                        name=sel.name
                    )
                )
            )

            if sel.selection_set is not None:
                spec.pathspec = self._process_pathspec(
                    sel.selection_set.selections)

            pathspec.append(spec)

        return pathspec


def translate(schema, graphql):
    parser = gqlparser.GraphQLParser()
    gqltree = parser.parse(graphql)
    edgeql_tree = GraphQLTranslator(schema).translate(gqltree)
    return caosql.generate_source(edgeql_tree)
