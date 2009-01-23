from copy import deepcopy
from semantix.parsers import caosql
from semantix.parsers.caosql import nodes
from semantix.caos import ConceptClass

class Query(object):
    def __init__(self, text, vars=None, context=None):
        self.text = text
        self.vars = vars
        self.context = context

class ParseContext(object):
    pass

class CaosQLQueryAdapter(object):
    def __init__(self):
        self.parser = caosql.CaosQLParser()

    def adapt(self, query, vars=None):
        xtree = self.parser.parse(query)

        qtree = self._transform_tree(xtree)

        qtext = self._render_query(qtree)

        return Query(qtext, vars)

    def _transform_tree(self, xtree):

        qtree = deepcopy(xtree)
        query = qtree.children[0]
        nodetype = type(query)

        if nodetype != nodes.SelectQueryNode:
            selnode = nodes.SelectQueryNode()
            selnode.targets = [query]
            query = selnode

        context = ParseContext()
        self._transform_select(context, query)

        return qtree

    def _transform_select(self, context, tree):
        self._transform_select_from(context, tree.fromlist)
        self._transform_select_targets(context, tree.targets)
        pass

    def _transform_select_from(self, context, fromlist):
        pass

    def _transform_select_targets(self, context, targets):
        pass

    def _render_query(self, qtree):
        return 'dummy'
