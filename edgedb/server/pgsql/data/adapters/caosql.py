from semantix.parsers import caosql

class Query(object):
    def __init__(self, text, vars=None, context=None):
        self.text = text
        self.vars = vars
        self.context = context

class CaosQLQueryAdapter(object):
    def __init__(self):
        self.parser = caosql.CaosQLParser()

    def adapt(self, query, vars=None):
        xtree = self.parse(query)

        expr = self._eval_tree(xtree)

        return self._render_query(expr)

    def _eval_tree(self, xtree):
        pass

    def _render_query(self, expr):
        pass
