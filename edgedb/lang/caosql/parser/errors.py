class CaosQLSyntaxError(Exception):
    def __init__(self, token, lineno, expr=None):
        self.token = token
        self.expr = expr
        self.lineno = lineno

    def __str__(self):
        return "unexpected `%s' on line %d" % (self.token, self.lineno)
