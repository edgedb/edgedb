from semantix.ast import *

class GraphExpr(AST): __fields = ['*paths', '*generator', '*selector']
class AtomicRef(AST): __fields = ['name', 'source']
class EntitySet(AST): __fields = ['name', 'concept', '*filters', '*links', '*altlinks', '*rlinks', '*selrefs']
class EntitySetRef(AST): __fields = [ 'name', 'concept', 'ptr', '*links']
class EntityLink(AST): __fields = ['label', 'source', 'target']
class Constant(AST): __fields = ['value']
class BinOp(AST): __fields = ['left', 'op', 'right']
class ExistPred(AST): __fields = ['expr', 'outer']
class AtomicExistPred(ExistPred): pass
