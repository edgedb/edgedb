from semantix.ast import *

class GraphExpr(AST): __fields = ['*paths', '*generator', '*selector']
class AtomicRef(AST): __fields = ['name', 'source']
class EntitySet(AST): __fields = ['name', 'concept', '*filters', '*links', '*altlinks', '*rlinks', '*selrefs']
class EntitySetRef(AST): __fields = [ 'name', 'concept', 'ptr', '*links']
class EntityLink(AST): __fields = ['filter', 'source', 'target']
class EntityLinkSpec(AST):
        __fields = ['*labels', 'direction']
        BACKWARD='<'
        FORWARD='>'
        BOTH='<>'
class Constant(AST): __fields = ['value']
class Sequence(AST): __fields = ['*elements']
class BinOp(AST): __fields = ['left', 'op', 'right']
class ExistPred(AST): __fields = ['expr', 'outer']
class AtomicExistPred(ExistPred): pass
