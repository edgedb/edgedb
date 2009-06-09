from semantix.ast import *

class Referrer(AST): __fields = ['*refs']
class GraphExpr(AST): __fields = ['*paths', '*generator', '*selector', '*sorter']
class AtomicRef(Referrer): __fields = ['name', 'expr']
class EntitySet(AST): __fields = ['name', 'concept', '*filters', '*links', '*altlinks', '*rlinks', '*selrefs']
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
class SortExpr(AST): __fields = ['expr', 'direction']
class SelectorExpr(AST): __fields = ['expr', 'name']
class FunctionCall(AST): __fields = ['name', '*args']
