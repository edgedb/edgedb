from semantix.ast import *

class Base(AST):
    __fields = ['!refs']

    def __init__(self, **kwargs):
        if 'refs' not in kwargs:
            kwargs['refs'] = set()

        for name, value in kwargs.items():
            if isinstance(value, Base):
                kwargs['refs'].update(value.refs)

        super().__init__(**kwargs)

    def __setattr__(self, name, value):
        if isinstance(value, Base):
            self.refs.update(value.refs)
        AST.__setattr__(self, name, value)

    def ref(self, slice_=None):
        if slice_ is None:
            if len(self.refs) > 0:
                return list(self.refs)[0]
            else:
                return None
        else:
            return list(self.refs)[slice_]

class GraphExpr(AST): __fields = ['*paths', '*generator', '*selector', '*sorter']
class AtomicRef(Base): __fields = ['name', 'expr']
class EntitySet(Base): __fields = ['name', 'concept', '*filters', '*links', '*altlinks', '*rlinks', '*selrefs']
class EntityLink(Base): __fields = ['filter', 'source', 'target']
class EntityLinkSpec(AST):
        __fields = ['*labels', 'direction']
        BACKWARD='<'
        FORWARD='>'
        BOTH='<>'
class Constant(Base): __fields = ['value']
class Sequence(Base): __fields = ['*elements']
class BinOp(Base): __fields = ['left', 'op', 'right']
class ExistPred(Base): __fields = ['expr', 'outer']
class AtomicExistPred(ExistPred): pass
class SortExpr(Base): __fields = ['expr', 'direction']
class SelectorExpr(Base): __fields = ['expr', 'name']
class FunctionCall(Base): __fields = ['name', '*args']
