##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix import ast

class Base(ast.AST):
    __fields = ['!!refs']

    def __init__(self, **kwargs):
        if 'refs' not in kwargs:
            kwargs['refs'] = set()

        for name, value in kwargs.items():
            if isinstance(value, Base):
                kwargs['refs'].update(value.refs)

        super().__init__(**kwargs)

    def __setattr__(self, name, value):
        if name in self._fields and isinstance(value, Base):
            self.refs.update(value.refs)
        ast.AST.__setattr__(self, name, value)

    def ref(self, slice_=None):
        if slice_ is None:
            if len(self.refs) > 0:
                return list(self.refs)[0]
            else:
                return None
        else:
            return list(self.refs)[slice_]

    def replace_refs(self, old, new):
        self.refs.discard(old)
        self.refs.add(new)

        for name in self._fields:
            value = getattr(self, name)
            if isinstance(value, Base):
                value.replace_refs(old, new)


class GraphExpr(ast.AST): __fields = ['*paths', '*generator', '*selector', '*sorter']

class AtomicRef(Base):
    __fields = ['name', 'expr']

class MetaRef(Base):
    __fields = ['name']

class EntitySet(Base):
    __fields = ['id', 'name', '!concepts', 'atom', 'filter', '*links', '*altlinks', '*rlinks', '!atomrefs']

class EntityLink(Base):
    __fields = ['filter', 'source', 'target']

class EntityLinkSpec(ast.AST):
    __fields = ['*labels', 'direction']

class Constant(Base): __fields = ['value']
class Sequence(Base): __fields = ['*elements']
class BinOp(Base):
    __fields = ['left', 'op', 'right']
    AND = 'and'
    OR = 'or'
class InlineFilter(Base): __fields  = ['expr']
class ExistPred(Base): __fields = ['expr', 'outer']
class AtomicExistPred(ExistPred): pass
class SortExpr(Base): __fields = ['expr', 'direction']
class SelectorExpr(Base): __fields = ['expr', 'name']
class FunctionCall(Base): __fields = ['name', '*args']
