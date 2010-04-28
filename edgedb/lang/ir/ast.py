##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import weakref

from semantix.utils import ast
from semantix.caos import name as caos_name
from semantix.caos import types as caos_types


class Base(ast.AST):
    __fields = [('refs', weakref.WeakSet, weakref.WeakSet, False),
                ('backrefs', weakref.WeakSet, weakref.WeakSet, False)]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        for field_name, field_value in kwargs.items():
            field_spec = self._fields.get(field_name)
            if field_spec and field_spec.traverse:
                if isinstance(field_value, Base):
                    self.merge_refs_from(field_value)

    def merge_refs_from(self, node):
        refs = set()

        if isinstance(node, EntitySet):
            refs.add(node)

        for ref in refs:
            ref.backrefs.add(self)

        self.refs.update(refs)

    def __setattr__(self, name, value):
        field_spec = self._fields.get(name)
        if field_spec and field_spec.traverse and isinstance(value, Base):
            self.merge_refs_from(value)
        super().__setattr__(name, value)

    def replace_refs(self, old, new, deep=False):
        self.refs.difference_update(old)
        self.refs.add(new)

        for name, field in self._fields.items():
            value = getattr(self, name)
            if isinstance(value, Base):
                if deep and field.traverse:
                    value.replace_refs(old, new, deep)
                if value in old:
                    setattr(self, name, new)

            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, Base):
                        if deep and field.traverse:
                            item.replace_refs(old, new, deep)
                        if item in old:
                            value[i] = new

            elif isinstance(value, (set, weakref.WeakSet)):
                for item in value.copy():
                    if isinstance(item, Base):
                        if deep and field.traverse:
                            item.replace_refs(old, new, deep)
                        if item in old:
                            value.remove(item)
                            value.add(new)

            elif isinstance(value, frozenset):
                newset = set(value)
                for item in value:
                    if isinstance(item, Base):
                        if deep and field.traverse:
                            item.replace_refs(old, new, deep)
                        if item in old:
                            newset.remove(item)
                            newset.add(new)
                setattr(self, name, frozenset(newset))

class GraphExpr(ast.AST):
    __fields = ['generator', ('selector', list), ('sorter', list), 'offset', 'limit',
                ('opselector', list), 'optarget', 'opvalues', 'op']


class AtomicRef(Base):
    __fields = ['id', ('ref', Base, None, False)]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.ref:
            self.refs.add(self.ref)
            self.ref.backrefs.add(self)

    def __setattr__(self, name, value):
        super().__setattr__(name, value)

        if name == 'ref':
            self.refs.clear()
            self.refs.add(self.ref)
            self.ref.backrefs.add(self)


class AtomicRefSimple(AtomicRef):
    __fields = [('name', caos_name.Name, None)]


class AtomicRefExpr(AtomicRef):
    __fields = ['expr']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.update_ref()

    def __setattr__(self, name, value):
        super().__setattr__(name, value)

        if name == 'expr':
            self.update_ref()

    def update_ref(self):
        refs = set(ast.find_children(self.expr, lambda n: isinstance(n, EntitySet)))
        assert(len(refs) == 1)
        ast.AST.__setattr__(self, 'ref', next(iter(refs)))
        self.ref.backrefs.add(self)


class MetaRef(AtomicRefSimple):
    __fields = ['name']

class EntityLink(Base):
    __fields = ['filter', 'source', 'target', 'link_proto']

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        if other is None:
            return False
        return self.filter == other.filter and self.source is other.source and self.target is other.target

    def __hash__(self):
        return hash((self.filter, self.source, self.target))


class EntityLinkSpec(ast.AST):
    __fields = [('labels', frozenset), 'direction']

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        if other is None:
            return False
        return self.labels == other.labels and self.direction == other.direction

    def __hash__(self):
        return hash((self.labels, self.direction))


class PathCombination(Base):
    __fields = [('paths', frozenset, frozenset, False)]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.update_refs()

    def __setattr__(self, name, value):
        super().__setattr__(name, value)

        if name == 'paths':
            self.update_refs()

    def update_refs(self):
        refs = [path for path in self.paths if isinstance(path, EntitySet)]

        for ref in refs:
            ref.backrefs.add(self)
        self.refs.update(refs)

    def update(self, other):
        if isinstance(other, PathCombination):
            self.paths = frozenset(self.paths | other.paths)
        elif isinstance(other, (EntitySet, EntityLink)):
            self.paths = frozenset(self.paths | {other})
        self.update_refs()


class Disjunction(PathCombination):
    pass


class Conjunction(PathCombination):
    pass


class EntitySet(Base):
    __fields = ['id', 'anchor', ('concept', caos_types.ProtoNode), 'atom',
                'filter',
                ('conjunction', Conjunction),
                ('disjunction', Disjunction),
                ('rlink', EntityLink, None, False),
                ('atomrefs', set), ('metarefs', set), ('users', set),
                ('joins', set, set, False)]


class Constant(Base): __fields = ['value', 'index', 'expr', 'type']

class Sequence(Base): __fields = [('elements', set)]

class BinOp(Base):
    __fields = ['left', 'right', 'op']

class InlineFilter(Base): __fields  = ['expr', 'ref']
class ExistPred(Base): __fields = ['expr', 'outer']
class AtomicExistPred(ExistPred): pass
class SortExpr(Base): __fields = ['expr', 'direction']
class SelectorExpr(Base): __fields = ['expr', 'name']
class UpdateExpr(Base): __fields = ['expr', 'value']
class FunctionCall(Base): __fields = ['name', ('args', list)]
