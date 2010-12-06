##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import itertools
import weakref

from semantix.exceptions import SemantixError
from semantix.utils import ast
from semantix.caos import name as caos_name
from semantix.caos import types as caos_types


class ASTError(SemantixError):
    pass


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

        if isinstance(node, (EntitySet, EntityLink)):
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
                if deep and field.child_traverse:
                    value.replace_refs(old, new, deep)
                if value in old:
                    setattr(self, name, new)

            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, Base):
                        if deep and field.child_traverse:
                            item.replace_refs(old, new, deep)
                        if item in old:
                            value[i] = new

            elif isinstance(value, (set, weakref.WeakSet)):
                for item in value.copy():
                    if isinstance(item, Base):
                        if deep and field.child_traverse:
                            item.replace_refs(old, new, deep)
                        if item in old:
                            value.remove(item)
                            value.add(new)

            elif isinstance(value, frozenset):
                newset = {v for v in value}
                for item in value:
                    if isinstance(item, Base):
                        if deep and field.child_traverse:
                            item.replace_refs(old, new, deep)
                        if item in old:
                            newset.remove(item)
                            newset.add(new)
                setattr(self, name, frozenset(newset))

    @classmethod
    def fixup_refs(cls, refs, newref):
        # Use list here, since the backref sets can be changed by replace_refs() call
        for referrer in list(itertools.chain.from_iterable(ref.backrefs for ref in refs)):
            referrer.replace_refs(refs, newref, deep=False)


class GraphExpr(Base):
    __fields = ['generator', ('selector', list), ('grouper', list), ('sorter', list),
                'offset', 'limit', ('opselector', list), 'optarget', 'opvalues', 'op',
                ('subgraphs', set)]


class Path(Base):
    pass


class SubgraphRef(Path):
    __fields = [('name', str, None), ('ref', Base, None, False)]


class BaseRef(Path):
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


class AtomicRef(BaseRef):
    pass


class AtomicRefSimple(AtomicRef):
    __fields = [('name', caos_name.Name, None), 'caoslink']


class BaseRefExpr(Path):
    __fields = ['expr', ('inline', bool, True)]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.update_ref()

    def __setattr__(self, name, value):
        super().__setattr__(name, value)

        if name == 'expr':
            self.update_ref()


class AtomicRefExpr(AtomicRef, BaseRefExpr):
    def update_ref(self):
        refs = set(ast.find_children(self.expr, lambda n: isinstance(n, EntitySet)))
        assert(len(refs) == 1)
        ast.AST.__setattr__(self, 'ref', next(iter(refs)))
        self.ref.backrefs.add(self)


class MetaRef(AtomicRefSimple):
    __fields = ['name']


class LinkPropRef(BaseRef):
    pass


class LinkPropRefSimple(LinkPropRef):
    __fields = [('name', caos_name.Name, None)]


class LinkPropRefExpr(LinkPropRef, BaseRefExpr):
    def update_ref(self):
        refs = set(ast.find_children(self.expr, lambda n: isinstance(n, EntityLink)))
        assert(len(refs) == 1)
        ast.AST.__setattr__(self, 'ref', next(iter(refs)))
        self.ref.backrefs.add(self)


class EntityLink(Base):
    __fields = ['filter', 'propfilter', 'source', 'target', 'link_proto', ('proprefs', set),
                ('users', set), 'anchor']

    def replace_refs(self, old, new, deep=False):
        # Since EntityLink can be a member of PathCombination set
        # we need to refresh our backrefs to make sure that set hashes are straight.
        replace = self.source in old or self.target in old
        super().replace_refs(old, new, deep)
        if replace:
            self.fixup_refs([self], self)


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


class PathCombination(Path):
    __fields = [('paths', frozenset, frozenset, False, True)]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.update_refs()

    def __setattr__(self, name, value):
        super().__setattr__(name, value)

        if name == 'paths':
            self.update_refs()

    def update_refs(self):
        refs = [path for path in self.paths if isinstance(path, (EntitySet, EntityLink))]

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


class EntitySet(Path):
    __fields = ['id', 'anchor', ('concept', caos_types.ProtoNode), 'atom',
                ('conceptfilter', dict),
                'filter',
                ('conjunction', Conjunction),
                ('disjunction', Disjunction),
                ('reference', Path, None, False),
                ('rlink', EntityLink, None, False),
                ('atomrefs', set), ('metarefs', set), ('users', set),
                ('joins', set, set, False)]

    def __setattr__(self, name, value):
        super().__setattr__(name, value)

        if name == 'rlink':
            value.backrefs.add(self)


class Constant(Base):
    __fields = ['value', 'index', 'expr', 'type', 'substitute_for']

    def __init__(self, **kwargs):
        self._check_type(kwargs.get('expr'), kwargs.get('type'))
        super().__init__(**kwargs)

    def __setattr__(self, name, value):
        if name in ('expr', 'type'):
            expr = value if name == 'expr' else self.expr
            type = value if name == 'value' else self.type
            self._check_type(expr, type)

    def _check_type(self, expr, type):
        if not type:
            if not expr:
                raise ASTError('simple constants must have type information')

        else:
            if isinstance(type, tuple):
                item_type = type[1]
            else:
                item_type = type

            if not isinstance(item_type, caos_types.ProtoObject):
                raise ASTError(('unexpected constant type representation, '
                                'expected ProtoObject, got "%r"') % (type,))


class Sequence(Base): __fields = [('elements', list)]

class Record(Base): __fields = [('elements', list), 'concept']

class BinOp(Base):
    __fields = ['left', 'right', 'op', ('aggregates', bool)]

class UnaryOp(Base):
    __fields = ['expr', 'op', ('aggregates', bool)]

class NoneTest(Base):
    __fields = ['expr']

class InlineFilter(Base): __fields  = ['expr', 'ref']
class InlinePropFilter(Base): __fields  = ['expr', 'ref']
class ExistPred(Base): __fields = ['expr', 'outer']
class AtomicExistPred(ExistPred): pass
class SortExpr(Base): __fields = ['expr', 'direction']
class SelectorExpr(Base): __fields = ['expr', 'name']
class UpdateExpr(Base): __fields = ['expr', 'value']
class FunctionCall(Base):
    __fields = ['name',
                'result_type',
                ('args', list),
                ('kwargs', dict),
                ('aggregates', bool)]

class TypeCast(Base):
    __fields = ['expr', 'type']

class CaosOperator(ast.ops.Operator):
    pass

class TextSearchOperator(CaosOperator):
    pass

SEARCH = TextSearchOperator('@@')
SEARCHEX = TextSearchOperator('@@!')

class CaosComparisonOperator(CaosOperator, ast.ops.ComparisonOperator):
    pass

LIKE = CaosComparisonOperator('like')
ILIKE = CaosComparisonOperator('ilike')
