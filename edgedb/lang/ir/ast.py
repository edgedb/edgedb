##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import itertools
import weakref

from metamagic.exceptions import MetamagicError
from metamagic.utils import ast
from metamagic.caos import name as caos_name
from metamagic.caos import types as caos_types
from metamagic.utils.datastructures import StrSingleton, typed


class ASTError(MetamagicError):
    pass


class Base(ast.AST):
    __fields = [('refs', weakref.WeakSet, weakref.WeakSet, False),
                ('backrefs', weakref.WeakSet, weakref.WeakSet, False),
                ('rewrite_flags', set)]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        for field_name, field_value in kwargs.items():
            field_spec = self._fields.get(field_name)
            if field_spec and self._can_pull_refs(field_spec):
                if isinstance(field_value, Base):
                    self.merge_refs_from(field_value)

    def _can_pull_refs(self, field):
        traverse = field.traverse
        return traverse[0] if isinstance(traverse, tuple) else traverse

    def _can_replace_refs(self, field):
        traverse = field.traverse
        return traverse[1] if isinstance(traverse, tuple) else field.child_traverse

    def merge_refs_from(self, node):
        refs = set()

        if isinstance(node, (EntitySet, EntityLink)):
            refs.add(node)

        for ref in refs:
            ref.backrefs.add(self)

        self.refs.update(refs)

    def __setattr__(self, name, value):
        field_spec = self._fields.get(name)
        if field_spec and self._can_pull_refs(field_spec) and isinstance(value, Base):
            self.merge_refs_from(value)
        super().__setattr__(name, value)

    def replace_refs(self, old, new, deep=False, _memo=None):
        if _memo is None:
            _memo = set()

        if self in _memo:
            return

        _memo.add(self)

        self.refs.difference_update(old)
        self.refs.add(new)

        new.backrefs.add(self)

        for name, field in self._fields.items():
            value = getattr(self, name)
            if isinstance(value, Base):
                if deep and self._can_replace_refs(field):
                    value.replace_refs(old, new, deep, _memo)
                if value in old:
                    setattr(self, name, new)

            elif isinstance(value, (list, typed.TypedList)):
                for i, item in enumerate(value):
                    if isinstance(item, Base):
                        if deep and self._can_replace_refs(field):
                            item.replace_refs(old, new, deep, _memo)
                        if item in old:
                            value[i] = new

            elif isinstance(value, (set, weakref.WeakSet, typed.TypedSet)):
                for item in value.copy():
                    if isinstance(item, Base):
                        if deep and self._can_replace_refs(field):
                            item.replace_refs(old, new, deep, _memo)
                        if item in old:
                            value.remove(item)
                            value.add(new)

            elif isinstance(value, frozenset):
                newset = {v for v in value}
                for item in value:
                    if isinstance(item, Base):
                        if deep and self._can_replace_refs(field):
                            item.replace_refs(old, new, deep, _memo)
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
                ('subgraphs', set), ('referrers', list), ('attrrefs', set),
                ('cges', list), 'recurse_link', 'recurse_depth', 'aggregate_result']


class CommonGraphExpr(Base):
    __fields = ['alias', 'expr']


class Path(Base):
    pass


class SubgraphRef(Path):
    __fields = [('name', str, None), ('ref', Base, None, False, True),
                ('rlink', Base, None, False, False, True), ('force_inline', bool)]


class BaseRef(Path):
    __fields = ['id', ('ref', Base, None, False), ('rlink', Base, None, (False, True), False, True),
                'ptr_proto', ('users', set)]

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
    __fields = [('name', caos_name.Name, None)]


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


class MetaRefBase(AtomicRef):
    pass


class MetaRef(AtomicRefSimple, MetaRefBase):
    __fields = ['name']


class MetaRefExpr(MetaRefBase, AtomicRefExpr):
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
    __fields = ['propfilter', 'source', 'target', 'link_proto', ('proprefs', set),
                ('metarefs', set), ('users', set), 'anchor', 'direction', 'pathspec_trigger']

    def replace_refs(self, old, new, deep=False, _memo=None):
        # Since EntityLink can be a member of PathCombination set
        # we need to refresh our backrefs to make sure that set hashes are straight.
        if _memo is not None and self in _memo:
            return

        replace = self.source in old or self.target in old
        super().replace_refs(old, new, deep, _memo)
        if replace:
            self.fixup_refs([self], self)


class PathCombination(Path):
    __fields = [('paths', frozenset, frozenset, False, True), ('fixed', bool)]

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


class AtomicRefSet(typed.TypedSet, type=AtomicRef):
    pass


class EntitySet(Path):
    __fields = ['id', 'anchor', ('concept', caos_types.ProtoNode), 'atom',
                'filter',
                ('conjunction', Conjunction),
                ('disjunction', Disjunction),
                ('reference', Path, None, False),
                ('origin', Path, None, False),
                ('rlink', EntityLink, None, False),
                ('atomrefs', set), ('metarefs', set), ('users', set),
                ('joins', set, set, False)]

    def __setattr__(self, name, value):
        super().__setattr__(name, value)

        if name == 'rlink' and value is not None:
            value.backrefs.add(self)


class PtrPathSpec(Base):
    __fields = ['ptr_proto', 'ptr_direction', 'pathspec', 'recurse', 'target_proto', 'sorter',
                'generator', 'trigger']


class ExplicitPathSpecTrigger(Base):
    pass


class PointerIteratorPathSpecTrigger(Base):
    __fields = ['filters']


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
        if type:
            if isinstance(type, tuple):
                item_type = type[1]
            else:
                item_type = type

            if not isinstance(item_type, (caos_types.ProtoObject, caos_types.PrototypeClass)):
                raise ASTError(('unexpected constant type representation, '
                                'expected ProtoObject, got "%r"') % (type,))


class Sequence(Base): __fields = [('elements', list), ('is_array', bool)]

class Record(Base):
    __fields = [('elements', list), 'concept', ('rlink', EntityLink, None, False),
                ('linkprop_xvalue', bool)]

class BinOp(Base):
    __fields = ['left', 'right', 'op', ('aggregates', bool), ('strong', bool)]

class UnaryOp(Base):
    __fields = ['expr', 'op', ('aggregates', bool)]

class NoneTest(Base):
    __fields = ['expr']

class InlineFilter(Base): __fields  = ['expr', 'ref']
class InlinePropFilter(Base): __fields  = ['expr', 'ref']
class ExistPred(Base): __fields = ['expr', 'outer']
class AtomicExistPred(ExistPred): pass

class SearchVector(Base):
    __fields = ['items']

class SearchVectorElement(Base):
    __fields = [('ref', Base, None, True), 'weight']

    def __setattr__(self, name, value):
        super().__setattr__(name, value)

        if name == 'ref':
            self.update_refs()

    def update_refs(self):
        if isinstance(self.ref, (EntitySet, EntityLink)):
            ref = self.ref
        elif isinstance(self.ref, (BaseRef,)):
            ref = self.ref.ref
        else:
            return

        ref.backrefs.add(self)
        self.refs.add(ref)

class SortOrder(StrSingleton):
    _map = {
        'ASC': 'SortAsc',
        'DESC': 'SortDesc',
        'SORT_DEFAULT': 'SortDefault'
    }

SortAsc = SortOrder('ASC')
SortDesc = SortOrder('DESC')
SortDefault = SortAsc


class NonesOrder(StrSingleton):
    _map = {
        'first': 'NonesFirst',
        'last': 'NonesLast'
    }

NonesFirst = NonesOrder('first')
NonesLast = NonesOrder('last')


class SortExpr(Base):
    __fields = ['expr', 'direction', ('nones_order', NonesOrder, None)]

class SelectorExpr(Base): __fields = ['expr', 'name', 'autoname']
class UpdateExpr(Base): __fields = ['expr', 'value']
class FunctionCall(Base):
    __fields = ['name',
                'result_type',
                ('args', list),
                ('kwargs', dict),
                ('aggregates', bool),
                ('window', bool),
                ('agg_sort', list)]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.update_refs()

    def __setattr__(self, name, value):
        super().__setattr__(name, value)

        if name == 'args':
            self.update_refs()

    def update_refs(self):
        args = [arg for arg in self.args if isinstance(arg, (EntitySet, EntityLink, BaseRef))]

        for arg in args:
            arg.backrefs.add(self)
        self.refs.update(args)


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
