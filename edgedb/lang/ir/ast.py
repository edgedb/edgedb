##
# Copyright (c) 2008-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import itertools
import weakref

from edgedb.lang.common.exceptions import EdgeDBError
from edgedb.lang.common import ast, parsing
from edgedb.lang.common.datastructures import typed

from edgedb.lang.schema import name as sn
from edgedb.lang.schema import objects as so

from edgedb.lang.edgeql import ast as qlast


class ASTError(EdgeDBError):
    pass


class Base(ast.AST):
    __fields = [('refs', weakref.WeakSet, weakref.WeakSet, False),
                ('backrefs', weakref.WeakSet, weakref.WeakSet, False),
                # Pointer to an original node replaced by this node during rewrites
                ('rewrite_original', object, None, False, False),
                # Whether or not the node is a product of a rewrite
                ('is_rewrite_product', bool, False),
                ('rewrite_flags', set),
                ('context', parsing.ParserContext, None,
                 True, None, True)  # this last True is "hidden" attribute
                ]

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
                ('cges', list), 'recurse_link', 'recurse_depth', 'aggregate_result',
                'backend_text_override', 'set_op', 'set_op_larg', 'set_op_rarg']


class CommonGraphExpr(Base):
    __fields = ['alias', 'expr']


class Path(Base):
    __fields = ['id', 'anchor', 'show_as_anchor', 'pathvar',
                ('reference', Base, None, False)]

    def get_id(self):
        return self.pathvar or self.id

    def __setattr__(self, name, value):
        assert not (name == 'pathvar' and value == 'self')
        super().__setattr__(name, value)

    def __getattr__(self, name):
        if name == 'paths':
            return (self,)
        else:
            return Base.__getattribute__(self, name)


class SubgraphRef(Path):
    __fields = [('name', str, None), ('ref', Base, None, False, True),
                ('rlink', Base, None, False, False, True), ('force_inline', bool)]


class BaseRef(Path):
    __fields = [('ref', Base, None, False),
                ('rlink', Base, None, (False, True), False, True),
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

    def is_terminal(self):
        return True


class AtomicRef(BaseRef):
    pass


class AtomicRefSimple(AtomicRef):
    __fields = [('name', sn.Name, None)]


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
    __fields = [('name', sn.Name, None)]


class LinkPropRefExpr(LinkPropRef, BaseRefExpr):
    def update_ref(self):
        refs = set(ast.find_children(self.expr, lambda n: isinstance(n, EntityLink)))
        assert(len(refs) == 1)
        ast.AST.__setattr__(self, 'ref', next(iter(refs)))
        self.ref.backrefs.add(self)


class EntityLink(Base):
    __fields = ['propfilter', 'source', 'target', 'link_proto', ('proprefs', set),
                ('metarefs', set), ('users', set), 'anchor', 'show_as_anchor',
                'pathvar', 'direction', 'pathspec_trigger']

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
    __fields = [('concept', so.ProtoNode), 'atom',
                'filter',
                ('conjunction', Conjunction),
                ('disjunction', Disjunction),
                ('origin', Path, None, False),
                ('rlink', EntityLink, None, False),
                ('atomrefs', set), ('metarefs', set), ('users', set),
                ('joins', set, set, False),
                '_backend_rel_suffix']

    def __setattr__(self, name, value):
        super().__setattr__(name, value)

        if name == 'rlink' and value is not None:
            value.backrefs.add(self)

    def is_terminal(self):
        return ((self.conjunction is None or not self.conjunction.paths)
                and (self.disjunction is None or not self.disjunction.paths)
                and not self.atomrefs)


class PtrPathSpec(Base):
    __fields = ['ptr_proto', 'ptr_direction', 'pathspec', 'recurse',
                'target_proto', 'sorter', 'generator', 'trigger',
                'offset', 'limit', 'compexpr', 'type_indirection']


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

            if not isinstance(item_type, (so.ProtoObject, so.PrototypeClass)):
                raise ASTError(('unexpected constant type representation, '
                                'expected ProtoObject, got "%r"') % (type,))

class Expr(Base):
    pass

class Sequence(Expr): __fields = [('elements', list), ('is_array', bool)]

class Record(Expr):
    __fields = [
        ('elements', list),
        'concept',
        ('rlink', EntityLink, None, False)
    ]

class BinOp(Expr):
    __fields = ['left', 'right', 'op', ('aggregates', bool), ('strong', bool)]

class UnaryOp(Expr):
    __fields = ['expr', 'op', ('aggregates', bool)]

class NoneTest(Expr):
    __fields = ['expr']

class InlineFilter(Base): __fields  = ['expr', 'ref']
class InlinePropFilter(Base): __fields  = ['expr', 'ref']
class ExistPred(Expr): __fields = ['expr', 'outer']
class AtomicExistPred(ExistPred): pass




class SortExpr(Base):
    __fields = ['expr', 'direction', ('nones_order', qlast.NonesOrder, None)]

class SelectorExpr(Base): __fields = ['expr', 'name', 'autoname']
class UpdateExpr(Base): __fields = ['expr', 'value']
class FunctionCall(Expr):
    __fields = ['name',
                'result_type',
                ('args', list),
                ('kwargs', dict),
                ('aggregates', bool),
                ('window', bool),
                ('agg_sort', list),
                'agg_filter',
                ('partition', list)]

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


class TypeCast(Expr):
    __fields = ['expr', 'type']


class CompositeType(Base):
    __fields = ['node', 'pathspec']


TextSearchOperator = qlast.TextSearchOperator
EdgeDBMatchOperator = qlast.EdgeQLMatchOperator
