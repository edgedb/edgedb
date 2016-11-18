##
# Copyright (c) 2015 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import functools
import itertools

from edgedb.lang.common import ast
from edgedb.lang.common import datastructures

from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import types as s_types
from edgedb.lang.schema import utils as s_utils

from . import ast as irast
from . import ast2 as irast2


class PathIndex(dict):
    """Graph path mapping path identifiers to AST nodes."""

    def update(self, other):
        for k, v in other.items():
            if k in self:
                super().__getitem__(k).update(v)
            else:
                self[k] = v

    def __setitem__(self, key, value):
        if not isinstance(key, (LinearPath, str)):
            raise TypeError('Invalid key type for PathIndex: %s' % key)

        if not isinstance(value, set):
            value = {value}

        super().__setitem__(key, value)


def infer_arg_types(ir, schema):
    def flt(n):
        if isinstance(n, irast.BinOp):
            return (isinstance(n.left, irast.Constant) or
                    isinstance(n.right, irast.Constant))

    ops = ast.find_children(ir, flt)

    arg_types = {}

    for binop in ops:
        typ = None

        if isinstance(binop.right, irast.Constant):
            expr = binop.left
            arg = binop.right
            reversed = False
        else:
            expr = binop.right
            arg = binop.left
            reversed = True

        if arg.index is None:
            continue

        if isinstance(binop.op, irast.EdgeDBMatchOperator):
            typ = schema.get('std::str')

        elif isinstance(binop.op, (ast.ops.ComparisonOperator,
                                   ast.ops.ArithmeticOperator)):
            typ = infer_type(expr, schema)

        elif isinstance(binop.op, ast.ops.MembershipOperator) and not reversed:
            from edgedb.lang.schema import objects as s_obj

            elem_type = infer_type(expr, schema)
            typ = s_obj.Set(element_type=elem_type)

        elif isinstance(binop.op, ast.ops.BooleanOperator):
            typ = schema.get('std::bool')

        else:
            msg = 'cannot infer expr type: unsupported ' \
                  'operator: {!r}'.format(binop.op)
            raise ValueError(msg)

        if typ is None:
            msg = 'cannot infer expr type'
            raise ValueError(msg)

        try:
            existing = arg_types[arg.index]
        except KeyError:
            arg_types[arg.index] = typ
        else:
            if existing != typ:
                msg = 'cannot infer expr type: ambiguous resolution: ' + \
                      '{!r} and {!r}'
                raise ValueError(msg.format(existing, typ))

    return arg_types


def infer_type(ir, schema):
    if isinstance(ir, irast.AtomicRefSimple):
        if isinstance(ir.ref, irast.PathCombination):
            targets = [t.concept for t in ir.ref.paths]
            concept = s_utils.get_class_nearest_common_ancestor(targets)
        else:
            concept = ir.ref.concept

        ptr = concept.resolve_pointer(schema, ir.name,
                                      look_in_children=True)
        if not ptr:
            msg = ('[{source}].[{link_name}] does not '
                   'resolve to any known path')
            msg = msg.format(source=concept.name, link_name=ir.name)
            raise LookupError(msg)

        result = ptr.target

    elif isinstance(ir, irast.LinkPropRefSimple):
        if isinstance(ir.ref, irast.PathCombination):
            targets = [t.link_class for t in ir.ref.paths]
            link = s_utils.get_class_nearest_common_ancestor(targets)
        else:
            link = ir.ref.link_class

        prop = link.getptr(schema, ir.name)
        assert prop, '"%s" is not a property of "%s"' % (ir.name, link.name)
        result = prop.target

    elif isinstance(ir, irast.BaseRefExpr):
        result = infer_type(ir.expr, schema)

    elif isinstance(ir, irast.Record):
        result = ir.concept

    elif isinstance(ir, irast.FunctionCall):
        # argtypes = tuple(infer_type(arg, schema) for arg in ir.args)

        func_obj = schema.get(ir.name)
        result = func_obj.returntype

    elif isinstance(ir, irast.Constant):
        if ir.expr:
            result = infer_type(ir.expr, schema)
        else:
            result = ir.type

    elif isinstance(ir, irast.BinOp):
        if isinstance(ir.op, (ast.ops.ComparisonOperator,
                              ast.ops.TypeCheckOperator,
                              ast.ops.MembershipOperator)):
            result = schema.get('std::bool')
        else:
            left_type = infer_type(ir.left, schema)
            right_type = infer_type(ir.right, schema)
            result = s_types.TypeRules.get_result(
                ir.op, (left_type, right_type), schema)
            if result is None:
                result = s_types.TypeRules.get_result(
                    (ir.op, 'reversed'), (right_type, left_type), schema)

    elif isinstance(ir, irast.UnaryOp):
        operand_type = infer_type(ir.expr, schema)
        result = s_types.TypeRules.get_result(
                            ir.op, (operand_type,), schema)

    elif isinstance(ir, irast.EntitySet):
        result = ir.concept

    elif isinstance(ir, irast.PathCombination):
        if ir.paths:
            result = infer_type(next(iter(ir.paths)), schema)
        else:
            result = None

    elif isinstance(ir, irast.TypeCast):
        if ir.type.subtypes:
            coll = s_obj.Collection.get_class(ir.type.maintype)
            result = coll.from_subtypes(
                [schema.get(t) for t in ir.type.subtypes])
        else:
            result = schema.get(ir.type.maintype)

    elif isinstance(ir, irast.GraphExpr):
        if len(ir.selector) == 1:
            result = infer_type(ir.selector[0].expr, schema)
        else:
            result = None

    elif isinstance(ir, irast.SubgraphRef):
        result = infer_type(ir.ref, schema)

    elif isinstance(ir, irast.ExistPred):
        result = schema.get('std::bool')

    else:
        result = None

    if result is not None:
        allowed = (s_obj.Class, s_obj.MetaClass)
        assert (isinstance(result, allowed) or
                (isinstance(result, (tuple, list)) and
                 isinstance(result[1], allowed))), \
               "infer_type({!r}) retured {!r} instead of a Class" \
                    .format(ir, result)

    return result


def infer_type2(ir, schema):
    if isinstance(ir, (irast2.Set, irast2.Shape)):
        result = ir.scls

    elif isinstance(ir, irast2.FunctionCall):
        func_obj = schema.get(ir.name)
        result = func_obj.returntype

    elif isinstance(ir, irast2.Constant):
        if ir.expr:
            result = infer_type2(ir.expr, schema)
        else:
            result = ir.type

    elif isinstance(ir, irast2.BinOp):
        if isinstance(ir.op, (ast.ops.ComparisonOperator,
                              ast.ops.TypeCheckOperator,
                              ast.ops.MembershipOperator)):
            result = schema.get('std::bool')
        else:
            left_type = infer_type2(ir.left, schema)
            right_type = infer_type2(ir.right, schema)

            result = s_types.TypeRules.get_result(
                ir.op, (left_type, right_type), schema)
            if result is None:
                result = s_types.TypeRules.get_result(
                    (ir.op, 'reversed'), (right_type, left_type), schema)

    elif isinstance(ir, irast2.UnaryOp):
        if ir.op == ast.ops.NOT:
            result = schema.get('std::bool')
        else:
            operand_type = infer_type2(ir.expr, schema)
            result = s_types.TypeRules.get_result(
                ir.op, (operand_type,), schema)

    elif isinstance(ir, irast2.TypeCast):
        if ir.type.subtypes:
            coll = s_obj.Collection.get_class(ir.type.maintype)
            result = coll.from_subtypes(
                [schema.get(t) for t in ir.type.subtypes])
        else:
            result = schema.get(ir.type.maintype)

    elif isinstance(ir, irast2.Stmt):
        result = infer_type2(ir.result, schema)

    elif isinstance(ir, irast2.SubstmtRef):
        result = infer_type2(ir.stmt, schema)

    elif isinstance(ir, irast2.ExistPred):
        result = schema.get('std::bool')

    elif isinstance(ir, irast2.SliceIndirection):
        result = infer_type2(ir.expr, schema)

    elif isinstance(ir, irast2.IndexIndirection):
        arg = infer_type2(ir.expr, schema)

        if arg is None:
            result = None
        else:
            str_t = schema.get('std::str')
            if arg.issubclass(str_t):
                result = arg
            else:
                result = None

    else:
        result = None

    if result is not None:
        allowed = (s_obj.Class, s_obj.MetaClass)
        assert (isinstance(result, allowed) or
                (isinstance(result, (tuple, list)) and
                 isinstance(result[1], allowed))), \
                 "infer_type2({!r}) retured {!r} instead of a Class" \
                    .format(ir, result)

    return result


def get_source_references(ir):
    result = []

    refs = extract_paths(ir, reverse=True, resolve_arefs=True,
                             recurse_subqueries=-1)

    if refs is not None:
        flt = lambda n: isinstance(n, (irast.EntitySet, irast.EntityLink))
        nodes = ast.find_children(refs, flt)
        if nodes:
            for node in nodes:
                if isinstance(node, irast.EntitySet):
                    result.append(node.concept)
                else:
                    result.append(node.link_class)

    return set(result)


def get_terminal_references(ir):
    result = set()

    refs = extract_paths(ir, reverse=True, resolve_arefs=True,
                             recurse_subqueries=1)

    if refs is not None:
        flt = lambda n: (callable(getattr(n, 'is_terminal', None))
                            and n.is_terminal())
        result.update(ast.find_children(refs, flt))

    return result


def get_variables(ir):
    result = set()

    flt = lambda n: isinstance(n, irast.Constant) and n.index is not None
    result.update(ast.find_children(ir, flt))

    return result


def is_const(ir):
    refs = extract_paths(ir, reverse=True, resolve_arefs=True,
                             recurse_subqueries=1)
    variables = get_variables(ir)
    return not refs and not variables


def is_weak_op(op):
    return op in (ast.ops.OR, ast.ops.IN, ast.ops.NOT_IN)


def flatten_path_combination(expr, recursive=False):
    paths = set()
    for path in expr.paths:
        if isinstance(path, expr.__class__) or \
                    (recursive and isinstance(path, irast.PathCombination)):
            if recursive:
                flatten_path_combination(path, recursive=True)
                paths.update(path.paths)
            else:
                paths.update(path.paths)
        else:
            paths.add(path)

    expr.paths = frozenset(paths)
    return expr


class PathExtractor(ast.NodeVisitor):
    def __init__(self, reverse=False, resolve_arefs=True, recurse_subqueries=0,
                 all_fragments=False, extract_subgraph_refs=False):
        super().__init__()
        self.reverse = reverse
        self.resolve_arefs = resolve_arefs
        self.recurse_subqueries = recurse_subqueries
        self.all_fragments = all_fragments
        self.extract_subgraph_refs = extract_subgraph_refs

    def combine_field_results(self, results, *,
                              combination=irast.Disjunction, flatten=True):
        paths = set(results)

        if len(paths) == 1:
            return next(iter(paths))
        elif len(paths) == 0:
            return None
        else:
            result = combination(paths=frozenset(paths))
            if flatten:
                return flatten_path_combination(result)
            else:
                return result

    def repeated_node_visit(self, node):
        return None

    def visit_GraphExpr(self, path):
        if self.recurse_subqueries <= 0:
            return None
        else:
            paths = set()

            self.recurse_subqueries -= 1

            if path.generator:
                normalized = self.visit(path.generator)
                if normalized:
                    paths.add(normalized)

            for part in ('selector', 'grouper', 'sorter'):
                e = getattr(path, part)
                if e:
                    for p in e:
                        normalized = self.visit(p)
                        if normalized:
                            paths.add(normalized)

            if path.set_op:
                for arg in (path.set_op_larg, path.set_op_rarg):
                    normalized = self.visit(arg)
                    if normalized:
                        paths.add(normalized)

            self.recurse_subqueries += 1

            return self.combine_field_results(paths)

    def visit_SubgraphRef(self, path):
        if not self.recurse_subqueries and self.extract_subgraph_refs:
            return path
        else:
            return self.visit(path.ref)

    def visit_EntitySet(self, path):
        result = path

        if self.reverse:
            paths = []
            paths.append(result)

            while result.rlink:
                result = result.rlink.source
                paths.append(result)

            if len(paths) == 1 or not self.all_fragments:
                result = paths[-1]
            else:
                result = irast.Disjunction(paths=frozenset(paths))

        return result

    def visit_InlineFilter(self, path):
        if self.resolve_arefs or self.reverse:
            return self.visit(path.ref)
        else:
            return path

    def visit_AtomicRef(self, path):
        if self.resolve_arefs or self.reverse:
            return self.visit(path.ref)
        else:
            return path

    def visit_LinkPropRef(self, path):
        if self.resolve_arefs or self.reverse:
            return self.visit(path.ref)
        else:
            return path

    def visit_EntityLink(self, path):
        if self.reverse:
            result = path
            if path.source:
                result = path.source
                while result.rlink:
                    result = result.rlink.source
        else:
            result = path
        return result

    def visit_PathCombination(self, path):
        return self.generic_visit(path, combination=path.__class__)

    def visit_BinOp(self, path):
        combination = \
            irast.Disjunction if is_weak_op(path.op) else irast.Conjunction

        return self.generic_visit(path, combination=combination)

    def visit_FunctionCall(self, path):
        return self.generic_visit(path, combination=irast.Conjunction,
                                  flatten=False)

    def generic_visit(self, node, *,
                      combine_results=None, combination=irast.Disjunction,
                      flatten=True):
        if combine_results is None:
            combine_results = functools.partial(
                self.combine_field_results,
                combination=combination, flatten=flatten)

        return super().generic_visit(node, combine_results=combine_results)


class LinearPath(list):
    """Denotes a linear path in the graph.

    The path is considered linear if it
    does not have branches and is in the form
    <concept> <link> <concept> <link> ... <concept>
    """

    def __eq__(self, other):
        if not isinstance(other, LinearPath):
            return NotImplemented

        if len(other) != len(self):
            return False
        elif len(self) == 0:
            return True

        if self[0] != other[0]:
            return False

        for i in range(1, len(self) - 1, 2):
            if self[i] != other[i]:
                break
            if self[i + 1] != other[i + 1]:
                break
        else:
            return True
        return False

    def add(self, link, direction, target):
        if not link.generic():
            link = link.bases[0]
        self.append((link, direction))
        self.append(target)

    def rptr(self):
        if len(self) > 1:
            return self[-2][0]
        else:
            return None

    def iter_prefixes(self):
        yield self[:1]

        for i in range(1, len(self) - 1, 2):
            if self[i + 1]:
                yield self[:i + 2]
            else:
                break

    def __hash__(self):
        return hash(tuple(self))

    def __str__(self):
        if not self:
            return ''

        result = '({})'.format(self[0].name)

        for i in range(1, len(self) - 1, 2):
            link = self[i][0].name
            if self[i + 1]:
                lexpr = '({} TO {})'.format(link, self[i + 1].name)
            else:
                lexpr = '({})'.format(link)
            result += '.{}{}'.format(self[i][1], lexpr)
        return result


def extract_prefixes(expr, prefixes=None):
    prefixes = prefixes if prefixes is not None else PathIndex()

    if isinstance(expr, irast.PathCombination):
        for path in expr.paths:
            extract_prefixes(path, prefixes)

    elif isinstance(expr, (irast.EntitySet, irast.AtomicRefSimple)):
        key = expr.get_id()

        if key:
            # XXX AtomicRefs with PathCombinations in ref don't have an id
            if key not in prefixes:
                prefixes[key] = {expr}
            else:
                prefixes[key].add(expr)

        if isinstance(expr, irast.EntitySet) and expr.rlink:
            extract_prefixes(expr.rlink.source, prefixes)
        elif isinstance(expr, irast.AtomicRefSimple):
            extract_prefixes(expr.ref, prefixes)

    elif isinstance(expr, irast.EntityLink):
        extract_prefixes(expr.target or expr.source, prefixes)

    elif isinstance(expr, irast.LinkPropRefSimple):
        extract_prefixes(expr.ref, prefixes)

    elif isinstance(expr, irast.BinOp):
        extract_prefixes(expr.left, prefixes)
        extract_prefixes(expr.right, prefixes)

    elif isinstance(expr, irast.UnaryOp):
        extract_prefixes(expr.expr, prefixes)

    elif isinstance(expr, irast.ExistPred):
        extract_prefixes(expr.expr, prefixes)

    elif isinstance(expr, (irast.InlineFilter, irast.InlinePropFilter)):
        extract_prefixes(expr.ref, prefixes)
        extract_prefixes(expr.expr, prefixes)

    elif isinstance(expr, (irast.AtomicRefExpr, irast.LinkPropRefExpr)):
        extract_prefixes(expr.expr, prefixes)

    elif isinstance(expr, irast.FunctionCall):
        for arg in expr.args:
            extract_prefixes(arg, prefixes)
        for sortexpr in expr.agg_sort:
            extract_prefixes(sortexpr.expr, prefixes)
        if expr.agg_filter:
            extract_prefixes(expr.agg_filter, prefixes)
        for partition_expr in expr.partition:
            extract_prefixes(partition_expr, prefixes)

    elif isinstance(expr, irast.TypeCast):
        extract_prefixes(expr.expr, prefixes)

    elif isinstance(expr, irast.NoneTest):
        extract_prefixes(expr.expr, prefixes)

    elif isinstance(expr, (irast.Sequence, irast.Record)):
        for path in expr.elements:
            extract_prefixes(path, prefixes)

    elif isinstance(expr, irast.Constant):
        pass

    elif isinstance(expr, irast.GraphExpr):
        pass

    elif isinstance(expr, irast.SubgraphRef):
        extract_prefixes(expr.ref, prefixes)

    else:
        assert False, 'unexpected node: "%r"' % expr

    return prefixes


def copy_path(path: (irast.EntitySet, irast.EntityLink, irast.BaseRef),
              connect_to_origin=False):

    if isinstance(path, irast.EntitySet):
        result = irast.EntitySet(
            id=path.id,
            context=path.context,
            pathvar=path.pathvar,
            concept=path.concept,
            users=path.users,
            joins=path.joins,
            rewrite_flags=path.rewrite_flags.copy(),
            anchor=path.anchor,
            show_as_anchor=path.show_as_anchor,
            _backend_rel_suffix=path._backend_rel_suffix)
        rlink = path.rlink

        if connect_to_origin:
            result.origin = \
                path.origin if path.origin is not None else path

    elif isinstance(path, irast.BaseRef):
        args = dict(
            id=path.id,
            context=path.context,
            ref=path.ref,
            ptr_class=path.ptr_class,
            rewrite_flags=path.rewrite_flags.copy(),
            pathvar=path.pathvar,
            anchor=path.anchor,
            show_as_anchor=path.show_as_anchor)

        if isinstance(path, irast.BaseRefExpr):
            args['expr'] = path.expr
            args['inline'] = path.inline

        result = path.__class__(**args)
        rlink = path.rlink

        if isinstance(path,
                      (irast.AtomicRefSimple, irast.LinkPropRefSimple)):
            result.name = path.name
    else:
        result = None
        rlink = path

    current = result

    while rlink:
        link = irast.EntityLink(
            context=rlink.context,
            target=current,
            link_class=rlink.link_class,
            direction=rlink.direction,
            propfilter=rlink.propfilter,
            users=rlink.users.copy(),
            pathvar=rlink.pathvar,
            anchor=rlink.anchor,
            show_as_anchor=rlink.show_as_anchor,
            rewrite_flags=rlink.rewrite_flags.copy(),
            pathspec_trigger=rlink.pathspec_trigger)

        if not result:
            result = link

        parent_path = rlink.source

        if parent_path:
            parent = irast.EntitySet(
                id=parent_path.id,
                context=parent_path.context,
                pathvar=parent_path.pathvar,
                anchor=parent_path.anchor,
                show_as_anchor=parent_path.show_as_anchor,
                concept=parent_path.concept,
                users=parent_path.users,
                joins=parent_path.joins,
                rewrite_flags=parent_path.rewrite_flags.copy(),
                _backend_rel_suffix=parent_path._backend_rel_suffix)
            parent.disjunction = irast.Disjunction(paths=frozenset(
                (link, )))

            if connect_to_origin:
                parent.origin = \
                    parent_path.origin if parent_path.origin is not None \
                    else parent_path

            link.source = parent

            if current:
                current.rlink = link
            current = parent
            rlink = parent_path.rlink

        else:
            rlink = None

    return result


def extract_paths(path, **kwargs):
    return PathExtractor.run(path, **kwargs)


def get_path_index(expr):
    paths = extract_paths(
        expr,
        reverse=True,
        resolve_arefs=False,
        recurse_subqueries=1,
        all_fragments=True)

    if isinstance(paths, irast.PathCombination):
        flatten_path_combination(paths, recursive=True)
        paths = paths.paths
    else:
        paths = [paths]

    path_idx = datastructures.Multidict()
    for path in paths:
        if isinstance(path, irast.EntitySet):
            path_idx.add(path.id, path)

    return path_idx


def extend_binop(binop,
                 *exprs,
                 op=ast.ops.AND,
                 reversed=False,
                 cls=irast.BinOp):
    exprs = list(exprs)
    binop = binop or exprs.pop(0)

    for expr in exprs:
        if expr is not binop:
            if reversed:
                binop = cls(right=binop, op=op, left=expr)
            else:
                binop = cls(left=binop, op=op, right=expr)

    return binop


def get_path_step(path, ptr_name):
    for p in itertools.chain(path.conjunction.paths, path.disjunction.paths):
        if p.link_class.normal_name() == ptr_name:
            return p.target


def extend_path(path, ptr_class,
                direction=s_pointers.PointerDirection.Outbound,
                location='selector', weak_path=False):

    target = ptr_class.get_far_endpoint(direction)

    if isinstance(target, s_concepts.Concept):
        target_set = irast.EntitySet()
        target_set.concept = target
        target_set.id = LinearPath(path.id)
        target_set.id.add(ptr_class, direction,
                          target_set.concept)
        target_set.users.add(location)

        link = irast.EntityLink(
            source=path,
            target=target_set,
            direction=direction,
            link_class=ptr_class,
            users={location})

        path.disjunction.update(link)
        path.disjunction.fixed = weak_path
        target_set.rlink = link

        result = target_set

    elif isinstance(target, s_atoms.Atom):
        target_set = irast.EntitySet()
        target_set.concept = target
        target_set.id = LinearPath(path.id)
        target_set.id.add(ptr_class, direction,
                          target_set.concept)
        target_set.users.add(location)

        link = irast.EntityLink(
            source=path,
            target=target_set,
            link_class=ptr_class,
            direction=direction,
            users={location})

        target_set.rlink = link

        atomref_id = LinearPath(path.id)
        atomref_id.add(ptr_class, direction, target)

        if not ptr_class.singular():
            ptr_name = sn.Name('std::target')
            ptr_class = ptr_class.pointers[ptr_name]
            atomref = irast.LinkPropRefSimple(
                name=ptr_name,
                ref=link,
                id=atomref_id,
                ptr_class=ptr_class)
            link.proprefs.add(atomref)
            path.disjunction.update(link)
            path.disjunction.fixed = weak_path
        else:
            atomref = irast.AtomicRefSimple(
                name=ptr_class.normal_name(),
                ref=path,
                id=atomref_id,
                rlink=link,
                ptr_class=ptr_class)
            path.atomrefs.add(atomref)
            link.target = atomref

        result = atomref

    return result
