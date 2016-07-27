##
# Copyright (c) 2015 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast

from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import types as s_types
from edgedb.lang.schema import utils as s_utils

from . import ast as irast


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
            typ = schema.get('std.str')

        elif isinstance(binop.op, (ast.ops.ComparisonOperator,
                                   ast.ops.ArithmeticOperator)):
            typ = infer_type(expr, schema)

        elif isinstance(binop.op, ast.ops.MembershipOperator) and not reversed:
            from edgedb.lang.schema import objects as s_obj

            elem_type = infer_type(expr, schema)
            typ = s_obj.Set(element_type=elem_type)

        elif isinstance(binop.op, ast.ops.BooleanOperator):
            typ = schema.get('std.bool')

        else:
            msg = 'cannot infer expr type: unsupported operator: {!r}'\
                    .format(binop.op)
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
    if isinstance(ir, irast.MetaRef):
        result = schema.get('std.str')

    elif isinstance(ir, irast.AtomicRefSimple):
        if isinstance(ir.ref, irast.PathCombination):
            targets = [t.concept for t in ir.ref.paths]
            concept = s_utils.get_prototype_nearest_common_ancestor(targets)
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
            targets = [t.link_proto for t in ir.ref.paths]
            link = s_utils.get_prototype_nearest_common_ancestor(targets)
        else:
            link = ir.ref.link_proto

        prop = link.getptr(schema, ir.name)
        assert prop, '"%s" is not a property of "%s"' % (ir.name, link.name)
        result = prop.target

    elif isinstance(ir, irast.BaseRefExpr):
        result = infer_type(ir.expr, schema)

    elif isinstance(ir, irast.Record):
        result = ir.concept

    elif isinstance(ir, irast.FunctionCall):
        argtypes = tuple(infer_type(arg, schema) for arg in ir.args)

        func_obj = schema.get(ir.name)
        result = func_obj.returntype

    elif isinstance(ir, irast.Constant):
        if ir.expr:
            result = infer_type(ir.expr, schema)
        else:
            result = ir.type

    elif isinstance(ir, irast.BinOp):
        if isinstance(ir.op, (ast.ops.ComparisonOperator,
                                ast.ops.EquivalenceOperator,
                                ast.ops.MembershipOperator)):
            result = schema.get('std.bool')
        else:
            left_type = infer_type(ir.left, schema)
            right_type = infer_type(ir.right, schema)
            result = s_types.TypeRules.get_result(
                            ir.op, (left_type, right_type), schema)
            if result is None:
                result = s_types.TypeRules.get_result(
                            (ir.op, 'reversed'),
                            (right_type, left_type), schema)

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
        result = ir.type

    elif isinstance(ir, irast.SubgraphRef):
        subgraph = ir.ref
        if len(subgraph.selector) == 1:
            result = infer_type(subgraph.selector[0].expr, schema)
        else:
            result = None

    elif isinstance(ir, irast.ExistPred):
        result = schema.get('std.bool')

    else:
        result = None

    if result is not None:
        allowed = (s_obj.ProtoObject, s_obj.PrototypeClass)
        assert (isinstance(result, allowed) or
                (isinstance(result, (tuple, list)) and
                 isinstance(result[1], allowed))), \
               "infer_type({!r}) retured {!r} instead of a prototype" \
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
                    result.append(node.link_proto)

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


def extract_paths(path, reverse=False, resolve_arefs=True, recurse_subqueries=0,
                        all_fragments=False, extract_subgraph_refs=False):
    if isinstance(path, irast.GraphExpr):
        if recurse_subqueries <= 0:
            return None
        else:
            paths = set()

            recurse_subqueries -= 1

            if path.generator:
                normalized = extract_paths(path.generator, reverse,
                                           resolve_arefs, recurse_subqueries,
                                           all_fragments,
                                           extract_subgraph_refs)
                if normalized:
                    paths.add(normalized)

            for part in ('selector', 'grouper', 'sorter'):
                e = getattr(path, part)
                if e:
                    for p in e:
                        normalized = extract_paths(p, reverse, resolve_arefs,
                                                   recurse_subqueries,
                                                   all_fragments,
                                                   extract_subgraph_refs)
                        if normalized:
                            paths.add(normalized)

            if path.set_op:
                for arg in (path.set_op_larg, path.set_op_rarg):
                    normalized = extract_paths(arg, reverse, resolve_arefs,
                                               recurse_subqueries,
                                               all_fragments,
                                               extract_subgraph_refs)
                    if normalized:
                        paths.add(normalized)

            if len(paths) == 1:
                return next(iter(paths))
            elif len(paths) == 0:
                return None
            else:
                result = irast.Disjunction(paths=frozenset(paths))
                return flatten_path_combination(result)

    elif isinstance(path, irast.SubgraphRef):
        if not recurse_subqueries and extract_subgraph_refs:
            return path
        else:
            return extract_paths(path.ref, reverse, resolve_arefs,
                                 recurse_subqueries, all_fragments,
                                 extract_subgraph_refs)

    elif isinstance(path, irast.SelectorExpr):
        return extract_paths(path.expr, reverse, resolve_arefs,
                             recurse_subqueries, all_fragments,
                             extract_subgraph_refs)

    elif isinstance(path, irast.SortExpr):
        return extract_paths(path.expr, reverse, resolve_arefs,
                             recurse_subqueries, all_fragments,
                             extract_subgraph_refs)

    elif isinstance(path, (irast.EntitySet, irast.InlineFilter,
                           irast.AtomicRef)):
        if isinstance(path, (irast.InlineFilter, irast.AtomicRef)) and \
                                                (resolve_arefs or reverse):
            result = path.ref
        else:
            result = path

        if isinstance(result, irast.EntitySet):
            if reverse:
                paths = []
                paths.append(result)

                while result.rlink:
                    result = result.rlink.source
                    paths.append(result)

                if len(paths) == 1 or not all_fragments:
                    result = paths[-1]
                else:
                    result = irast.Disjunction(paths=frozenset(paths))

        return result

    elif isinstance(path, irast.InlinePropFilter):
        return extract_paths(path.ref, reverse, resolve_arefs,
                             recurse_subqueries, all_fragments,
                             extract_subgraph_refs)

    elif isinstance(path, irast.LinkPropRef):
        if resolve_arefs or reverse:
            return extract_paths(path.ref, reverse, resolve_arefs,
                                 recurse_subqueries, all_fragments,
                                 extract_subgraph_refs)
        else:
            return path

    elif isinstance(path, irast.EntityLink):
        if reverse:
            result = path
            if path.source:
                result = path.source
                while result.rlink:
                    result = result.rlink.source
        else:
            result = path
        return result

    elif isinstance(path, irast.PathCombination):
        result = set()
        for p in path.paths:
            normalized = extract_paths(p, reverse, resolve_arefs,
                                       recurse_subqueries, all_fragments,
                                       extract_subgraph_refs)
            if normalized:
                result.add(normalized)
        if len(result) == 1:
            return next(iter(result))
        elif len(result) == 0:
            return None
        else:
            return flatten_path_combination(
                        path.__class__(paths=frozenset(result)))

    elif isinstance(path, irast.BinOp):
        combination = irast.Disjunction if is_weak_op(path.op) \
                                           else irast.Conjunction

        paths = set()
        for p in (path.left, path.right):
            normalized = extract_paths(p, reverse, resolve_arefs,
                                       recurse_subqueries, all_fragments,
                                       extract_subgraph_refs)
            if normalized:
                paths.add(normalized)

        if len(paths) == 1:
            return next(iter(paths))
        elif len(paths) == 0:
            return None
        else:
            return flatten_path_combination(combination(paths=frozenset(paths)))

    elif isinstance(path, irast.UnaryOp):
        return extract_paths(path.expr, reverse, resolve_arefs,
                             recurse_subqueries, all_fragments,
                             extract_subgraph_refs)

    elif isinstance(path, irast.ExistPred):
        return extract_paths(path.expr, reverse, resolve_arefs,
                             recurse_subqueries, all_fragments,
                             extract_subgraph_refs)

    elif isinstance(path, irast.TypeCast):
        return extract_paths(path.expr, reverse, resolve_arefs,
                             recurse_subqueries, all_fragments,
                             extract_subgraph_refs)

    elif isinstance(path, irast.NoneTest):
        return extract_paths(path.expr, reverse, resolve_arefs,
                             recurse_subqueries, all_fragments,
                             extract_subgraph_refs)

    elif isinstance(path, irast.FunctionCall):
        paths = set()
        for p in path.args:
            p = extract_paths(p, reverse, resolve_arefs, recurse_subqueries,
                              all_fragments, extract_subgraph_refs)
            if p:
                paths.add(p)

        for p in path.agg_sort:
            p = extract_paths(p, reverse, resolve_arefs, recurse_subqueries,
                              all_fragments, extract_subgraph_refs)
            if p:
                paths.add(p)

        for p in path.partition:
            p = extract_paths(p, reverse, resolve_arefs, recurse_subqueries,
                              all_fragments, extract_subgraph_refs)
            if p:
                paths.add(p)

        if len(paths) == 1:
            return next(iter(paths))
        elif len(paths) == 0:
            return None
        else:
            return irast.Conjunction(paths=frozenset(paths))

    elif isinstance(path, (irast.Sequence, irast.Record)):
        paths = set()
        for p in path.elements:
            p = extract_paths(p, reverse, resolve_arefs, recurse_subqueries,
                                  all_fragments, extract_subgraph_refs)
            if p:
                paths.add(p)

        if len(paths) == 1:
            return next(iter(paths))
        elif len(paths) == 0:
            return None
        else:
            return irast.Disjunction(paths=frozenset(paths))

    elif isinstance(path, irast.Constant):
        return None

    else:
        assert False, 'unexpected node "%r"' % path


class LinearPath(list):
    """
    Denotes a linear path in the graph.  The path is considered linear if it does not have
    branches and is in the form <concept> <link> <concept> <link> ... <concept>
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

    def __hash__(self):
        return hash(tuple(self))

    def __str__(self):
        if not self:
            return ''

        result = '%s' % self[0].name

        for i in range(1, len(self) - 1, 2):
            link = self[i][0].name
            if self[i + 1]:
                if isinstance(self[i + 1], tuple):
                    concept = '%s(%s)' % (self[i + 1][0].name, self[i + 1][1])
                else:
                    concept = self[i + 1].name
            else:
                concept = 'NONE'
            result += '[%s%s]%s' % (self[i][1], link, concept)
        return result


def walk_path_towards_root(expr, trail):
    step = expr
    while step is not None:
        link = step.as_link()
        if link is not None:
            link_proto = link.__sx_prototype__
            direction = step._class_metadata.link_direction
            trail.add(link_proto, direction, link.source.__sx_prototype__)
            step = link.source
        else:
            step = None


def get_path_id(node, join=None):
    """Return a LinearPath by walking the given expression's link chain."""
    path = LinearPath()

    concept = node.__sx_prototype__

    path.append(concept)
    walk_path_towards_root(node, path)

    if join:
        joinpoint = join(path[-1].name)
        walk_path_towards_root(joinpoint, path)

    # Since we walked backwards, the final path needs to be reversed
    path.reverse()
    return path


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
