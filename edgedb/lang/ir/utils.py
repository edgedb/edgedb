##
# Copyright (c) 2015 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos import types as caos_types
from metamagic.caos import utils as caos_utils

from metamagic.utils import ast

from . import ast as irast


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

        if isinstance(binop.op, irast.CaosMatchOperator):
            typ = schema.get('metamagic.caos.builtins.str')

        elif isinstance(binop.op, (ast.ops.ComparisonOperator,
                                   ast.ops.ArithmeticOperator)):
            typ = infer_type(expr, schema)

        elif isinstance(binop.op, ast.ops.MembershipOperator) and not reversed:
            from metamagic.caos import proto

            elem_type = infer_type(expr, schema)
            typ = proto.Set(element_type=elem_type)

        elif isinstance(binop.op, ast.ops.BooleanOperator):
            typ = schema.get('metamagic.caos.builtins.bool')

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
        result = schema.get('metamagic.caos.builtins.str')

    elif isinstance(ir, irast.AtomicRefSimple):
        if isinstance(ir.ref, irast.PathCombination):
            targets = [t.concept for t in ir.ref.paths]
            concept = caos_utils.get_prototype_nearest_common_ancestor(targets)
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
            link = caos_utils.get_prototype_nearest_common_ancestor(targets)
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
        result = caos_types.TypeRules.get_result(ir.name, argtypes, schema)

        if result is None:
            fcls = caos_types.FunctionMeta.get_function_class(ir.name)
            if fcls:
                signature = fcls.get_signature(argtypes, schema=schema)
                if signature and signature[2]:
                    if isinstance(signature[2], tuple):
                        result = (signature[2][0], schema.get(signature[2][1]))
                    else:
                        result = schema.get(signature[2])

    elif isinstance(ir, irast.Constant):
        if ir.expr:
            result = infer_type(ir.expr, schema)
        else:
            result = ir.type

    elif isinstance(ir, irast.BinOp):
        if isinstance(ir.op, (ast.ops.ComparisonOperator,
                                ast.ops.EquivalenceOperator,
                                ast.ops.MembershipOperator)):
            result = schema.get('metamagic.caos.builtins.bool')
        else:
            left_type = infer_type(ir.left, schema)
            right_type = infer_type(ir.right, schema)
            result = caos_types.TypeRules.get_result(
                            ir.op, (left_type, right_type), schema)
            if result is None:
                result = caos_types.TypeRules.get_result(
                            (ir.op, 'reversed'),
                            (right_type, left_type), schema)

    elif isinstance(ir, irast.UnaryOp):
        operand_type = infer_type(ir.expr, schema)
        result = caos_types.TypeRules.get_result(
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
        result = schema.get('metamagic.caos.builtins.bool')

    else:
        result = None

    if result is not None:
        allowed = (caos_types.ProtoObject, caos_types.PrototypeClass)
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
