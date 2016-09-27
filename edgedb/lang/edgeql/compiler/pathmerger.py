##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import collections

from edgedb.lang.common import ast
from edgedb.lang.common import debug
from edgedb.lang.common.algos import boolean
from edgedb.lang.common import markup  # NOQA

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils


def fixup_refs(refs, newref):
    irast.Base.fixup_refs(refs, newref)


def is_weak_op(context, op):
    return irutils.is_weak_op(op) or context.current.location != 'generator'


def flatten_and_unify_path_combination(
        context, expr, deep=False, merge_filters=False, memo=None):
    # Flatten nested disjunctions and conjunctions since
    # they are associative.
    #
    assert isinstance(expr, irast.PathCombination)

    irutils.flatten_path_combination(expr)

    if deep:
        newpaths = set()
        for path in expr.paths:
            path = PathMerger.run(path, context=context, memo=memo)
            newpaths.add(path)

        expr = expr.__class__(paths=frozenset(newpaths))

    unify_paths(
        context, expr.paths, mode=expr.__class__, merge_filters=merge_filters)

    expr.paths = frozenset(p for p in expr.paths)
    return expr


@debug.debug
def unify_paths(context, paths, mode, reverse=True, merge_filters=False):
    mypaths = set(paths)

    result = None

    while mypaths and not result:
        result = irutils.extract_paths(mypaths.pop(), reverse=reverse)

    if result is not None:
        paths_to_merge = set()
        for path in mypaths:
            path2 = irutils.extract_paths(path, reverse=reverse)
            if path2 is not None and path2 is not result:
                paths_to_merge.add(path2)

        if paths_to_merge:
            """LOG [path.merge] PREPARING TO UNIFY
            markup.dump([result] + list(paths_to_merge))
            """

            result = _unify_paths(
                context, result, paths_to_merge, mode, merge_filters)

            """LOG [path.merge] UNIFICATION RESULT
            markup.dump(result)
            """

    return result


nest = 0


@debug.debug
def _unify_paths(context, result, paths, mode, merge_filters=False):
    global nest

    mypaths = set(paths)

    while mypaths:
        path = mypaths.pop()

        if result is path:
            continue

        if issubclass(mode, irast.Disjunction):
            """LOG [path.merge] ADDING
            print(' ' * nest, 'ADDING', result, path,
                  getattr(result, 'id', '??'),
                  getattr(path, 'id', '??'), merge_filters)
            nest += 2

            markup.dump(result)
            markup.dump(path)
            """

            result = add_paths(
                context, result, path, merge_filters=merge_filters)
            assert result
            """LOG [path.merge] ADDITION RESULT
            nest -= 2
            markup.dump(result)
            """
        else:
            """LOG [path.merge] INTERSECTING
            print(' ' * nest, result, path,
                  getattr(result, 'id', '??'),
                  getattr(path, 'id', '??'), merge_filters)
            nest += 2
            """

            result = intersect_paths(
                context, result, path, merge_filters=merge_filters)
            assert result
            """LOG [path.merge] INTERSECTION RESULT
            markup.dump(result)
            nest -= 2
            """

    return result


def miniterms_from_conjunctions(paths):
    variables = collections.OrderedDict()

    terms = []

    for path in paths:
        term = 0

        if isinstance(path, irast.Conjunction):
            for subpath in path.paths:
                if subpath not in variables:
                    variables[subpath] = len(variables)
                term += 1 << variables[subpath]

        elif isinstance(path, irast.EntityLink):
            if path not in variables:
                variables[path] = len(variables)
            term += 1 << variables[path]

        terms.append(term)

    return list(variables), boolean.ints_to_terms(*terms)


def conjunctions_from_miniterms(terms, variables):
    paths = set()

    for term in terms:
        conjpaths = [variables[i] for i, bit in enumerate(term) if bit]
        if len(conjpaths) > 1:
            paths.add(irast.Conjunction(paths=frozenset(conjpaths)))
        else:
            paths.add(conjpaths[0])
    return paths


def minimize_disjunction(paths):
    variables, miniterms = miniterms_from_conjunctions(paths)
    minimized = boolean.minimize(miniterms)
    paths = conjunctions_from_miniterms(minimized, variables)
    result = irast.Disjunction(paths=frozenset(paths))
    return result


def add_sets(context, left, right, merge_filters=False):
    if left is right:
        return left

    if merge_filters:
        if (isinstance(merge_filters, ast.ops.Operator) and
                is_weak_op(context, merge_filters)):
            merge_op = ast.ops.OR
        else:
            merge_op = ast.ops.AND

    match = match_prefixes(left, right, ignore_filters=merge_filters)
    if match:
        if isinstance(left, irast.EntityLink):
            left_link = left
            left = left.target
        else:
            left_link = left.rlink

        if isinstance(right, irast.EntityLink):
            right_link = right
            right = right.target
        else:
            right_link = right.rlink

        if left_link:
            fixup_refs([right_link], left_link)
            if merge_filters and right_link.propfilter:
                left_link.propfilter = irutils.extend_binop(
                    left_link.propfilter,
                    right_link.propfilter,
                    op=merge_op)

            left_link.proprefs.update(right_link.proprefs)
            left_link.users.update(right_link.users)
            if right_link.target:
                left_link.target = right_link.target

        if left and right:
            fixup_refs([right], left)

            if merge_filters and right.filter:
                left.filter = irutils.extend_binop(
                    left.filter, right.filter, op=ast.ops.AND)

            if merge_filters:
                paths_left = set()
                for dpath in right.disjunction.paths:
                    if isinstance(dpath,
                                  (irast.EntitySet, irast.EntityLink)):
                        merged = intersect_paths(context, left.conjunction,
                                                 dpath, merge_filters)
                        if merged is not left.conjunction:
                            paths_left.add(dpath)
                    else:
                        paths_left.add(dpath)
                right.disjunction = irast.Disjunction(
                    paths=frozenset(paths_left))

            left.disjunction = add_paths(
                context, left.disjunction, right.disjunction, merge_filters)

            if merge_filters and merge_op == ast.ops.OR:
                left.disjunction.fixed = True

            left.atomrefs.update(right.atomrefs)
            left.metarefs.update(right.metarefs)
            left.users.update(right.users)
            left.joins.update(right.joins)
            left.joins.discard(left)

            if left.origin is None and right.origin is not None:
                left.origin = right.origin

            if right.concept.issubclass(left.concept):
                left.concept = right.concept

            if merge_filters:
                left.conjunction = intersect_paths(
                    context, left.conjunction, right.conjunction,
                    merge_filters)

                # If greedy disjunction merging is requested, we must
                # also try to merge disjunctions.
                paths = frozenset(left.conjunction.paths) | frozenset(
                    left.disjunction.paths)
                unify_paths(
                    context,
                    paths,
                    mode=irast.Conjunction,
                    reverse=False,
                    merge_filters=merge_filters)
                left.disjunction.paths = \
                    left.disjunction.paths - left.conjunction.paths
            else:
                conjunction = add_paths(
                    context, left.conjunction, right.conjunction,
                    merge_filters)
                if conjunction.paths:
                    left.disjunction.update(conjunction)
                left.conjunction.paths = frozenset()

        if isinstance(left, irast.EntitySet):
            return left
        elif isinstance(right, irast.EntitySet):
            return right
        else:
            return left_link
    else:
        result = irast.Disjunction(paths=frozenset((left, right)))

    return result


def add_to_disjunction(context, disjunction, path, merge_filters):
    # Other operand is a disjunction -- look for path we can merge with,
    # if not found, append to disjunction.
    for dpath in disjunction.paths:
        if isinstance(dpath, (irast.EntityLink, irast.EntitySet)):
            merge = add_sets(context, dpath, path, merge_filters)
            if merge is dpath:
                break
    else:
        disjunction.update(path)

    return disjunction


def add_to_conjunction(context, conjunction, path, merge_filters):
    result = None
    if merge_filters:
        for cpath in conjunction.paths:
            if isinstance(cpath, (irast.EntityLink, irast.EntitySet)):
                merge = add_sets(context, cpath, path, merge_filters)
                if merge is cpath:
                    result = conjunction
                    break

    if not result:
        result = irast.Disjunction(paths=frozenset({conjunction, path}))

    return result


def add_disjunctions(context, left, right, merge_filters=False):
    result = irast.Disjunction()
    result.update(left)
    result.update(right)

    if len(result.paths) > 1:
        unify_paths(
            context,
            result.paths,
            mode=result.__class__,
            reverse=False,
            merge_filters=merge_filters)
        result.paths = frozenset(p for p in result.paths)

    return result


def add_conjunction_to_disjunction(context, disjunction, conjunction):
    if disjunction.paths and conjunction.paths:
        return irast.Disjunction(
            paths=frozenset({disjunction, conjunction}))
    elif disjunction.paths:
        return disjunction
    elif conjunction.paths:
        return irast.Disjunction(paths=frozenset({conjunction}))
    else:
        return irast.Disjunction()


def add_conjunctions(context, left, right):
    paths = frozenset(p for p in (left, right) if p.paths)
    return irast.Disjunction(paths=paths)


def add_paths(context, left, right, merge_filters=False):
    if isinstance(left, (irast.EntityLink, irast.EntitySet)):
        if isinstance(right, (irast.EntityLink, irast.EntitySet)):
            # Both operands are sets -- simply merge them
            result = add_sets(context, left, right, merge_filters)

        elif isinstance(right, irast.Disjunction):
            result = add_to_disjunction(context, right, left, merge_filters)

        elif isinstance(right, irast.Conjunction):
            result = add_to_conjunction(context, right, left, merge_filters)

    elif isinstance(left, irast.Disjunction):
        if isinstance(right, (irast.EntityLink, irast.EntitySet)):
            result = add_to_disjunction(context, left, right, merge_filters)

        elif isinstance(right, irast.Disjunction):
            result = add_disjunctions(context, left, right, merge_filters)

        elif isinstance(right, irast.Conjunction):
            result = add_conjunction_to_disjunction(context, left, right)

    elif isinstance(left, irast.Conjunction):
        if isinstance(right, (irast.EntityLink, irast.EntitySet)):
            result = add_to_conjunction(context, left, right, merge_filters)

        elif isinstance(right, irast.Disjunction):
            result = add_conjunction_to_disjunction(context, right, left)

        elif isinstance(right, irast.Conjunction):
            result = add_conjunctions(context, left, right)

    else:
        assert False, 'unexpected nodes "{!r}", "{!r}"'.format(left, right)

    return result


def intersect_sets(context, left, right, merge_filters=False):
    if left is right:
        return left

    match = match_prefixes(left, right, ignore_filters=True)
    if match:
        if isinstance(left, irast.EntityLink):
            left_set = left.target
            right_set = right.target
            left_link = left
            right_link = right
        else:
            left_set = left
            right_set = right
            left_link = left.rlink
            right_link = right.rlink

        if left_link:
            fixup_refs([right_link], left_link)
            if right_link.propfilter:
                left_link.propfilter = irutils.extend_binop(
                    left_link.propfilter,
                    right_link.propfilter,
                    op=ast.ops.AND)

            left_link.proprefs.update(right_link.proprefs)
            left_link.users.update(right_link.users)
            if right_link.target:
                left_link.target = right_link.target

        if right_set and left_set:
            fixup_refs([right_set], left_set)

            if right_set.filter:
                left_set.filter = irutils.extend_binop(
                    left_set.filter, right_set.filter, op=ast.ops.AND)

            left_set.conjunction = intersect_paths(
                context, left_set.conjunction, right_set.conjunction,
                merge_filters)
            left_set.atomrefs.update(right_set.atomrefs)
            left_set.metarefs.update(right_set.metarefs)
            left_set.users.update(right_set.users)
            left_set.joins.update(right_set.joins)
            left_set.joins.discard(left_set)

            if left_set.origin is None and right_set.origin is not None:
                left_set.origin = right_set.origin

            if right_set.concept.issubclass(left_set.concept):
                left_set.concept = right_set.concept

            disjunction = intersect_paths(
                context, left_set.disjunction, right_set.disjunction,
                merge_filters)

            left_set.disjunction = irast.Disjunction()

            if isinstance(disjunction, irast.Disjunction):
                unify_paths(
                    context,
                    left_set.conjunction.paths | disjunction.paths,
                    mode=irast.Conjunction,
                    reverse=False,
                    merge_filters=merge_filters)

                left_set.disjunction = disjunction

                if len(left_set.disjunction.paths) == 1:
                    first_disj = next(iter(left_set.disjunction.paths))
                    if isinstance(first_disj, irast.Conjunction):
                        left_set.conjunction = first_disj
                        left_set.disjunction = irast.Disjunction()

            elif disjunction.paths:
                left_set.conjunction = intersect_paths(
                    context, left_set.conjunction, disjunction, merge_filters)

                irutils.flatten_path_combination(left_set.conjunction)

                if len(left_set.conjunction.paths) == 1:
                    first_conj = next(iter(left_set.conjunction.paths))
                    if isinstance(first_conj, irast.Disjunction):
                        left_set.disjunction = first_conj
                        left_set.conjunction = irast.Conjunction()

        if isinstance(left, irast.EntitySet):
            return left
        elif isinstance(right, irast.EntitySet):
            return right
        else:
            return left_link

    else:
        result = irast.Conjunction(paths=frozenset({left, right}))

    return result


def intersect_with_disjunction(context, disjunction, path):
    result = irast.Conjunction(paths=frozenset((disjunction, path)))
    return result


def intersect_with_conjunction(context, conjunction, path):
    # Other operand is a disjunction -- look for path we can merge with,
    # if not found, append to conjunction.
    for cpath in conjunction.paths:
        if isinstance(cpath, (irast.EntityLink, irast.EntitySet)):
            merge = intersect_sets(context, cpath, path)
            if merge is cpath:
                break
    else:
        conjunction = irast.Conjunction(
            paths=frozenset(conjunction.paths | {path}))

    return conjunction


def intersect_conjunctions(context, left, right, merge_filters=False):
    result = irast.Conjunction(paths=left.paths)
    result.update(right)

    if len(result.paths) > 1:
        irutils.flatten_path_combination(result)
        unify_paths(
            context,
            result.paths,
            mode=result.__class__,
            reverse=False,
            merge_filters=merge_filters)
        result.paths = frozenset(p for p in result.paths)

    return result


def intersect_disjunctions(context, left, right):
    """Produce a conjunction of two disjunctions."""
    if left.paths and right.paths:
        # (a | b) & (c | d) --> a & c | a & d | b & c | b & d
        # We unroll the expression since it is highly probable that
        # the resulting conjunctions will merge and we'll get a simpler
        # expression which is we further attempt to minimize using boolean
        # minimizer.
        #
        paths = set()

        for l in left.paths:
            for r in right.paths:
                paths.add(intersect_paths(context, l, r))

        result = minimize_disjunction(paths)
        return result

    else:
        # Degenerate case
        if not left.paths:
            paths = right.paths
            fixed = right.fixed
        elif not right.paths:
            paths = left.paths
            fixed = left.fixed

        if len(paths) <= 1 and not fixed:
            return irast.Conjunction(paths=frozenset(paths))
        else:
            return irast.Disjunction(paths=frozenset(paths), fixed=fixed)


def intersect_disjunction_with_conjunction(context, disjunction, conjunction):
    if disjunction.paths and conjunction.paths:
        return irast.Disjunction(paths=frozenset(
            {disjunction, conjunction}))
    elif conjunction.paths:
        return conjunction
    elif disjunction.paths:
        return irast.Conjunction(paths=frozenset({disjunction}))
    else:
        return irast.Conjunction()


def intersect_paths(context, left, right, merge_filters=False):
    if isinstance(left, (irast.EntityLink, irast.EntitySet)):
        if isinstance(right, (irast.EntityLink, irast.EntitySet)):
            # Both operands are sets -- simply merge them
            result = intersect_sets(context, left, right, merge_filters)

        elif isinstance(right, irast.Disjunction):
            result = intersect_with_disjunction(context, right, left)

        elif isinstance(right, irast.Conjunction):
            result = intersect_with_conjunction(context, right, left)

    elif isinstance(left, irast.Disjunction):
        if isinstance(right, (irast.EntityLink, irast.EntitySet)):
            result = intersect_with_disjunction(context, left, right)

        elif isinstance(right, irast.Disjunction):
            result = intersect_disjunctions(context, left, right)

        elif isinstance(right, irast.Conjunction):
            result = intersect_disjunction_with_conjunction(
                context, left, right)

    elif isinstance(left, irast.Conjunction):
        if isinstance(right, (irast.EntityLink, irast.EntitySet)):
            result = intersect_with_conjunction(context, left, right)

        elif isinstance(right, irast.Disjunction):
            result = intersect_disjunction_with_conjunction(
                context, right, left)

        elif isinstance(right, irast.Conjunction):
            result = intersect_conjunctions(
                context, left, right, merge_filters)

    return result


@debug.debug
def match_prefixes(our, other, ignore_filters):
    result = None

    if isinstance(our, irast.EntityLink):
        link = our
        our_node = our.target
        if our_node is None:
            our_id = irutils.LinearPath(our.source.id)
            our_id.add(link.link_proto, link.direction, None)
            our_node = our.source
        else:
            our_id = our_node.id
    else:
        link = None
        our_node = our
        our_id = our.id

    if isinstance(other, irast.EntityLink):
        other_link = other
        other_node = other.target
        if other_node is None:
            other_node = other.source
            other_id = irutils.LinearPath(other.source.id)
            other_id.add(other_link.link_proto, other_link.direction, None)
        else:
            other_id = other_node.id
    else:
        other_link = None
        other_node = other
        other_id = other.id

    if our_id[-1] is None and other_id[-1] is not None:
        other_id = irutils.LinearPath(other_id)
        other_id[-1] = None

    if other_id[-1] is None and our_id[-1] is not None:
        our_id = irutils.LinearPath(our_id)
        our_id[-1] = None

    ok = (
        (our_node is None and other_node is None)
        or (our_node is not None and other_node is not None
            and (our_id == other_id
                 and our_node.pathvar == other_node.pathvar
                 and (ignore_filters
                      or (not our_node.filter
                          and not other_node.filter
                          and not our_node.conjunction.paths
                          and not other_node.conjunction.paths))))
        and (not link or (link.link_proto == other_link.link_proto
                          and link.direction == other_link.direction))
    )

    """LOG [path.merge] MATCH PREFIXES
    print(' ' * nest, our, other, ignore_filters)
    print(' ' * nest, '   PATHS: ', our_id)
    print(' ' * nest, '      *** ', other_id)
    print(' ' * nest, 'PATHVARS: ',
          our_node.pathvar if our_node is not None else None)
    print(' ' * nest, '      *** ',
          other_node.pathvar if other_node is not None else None)
    print(' ' * nest, '    LINK: ', link.link_proto if link else None)
    print(' ' * nest, '      *** ',
          other_link.link_proto if other_link else None)
    print(' ' * nest, '     DIR: ', link.direction if link else None)
    print(' ' * nest, '      *** ',
          other_link.direction if other_link else None)
    print(' ' * nest, '      EQ: ', ok)
    """

    if ok:
        if other_link:
            result = other_link
        else:
            result = other_node
    """LOG [path.merge] MATCH PREFIXES RESULT
    print(' ' * nest, '    ----> ', result)
    """

    return result


class PathMerger(ast.NodeTransformer):


    def visit_AtomicRefExpr(self, expr):
        if self._context.current.location == 'generator' and expr.inline:
            expr.ref.filter = irutils.extend_binop(expr.ref.filter, expr.expr)
            self.visit(expr.ref)
            arefs = ast.find_children(
                expr, lambda i: isinstance(i, irast.AtomicRefSimple))
            for aref in arefs:
                self.visit(aref)
            expr = irast.InlineFilter(expr=expr.ref.filter, ref=expr.ref)
        else:
            self.visit(expr.expr)

        return expr

    def visit_LinkPropRefExpr(self, expr):
        if self._context.current.location == 'generator' and expr.inline:
            prefs = ast.find_children(
                expr.expr,
                lambda i: (isinstance(i, irast.LinkPropRefSimple)
                           and i.ref == expr.ref))
            expr.ref.proprefs.update(prefs)
            expr.ref.propfilter = irutils.extend_binop(expr.ref.propfilter,
                                                       expr.expr)
            if expr.ref.target:
                self.visit(expr.ref.target)
            else:
                self.visit(expr.ref.source)
            expr = irast.InlinePropFilter(
                expr=expr.ref.propfilter, ref=expr.ref)
        else:
            self.visit(expr.expr)

        return expr

    def visit_BinOp(self, expr):
        left = self.visit(expr.left)
        right = self.visit(expr.right)

        weak_op = is_weak_op(self._context, expr.op)

        if weak_op:
            combination = irast.Disjunction
        else:
            combination = irast.Conjunction

        paths = set()
        for operand in (left, right):
            if isinstance(operand,
                          (irast.InlineFilter, irast.AtomicRefSimple)):
                paths.add(operand.ref)
            else:
                paths.add(operand)

        e = combination(paths=frozenset(paths))
        merge_filters = \
            self._context.current.location != 'generator' or weak_op
        if merge_filters:
            merge_filters = expr.op
        flatten_and_unify_path_combination(
            self._context, e, deep=False, merge_filters=merge_filters,
            memo=self._memo)

        if len(e.paths) > 1:
            expr = irast.BinOp(
                left=left,
                op=expr.op,
                right=right,
                aggregates=expr.aggregates)
        else:
            expr = next(iter(e.paths))

        return expr

    def visit_PathCombination(self, expr):
        return flatten_and_unify_path_combination(
            self._context, expr, deep=True, memo=self._memo)

    def visit_MetaRef(self, expr):
        expr.ref.metarefs.add(expr)
        return expr

    def visit_AtomicRefSimple(self, expr):
        expr.ref.atomrefs.add(expr)
        return expr

    def visit_LinkPropRefSimple(self, expr):
        expr.ref.proprefs.add(expr)
        return expr

    def visit_FunctionCall(self, expr):
        args = []
        for arg in expr.args:
            args.append(self.visit(arg))

        for sortexpr in expr.agg_sort:
            self.visit(sortexpr.expr)

        if expr.agg_filter:
            self.visit(expr.agg_filter)

        for partition_expr in expr.partition:
            self.visit(partition_expr)

        if (len(args) > 1 or expr.agg_sort or expr.agg_filter or
                expr.partition):
            # Make sure that function args are properly merged against
            # each other. This is simply a matter of unification of the
            # conjunction of paths generated by function argument
            # expressions.
            #
            paths = []
            for arg in args:
                path = irutils.extract_paths(arg, reverse=True)
                if path:
                    paths.append(path)
            for sortexpr in expr.agg_sort:
                path = irutils.extract_paths(sortexpr, reverse=True)
                if path:
                    paths.append(path)
            if expr.agg_filter:
                paths.append(
                    irutils.extract_paths(
                        expr.agg_filter, reverse=True))
            for partition_expr in expr.partition:
                path = irutils.extract_paths(partition_expr, reverse=True)
                if path:
                    paths.append(path)
            e = irast.Conjunction(paths=frozenset(paths))
            flatten_and_unify_path_combination(self._context, e,
                                               memo=self._memo)

        expr = expr.__class__(
            name=expr.name,
            args=args,
            aggregates=expr.aggregates,
            kwargs=expr.kwargs,
            agg_sort=expr.agg_sort,
            agg_filter=expr.agg_filter,
            window=expr.window,
            partition=expr.partition)

        return expr

    def visit_Sequence(self, expr):
        elements = []
        for element in expr.elements:
            elements.append(self.visit(element))

        unify_paths(self._context, paths=elements, mode=irast.Disjunction)

        return expr.__class__(
            elements=elements, is_array=expr.is_array)

    def visit_Record(self, expr):
        elements = []
        for element in expr.elements:
            elements.append(self.visit(element))

        unify_paths(self._context, paths=elements, mode=irast.Disjunction)

        return expr.__class__(
            elements=elements, concept=expr.concept, rlink=expr.rlink)
