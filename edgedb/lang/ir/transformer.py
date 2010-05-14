##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import itertools

from semantix.caos import name as caos_name
from semantix.caos import error as caos_error
from semantix.caos import types as caos_types
from semantix.caos.tree import ast as caos_ast

from semantix.utils.algos import boolean
from semantix.utils import datastructures, ast, debug
from semantix.utils.functional import checktypes


@checktypes
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

    def add(self, links, direction:caos_types.LinkDirection, target:caos_types.ProtoNode):
        self.append((frozenset(links), direction))
        self.append(target)

    def __hash__(self):
        return hash(tuple(self))

    def __str__(self):
        if not self:
            return '';

        result = '%s' % self[0].name

        for i in range(1, len(self) - 1, 2):
            result += '[%s%s]%s' % (self[i][1], str(self[i][0]), self[i + 1].name)
        return result


@checktypes
class MultiPath(LinearPath):
    def add(self, links, direction:caos_types.LinkDirection, target):
        self.append((frozenset(links), direction))
        self.append(target)

    def __hash__(self):
        return hash(tuple(self))

    def __str__(self):
        if not self:
            return '';

        result = '%s' % ','.join(str(c.name) for c in self[0])

        for i in range(1, len(self) - 1, 2):
            result += '[%s%s]%s' % (self[i][1], str(self[i][0]),
                                    ','.join(str(c.name) for c in self[i + 1]))
        return result


class PathIndex(dict):
    """
    Graph path mapping path identifiers to AST nodes
    """

    def update(self, other):
        for k, v in other.items():
            if k in self:
                super().__getitem__(k).update(v)
            else:
                self[k] = v

    def __setitem__(self,  key, value):
        if not isinstance(key, (LinearPath, str)):
            raise TypeError('Invalid key type for PathIndex: %s' % key)

        if not isinstance(value, set):
            value = {value}

        super().__setitem__(key, value)

    """
    def __getitem__(self, key):
        result = set()
        for k, v in self.items():
            if k == key:
                result.update(v)
        if not result:
            raise KeyError
        return result
    """

    """
    def __contains__(self, key):
        for k in self.keys():
            if k == key:
                return True
        return False
    """


class TreeError(Exception):
    pass


class TreeTransformer:

    def extract_prefixes(self, expr, prefixes=None):
        prefixes = prefixes if prefixes is not None else PathIndex()

        if isinstance(expr, caos_ast.PathCombination):
            for path in expr.paths:
                self.extract_prefixes(path, prefixes)
        elif isinstance(expr, (caos_ast.EntitySet, caos_ast.AtomicRefSimple)):
            key = getattr(expr, 'anchor', None) or expr.id

            if key not in prefixes:
                prefixes[key] = {expr}
            else:
                prefixes[key].add(expr)

            if isinstance(expr, caos_ast.EntitySet) and expr.rlink:
                self.extract_prefixes(expr.rlink.source, prefixes)
            elif isinstance(expr, caos_ast.AtomicRefSimple):
                self.extract_prefixes(expr.ref, prefixes)

        elif isinstance(expr, caos_ast.BinOp):
            self.extract_prefixes(expr.left, prefixes)
            self.extract_prefixes(expr.right, prefixes)

        elif isinstance(expr, caos_ast.InlineFilter):
            self.extract_prefixes(expr.ref, prefixes)
            self.extract_prefixes(expr.expr, prefixes)

        elif isinstance(expr, caos_ast.AtomicRefExpr):
            self.extract_prefixes(expr.expr, prefixes)

        elif isinstance(expr, caos_ast.FunctionCall):
            for arg in expr.args:
                self.extract_prefixes(arg, prefixes)

        elif isinstance(expr, caos_ast.Sequence):
            for path in expr.elements:
                self.extract_prefixes(path, prefixes)

        elif isinstance(expr, caos_ast.Constant):
            pass
        else:
            assert False, 'Unexpected node type: %s' % type(expr)

        return prefixes

    def replace_atom_refs(self, expr, prefixes):
        arefs = ast.find_children(expr, lambda i: isinstance(i, caos_ast.AtomicRefSimple))

        for aref in arefs:
            prefix = getattr(aref.ref, 'anchor', None) or aref.ref.id
            newref = prefixes[prefix]
            if len(newref) > 1:
                newref = caos_ast.Disjunction(paths=frozenset(newref))
            else:
                newref = next(iter(newref))
            aref.ref = newref

        return expr

    def _dump(self, tree):
        if tree is not None:
            print(tree.dump(pretty=True, colorize=True, width=180, field_mask='^(_.*|refs|backrefs)$'))
        else:
            print('None')

    def extend_binop(self, binop, *exprs, op=ast.ops.AND, reversed=False):
        exprs = list(exprs)
        binop = binop or exprs.pop(0)

        for expr in exprs:
            if expr is not binop:
                if reversed:
                    binop = caos_ast.BinOp(right=binop, op=op, left=expr)
                else:
                    binop = caos_ast.BinOp(left=binop, op=op, right=expr)

        return binop

    def postprocess_expr(self, expr):
        paths = self.extract_paths(expr, reverse=True)

        if isinstance(paths, caos_ast.PathCombination):
            paths = paths.paths
        else:
            paths = {paths}

        for path in paths:
            self._postprocess_expr(path)

    def _postprocess_expr(self, expr):
        if isinstance(expr, caos_ast.EntitySet):
            if self.context.current.location == 'generator':
                if len(expr.disjunction.paths) == 1 and len(expr.conjunction.paths) == 0:
                    # Generator by default produces strong paths, that must limit every other
                    # path in the query.  However, to accommodate for possible disjunctions
                    # in generator expressions, links are put into disjunction.  If, in fact,
                    # there was not disjunctive expressions in generator, the link must
                    # be turned into conjunction.
                    #
                    expr.conjunction = caos_ast.Conjunction(paths=expr.disjunction.paths)
                    expr.disjunction = caos_ast.Disjunction()

            for path in expr.conjunction.paths:
                self._postprocess_expr(path)

            for path in expr.disjunction.paths:
                self._postprocess_expr(path)

        elif isinstance(expr, caos_ast.PathCombination):
            for path in expr.paths:
                self._postprocess_expr(path)

        elif isinstance(expr, caos_ast.EntityLink):
            self._postprocess_expr(expr.target)

        else:
            assert False, "Unexpexted expression: %s" % expr

    def merge_paths(self, expr):
        if isinstance(expr, caos_ast.AtomicRefExpr):
            if self.context.current.location == 'generator':
                expr.ref.filter = self.extend_binop(expr.ref.filter, expr.expr)
                self.merge_paths(expr.ref)
                expr = caos_ast.InlineFilter(expr=expr.ref.filter, ref=expr.ref)
            else:
                self.merge_paths(expr.expr)

        elif isinstance(expr, caos_ast.BinOp):
            left = self.merge_paths(expr.left)
            right = self.merge_paths(expr.right)

            if expr.op in (ast.ops.OR, ast.ops.IN, ast.ops.NOT_IN):
                combination = caos_ast.Disjunction
            else:
                combination = caos_ast.Conjunction

            paths = set()
            for operand in (left, right):
                if isinstance(operand, (caos_ast.InlineFilter, caos_ast.AtomicRefSimple)):
                    paths.add(operand.ref)
                else:
                    paths.add(operand)

            e = combination(paths=frozenset(paths))
            self.flatten_and_unify_path_combination(e, deep=False)

            if len(e.paths) > 1:
                expr = caos_ast.BinOp(left=left, op=expr.op, right=right)
            else:
                expr = next(iter(expr.paths))

        elif isinstance(expr, caos_ast.PathCombination):
            expr = self.flatten_and_unify_path_combination(expr, deep=True)

        elif isinstance(expr, caos_ast.AtomicRefSimple):
            expr.ref.atomrefs.add(expr)

        elif isinstance(expr, caos_ast.EntitySet):
            if expr.rlink:
                self.merge_paths(expr.rlink.source)

        elif isinstance(expr, (caos_ast.InlineFilter, caos_ast.Constant)):
            pass

        elif isinstance(expr, caos_ast.FunctionCall):
            args = []
            for arg in expr.args:
                args.append(self.merge_paths(arg))
            expr = expr.__class__(name=expr.name, args=args)

        elif isinstance(expr, caos_ast.Sequence):
            elements = []
            for element in expr.elements:
                elements.append(self.merge_paths(element))
            expr = expr.__class__(elements=elements)

        else:
            assert False

        return expr

    def flatten_path_combination(self, expr):
        paths = set()
        for path in expr.paths:
            if isinstance(path, expr.__class__):
                paths.update(path.paths)
            else:
                paths.add(path)

        expr.paths = frozenset(paths)
        return expr

    def flatten_and_unify_path_combination(self, expr, deep=False, merge_filters=False):
        ##
        # Flatten nested disjunctions and conjunctions since they are associative
        #
        assert isinstance(expr, caos_ast.PathCombination)

        self.flatten_path_combination(expr)

        if deep:
            newpaths = set()
            for path in expr.paths:
                path = self.merge_paths(path)
                newpaths.add(path)

            expr = expr.__class__(paths=frozenset(newpaths))

        self.unify_paths(expr.paths, mode=expr.__class__, merge_filters=merge_filters)

        expr.paths = frozenset(p for p in expr.paths)
        return expr

    nest = 0

    def unify_paths(self, paths, mode, reverse=True, merge_filters=False):
        mypaths = set(paths)

        result = None

        while mypaths and not result:
            result = self.extract_paths(mypaths.pop(), reverse)

        while mypaths:
            path = self.extract_paths(mypaths.pop(), reverse)

            if not path:
                continue

            if issubclass(mode, caos_ast.Disjunction):
                """LOG [caos.graph.merge] ADDING
                print(' ' * self.nest, 'ADDING', result, path, getattr(result, 'id', '??'), getattr(path, 'id', '??'), merge_filters)
                self.nest += 2
                """

                result = self.add_paths(result, path, merge_filters=merge_filters)

                """LOG [caos.graph.merge] ADDITION RESULT
                self.nest -= 2
                if not self.nest:
                    self._dump(result)
                """
            else:
                """LOG [caos.graph.merge] INTERSECTING
                print(' ' * self.nest, getattr(result, 'id', result), getattr(path, 'id', path))
                self.nest += 2
                """

                result = self.intersect_paths(result, path, merge_filters=merge_filters)

                """LOG [caos.graph.merge] INTERSECTION RESULT
                self._dump(result)
                self.nest -= 2
                """

        return result

    def miniterms_from_conjunctions(self, paths):
        variables = datastructures.OrderedSet()

        terms = []

        for path in paths:
            term = 0

            if isinstance(path, caos_ast.Conjunction):
                for subpath in path.paths:
                    if subpath not in variables:
                        variables.add(subpath)
                    term += 1 << variables.index(subpath)

            elif isinstance(path, caos_ast.EntityLink):
                if path not in variables:
                    variables.add(path)
                term += 1 << variables.index(path)

            terms.append(term)

        return variables, boolean.ints_to_terms(*terms)

    def conjunctions_from_miniterms(self, terms, variables):
        paths = set()

        for term in terms:
            conjpaths = [variables[i] for i, bit in enumerate(term) if bit]
            if len(conjpaths) > 1:
                paths.add(caos_ast.Conjunction(paths=frozenset(conjpaths)))
            else:
                paths.add(conjpaths[0])
        return paths

    def minimize_disjunction(self, paths):
        variables, miniterms = self.miniterms_from_conjunctions(paths)
        minimized = boolean.minimize(miniterms)
        paths = self.conjunctions_from_miniterms(minimized, variables)
        result = caos_ast.Disjunction(paths=frozenset(paths))
        return result

    def add_sets(self, left, right, merge_filters=False):
        if left is right:
            return left

        match = self.match_prefixes(left, right, ignore_filters=merge_filters)
        if match:
            if isinstance(left, caos_ast.EntityLink):
                left = left.target
                right = right.target

            self.fixup_refs([right], left)

            if merge_filters and right.filter:
                left.filter = self.extend_binop(left.filter, right.filter, op=ast.ops.AND)

            if merge_filters:
                paths_left = set()
                for dpath in right.disjunction.paths:
                    if isinstance(dpath, (caos_ast.EntitySet, caos_ast.EntityLink)):
                        merged = self.intersect_paths(left.conjunction, dpath)
                        if merged is not left.conjunction:
                            paths_left.add(dpath)
                    else:
                        paths_left.add(dpath)
                right.disjunction = caos_ast.Disjunction(paths=frozenset(paths_left))

            left.disjunction = self.add_paths(left.disjunction, right.disjunction, merge_filters)
            left.atomrefs.update(right.atomrefs)
            left.metarefs.update(right.metarefs)
            left.users.update(right.users)
            left.joins.update(right.joins)
            left.joins.discard(left)

            if merge_filters:
                left.conjunction = self.intersect_paths(left.conjunction,
                                                        right.conjunction, merge_filters)

                # If greedy disjunction merging is requested, we must also try to
                # merge disjunctions.
                paths = frozenset(left.conjunction.paths) | frozenset(left.disjunction.paths)
                self.unify_paths(paths, caos_ast.Conjunction, reverse=False, merge_filters=True)
                left.disjunction.paths = left.disjunction.paths - left.conjunction.paths
            else:
                conjunction = self.add_paths(left.conjunction, right.conjunction, merge_filters)
                if conjunction.paths:
                    left.disjunction.update(conjunction)
                left.conjunction.paths = frozenset()

            result = left
        else:
            result = caos_ast.Disjunction(paths=frozenset((left, right)))

        return result

    def add_to_disjunction(self, disjunction, path, merge_filters):
        # Other operand is a disjunction -- look for path we can merge with,
        # if not found, append to disjunction.
        for dpath in disjunction.paths:
            if isinstance(dpath, (caos_ast.EntityLink, caos_ast.EntitySet)):
                merge = self.add_sets(dpath, path, merge_filters)
                if merge is dpath:
                    break
        else:
            disjunction.update(path)

        return disjunction

    def add_to_conjunction(self, conjunction, path, merge_filters):
        result = None
        if merge_filters:
            for cpath in conjunction.paths:
                if isinstance(cpath, (caos_ast.EntityLink, caos_ast.EntitySet)):
                    merge = self.add_sets(cpath, path, merge_filters)
                    if merge is cpath:
                        result = conjunction
                        break

        if not result:
            result = caos_ast.Disjunction(paths=frozenset({conjunction, path}))

        return result

    def add_disjunctions(self, left, right, merge_filters=False):
        result = caos_ast.Disjunction()
        result.update(left)
        result.update(right)

        if len(result.paths) > 1:
            self.unify_paths(result.paths, mode=result.__class__, reverse=False,
                             merge_filters=merge_filters)
            result.paths = frozenset(p for p in result.paths)

        return result

    def add_conjunction_to_disjunction(self, disjunction, conjunction):
        if disjunction.paths and conjunction.paths:
            return caos_ast.Disjunction(paths=frozenset({disjunction, conjunction}))
        elif disjunction.paths:
            return disjunction
        elif conjunction.paths:
            return caos_ast.Disjunction(paths=frozenset({conjunction}))
        else:
            return caos_ast.Disjunction()

    def add_conjunctions(self, left, right):
        paths = frozenset(p for p in (left, right) if p.paths)
        return caos_ast.Disjunction(paths=paths)

    def add_paths(self, left, right, merge_filters=False):
        if isinstance(left, (caos_ast.EntityLink, caos_ast.EntitySet)):
            if isinstance(right, (caos_ast.EntityLink, caos_ast.EntitySet)):
                # Both operands are sets -- simply merge them
                result = self.add_sets(left, right, merge_filters)

            elif isinstance(right, caos_ast.Disjunction):
                result = self.add_to_disjunction(right, left, merge_filters)

            elif isinstance(right, caos_ast.Conjunction):
                result = self.add_to_conjunction(right, left, merge_filters)

        elif isinstance(left, caos_ast.Disjunction):
            if isinstance(right, (caos_ast.EntityLink, caos_ast.EntitySet)):
                result = self.add_to_disjunction(left, right, merge_filters)

            elif isinstance(right, caos_ast.Disjunction):
                result = self.add_disjunctions(left, right, merge_filters)

            elif isinstance(right, caos_ast.Conjunction):
                result = self.add_conjunction_to_disjunction(left, right)

        elif isinstance(left, caos_ast.Conjunction):
            if isinstance(right, (caos_ast.EntityLink, caos_ast.EntitySet)):
                result = self.add_to_conjunction(left, right, merge_filters)

            elif isinstance(right, caos_ast.Disjunction):
                result = self.add_conjunction_to_disjunction(right, left)

            elif isinstance(right, caos_ast.Conjunction):
                result = self.add_conjunctions(left, right)

        return result


    def intersect_sets(self, left, right, merge_filters=False):
        if left is right:
            return left

        match = self.match_prefixes(left, right, ignore_filters=True)
        if match:
            if isinstance(left, caos_ast.EntityLink):
                left_set = left.target
                right_set = right.target
            else:
                left_set = left
                right_set = right

            self.fixup_refs([right_set], left_set)

            if right_set.filter:
                left_set.filter = self.extend_binop(left_set.filter, right_set.filter, op=ast.ops.AND)

            left_set.conjunction = self.intersect_paths(left_set.conjunction, right_set.conjunction, merge_filters)
            left_set.atomrefs.update(right_set.atomrefs)
            left_set.metarefs.update(right_set.metarefs)
            left_set.users.update(right_set.users)
            left_set.joins.update(right_set.joins)
            left_set.joins.discard(left_set)

            disjunction = self.intersect_paths(left_set.disjunction, right_set.disjunction, merge_filters)

            left_set.disjunction = caos_ast.Disjunction()

            if isinstance(disjunction, caos_ast.Disjunction):
                left_set.disjunction = disjunction

                if len(left_set.disjunction.paths) == 1:
                    first_disj = next(iter(left_set.disjunction.paths))
                    if isinstance(first_disj, caos_ast.Conjunction):
                        left_set.conjunction = first_disj
                        left_set.disjunction = caos_ast.Disjunction()

            elif disjunction.paths:
                left_set.conjunction = self.intersect_paths(left_set.conjunction, disjunction, merge_filters)

                self.flatten_path_combination(left_set.conjunction)

                if len(left_set.conjunction.paths) == 1:
                    first_conj = next(iter(left_set.conjunction.paths))
                    if isinstance(first_conj, caos_ast.Disjunction):
                        left_set.disjunction = first_conj
                        left_set.conjunction = caos_ast.Conjunction()

            result = left
        else:
            result = caos_ast.Conjunction(paths=frozenset({left, right}))

        return result

    def intersect_with_disjunction(self, disjunction, path):
        result = caos_ast.Conjunction(paths=frozenset((disjunction, path)))
        return result

    def intersect_with_conjunction(self, conjunction, path):
        # Other operand is a disjunction -- look for path we can merge with,
        # if not found, append to conjunction.
        for cpath in conjunction.paths:
            if isinstance(cpath, (caos_ast.EntityLink, caos_ast.EntitySet)):
                merge = self.intersect_sets(cpath, path)
                if merge is cpath:
                    break
        else:
            conjunction = caos_ast.Conjunction(paths=frozenset(conjunction.paths | {path}))

        return conjunction

    def intersect_conjunctions(self, left, right, merge_filters=False):
        result = caos_ast.Conjunction(paths=left.paths)
        result.update(right)

        if len(result.paths) > 1:
            self.flatten_path_combination(result)
            self.unify_paths(result.paths, mode=result.__class__, reverse=False,
                             merge_filters=merge_filters)
            result.paths = frozenset(p for p in result.paths)

        return result

    def intersect_disjunctions(self, left, right):
        """Produce a conjunction of two disjunctions"""

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
                    paths.add(self.intersect_paths(l, r))

            result = self.minimize_disjunction(paths)
            return result

        else:
            # Degenerate case
            if not left.paths:
                paths = right.paths
            elif not right.paths:
                paths = left.paths

            if len(paths) <= 1:
                return caos_ast.Conjunction(paths=frozenset(paths))
            else:
                return caos_ast.Disjunction(paths=frozenset(paths))

    def intersect_disjunction_with_conjunction(self, disjunction, conjunction):
        if disjunction.paths and conjunction.paths:
            return caos_ast.Disjunction(paths=frozenset({disjunction, conjunction}))
        elif conjunction.paths:
            return conjunction
        elif disjunction.paths:
            return caos_ast.Conjunction(paths=frozenset({disjunction}))
        else:
            return caos_ast.Conjunction()

    def intersect_paths(self, left, right, merge_filters=False):
        if isinstance(left, (caos_ast.EntityLink, caos_ast.EntitySet)):
            if isinstance(right, (caos_ast.EntityLink, caos_ast.EntitySet)):
                # Both operands are sets -- simply merge them
                result = self.intersect_sets(left, right)

            elif isinstance(right, caos_ast.Disjunction):
                result = self.intersect_with_disjunction(right, left)

            elif isinstance(right, caos_ast.Conjunction):
                result = self.intersect_with_conjunction(right, left)

        elif isinstance(left, caos_ast.Disjunction):
            if isinstance(right, (caos_ast.EntityLink, caos_ast.EntitySet)):
                result = self.intersect_with_disjunction(left, right)

            elif isinstance(right, caos_ast.Disjunction):
                result = self.intersect_disjunctions(left, right)

            elif isinstance(right, caos_ast.Conjunction):
                result = self.intersect_disjunction_with_conjunction(left, right)

        elif isinstance(left, caos_ast.Conjunction):
            if isinstance(right, (caos_ast.EntityLink, caos_ast.EntitySet)):
                result = self.intersect_with_conjunction(left, right)

            elif isinstance(right, caos_ast.Disjunction):
                result = self.intersect_disjunction_with_conjunction(right, left)

            elif isinstance(right, caos_ast.Conjunction):
                result = self.intersect_conjunctions(left, right, merge_filters)

        return result

    def match_prefixes(self, our, other, ignore_filters):
        result = None

        if isinstance(our, caos_ast.EntityLink):
            link = our
            our_node = our.target
        else:
            link = None
            our_node = our


        if isinstance(other, caos_ast.EntityLink):
            other_link = other
            other_node = other.target
        else:
            other_link = None
            other_node = other

        """LOG [caos.graph.merge] MATCH PREFIXES
        print(' ' * self.nest, our, other, ignore_filters)
        print(' ' * self.nest, '   PATHS: ', our_node.id)
        print(' ' * self.nest, '      *** ', other_node.id)
        """

        ok = (our_node.id == other_node.id
              and our_node.anchor == other_node.anchor
              and (ignore_filters or (not our_node.filter and not other_node.filter
                                      and not our_node.conjunction.paths
                                      and not other_node.conjunction.paths))
              and (not link or (link.filter == other_link.filter)))

        if ok:
            if other_link:
                result = other_link
            else:
                result = other_node

        """LOG [caos.graph.merge] MATCH PREFIXES RESULT
        print(' ' * self.nest, '    ----> ', result)
        """

        return result

    def fixup_refs(self, refs, newref):
        caos_ast.Base.fixup_refs(refs, newref)

    def extract_paths(self, path, reverse=False):
        if isinstance(path, (caos_ast.EntitySet, caos_ast.InlineFilter, caos_ast.AtomicRef)):
            if isinstance(path, (caos_ast.InlineFilter, caos_ast.AtomicRef)):
                result = path.ref
            else:
                result = path

            if reverse:
                while result.rlink:
                    result = result.rlink.source
            return result

        elif isinstance(path, caos_ast.EntityLink):
            return path

        elif isinstance(path, caos_ast.PathCombination):
            result = set()
            for p in path.paths:
                normalized = self.extract_paths(p, reverse)
                if normalized:
                    result.add(normalized)
            if len(result) == 1:
                return next(iter(result))
            else:
                return self.flatten_path_combination(path.__class__(paths=frozenset(result)))

        elif isinstance(path, caos_ast.BinOp):
            combination = caos_ast.Disjunction if (path.op == ast.ops.OR) else caos_ast.Conjunction

            paths = set()
            for p in (path.left, path.right):
                normalized = self.extract_paths(p, reverse)
                if normalized:
                    paths.add(normalized)

            if len(paths) == 1:
                return next(iter(paths))
            else:
                return self.flatten_path_combination(combination(paths=frozenset(paths)))

        elif isinstance(path, caos_ast.FunctionCall):
            paths = set()
            for p in path.args:
                p = self.extract_paths(p, reverse)
                if p:
                    paths.add(p)

            if len(paths) == 1:
                return next(iter(paths))
            else:
                return caos_ast.Conjunction(paths=frozenset(paths))

        elif isinstance(path, caos_ast.Sequence):
            paths = set()
            for p in path.elements:
                p = self.extract_paths(p, reverse)
                if p:
                    paths.add(p)

            if len(paths) == 1:
                return next(iter(paths))
            else:
                return caos_ast.Disjunction(paths=frozenset(paths))

        elif isinstance(path, caos_ast.Constant):
            return None

        else:
            assert False, "Unexpected expression type %s" % path

    def copy_path(self, path):
        result = caos_ast.EntitySet(id=path.id, anchor=path.anchor, concept=path.concept,
                                    users=path.users, joins=path.joins)
        current = result

        while path.rlink:
            parent_path = path.rlink.source
            parent = caos_ast.EntitySet(id=parent_path.id, anchor=parent_path.anchor,
                                        concept=parent_path.concept, users=parent_path.users,
                                        joins=parent_path.joins)
            link = caos_ast.EntityLink(filter=path.rlink.filter, source=parent, target=current,
                                       link_proto=path.rlink.link_proto)
            parent.disjunction = caos_ast.Disjunction(paths=frozenset((link,)))
            current.rlink = link
            current = parent
            path = parent_path

        return result

    def process_function_call(self, node):
        if node.name in (('search', 'rank'), ('search', 'headline')):
            refs = set()
            for arg in node.args:
                if isinstance(arg, caos_ast.EntitySet):
                    refs.add(arg)
                else:
                    refs.update(ast.find_children(arg, lambda n: isinstance(n, caos_ast.EntitySet),
                                                  force_traversal=True))

            assert len(refs) == 1

            ref = next(iter(refs))

            cols = []
            for link_name, link in ref.concept.get_searchable_links():
                id = LinearPath(ref.id)
                id.add(frozenset((link.first,)), caos_types.OutboundDirection, link.first.target)
                cols.append(caos_ast.AtomicRefSimple(ref=ref, name=link_name,
                                                     caoslink=link.first,
                                                     id=id))

            if not cols:
                raise caos_error.CaosError('%s call on concept %s without any search configuration'\
                                           % (node.name, ref.concept.name),
                                           hint='Configure search for "%s"' % ref.concept.name)

            ref.atomrefs.update(cols)

            node = caos_ast.FunctionCall(name=node.name,
                                         args=[caos_ast.Sequence(elements=cols), node.args[1]])

        return node

    def process_binop(self, left, right, op):
        try:
            result = self._process_binop(left, right, op, reversed=False)
        except TreeError:
            result = self._process_binop(right, left, op, reversed=True)

        return result

    def _process_binop(self, left, right, op, reversed=False):
        result = None

        def newbinop(left, right, operation=None):
            operation = operation or op
            if reversed:
                return caos_ast.BinOp(left=right, op=operation, right=left)
            else:
                return caos_ast.BinOp(left=left, op=operation, right=right)

        if isinstance(left, (caos_ast.AtomicRef, caos_ast.Disjunction)):
            # If both left and right operands are references to atoms of the same node,
            # or one of the operands is a reference to an atom and other is a constant,
            # then fold the expression into an in-line filter of that node.
            #
            if isinstance(left, caos_ast.AtomicRef):
                left_exprs = caos_ast.Disjunction(paths=frozenset({left}))
            else:
                left_exprs = left

            def check_atomic_disjunction(expr):
                """Check that all paths in disjunction are atom references.

                   Return a dict mapping path prefixes to a corresponding node.
                """
                pathdict = {}
                for ref in expr.paths:
                    # Check that refs in the operand are all atomic: non-atoms do not coerse
                    # to literals.
                    #
                    if not isinstance(ref, caos_ast.AtomicRef):
                        return None

                    ref_id = ref.ref.id

                    assert not pathdict.get(ref_id)
                    pathdict[ref_id] = ref
                return pathdict

            pathdict = check_atomic_disjunction(left_exprs)

            if not pathdict:
                if isinstance(right, (caos_ast.Disjunction, caos_ast.EntitySet)):
                    if isinstance(right, caos_ast.EntitySet):
                        right_exprs = caos_ast.Disjunction(paths=frozenset({right}))
                    else:
                        right_exprs = right

                    id_col = caos_name.Name('semantix.caos.builtins.id')
                    if op in (ast.ops.EQ, ast.ops.NE):
                        lrefs = [caos_ast.AtomicRefSimple(ref=p, name=id_col)
                                    for p in left_exprs.paths]
                        rrefs = [caos_ast.AtomicRefSimple(ref=p, name=id_col)
                                    for p in right_exprs.paths]

                        l = caos_ast.Disjunction(paths=frozenset(lrefs))
                        r = caos_ast.Disjunction(paths=frozenset(rrefs))
                        result = newbinop(l, r)

                        for lset, rset in itertools.product(left_exprs.paths, right_exprs.paths):
                            lset.joins.add(rset)
                            rset.backrefs.add(lset)
                            rset.joins.add(lset)
                            lset.backrefs.add(rset)

                elif op in (ast.ops.IS, ast.ops.IS_NOT):
                    paths = set()

                    for path in left_exprs.paths:
                        if (op == ast.ops.IS) == (path.concept == right):
                            paths.add(path)

                    if len(paths) == 1:
                        result = next(iter(paths))
                    else:
                        result = caos_ast.Disjunction(paths=frozenset(paths))

                elif op in (ast.ops.IN, ast.ops.NOT_IN) and reversed:
                    if isinstance(right, caos_ast.Constant):

                        # <Constant> IN <EntitySet> is interpreted as a membership
                        # check of entity with ID represented by Constant in the EntitySet,
                        # which is equivalent to <EntitySet>.id = <Constant>
                        #
                        id_col = caos_name.Name('semantix.caos.builtins.id')

                        membership_op = ast.ops.EQ if op == ast.ops.IN else ast.ops.NE
                        paths = set()
                        for p in left_exprs.paths:
                            ref = caos_ast.AtomicRefSimple(ref=p, name=id_col)
                            expr = caos_ast.BinOp(left=ref, right=right, op=membership_op)
                            paths.add(caos_ast.AtomicRefExpr(expr=expr))

                        result = caos_ast.Disjunction(paths=frozenset(paths))

                elif op == caos_ast.SEARCH:
                    paths = set()
                    for p in left_exprs.paths:
                        expr = caos_ast.BinOp(left=p, right=right, op=op)
                        paths.add(caos_ast.AtomicRefExpr(expr=expr))

                    result = caos_ast.Disjunction(paths=frozenset(paths))

                if not result:
                    result = newbinop(left, right)
            else:
                if isinstance(right, caos_ast.Constant):
                    paths = set()

                    for ref in left_exprs.paths:
                        if isinstance(ref, caos_ast.AtomicRefExpr) \
                           and isinstance(op, ast.ops.BooleanOperator):
                            # We must not inline boolean expressions beyond the original bin-op
                            result = newbinop(left, right)
                            break
                        paths.add(caos_ast.AtomicRefExpr(expr=newbinop(ref, right)))
                    else:
                        if len(paths) == 1:
                            result = next(iter(paths))
                        else:
                            result = caos_ast.Disjunction(paths=frozenset(paths))


                elif isinstance(right, (caos_ast.AtomicRef, caos_ast.Disjunction)):

                    if isinstance(right, caos_ast.AtomicRef):
                        right_exprs = caos_ast.Disjunction(paths=frozenset((right,)))
                    else:
                        right_exprs = right

                    rightdict = check_atomic_disjunction(right_exprs)

                    if rightdict:
                        paths = set()

                        # If both operands are atom references, then we check if the referenced
                        # atom parent concepts intersect, and if they do we fold the expression
                        # into the atom ref for those common concepts only.  If there are no common
                        # concepts, a regular binary operation is returned.
                        #
                        for ref in left_exprs.paths:
                            left_id = ref.ref.id

                            right_expr = rightdict.get(left_id)

                            if right_expr:
                                right_expr.replace_refs([right_expr.ref], ref.ref, deep=True)
                                filterop = newbinop(ref, right_expr)
                                paths.add(caos_ast.AtomicRefExpr(expr=filterop))

                        if paths:
                            if len(paths) == 1:
                                result = next(iter(paths))
                            else:
                                result = caos_ast.Disjunction(paths=frozenset(paths))
                        else:
                            result = newbinop(left, right)
                    else:
                        result = newbinop(left, right)

                elif isinstance(right, caos_ast.BinOp) and op == right.op:
                    # Got a bin-op, that was not folded into an atom ref.  Re-check it since
                    # we may use operator associativity to fold one of the operands
                    #
                    operands = [right.left, right.right]
                    while operands:
                        operand = operands.pop()
                        if isinstance(operand, caos_ast.AtomicRef):
                            operand_id = operand.ref.id
                            ref = pathdict.get(operand_id)
                            if ref:
                                ref.expr = self.extend_binop(ref.expr, operand, op=op,
                                                                                reverse=reversed)
                                break

                    if len(operands) == 2:
                        result = newbinop(left, right)
                    else:
                        result = newbinop(left, operands[0])

        elif isinstance(left, caos_ast.Constant):
            if isinstance(right, caos_ast.Constant):
                result = caos_ast.Constant(expr=newbinop(left, right))

        elif isinstance(left, caos_ast.EntitySet):
            if op in (ast.ops.EQ, ast.ops.NE):
                # Comparison of two entity sets or a constant and an entity set is
                # considered to be a comparison of entity ids.
                #
                if isinstance(right, caos_ast.EntitySet):
                    left.joins.add(right)
                    right.backrefs.add(left)
                    right.joins.add(left)
                    left.backrefs.add(right)

                    l = caos_ast.AtomicRefSimple(ref=left, name='semantix.caos.builtin.id')
                    r = caos_ast.AtomicRefSimple(ref=right, name='semantix.caos.builtin.id')
                    result = newbinop(l, r)

                elif isinstance(right, caos_ast.Constant):
                    l = caos_ast.AtomicRefSimple(ref=left, name='semantix.caos.builtin.id')
                    result = newbinop(l, right)

            elif op == caos_ast.SEARCH:
                # A SEARCH operation on an entity set is always an inline filter ATM
                cols = list(left.concept.get_searchable_links())
                if not cols:
                    err = '%s operator called on concept %s without any search configuration' \
                                               % (caos_ast.SEARCH, left.concept.name)
                    hint = 'Configure search for "%s"' % left.concept.name
                    raise caos_error.CaosError(err, hint=hint)

                result = caos_ast.AtomicRefExpr(expr=newbinop(left, right))

            elif op in (ast.ops.IS, ast.ops.IS_NOT):
                assert False
            else:
                result = newbinop(left, right)

        elif isinstance(left, caos_ast.BinOp):
            result = newbinop(left, right)

        if not result:
            raise TreeError('Unexpected binop operands: %s, %s' % (left, right))

        return result

    def eval_const_bool_expr(self, left, right, op, reversed):
        if op == 'and':
            if not left.value:
                return caos_ast.Constant(value=False)
            else:
                return right
        elif op == 'or':
            if left.value:
                return caos_ast.Constant(value=True)
            else:
                return right

    def eval_const_expr(self, left, right, op, reversed):
        if isinstance(op, ast.ops.BooleanOperator):
            return self.eval_const_bool_expr(left, right, op, reversed)
        elif op == '=':
            op = '=='

        if reversed:
            params = (right.value, op, left.value)
        else:
            params = (left.value, op, right.value)

        return caos_ast.Constant(value=eval('%r %s %r' % params))
