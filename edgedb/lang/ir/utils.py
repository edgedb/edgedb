##
# Copyright (c) 2015-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

from edgedb.lang.common import ast

from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import sources as s_src

from . import ast as irast
from .typing import infer_arg_types, infer_type  # NOQA


class PathIndex(collections.OrderedDict):
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


def get_source_references(ir):
    result = set()

    flt = lambda n: isinstance(n, irast.Set) and n.expr is None
    ir_sets = ast.find_children(ir, flt)
    for ir_set in ir_sets:
        result.add(ir_set.scls)

    return result


def get_terminal_references(ir):
    result = set()
    parents = set()

    flt = lambda n: isinstance(n, irast.Set) and n.expr is None
    ir_sets = ast.find_children(ir, flt)
    for ir_set in ir_sets:
        result.add(ir_set)
        if ir_set.rptr:
            parents.add(ir_set.rptr.source)

    return result - parents


def get_variables(ir):
    result = set()
    flt = lambda n: isinstance(n, irast.Parameter)
    result.update(ast.find_children(ir, flt))
    return result


def is_const(ir):
    flt = lambda n: isinstance(n, irast.Set) and n.expr is None
    ir_sets = ast.find_children(ir, flt)
    variables = get_variables(ir)
    return not ir_sets and not variables


def is_aggregated_expr(ir):
    def flt(n):
        if isinstance(n, irast.FunctionCall):
            return n.func.aggregate
        elif isinstance(n, irast.Stmt):
            # Make sure we don't dip into subqueries
            raise ast.SkipNode()

    return bool(set(ast.find_children(ir, flt)))


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
            genptr = self[-2][0]
            direction = self[-2][1]
            if direction == s_pointers.PointerDirection.Outbound:
                src = self[-3]
            else:
                src = self[-1]

            if isinstance(src, s_src.Source):
                return src.pointers.get(genptr.name)
            else:
                return None
        else:
            return None

    def rptr_dir(self):
        if len(self) > 1:
            return self[-2][1]
        else:
            return None

    def iter_prefixes(self):
        yield self.__class__(self[:1])

        for i in range(1, len(self) - 1, 2):
            if self[i + 1]:
                yield self.__class__(self[:i + 2])
            else:
                break

    def startswith(self, path_id):
        return self[:len(path_id)] == path_id

    def __hash__(self):
        return hash(tuple(self))

    def __str__(self):
        if not self:
            return ''

        result = f'({self[0].name})'

        for i in range(1, len(self) - 1, 2):
            ptr = self[i][0]
            ptrdir = self[i][1]
            tgt = self[i + 1]

            if tgt:
                lexpr = f'({ptr.name} [IS {tgt.name}])'
            else:
                lexpr = f'({ptr.name})'

            if isinstance(ptr, s_lprops.LinkProperty):
                step = '@'
            else:
                step = f'.{ptrdir}'

            result += f'{step}{lexpr}'

        return result

    __repr__ = __str__


def extend_path(self, schema, source_set, ptr):
    scls = source_set.scls

    if isinstance(ptr, str):
        ptrcls = scls.resolve_pointer(schema, ptr)
    else:
        ptrcls = ptr

    path_id = LinearPath(source_set.path_id)
    path_id.add(ptrcls, s_pointers.PointerDirection.Outbound, ptrcls.target)

    target_set = irast.Set()
    target_set.scls = ptrcls.target
    target_set.path_id = path_id

    ptr = irast.Pointer(
        source=source_set,
        target=target_set,
        ptrcls=ptrcls,
        direction=s_pointers.PointerDirection.Outbound
    )

    target_set.rptr = ptr

    return target_set
