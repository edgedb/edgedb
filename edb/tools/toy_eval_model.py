# mypy: no-ignore-errors, strict-optional, disallow-any-generics

"""Toy evaluator model for an edgeql subset.

The idea here is to be able to test queries against a simple semantics
driven evaluator model.

This version does not have any understanding of schemas and has the
function signatures and behaviors of a bunch of basic functions
hardcoded in.

The data model is a super simple in-memory one, though it shouldn't be
too hard to populate it from a real DB or vice versa to do testing.

It is a goal that this can be scaled up to be pointed at different
corners of the language. It is a non-goal that it can be scaled up to
be a full evaluator model.

Also a non-goal: performance.

"""

from __future__ import annotations

from pathlib import Path
import sys
EDB_DIR = Path(__file__).parent.parent.parent.resolve()
sys.path.insert(0, str(EDB_DIR))

from typing import *


from edb.common import debug
from edb import edgeql

from edb.common.ast import NodeVisitor
from edb.edgeql import ast as qlast
from edb.edgeql import qltypes as ft

from dataclasses import dataclass
from collections import defaultdict

import uuid
import itertools
import operator
import functools


T = TypeVar('T')


# This is just documentation I guess.
SCHEMA = '''
type Note {
    required single property name -> str;
    optional single property note -> str;
}
type Person {
    required single property name -> str;
    optional multi property multi_prop -> str;
    multi link notes -> Note;
    optional single property tag -> str;
'''


def bsid(n: int) -> uuid.UUID:
    return uuid.UUID(f'ffffffff-ffff-ffff-ffff-{n:012x}')


# ############# Data model

# Make this UUIDs?
PersonT = "Person"
NoteT = "Note"

Data = Any
DB = Dict[uuid.UUID, Dict[str, Data]]


def mk_db(data: Iterable[Dict[str, Data]]) -> DB:
    return {x["id"]: x for x in data}


def mk_obj(x: uuid.UUID) -> Data:
    return {"id": x}


def is_obj(x: Data) -> bool:
    # ehhhhh
    return isinstance(x, dict) and x.keys() == {"id"}


def bslink(n: int) -> Data:
    return mk_obj(bsid(n))


DB1 = mk_db([
    # Person
    {"id": bsid(0x10), "__type__": PersonT,
     "name": "Phil Emarg", "notes": [bslink(0x20), bslink(0x21)]},
    {"id": bsid(0x11), "__type__": PersonT,
     "name": "Madeline Hatch", "notes": [bslink(0x21)]},
    {"id": bsid(0x12), "__type__": PersonT,
     "name": "Emmanuel Villip"},
    # Note
    {"id": bsid(0x20), "__type__": NoteT, "name": "boxing"},
    {"id": bsid(0x21), "__type__": NoteT, "name": "unboxing", "note": "lolol"},
])


# # Toy basis stuff

SET_OF, OPTIONAL, SINGLETON = (
    ft.TypeModifier.SetOfType, ft.TypeModifier.OptionalType,
    ft.TypeModifier.SingletonType)

# We just list things with weird behavior
BASIS = {
    'count': [SET_OF],
    'IN': [SINGLETON, SET_OF],
    'NOT IN': [SINGLETON, SET_OF],
    '??': [OPTIONAL, SET_OF],
    'EXISTS': [SET_OF],
    'IF': [SET_OF, SINGLETON, SET_OF],
    'UNION': [SET_OF, SET_OF],
    '?=': [OPTIONAL, OPTIONAL],
    '?!=': [OPTIONAL, OPTIONAL],
}


# #############
class IORef(NamedTuple):
    name: str


class IPtr(NamedTuple):
    ptr: str
    direction: Optional[str]


IPathElement = Union[IORef, IPtr]
IPath = Tuple[IPathElement, ...]

# ############### Evaluation???

# Basic functions


class LiftedFunc(Protocol):
    def __call__(self, *args: List[Data]) -> List[Data]:
        pass


def lift(f: Callable[..., Union[Data, List[Data]]]) -> LiftedFunc:
    def inner(*args: Data) -> List[Data]:
        out = []
        for args1 in itertools.product(*args):
            val = f(*args1)
            if isinstance(val, list):
                out.extend(val)
            else:
                out.append(val)
        return out
    return inner


def opt_eq(x: List[Data], y: List[Data]) -> List[Data]:
    if not x or not y:
        return [len(x) == len(y)]
    return lift(operator.eq)(x, y)


def opt_ne(x: List[Data], y: List[Data]) -> List[Data]:
    if not x or not y:
        return [len(x) != len(y)]
    return lift(operator.ne)(x, y)


def count(x: List[Data]) -> List[Data]:
    return [len(x)]


def contains(es: List[Data], s: List[Data]) -> List[Data]:
    return [e in s for e in es]


def not_contains(es: List[Data], s: List[Data]) -> List[Data]:
    return [e not in s for e in es]


def coalesce(x: List[Data], y: List[Data]) -> List[Data]:
    return x or y


def exists(x: List[Data]) -> List[Data]:
    return [bool(x)]


def union(x: List[Data], y: List[Data]) -> List[Data]:
    return x + y


_BASIS_IMPLS: Any = {
    '+': lift(operator.add),
    '++': lift(operator.add),
    '=': lift(operator.eq),
    '!=': lift(operator.ne),
    'str': lift(str),
    'int32': lift(int),
    'int64': lift(int),
    '?=': opt_eq,
    '?!=': opt_ne,
    'count': count,
    'IN': contains,
    'NOT IN': not_contains,
    '??': coalesce,
    'EXISTS': exists,
    'UNION': union,
}
BASIS_IMPLS: Dict[str, LiftedFunc] = _BASIS_IMPLS


@dataclass
class EvalContext:
    query_input_list: List[IPath]
    input_tuple: Tuple[Data, ...]
    db: DB

#


@functools.singledispatch
def _eval(
    node: qlast.Base,
    ctx: EvalContext,
) -> List[Data]:
    raise NotImplementedError(
        f'no EdgeQL eval handler for {node.__class__}')


@_eval.register(qlast.SelectQuery)
def eval_Select(node: qlast.SelectQuery, ctx: EvalContext) -> List[Data]:
    # TODO like all the complicated stuff
    return subquery(node.result, ctx)


def eval_func_or_op(op: str, args: List[qlast.Expr],
                    ctx: EvalContext) -> List[Data]:
    arg_specs = BASIS.get(op)

    results = []
    for i, arg in enumerate(args):
        if arg_specs and arg_specs[i] == SET_OF:
            # SET OF is a subquery so we skip it
            results.append(subquery(arg, ctx))
        else:
            results.append(eval(arg, ctx))

    f = BASIS_IMPLS[op]
    return f(*results)


@_eval.register(qlast.BinOp)
def eval_BinOp(node: qlast.BinOp, ctx: EvalContext) -> List[Data]:
    return eval_func_or_op(node.op, [node.left, node.right], ctx)


@_eval.register(qlast.FunctionCall)
def eval_Call(node: qlast.FunctionCall, ctx: EvalContext) -> List[Data]:
    assert isinstance(node.func, str)
    return eval_func_or_op(node.func, node.args, ctx)


@_eval.register(qlast.IfElse)
def visit_IfElse(self, query: qlast.IfElse, ctx: EvalContext) -> List[Data]:
    return self.visit_func_or_op(
        'IF', [query.if_expr, query.condition, query.else_expr])


@_eval.register(qlast.StringConstant)
def eval_StringConstant(
        node: qlast.StringConstant, ctx: EvalContext) -> List[Data]:
    return [node.value]


@_eval.register(qlast.IntegerConstant)
def eval_IntegerConstant(
        node: qlast.IntegerConstant, ctx: EvalContext) -> List[Data]:
    return [int(node.value)]


@_eval.register(qlast.Set)
def eval_Set(
        node: qlast.Set, ctx: EvalContext) -> List[Data]:
    out = []
    for elem in node.elements:
        out.extend(eval(elem, ctx))
    return out


@_eval.register(qlast.Tuple)
def eval_Tuple(
        node: qlast.Tuple, ctx: EvalContext) -> List[Data]:
    # XXX: only unnamed ones
    args = [eval(arg, ctx) for arg in node.elements]

    def get(*va):
        return va

    return lift(get)(*args)


@_eval.register(qlast.TypeCast)
def eval_TypeCast(node: qlast.TypeCast, ctx: EvalContext) -> List[Data]:
    typ = node.type.maintype.name  # type: ignore  # our types are hinky.
    f = BASIS_IMPLS[typ]
    return f(eval(node.expr, ctx))


@_eval.register(qlast.Path)
def eval_Path(node: qlast.Path, ctx: EvalContext) -> List[Data]:
    return eval_path(simplify_path(node), ctx)


def eval(node: qlast.Base, ctx: EvalContext) -> List[Data]:
    return _eval(node, ctx)

# Query setup


def eval_ptr(base: Data, ptr: IPtr, ctx: EvalContext) -> List[Data]:
    assert ptr.direction == '>'
    obj = ctx.db[base["id"]]
    out = obj.get(ptr.ptr, [])
    if not isinstance(out, list):
        out = [out]
    return out


# This should only get called during input tuple building
def eval_objref(name: str, ctx: EvalContext) -> List[Data]:
    return [
        mk_obj(obj["id"]) for obj in ctx.db.values()
        if obj["__type__"] == name
    ]


def eval_path(path: IPath, ctx: EvalContext) -> List[Data]:
    # Base case for stuff in the input list
    if path in ctx.query_input_list:
        i = ctx.query_input_list.index(path)
        obj = ctx.input_tuple[i]
        return [obj] if obj else []

    if len(path) == 1:
        assert(isinstance(path[0], IORef))
        return eval_objref(path[0].name, ctx)

    base = eval_path(path[:-1], ctx)
    out = []
    ptr = path[-1]
    assert isinstance(ptr, IPtr)
    for obj in base:
        out.extend(eval_ptr(obj, ptr, ctx))
    # We need to deduplicate links.
    if out and is_obj(out[0]):
        out = dedup(out)

    return out


def build_input_tuples(
        qil: List[IPath], always_optional: Dict[IPath, bool],
        ctx: EvalContext) -> List[Tuple[Data, ...]]:
    data: List[Tuple[Data, ...]] = [ctx.input_tuple]
    for i, in_path in enumerate(qil):
        new_data: List[Tuple[Data, ...]] = []
        new_qil = ctx.query_input_list + qil[:i]
        for row in data:
            eval_ctx = EvalContext(
                query_input_list=new_qil, input_tuple=row, db=ctx.db)
            out = eval_path(in_path, eval_ctx)
            for val in out:
                new_data.append(row + (val,))
            if not out and always_optional[in_path]:
                new_data.append(row + (None,))
        data = new_data

    return data

# ############### Preparation


class PathFinder(NodeVisitor):
    def __init__(self) -> None:
        super().__init__()
        self.in_optional = False
        self.paths: List[Tuple[qlast.Path, bool]] = []

    def visit_Path(self, path: qlast.Path) -> None:
        self.paths.append((path, self.in_optional))
        self.generic_visit(path)

    def visit_SelectQuery(self, query: qlast.SelectQuery) -> None:
        # TODO: there are lots of implicit subqueries (clauses)
        # and we need to care about them
        return

    def visit_func_or_op(self, op: str, args: List[qlast.Expr]) -> None:
        # Totally ignoring that polymorpic whatever is needed
        arg_specs = BASIS.get(op)
        old = self.in_optional
        for i, arg in enumerate(args):
            if arg_specs:
                # SET OF is a subquery so we skip it
                if arg_specs[i] == SET_OF:
                    continue
                elif arg_specs[i] == OPTIONAL:
                    self.in_optional = True
            self.visit(arg)
            self.in_optional = old

    def visit_BinOp(self, query: qlast.BinOp) -> None:
        self.visit_func_or_op(query.op, [query.left, query.right])

    def visit_FunctionCall(self, query: qlast.FunctionCall) -> None:
        assert not query.kwargs
        assert isinstance(query.func, str)
        self.visit_func_or_op(query.func, query.args)
        assert not query.window  # done last or we get dced.

    def visit_IfElse(self, query: qlast.IfElse) -> None:
        self.visit_func_or_op(
            'IF', [query.if_expr, query.condition, query.else_expr])


def find_paths(e: qlast.Expr) -> List[Tuple[qlast.Path, bool]]:
    pf = PathFinder()
    pf.visit(e)
    return pf.paths


def longest_common_prefix(p1: IPath, p2: IPath) -> IPath:
    common = []
    for a, b in zip(p1, p2):
        if a == b:
            common.append(a)
        else:
            break
    return tuple(common)


def dedup(old: List[T]) -> List[T]:
    new: List[T] = []
    for x in old:
        if x not in new:
            new.append(x)
    return new


def find_common_prefixes(refs: List[IPath]) -> Set[IPath]:
    prefixes = set()
    for i, x in enumerate(refs):
        added = False
        for y in refs[i:]:
            pfx = longest_common_prefix(x, y)
            if pfx:
                prefixes.add(pfx)
                added = True
        if not added:
            prefixes.add(x)
    return prefixes


def make_query_input_list(refs: List[IPath], old: List[IPath]) -> List[IPath]:
    # XXX: assuming everything is simple
    qil = find_common_prefixes(refs)
    return sorted(x for x in qil if x not in old)


def simplify_path(path: qlast.Path) -> IPath:
    spath: List[IPathElement] = []
    assert not path.partial
    for step in path.steps:
        if isinstance(step, qlast.ObjectRef):
            spath.append(IORef(step.name))
        elif isinstance(step, qlast.Ptr):
            spath.append(IPtr(step.ptr.name, step.direction))
        else:
            raise AssertionError(f'{type(step)} not supported yet')

    return tuple(spath)


def parse(querystr: str) -> qlast.Statement:
    source = edgeql.Source.from_string(querystr)
    statements = edgeql.parse_block(source)
    assert len(statements) == 1
    return statements[0]


def subquery(q: qlast.Expr, ctx: EvalContext) -> List[Data]:
    paths_opt = [(simplify_path(p), optional)
                 for p, optional in find_paths(q)]
    always_optional = defaultdict(lambda: True)
    for path, optional in paths_opt:
        if not optional:
            for i in range(1, len(path) + 1):
                always_optional[path[:i]] = False

    paths = [path for path, _ in paths_opt]

    qil = make_query_input_list(paths, ctx.query_input_list)

    in_tuples = build_input_tuples(qil, always_optional, ctx)

    # Actually eval it
    out = []
    new_qil = ctx.query_input_list + qil
    for row in in_tuples:
        subctx = EvalContext(
            query_input_list=new_qil, input_tuple=row, db=ctx.db)
        out.extend(eval(q, subctx))

    return out


def go(q: qlast.Expr, db: DB=DB1) -> None:
    ctx = EvalContext(
        query_input_list=[],
        input_tuple=(),
        db=db,
    )
    out = subquery(q, ctx)
    debug.dump(out)


def repl() -> None:
    # for now just use rlwrap since I don't want to fiddle with
    # history or anything
    while True:
        print("> ", end="", flush=True)
        s = ""
        while ';' not in s:
            s += sys.stdin.readline()
            if not s:
                return
        go(parse(s))


QUERY = '''
SELECT Person.name ++ "-" ++ Person.notes.name
'''
QUERY1 = '''
SELECT (Person.name, Person.name)
'''
QUERY2 = '''
SELECT (Note.note ?= "lolol", Note)
'''
QUERY3 = '''
SELECT (Person.name, (SELECT Note.name), (SELECT Note.name));
'''


def main() -> None:
    if sys.argv[1:] == ['-i']:
        return repl()

    q = parse(QUERY3)
    debug.dump(q)
    go(q)


if __name__ == '__main__':
    main()
