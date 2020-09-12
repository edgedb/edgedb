# mypy: no-ignore-errors, strict-optional, disallow-any-generics

"""Toy evaluator model for an edgeql subset.

The idea here is to be able to test queries against a simple semantics
driven evaluator model.

This version does not have any understanding of schemas.

It is a goal that this can be scaled up to be pointed at different
corners of the language. It is a non-goal that it can be scaled up to
be a full evaluator model.

Also a non-goal: performance.

"""

from typing import *

from pathlib import Path
import sys
EDB_DIR = Path(__file__).parent.parent.parent.resolve()
sys.path.insert(0, str(EDB_DIR))

from edb.common import debug
from edb import edgeql

from edb.common.ast import NodeVisitor
from edb.edgeql import ast as qlast
# from edb.edgeql import qltypes as ft

from dataclasses import dataclass

import uuid


T = TypeVar('T')


SCHEMA = '''
type Note {
    required single property name -> str;
    optional single property note -> str;
}
type Noob {
    required single property name -> str; // exclusive
    optional multi property multi_prop -> str; // exclusive
    multi link notes -> Note;
    optional single property tag -> str;
'''


def bsid(n: int) -> uuid.UUID:
    return uuid.UUID(f'ffffffff-ffff-ffff-ffff-{n:012x}')


# ############# Data model

NoobT = "Noob"
NoteT = "Note"

Data = Any
DB = Dict[uuid.UUID, Dict[str, Data]]


def mk_db(data: Iterable[Dict[str, Data]]) -> DB:
    return {x["id"]: x for x in data}


def mk_obj(x: uuid.UUID) -> Data:
    return {"id": x}


def bslink(n: int) -> Data:
    return mk_obj(bsid(n))


DB1 = mk_db([
    # Noob
    {"id": bsid(0x10), "__type__": NoobT,
     "name": "Phil Emarg", "notes": [bslink(0x20), bslink(0x21)]},
    {"id": bsid(0x11), "__type__": NoobT,
     "name": "Madeline Hatch", "notes": [bslink(0x21)]},
    # Note
    {"id": bsid(0x20), "__type__": NoteT, "name": "boxing"},
    {"id": bsid(0x21), "__type__": NoteT, "name": "unboxing", "note": "lolol"},
])


# #############
class IORef(NamedTuple):
    name: str


class IPtr(NamedTuple):
    ptr: str
    direction: Optional[str]


IPathElement = Union[IORef, IPtr]
IPath = Tuple[IPathElement, ...]

# ############### Evaluation???


@dataclass
class EvalContext:
    query_input_list: List[IPath]
    input_tuple: Tuple[Data, ...]
    db: DB  # or not?


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

    return out


@dataclass
class PrepContext:
    db: DB


def build_input_tuples(
        qil: List[IPath], ctx: PrepContext) -> List[Tuple[Data, ...]]:
    data: List[Tuple[Data, ...]] = [()]
    for i, in_path in enumerate(qil):
        new_data: List[Tuple[Data, ...]] = []
        new_qil = qil[:i]
        for row in data:
            eval_ctx = EvalContext(
                query_input_list=new_qil, input_tuple=row, db=ctx.db)
            out = eval_path(in_path, eval_ctx)
            # TODO: OPTIONAL/SET OF
            for val in out:
                new_data.append(row + (val,))
        data = new_data

    return data

# ############### Preparation


class PathFinder(NodeVisitor):
    def __init__(self) -> None:
        super().__init__()
        self.paths: List[qlast.Path] = []

    def visit_Path(self, path: qlast.Path):
        self.paths.append(path)
        self.generic_visit(path)


def find_paths(e: qlast.Statement) -> List[qlast.Path]:
    pf = PathFinder()
    pf.visit(e)
    return pf.paths


def longest_common_prefix(p1: IPath, p2: IPath) -> IPath:
    common = []
    for a, b in zip(p1[:-1], p2[:-1]):
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
        for y in refs[i:]:
            pfx = longest_common_prefix(x, y)
            if pfx:
                prefixes.add(pfx)
    return prefixes


def make_query_input_list(refs: List[IPath]) -> List[IPath]:
    refs = dedup(refs)
    # XXX: assuming everything is simple
    qil: Set[IPath] = {(x[0],) for x in refs}
    qil.update(find_common_prefixes(refs))
    return sorted(qil)


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


def main() -> None:
    debug.dump(DB1)

    query = '''
    SELECT Noob.name ++ Noob.notes.name ++ Noob.notes.note;
    '''

    q = parse(query)

    debug.dump(q)

    paths = [simplify_path(p) for p in find_paths(q)]
    debug.dump(paths)
    qil = make_query_input_list(paths)
    debug.dump(qil)

    ctx = PrepContext(db=DB1)
    in_tuples = build_input_tuples(qil, ctx)
    debug.dump(in_tuples)


if __name__ == '__main__':
    main()
