from __future__ import annotations
from typing import Sequence
from ..data import data_ops as e


def find_all_subtypes_of_tp_in_schema(
    schema: e.DBSchema, tp: e.QualifiedName
) -> Sequence[e.QualifiedName]:
    checked_tps = []
    frontier = [tp]

    while len(frontier) > 0:
        next_tp = frontier.pop()
        if next_tp in checked_tps:
            continue
        checked_tps.append(next_tp)
        frontier.extend(
            [
                subtype
                for subtype in schema.subtyping_relations
                if next_tp in schema.subtyping_relations[subtype]
            ]
        )

    return checked_tps


def find_all_supertypes_of_tp_in_schema(
    schema: e.DBSchema, tp: e.QualifiedName
) -> Sequence[e.QualifiedName]:
    checked_tps = []
    frontier = [tp]

    while len(frontier) > 0:
        next_tp = frontier.pop()
        if next_tp in checked_tps:
            continue
        checked_tps.append(next_tp)
        frontier.extend(schema.subtyping_relations[next_tp])

    return checked_tps
