from typing import Optional

from ..data import data_ops as e
from ..data import casts as casts
from ..data import path_factor as path_factor
from ..basis import server_funcs as server_funcs


def check_castable(
    ctx: e.TcCtx, from_tp: e.Tp, to_tp: e.Tp
) -> Optional[e.TpCast]:
    if to_tp == e.ScalarTp(e.QualifiedName(["std", "json"])):
        return casts.get_json_cast(from_tp, ctx.schema)
    if (from_tp, to_tp) in ctx.schema.casts:
        return ctx.schema.casts[(from_tp, to_tp)]
    else:
        match from_tp:
            case e.ScalarTp(name=name):
                for supertype in ctx.schema.subtyping_relations[name]:
                    candidate = check_castable(
                        ctx, e.ScalarTp(supertype), to_tp
                    )
                    if candidate is not None:
                        return candidate
                else:
                    return None
            case _:
                return None
