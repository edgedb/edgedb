
from functools import reduce
import operator
from typing import Tuple, Dict, Sequence, Optional, List

from ..data import data_ops as e
from ..data import expr_ops as eops
from ..data import type_ops as tops
from edb.common import debug
from ..data import path_factor as path_factor
from .dml_checking import *
from ..data import expr_to_str as pp
from .function_checking import *


def check_castable(ctx: e.TcCtx, 
                from_tp : e.Tp, 
                to_tp: e.Tp) -> bool:
    if (from_tp, to_tp) in ctx.schema.casts:
        return True
    else:
        match from_tp:
            case e.ScalarTp(name=name):
                for supertype in ctx.schema.subtyping_relations[name]:
                    if check_castable(ctx, e.ScalarTp(supertype), to_tp):
                        return True
                else:
                    return False
                return False
            case _:
                return False
