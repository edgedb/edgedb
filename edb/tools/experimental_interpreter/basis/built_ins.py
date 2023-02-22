from typing import *
from ..data.data_ops import *
from .errors import FunCallErr
from .std_funcs import all_std_funcs
from .builtin_bin_ops import all_builtin_ops
from .reserved_ops import all_reserved_ops



all_builtin_funcs = {**all_builtin_ops, **all_reserved_ops, **all_std_funcs}


