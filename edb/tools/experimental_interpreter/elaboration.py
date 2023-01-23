
from data_ops import *


from functools import singledispatch



from data_ops import *
from helper_funcs import *
import sys
import traceback
from edb.edgeql import ast as qlast
from edb import edgeql

import pprint

from edb.common import debug

@singledispatch
def elab(node: qlast.Base):
    debug.dump(node)
    raise ValueError("Not Implemented!")

