# mypy: no-ignore-errors, strict-optional, disallow-any-generics


from __future__ import annotations
from edb.testbase.connection import Retry
from typing import *

from pathlib import Path
import sys
EDB_DIR = Path(__file__).parent.parent.parent.resolve()
sys.path.insert(0, str(EDB_DIR))


class AbstractConnection():
    async def execute(self, cmd: str):
        print("Executing Command in Experimental Interperter: ", cmd)

    async def aclose(self):
        print("Closing Experimental Interperter ")


default_connection = AbstractConnection()

admin_connection = default_connection
db_connection = default_connection
