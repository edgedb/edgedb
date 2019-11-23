#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""A fake flake8 plugin to teach pyflakes the `from typing import *` idiom.

The purpose of this module is to act as if it is a valid flake8 plugin.

flake8 then can happily import it, which would enable us to monkey-patch
the `pyflakes.checker.Checker` class to extend builtins with stuff from
the `typing` module (if it was imported with `from typing import *`).
"""


import re
import typing  # NoQA

from pyflakes import checker


typing_star_import_re = re.compile(r'''
    ^ (?: from \s* typing \s* import \s* \* ) (?:\s*) (?:\#[^\n]*)? $
''', re.X | re.M)


# Remember the old pyflakes.Checker.__init__
old_init = checker.Checker.__init__


def __init__(self, tree, filename='(none)', builtins=None, *args, **kwargs):
    try:
        with open(filename, 'rt') as f:
            source = f.read()
    except FileNotFoundError:
        pass
    else:
        typing_all = set(typing.__all__)
        # Travis runs Python 3.7.1 which has a typing module with missing
        # entries in __all__.  See BPO-36983.  Fix 'em manually here:
        typing_all.add("ChainMap")  # added: 3.5.4; in __all__ since 3.7.4
        typing_all.add("ForwardRef")  # added: 3.7.0; in __all__ since 3.7.4
        typing_all.add("OrderedDict")  # added: 3.7.2
        typing_all.add("Protocol")  # added: 3.8.0
        if typing_star_import_re.search(source):
            if builtins:
                builtins = set(builtins) | typing_all
            else:
                builtins = typing_all

    old_init(self, tree, filename, builtins, *args, **kwargs)


# Monkey-patch pyflakes.Checker.__init__
checker.Checker.__init__ = __init__


class MonkeyPatchPyFlakesChecker:

    name = "monkey-patch-pyflakes"
    version = "0.0.1"

    def __init__(self, tree, filename):
        pass

    def run(self):
        return iter(())
