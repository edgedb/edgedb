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
import typing

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
        if typing_star_import_re.search(source):
            if builtins:
                builtins = set(builtins) | set(typing.__all__)
            else:
                builtins = set(typing.__all__)

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
