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

"""
Rewrites all of typing.X names into just X.  Changes the "import typing"
import into a "from typing import *  # NoQA" import.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys
from typing import *

from black import lib2to3_parse, Visitor, Leaf, Node, LN, syms
from blib2to3.pgen2 import token


EDB_DIR = Path(__file__).parent.parent.resolve()


def shadowed_typing_names(path: Path) -> Iterable[str]:
    """Returns global names that are also present in typing.__all__."""
    import typing  # NoQA

    mod_name = str(path.relative_to(EDB_DIR.parent).with_suffix(""))
    if mod_name.startswith("./"):
        mod_name = mod_name[2:]
    mod_name = mod_name.replace(os.sep, ".")
    try:
        mod = __import__(mod_name, fromlist=["__all__"])
    except BaseException as be:
        yield f"(failed to import: {be})"
        return
    missing = object()
    for name in sorted(typing.__all__):
        if getattr(mod, name, missing) is not missing:
            yield name


class TypingRewriter(Visitor[str]):
    """Emits leaf strings."""

    def visit_default(self, node: LN) -> Iterator[str]:
        if isinstance(node, Leaf):
            yield str(node)
        else:
            yield from super().visit_default(node)

    def visit_power(self, node: Node) -> Iterator[str]:
        """typing.X[Y]  ->  X[Y]"""
        if len(node.children) < 2:
            yield from self.visit_default(node)
            return

        first_child = node.children[0]
        if first_child.type != token.NAME or first_child.value != "typing":
            yield from self.visit_default(node)
            return

        second_child = node.children[1]
        if second_child.type != syms.trailer:
            yield from self.visit_default(node)
            return

        if len(second_child.children) != 2:
            yield from self.visit_default(node)
            return

        if second_child.children[0].type != token.DOT:
            yield from self.visit_default(node)
            return

        # OK, looks like this is a construct we want to rewrite.
        # Let's emit the correct whitespace first.
        yield first_child.prefix

        # Now just the name, omitting NAME "typing" and DOT ".".
        yield second_child.children[1].value

        # And recurse: further children might have more power symbols.
        for child in node.children[2:]:
            yield from self.visit_default(child)

    def visit_import_name(self, node: Node) -> Iterator[str]:
        """import typing  ->  from typing import *  # NoQA"""
        if len(node.children) != 2:
            yield from self.visit_default(node)
            return

        first, second = node.children
        if (
            first.type != token.NAME
            or first.value != "import"
            or second.type != token.NAME
            or second.value != "typing"
        ):
            yield from self.visit_default(node)
            return

        yield first.prefix
        yield "from typing import *  # NoQA"


def show(code: LN) -> None:
    from black import DebugVisitor

    dv = DebugVisitor[None]()
    list(dv.visit(code))


def fix_one(path: Path) -> bool:
    path = path.resolve()
    shadowed = list(shadowed_typing_names(path))
    if shadowed:
        print(
            f"warning: {path} skipped, shadows typing names: {shadowed}",
            file=sys.stderr,
        )
        return False

    v = TypingRewriter()
    with path.open() as f:
        contents = f.read()
        if "import typing\n" not in contents:
            return False

        if "import typing  # NoQA\n" in contents:
            print(
                f"warning: {path} skipped, typing import special-cased.",
                file=sys.stderr,
            )
            return False

        code = lib2to3_parse(contents)
    with path.open("w") as f:
        for chunk in v.visit(code):
            f.write(chunk)
    return True


def fix_directory(path: Path) -> None:
    for py_file in sorted(path.glob("**/*.py")):
        if fix_one(py_file):
            print(py_file)


def main(files: List[str]) -> None:
    if not files:
        fix_directory(EDB_DIR)
        return

    for file in sys.argv[1:]:
        path = Path(file)
        if path.is_dir():
            fix_directory(path)
        elif path.is_file():
            fix_one(path)
        else:
            print(
                f"warning: skipped {file}, not a file or directory.",
                file=sys.stderr,
            )


if __name__ == "__main__":
    main(sys.argv[1:])
