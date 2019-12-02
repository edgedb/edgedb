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


from __future__ import annotations

from edb.edgeql import ast as qlast

from .expressions import Nonterm
from .sdl import *  # NOQA

from . import commondl


class SDLDocument(Nonterm):
    "%start"

    def reduce_OptSemicolons_EOF(self, *kids):
        self.val = qlast.Schema(declarations=[])

    def reduce_statement_without_semicolons(self, *kids):
        r"""%reduce \
            OptSemicolons SDLShortStatement EOF
        """
        declarations = [kids[1].val]
        commondl._validate_declarations(declarations)
        self.val = qlast.Schema(declarations=declarations)

    def reduce_statements_without_optional_trailing_semicolons(self, *kids):
        r"""%reduce \
            OptSemicolons SDLStatements \
            OptSemicolons SDLShortStatement EOF
        """
        declarations = kids[1].val + [kids[3].val]
        commondl._validate_declarations(declarations)
        self.val = qlast.Schema(declarations=declarations)

    def reduce_OptSemicolons_SDLStatements_EOF(self, *kids):
        declarations = kids[1].val
        commondl._validate_declarations(declarations)
        self.val = qlast.Schema(declarations=declarations)

    def reduce_OptSemicolons_SDLStatements_Semicolons_EOF(self, *kids):
        declarations = kids[1].val
        commondl._validate_declarations(declarations)
        self.val = qlast.Schema(declarations=declarations)
