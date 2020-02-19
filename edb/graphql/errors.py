#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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

from typing import *

from edb import errors


class GraphQLError(errors.QueryError):

    def __init__(self, msg, *,
                 loc: Optional[Tuple[int, int]]=None):

        super().__init__(msg)

        if loc:
            # XXX Will be fixes when we have proper LSP SourceLocation
            # abstraction.
            self.set_linecol(loc[0], loc[1])


class GraphQLTranslationError(GraphQLError):
    pass


class GraphQLValidationError(GraphQLTranslationError):
    pass


class GraphQLCoreError(GraphQLError):
    pass
