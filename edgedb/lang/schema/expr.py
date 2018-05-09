#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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


from edgedb.lang.common import typed
from . import literal


class ExpressionText(str):
    pass


class ExpressionList(typed.TypedList, type=literal.Literal):
    @classmethod
    def merge_values(cls, ours, theirs, schema):
        if not ours:
            if theirs:
                ours = theirs[:]
        elif theirs and isinstance(ours[-1], ExpressionText):
            ours.extend(theirs)

        return ours


class ExpressionDict(typed.TypedDict, keytype=str, valuetype=literal.Literal):
    @classmethod
    def compare_values(cls, ours, theirs, context, compcoef):
        if not ours and not theirs:
            basecoef = 1.0
        elif not ours or not theirs:
            basecoef = 0.2
        else:
            similarity = []

            for k, v in ours.items():
                try:
                    theirsv = theirs[k]
                except KeyError:
                    # key only in ours
                    similarity.append(0.2)
                else:
                    similarity.append(1.0 if v == theirsv else 0.4)

            similarity.extend(0.2 for k in set(theirs) - set(ours))
            basecoef = sum(similarity) / len(similarity)

        return basecoef + (1 - basecoef) * compcoef
