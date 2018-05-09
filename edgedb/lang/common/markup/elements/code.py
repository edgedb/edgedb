#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2011-present MagicStack Inc. and the EdgeDB authors.
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


from edgedb.lang.common import struct, typed
from . import base


class BaseCode(base.Markup, ns='code'):
    pass


class Token(BaseCode):
    val = struct.Field(str)


class TokenList(typed.TypedList, type=Token):
    pass


class Code(BaseCode):
    tokens = struct.Field(TokenList, default=None, coerce=True)


class Whitespace(Token):
    pass


class Comment(Token):
    pass


class Keyword(Token):
    pass


class Type(Token):
    pass


class Operator(Token):
    pass


class Name(Token):
    pass


class Constant(Name):
    pass


class BuiltinName(Name):
    pass


class FunctionName(Name):
    pass


class ClassName(Name):
    pass


class Decorator(Token):
    pass


class Attribute(Token):
    pass


class Tag(Token):
    pass


class Literal(Token):
    pass


class String(Literal):
    pass


class Number(Literal):
    pass


class Punctuation(Token):
    """Characters ',', ':', '[', etc."""


class Error(Token):
    pass
