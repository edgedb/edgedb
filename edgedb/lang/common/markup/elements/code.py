##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.datastructures.all import Field
from semantix.utils.datastructures import typed
from . import base


class BaseCode(base.Markup, ns='code'):
    pass


class Token(BaseCode):
    val = Field(str)


class TokenList(typed.TypedList, type=Token):
    pass


class Code(BaseCode):
    tokens = Field(TokenList, default=None, coerce=True)


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
    """Characters ',', ':', '[', etc"""

class Error(Token):
    pass

