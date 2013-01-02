##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import copy

from metamagic.utils.lang.javascript.parser.keywords import RESERVED_KEYWORD, NULL, BOOL


js_keywords = {
    "break" : RESERVED_KEYWORD,
    "case" : RESERVED_KEYWORD,
    "catch" : RESERVED_KEYWORD,
    "continue" : RESERVED_KEYWORD,
    "debugger" : RESERVED_KEYWORD,
    "delete" : RESERVED_KEYWORD,
    "do" : RESERVED_KEYWORD,
    "else" : RESERVED_KEYWORD,
    "finally" : RESERVED_KEYWORD,
    "for" : RESERVED_KEYWORD,
    "function" : RESERVED_KEYWORD,
    "if" : RESERVED_KEYWORD,
    "in" : RESERVED_KEYWORD,
    "new" : RESERVED_KEYWORD,
    "return" : RESERVED_KEYWORD,
    "switch" : RESERVED_KEYWORD,
    "this" : RESERVED_KEYWORD,
    "throw" : RESERVED_KEYWORD,
    "try" : RESERVED_KEYWORD,
    "typeof" : RESERVED_KEYWORD,
    "void" : RESERVED_KEYWORD,
    "while" : RESERVED_KEYWORD,

    "with" : RESERVED_KEYWORD,

    "null" : NULL,
    "true" : BOOL,
    "false" : BOOL,

    'class': RESERVED_KEYWORD,
    'static': RESERVED_KEYWORD,
    'from': RESERVED_KEYWORD,
    'import': RESERVED_KEYWORD,
    'as': RESERVED_KEYWORD,
    'of': RESERVED_KEYWORD,
    'nonlocal': RESERVED_KEYWORD,

    "yield" : RESERVED_KEYWORD
}
