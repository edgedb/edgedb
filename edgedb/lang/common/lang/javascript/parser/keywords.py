##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


keyword_types = range(4)
RESERVED_KEYWORD, FUTURE_RESERVED_WORD, NULL, BOOL = keyword_types

# keyword dict needed
js_keywords = {
    "break" : RESERVED_KEYWORD,
    "case" : RESERVED_KEYWORD,
    "catch" : RESERVED_KEYWORD,
    "continue" : RESERVED_KEYWORD,
    "debugger" : RESERVED_KEYWORD,
    "default" : RESERVED_KEYWORD,
    "delete" : RESERVED_KEYWORD,
    "do" : RESERVED_KEYWORD,
    "else" : RESERVED_KEYWORD,
    "finally" : RESERVED_KEYWORD,
    "for" : RESERVED_KEYWORD,
    "function" : RESERVED_KEYWORD,
    "if" : RESERVED_KEYWORD,
    "in" : RESERVED_KEYWORD,
    "instanceof" : RESERVED_KEYWORD,
    "new" : RESERVED_KEYWORD,
    "return" : RESERVED_KEYWORD,
    "switch" : RESERVED_KEYWORD,
    "this" : RESERVED_KEYWORD,
    "throw" : RESERVED_KEYWORD,
    "try" : RESERVED_KEYWORD,
    "typeof" : RESERVED_KEYWORD,
    "var" : RESERVED_KEYWORD,
    "void" : RESERVED_KEYWORD,
    "while" : RESERVED_KEYWORD,
    "with" : RESERVED_KEYWORD,

    "null" : NULL,
    "true" : BOOL,
    "false" : BOOL,

    "class" : FUTURE_RESERVED_WORD,
    "enum" : FUTURE_RESERVED_WORD,
    "extends" : FUTURE_RESERVED_WORD,
    "super" : FUTURE_RESERVED_WORD,
    "const" : FUTURE_RESERVED_WORD,
    "export" : FUTURE_RESERVED_WORD,
    "import" : FUTURE_RESERVED_WORD,

    "implements" : FUTURE_RESERVED_WORD,
    "let" : FUTURE_RESERVED_WORD,
    "private" : FUTURE_RESERVED_WORD,
    "public" : FUTURE_RESERVED_WORD,
    "interface" : FUTURE_RESERVED_WORD,
    "package" : FUTURE_RESERVED_WORD,
    "protected" : FUTURE_RESERVED_WORD,
    "static" : FUTURE_RESERVED_WORD,
    "yield" : FUTURE_RESERVED_WORD
    }


# quick and dirty keyword TOKENS
for val, typ in js_keywords.items():
    js_keywords[val] = (val.upper(), typ)


by_type = {typ: {} for typ in keyword_types}


for val, spec in js_keywords.items():
    by_type[spec[1]][val] = spec[0]
