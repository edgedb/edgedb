##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import copy

from metamagic.utils.lang.javascript.parser.keywords import RESERVED_KEYWORD, NULL, BOOL, \
                                                            js_keywords as base_js_keywords

js_keywords = copy.copy(base_js_keywords)

js_keywords.update({
    "with" : RESERVED_KEYWORD,
    'class': RESERVED_KEYWORD,
    'super': RESERVED_KEYWORD,
    'except': RESERVED_KEYWORD,
    'static': RESERVED_KEYWORD,
    'from': RESERVED_KEYWORD,
    'import': RESERVED_KEYWORD,
    'as': RESERVED_KEYWORD,
    'of': RESERVED_KEYWORD,
    'nonlocal': RESERVED_KEYWORD,
    'each': RESERVED_KEYWORD,
})
