##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import copy

from metamagic.utils.lang.javascript.parser import keywords as base_keywords


js_keywords = copy.copy(base_keywords.js_keywords)
js_keywords.update({
    'from': base_keywords.RESERVED_KEYWORD,
    'import': base_keywords.RESERVED_KEYWORD,
    'as': base_keywords.RESERVED_KEYWORD
})
