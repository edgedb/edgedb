##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


keyword_types = range(1, 3)
UNRESERVED_KEYWORD, RESERVED_KEYWORD = keyword_types

edge_schema_keywords = {
    "abstract": ("ABSTRACT", RESERVED_KEYWORD),
    "action": ("ACTION", UNRESERVED_KEYWORD),
    "aggregate": ("AGGREGATE", UNRESERVED_KEYWORD),
    "as": ("AS", UNRESERVED_KEYWORD),
    "atom": ("ATOM", UNRESERVED_KEYWORD),
    "attribute": ("ATTRIBUTE", UNRESERVED_KEYWORD),
    "concept": ("CONCEPT", UNRESERVED_KEYWORD),
    "constraint": ("CONSTRAINT", UNRESERVED_KEYWORD),
    "event": ("EVENT", UNRESERVED_KEYWORD),
    "extends": ("EXTENDS", RESERVED_KEYWORD),
    "false": ("FALSE", RESERVED_KEYWORD),
    "final": ("FINAL", UNRESERVED_KEYWORD),
    "from": ("FROM", UNRESERVED_KEYWORD),
    "function": ("FUNCTION", UNRESERVED_KEYWORD),
    "index": ("INDEX", UNRESERVED_KEYWORD),
    "import": ("IMPORT", UNRESERVED_KEYWORD),
    "link": ("LINK", UNRESERVED_KEYWORD),
    "linkproperty": ("LINKPROPERTY", UNRESERVED_KEYWORD),
    "on": ("ON", UNRESERVED_KEYWORD),
    "properties": ("PROPERTIES", UNRESERVED_KEYWORD),
    "required": ("REQUIRED", RESERVED_KEYWORD),
    "to": ("TO", UNRESERVED_KEYWORD),
    "true": ("TRUE", RESERVED_KEYWORD),
}


by_type = {typ: {} for typ in keyword_types}

for val, spec in edge_schema_keywords.items():
    by_type[spec[1]][val] = spec[0]
