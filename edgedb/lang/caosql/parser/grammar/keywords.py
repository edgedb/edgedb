##
# Copyright (c) 2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


keyword_types = range(1, 4)
UNRESERVED_KEYWORD, RESERVED_KEYWORD, TYPE_FUNC_NAME_KEYWORD = keyword_types

caosql_keywords = {
    "all": ("ALL", RESERVED_KEYWORD),
    "and": ("AND", RESERVED_KEYWORD),
    "any": ("ANY", RESERVED_KEYWORD),
    "as": ("AS", RESERVED_KEYWORD),
    "asc": ("ASC", RESERVED_KEYWORD),
    "by": ("BY", UNRESERVED_KEYWORD),
    "cast": ("CAST", RESERVED_KEYWORD),
    "desc": ("DESC", RESERVED_KEYWORD),
    "distinct": ("DISTINCT", RESERVED_KEYWORD),
    "except": ("EXCEPT", RESERVED_KEYWORD),
    "exists": ("EXISTS", RESERVED_KEYWORD),
    "false": ("FALSE", RESERVED_KEYWORD),
    "first": ("FIRST", UNRESERVED_KEYWORD),
    "group": ("GROUP", RESERVED_KEYWORD),
    "ilike": ("ILIKE", TYPE_FUNC_NAME_KEYWORD),
    "in": ("IN", RESERVED_KEYWORD),
    "intersect": ("INTERSECT", RESERVED_KEYWORD),
    "is": ("IS", TYPE_FUNC_NAME_KEYWORD),
    "last": ("LAST", UNRESERVED_KEYWORD),
    "like": ("LIKE", TYPE_FUNC_NAME_KEYWORD),
    "limit": ("LIMIT", RESERVED_KEYWORD),
    "mod": ("MOD", RESERVED_KEYWORD),
    "none": ("NONE", RESERVED_KEYWORD),
    "nones": ("NONES", RESERVED_KEYWORD),
    "not": ("NOT", RESERVED_KEYWORD),
    "of": ("OF", UNRESERVED_KEYWORD),
    "offset": ("OFFSET", RESERVED_KEYWORD),
    "operator": ("OPERATOR", UNRESERVED_KEYWORD),
    "or": ("OR", RESERVED_KEYWORD),
    "order": ("ORDER", RESERVED_KEYWORD),
    "over": ("OVER", RESERVED_KEYWORD),
    "partition": ("PARTITION", RESERVED_KEYWORD),
    "select": ("SELECT", RESERVED_KEYWORD),
    "set": ("SET", RESERVED_KEYWORD),
    "delete": ("DELETE", RESERVED_KEYWORD),
    "update": ("UPDATE", RESERVED_KEYWORD),
    "returning": ("RETURNING", RESERVED_KEYWORD),
    "some": ("SOME", RESERVED_KEYWORD),
    "true": ("TRUE", RESERVED_KEYWORD),
    "union": ("UNION", RESERVED_KEYWORD),
    "using": ("USING", RESERVED_KEYWORD),
    "where": ("WHERE", RESERVED_KEYWORD),
    "with": ("WITH", RESERVED_KEYWORD)
}


by_type = {typ: {} for typ in keyword_types}

for val, spec in caosql_keywords.items():
    by_type[spec[1]][val] = spec[0]
