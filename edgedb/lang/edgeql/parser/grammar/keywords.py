##
# Copyright (c) 2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


keyword_types = range(1, 4)
UNRESERVED_KEYWORD, RESERVED_KEYWORD, TYPE_FUNC_NAME_KEYWORD = keyword_types

edgeql_keywords = {
    "abstract": ("ABSTRACT", UNRESERVED_KEYWORD),
    "action": ("ACTION", UNRESERVED_KEYWORD),
    "after": ("AFTER", UNRESERVED_KEYWORD),
    "all": ("ALL", RESERVED_KEYWORD),
    "alter": ("ALTER", RESERVED_KEYWORD),
    "and": ("AND", RESERVED_KEYWORD),
    "any": ("ANY", RESERVED_KEYWORD),
    "as": ("AS", RESERVED_KEYWORD),
    "asc": ("ASC", RESERVED_KEYWORD),
    "atom": ("ATOM", UNRESERVED_KEYWORD),
    "attribute": ("ATTRIBUTE", UNRESERVED_KEYWORD),
    "before": ("BEFORE", UNRESERVED_KEYWORD),
    "by": ("BY", UNRESERVED_KEYWORD),
    "commit": ("COMMIT", UNRESERVED_KEYWORD),
    "concept": ("CONCEPT", UNRESERVED_KEYWORD),
    "constraint": ("CONSTRAINT", UNRESERVED_KEYWORD),
    "create": ("CREATE", RESERVED_KEYWORD),
    "database": ("DATABASE", UNRESERVED_KEYWORD),
    "delete": ("DELETE", RESERVED_KEYWORD),
    "delta": ("DELTA", UNRESERVED_KEYWORD),
    "desc": ("DESC", RESERVED_KEYWORD),
    "distinct": ("DISTINCT", RESERVED_KEYWORD),
    "drop": ("DROP", RESERVED_KEYWORD),
    "except": ("EXCEPT", RESERVED_KEYWORD),
    "exists": ("EXISTS", RESERVED_KEYWORD),
    "event": ("EVENT", UNRESERVED_KEYWORD),
    "false": ("FALSE", RESERVED_KEYWORD),
    "filter": ("FILTER", UNRESERVED_KEYWORD),
    "final": ("FINAL", UNRESERVED_KEYWORD),
    "first": ("FIRST", UNRESERVED_KEYWORD),
    "for": ("FOR", UNRESERVED_KEYWORD),
    "from": ("FROM", UNRESERVED_KEYWORD),
    "function": ("FUNCTION", UNRESERVED_KEYWORD),
    "group": ("GROUP", RESERVED_KEYWORD),
    "ilike": ("ILIKE", TYPE_FUNC_NAME_KEYWORD),
    "in": ("IN", RESERVED_KEYWORD),
    "index": ("INDEX", UNRESERVED_KEYWORD),
    "inherit": ("INHERIT", UNRESERVED_KEYWORD),
    "inheriting": ("INHERITING", UNRESERVED_KEYWORD),
    "inout": ("INOUT", UNRESERVED_KEYWORD),
    "insert": ("INSERT", RESERVED_KEYWORD),
    "instanceof": ("INSTANCEOF", RESERVED_KEYWORD),
    "intersect": ("INTERSECT", RESERVED_KEYWORD),
    "is": ("IS", TYPE_FUNC_NAME_KEYWORD),
    "last": ("LAST", UNRESERVED_KEYWORD),
    "like": ("LIKE", TYPE_FUNC_NAME_KEYWORD),
    "limit": ("LIMIT", RESERVED_KEYWORD),
    "link": ("LINK", UNRESERVED_KEYWORD),
    "mod": ("MOD", RESERVED_KEYWORD),
    "module": ("MODULE", UNRESERVED_KEYWORD),
    "namespace": ("NAMESPACE", RESERVED_KEYWORD),
    "no": ("NO", UNRESERVED_KEYWORD),
    "none": ("NONE", RESERVED_KEYWORD),
    "nones": ("NONES", RESERVED_KEYWORD),
    "not": ("NOT", RESERVED_KEYWORD),
    "of": ("OF", UNRESERVED_KEYWORD),
    "offset": ("OFFSET", RESERVED_KEYWORD),
    "operator": ("OPERATOR", UNRESERVED_KEYWORD),
    "or": ("OR", RESERVED_KEYWORD),
    "order": ("ORDER", RESERVED_KEYWORD),
    "out": ("OUT", UNRESERVED_KEYWORD),
    "over": ("OVER", RESERVED_KEYWORD),
    "partition": ("PARTITION", RESERVED_KEYWORD),
    "policy": ("POLICY", RESERVED_KEYWORD),
    "property": ("PROPERTY", UNRESERVED_KEYWORD),
    "required": ("REQUIRED", UNRESERVED_KEYWORD),
    "rename": ("RENAME", UNRESERVED_KEYWORD),
    "returning": ("RETURNING", RESERVED_KEYWORD),
    "rollback": ("ROLLBACK", UNRESERVED_KEYWORD),
    "select": ("SELECT", RESERVED_KEYWORD),
    "set": ("SET", RESERVED_KEYWORD),
    "some": ("SOME", RESERVED_KEYWORD),
    "start": ("START", UNRESERVED_KEYWORD),
    "target": ("TARGET", UNRESERVED_KEYWORD),
    "then": ("THEN", UNRESERVED_KEYWORD),
    "to": ("TO", UNRESERVED_KEYWORD),
    "transaction": ("TRANSACTION", UNRESERVED_KEYWORD),
    "true": ("TRUE", RESERVED_KEYWORD),
    "update": ("UPDATE", RESERVED_KEYWORD),
    "union": ("UNION", RESERVED_KEYWORD),
    "using": ("USING", RESERVED_KEYWORD),
    "where": ("WHERE", RESERVED_KEYWORD),
    "with": ("WITH", RESERVED_KEYWORD)
}


by_type = {typ: {} for typ in keyword_types}

for val, spec in edgeql_keywords.items():
    by_type[spec[1]][val] = spec[0]
