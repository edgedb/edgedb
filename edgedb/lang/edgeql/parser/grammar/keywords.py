##
# Copyright (c) 2010-present MagicStack Inc.
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
    "aggregate": ("AGGREGATE", UNRESERVED_KEYWORD),
    "all": ("ALL", RESERVED_KEYWORD),
    "alter": ("ALTER", RESERVED_KEYWORD),
    "and": ("AND", UNRESERVED_KEYWORD),
    "any": ("ANY", RESERVED_KEYWORD),
    "as": ("AS", UNRESERVED_KEYWORD),
    "asc": ("ASC", UNRESERVED_KEYWORD),
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
    "desc": ("DESC", UNRESERVED_KEYWORD),
    "drop": ("DROP", RESERVED_KEYWORD),
    "else": ("ELSE", UNRESERVED_KEYWORD),
    "empty": ("EMPTY", RESERVED_KEYWORD),
    "except": ("EXCEPT", UNRESERVED_KEYWORD),
    "exists": ("EXISTS", RESERVED_KEYWORD),
    "event": ("EVENT", UNRESERVED_KEYWORD),
    "false": ("FALSE", RESERVED_KEYWORD),
    "final": ("FINAL", UNRESERVED_KEYWORD),
    "first": ("FIRST", UNRESERVED_KEYWORD),
    "for": ("FOR", UNRESERVED_KEYWORD),
    "from": ("FROM", UNRESERVED_KEYWORD),
    "function": ("FUNCTION", UNRESERVED_KEYWORD),
    "get": ("GET", RESERVED_KEYWORD),
    "group": ("GROUP", RESERVED_KEYWORD),
    "having": ("HAVING", RESERVED_KEYWORD),
    "if": ("IF", UNRESERVED_KEYWORD),
    "ilike": ("ILIKE", UNRESERVED_KEYWORD),
    "in": ("IN", UNRESERVED_KEYWORD),
    "index": ("INDEX", UNRESERVED_KEYWORD),
    "inherit": ("INHERIT", UNRESERVED_KEYWORD),
    "inheriting": ("INHERITING", UNRESERVED_KEYWORD),
    "insert": ("INSERT", RESERVED_KEYWORD),
    "intersect": ("INTERSECT", UNRESERVED_KEYWORD),
    "is": ("IS", UNRESERVED_KEYWORD),
    "last": ("LAST", UNRESERVED_KEYWORD),
    "like": ("LIKE", UNRESERVED_KEYWORD),
    "limit": ("LIMIT", RESERVED_KEYWORD),
    "link": ("LINK", UNRESERVED_KEYWORD),
    "migration": ("MIGRATION", UNRESERVED_KEYWORD),
    "module": ("MODULE", UNRESERVED_KEYWORD),
    "not": ("NOT", RESERVED_KEYWORD),
    "offset": ("OFFSET", RESERVED_KEYWORD),
    "or": ("OR", UNRESERVED_KEYWORD),
    "order": ("ORDER", RESERVED_KEYWORD),
    "over": ("OVER", RESERVED_KEYWORD),
    "partition": ("PARTITION", RESERVED_KEYWORD),
    "policy": ("POLICY", UNRESERVED_KEYWORD),
    "property": ("PROPERTY", UNRESERVED_KEYWORD),
    "required": ("REQUIRED", UNRESERVED_KEYWORD),
    "rename": ("RENAME", UNRESERVED_KEYWORD),
    "returning": ("RETURNING", RESERVED_KEYWORD),
    "rollback": ("ROLLBACK", UNRESERVED_KEYWORD),
    "select": ("SELECT", RESERVED_KEYWORD),
    "set": ("SET", RESERVED_KEYWORD),
    "singleton": ("SINGLETON", UNRESERVED_KEYWORD),
    "start": ("START", UNRESERVED_KEYWORD),
    "target": ("TARGET", UNRESERVED_KEYWORD),
    "then": ("THEN", UNRESERVED_KEYWORD),
    "to": ("TO", UNRESERVED_KEYWORD),
    "transaction": ("TRANSACTION", UNRESERVED_KEYWORD),
    "true": ("TRUE", RESERVED_KEYWORD),
    "update": ("UPDATE", RESERVED_KEYWORD),
    "union": ("UNION", UNRESERVED_KEYWORD),
    "values": ("VALUES", RESERVED_KEYWORD),
    "where": ("WHERE", RESERVED_KEYWORD),
    "with": ("WITH", RESERVED_KEYWORD)
}


by_type = {typ: {} for typ in keyword_types}

for val, spec in edgeql_keywords.items():
    by_type[spec[1]][val] = spec[0]
